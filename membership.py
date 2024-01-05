import logging
import math
import random
import socket
import time
from datetime import datetime
from threading import RLock
from threading import Thread
from threading import Timer
from typing import Dict
from typing import Tuple
from typing import Union

import message_pb2

from type_hints import Events
from type_hints import Queues
from utils import ELECTION_QUEUE
from utils import HOST_IP_ADDRESS
from utils import LocalMembershipListEntry
from utils import LOG_DIR
from utils import LOG_FORMATTER
from utils import MAX_ADMIN_COMMAND_SIZE
from utils import MAX_HEARTBEAT_SIZE
from utils import MembershipAdminCommand
from utils import MEMBERSHIP_ADMIN_PORT
from utils import MEMBERSHIP_ADMIN_QUEUE
from utils import MEMBERSHIP_LIST_QUEUE
from utils import MEMBERSHIP_PORT
from utils import NAME_NODE_QUEUE
from utils import QueueMessageType
from utils import SEND_HEARTBEATS_EVENT
from utils import SHUTDOWN_EVENT
from utils import USE_SUSPICION_EVENT


logger = logging.getLogger("Membership")
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(f"{LOG_DIR}/membership.log", "w")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(LOG_FORMATTER)
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(LOG_FORMATTER)
logger.addHandler(stream_handler)


def get_node_id(member: message_pb2.Member) -> str:
    return f"{member.address}:{member.port}:{member.version_number}"


def parse_node_id(node_id: str) -> Tuple[str, str, str]:
    values = node_id.split(":")
    return values[0], values[1], values[2]


class CleanupCacheEntry:
    def __init__(self, node_id: str, timer: Timer):
        self.node_id = node_id
        self.timer = timer


# TODO: Make singleton
class LocalMembershipList:
    def __init__(
            self, membership_list: Dict[str, LocalMembershipListEntry], initial_name_node_ip: str,
            queues: Queues, events: Events
    ) -> None:
        # Number of seconds, until a membership list entry is considered failed
        self.t_fail = 15

        # Number of seconds, until a membership list entry is considered suspicious
        self.t_expire = 15

        # Number of seconds, until a recently deleted member can be re-added
        self.t_cleanup = 15

        # Gossip rate
        self.t_gossip_seconds = 1

        # Suspicion incarnation number to filter out old suspicion messages
        self.incarnation_number = 0

        # Global membership list shared between all processes
        self.members: Dict[str, LocalMembershipListEntry] = membership_list
        self.members_r_lock = RLock()

        # When expired, marks members as failed or suspicious
        self.member_timers: Dict[str, Timer] = dict()
        self.member_timers_r_lock = RLock()

        # Remember recently deleted members, to prevent ghost entries
        self.cleanup_cache: Dict[str, CleanupCacheEntry] = dict()
        self.cleanup_cache_r_lock = RLock()

        # Trigger election on leader failures
        self.election_queue = queues[ELECTION_QUEUE]

        # Trigger re-replication on data node failures
        self.name_node_queue = queues[NAME_NODE_QUEUE]

        # A toggle to stop threads gracefully
        self.shutdown = events[SHUTDOWN_EVENT]

        # A toggle to control if this node sends heartbeats or not
        self.heartbeats = events[SEND_HEARTBEATS_EVENT]

        # A toggle to control if this node uses suspicion in the gossip failure detection
        self.suspicion = events[USE_SUSPICION_EVENT]

        # Initialize membership list
        # The Name Node serves as the introducer too
        name_node_address = initial_name_node_ip
        name_node = message_pb2.Member()
        name_node.address = name_node_address
        name_node.port = MEMBERSHIP_PORT
        name_node.version_number = 0
        name_node.heartbeat_counter = 0

        if self.suspicion.is_set():
            name_node.status = message_pb2.Status.ALIVE
            name_node.incarnation_number = 0
            self.add_member_suspicion(name_node)
        else:
            self.add_member(name_node)

        # Add myself, if I am not the introducer
        my_address = socket.gethostbyname(socket.gethostname())
        if name_node_address != my_address:
            me = message_pb2.Member()
            me.address = my_address
            me.port = MEMBERSHIP_PORT
            me.version_number = int(round(datetime.now().timestamp()))
            me.heartbeat_counter = 0
            me.incarnation_number = 0
            if self.suspicion.is_set():
                me.status = message_pb2.Status.ALIVE
                me.incarnation_number = 0
                self.add_member_suspicion(me)
            else:
                self.add_member(me)

        self.membership_list_queue = queues[MEMBERSHIP_LIST_QUEUE]
        self.udp_socket: Union[socket.socket, None] = None

        self.queue_listener = Thread(target=self.__queue_listener__)
        self.queue_listener.start()

        # Heartbeat Sender
        self.heartbeat_count = 0
        self.heartbeat_sender = Thread(target=self.__heartbeat_sender__)
        self.heartbeat_sender.start()

        # Heartbeat Receiver
        self.heartbeat_listener = Thread(target=self.__heartbeat_listener__)
        self.heartbeat_listener.start()

    def __queue_listener__(self) -> None:

        logger.debug("Listening to queue...")

        while not self.shutdown.is_set():

            event = self.membership_list_queue.get(block=True, timeout=None)

            if event is None:
                if self.udp_socket is not None:
                    try:
                        self.udp_socket.close()
                        logger.debug("Queue Listener SHUTDOWN")
                    except Exception as e:
                        logger.error("Queue Listener SHUTDOWN: %s", e)
                return

            elif event["type"] == QueueMessageType.ADD_MEMBER:
                if self.suspicion.is_set():
                    (Thread(target=self.add_member_suspicion, args=(event["member"],))).start()
                else:
                    (Thread(target=self.add_member, args=(event["member"],))).start()

    def __cache_expire__(self, node_id: str) -> None:
        """ Removes member from clean up cache """

        cleanup_entry = None

        with self.cleanup_cache_r_lock:
            if node_id in self.cleanup_cache:
                cleanup_entry = self.cleanup_cache.pop(node_id)

        if cleanup_entry is not None:
            logger.debug("Removed node %s from cleanup cache", node_id)
        else:
            logger.debug("Node %s already removed from cleanup cache", node_id)

    def __member_fail__(self, node_id: str) -> None:
        """ Removes failed member from membership list and adds failed member to clean up cache """

        address, port, version = parse_node_id(node_id)

        if address == HOST_IP_ADDRESS:
            # Don't remove myself from membership list
            logger.debug("Skipped deleting myself (%s)!", address)
            return

        deleted_entry = None

        with self.cleanup_cache_r_lock, self.member_timers_r_lock, self.members_r_lock:
            if node_id in self.member_timers:
                t = self.member_timers.pop(node_id)
                t.cancel()

            if node_id in self.members:
                deleted_entry = self.members.pop(node_id)

                timer = Timer(self.t_cleanup, self.__cache_expire__, args=(node_id,))
                self.cleanup_cache.update({node_id: CleanupCacheEntry(node_id, timer)})
                timer.start()

                deleted_entry.member.status = message_pb2.Status.CONFIRM

                # Let BullyElectionWorker processes know of failures
                self.election_queue.put({
                    "type": QueueMessageType.NODE_FAILURE.value,
                    "address": deleted_entry.member.address
                })

                # Let NameNodeWorker processes know of failures
                self.name_node_queue.put({
                    "type": QueueMessageType.NODE_FAILURE.value,
                    "address": deleted_entry.member.address
                })
            else:
                logger.debug("Node %s already removed from membership list", node_id)

        if deleted_entry is not None:
            logger.info("Deleted FAILED Member:\n%s", deleted_entry)

    def __member_suspicious__(self, node_id: str) -> None:
        """ Marks member's status as suspicious and start a Fail Timer """

        address, port, version = parse_node_id(node_id)

        if address == HOST_IP_ADDRESS:
            # Don't mark myself as suspicious
            logger.debug("Skipped marking myself (%s) as suspicious!", address)
            return

        membership_list_entry = None

        with self.members_r_lock:

            if node_id in self.members:
                membership_list_entry = self.members[node_id]
                membership_list_entry.member.status = message_pb2.Status.SUSPICIOUS
                self.members.update({node_id: membership_list_entry})
                self.__set_fail_timer__(node_id)

        if membership_list_entry is not None:
            logger.info("Marked Member SUSPICIOUS:\n%s", membership_list_entry)

    def add_member(self, member: message_pb2.Member) -> None:
        """ Adds member to membership list with a Fail Timer """

        node_id = get_node_id(member)
        new_entry = None

        with self.cleanup_cache_r_lock, self.members_r_lock, self.member_timers_r_lock:
            if node_id not in self.cleanup_cache:
                if node_id in self.members:
                    entry = self.members[node_id]
                    if member.heartbeat_counter > entry.member.heartbeat_counter:
                        entry.member.heartbeat_counter = member.heartbeat_counter
                        entry.timestamp = datetime.now()
                        self.__set_fail_timer__(node_id)
                else:
                    new_entry = LocalMembershipListEntry(member, datetime.now())
                    self.members.update({node_id: new_entry})
                    timer = Timer(self.t_fail, self.__member_fail__, args=(node_id,))
                    self.member_timers.update({node_id: timer})
                    timer.start()

        if new_entry is not None:
            logger.info("Received a new Member:\n%s", new_entry)

    def __handle_status__(self, node_id: str, entry: LocalMembershipListEntry) -> None:
        """ Assigns a member a Suspicious Timer or Fail Timer based on the member's status """

        if entry.member.status == message_pb2.Status.ALIVE:
            self.__set_suspicious_timer__(node_id)
        elif entry.member.status == message_pb2.Status.SUSPICIOUS:
            self.__set_fail_timer__(node_id)
            logger.info("Member received is SUSPICIOUS:\n%s", entry)

    def add_member_suspicion(self, member: message_pb2.Member) -> None:
        """ Adds member to membership list with a new Fail Timer or Suspicious Timer """

        node_id = get_node_id(member)

        new_entry = None

        with self.cleanup_cache_r_lock, self.members_r_lock, self.member_timers_r_lock:

            if node_id not in self.cleanup_cache:

                if member.address == HOST_IP_ADDRESS and member.status == message_pb2.Status.SUSPICIOUS:
                    if member.incarnation_number > self.incarnation_number:
                        # Increment incarnation to be greater than all, to ensure others will merge my ALIVE message
                        self.incarnation_number = member.incarnation_number + 1

                if node_id in self.members:
                    entry = self.members[node_id]

                    if entry.member.incarnation_number > member.incarnation_number:
                        # Ignore lower incarnation numbers
                        pass
                    elif member.incarnation_number > entry.member.incarnation_number:
                        # Local member is overriden by higher incarnation numbers
                        entry.member.heartbeat_counter = member.heartbeat_counter
                        entry.member.incarnation_number = member.incarnation_number
                        entry.member.status = member.status
                        entry.timestamp = datetime.now()
                        self.members.update({node_id: entry})
                        self.__handle_status__(node_id, entry)
                    else:
                        if member.heartbeat_counter > entry.member.heartbeat_counter:
                            # Merge heartbeat counters and refresh timer
                            entry.member.heartbeat_counter = member.heartbeat_counter
                            entry.member.incarnation_number = member.incarnation_number
                            entry.member.status = member.status
                            entry.timestamp = datetime.now()
                            self.members.update({node_id: entry})
                            self.__handle_status__(node_id, entry)
                else:
                    # New member
                    new_entry = LocalMembershipListEntry(member, datetime.now())
                    self.members.update({node_id: new_entry})
                    self.__handle_status__(node_id, new_entry)

        if new_entry is not None:
            logger.debug("Received a new Member:\n%s", new_entry)

    def __set_suspicious_timer__(self, node_id: str) -> None:
        """ Replaces existing Fail Timer or Suspicious Timer with a new Suspicious Timer (cancels old timer) """

        timer = Timer(
            self.t_expire,
            self.__member_suspicious__,
            args=(node_id,)
        )

        with self.member_timers_r_lock:

            if node_id in self.member_timers:
                timer_entry = self.member_timers.pop(node_id)
                timer_entry.cancel()

            self.member_timers.update({node_id: timer})
            timer.start()

    def __set_fail_timer__(self, node_id: str) -> None:
        """ Replaces existing Fail Timer or Suspicious Timer with a new Fail Timer (cancels old timer) """

        timer = Timer(self.t_fail, self.__member_fail__, args=(node_id,))

        with self.member_timers_r_lock:

            if node_id in self.member_timers:
                timer_entry = self.member_timers.pop(node_id)
                timer_entry.cancel()

            self.member_timers.update({node_id: timer})
            timer.start()

    def __send_heartbeat__(self, target: message_pb2.Member, udp_socket: socket.socket) -> None:
        """ Send heartbeat to randomly select member from the membership list """

        heartbeat = message_pb2.Heartbeat()
        for node_id, entry in self.members.items():
            heartbeat.members.append(entry.member)
        udp_socket.sendto(heartbeat.SerializeToString(), (target.address, target.port))
        # logger.debug("Heartbeat %s:\n%s", target.address, text_format.MessageToString(heartbeat, indent=2))

    def __heartbeat_sender__(self) -> None:
        """ Continuously send heartbeats in a given interval to n random targets from the membership list """

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_socket:
            while not self.shutdown.is_set():
                self.heartbeats.wait()
                self.heartbeat_count += 1

                # Increment heartbeat counter in membership list
                for node_id, entry in self.members.items():
                    if entry.member.address == HOST_IP_ADDRESS:
                        entry.member.status = message_pb2.Status.ALIVE
                        entry.member.incarnation_number = self.incarnation_number
                        entry.member.heartbeat_counter = self.heartbeat_count
                        entry.timestamp = datetime.now()
                        self.members.update({node_id: entry})

                # Send heartbeats to log(n) random targets from membership list
                others = [e.member for n, e in self.members.items() if e.member.address != HOST_IP_ADDRESS]
                num_others = len(others)
                if num_others > 0:
                    n_random_targets = min(math.ceil(math.log(num_others)), num_others)
                    # n_random_targets = num_others
                    if n_random_targets == 0:
                        n_random_targets = num_others
                    for random_member in random.sample(others, n_random_targets):
                        (Thread(target=self.__send_heartbeat__, args=(random_member, udp_socket))).start()

                # Wait t_gossip before sending next heartbeat
                time.sleep(self.t_gossip_seconds)

        logger.debug("Heartbeat Sender finished gracefully")

    def __handle_heartbeat__(self, byte_heartbeat: bytes) -> None:
        """ Handle a heartbeat received from other virtual machines """

        try:
            # Parse bytes to a protobuf Heartbeat Object (see src/message.proto)
            heartbeat = message_pb2.Heartbeat()
            heartbeat.ParseFromString(byte_heartbeat)

            # Merge local membership list with foreign membership list
            if self.suspicion.is_set():
                for foreign_member in heartbeat.members:
                    self.add_member_suspicion(foreign_member)
            else:
                for foreign_member in heartbeat.members:
                    self.add_member(foreign_member)
        except Exception as ex:
            logger.error(ex)

    def __heartbeat_listener__(self) -> None:
        """ Start a socket server that spawns a thread per new connection """

        logger.debug("Listening for heartbeats...")

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:

            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.udp_socket = s
            self.udp_socket.bind((HOST_IP_ADDRESS, MEMBERSHIP_PORT))

            while not self.shutdown.is_set():
                try:
                    _bytes, _address = self.udp_socket.recvfrom(MAX_HEARTBEAT_SIZE)
                    (Thread(target=self.__handle_heartbeat__, args=(_bytes,))).start()
                except Exception as ex:
                    logger.error(ex)

        logger.debug("Heartbeat Listener finished gracefully")


class MembershipAdminWorker:
    def __init__(
            self,
            membership_list: Dict[str, LocalMembershipListEntry],
            initial_name_node_ip: str,
            queues: Queues,
            events: Events
    ) -> None:
        self.membership_list = membership_list
        self.introducer = initial_name_node_ip

        self.heartbeats = events[SEND_HEARTBEATS_EVENT]
        self.suspicion = events[USE_SUSPICION_EVENT]
        self.shutdown = events[SHUTDOWN_EVENT]

        self.membership_admin_queue = queues[MEMBERSHIP_ADMIN_QUEUE]
        self.membership_list_queue = queues[MEMBERSHIP_LIST_QUEUE]

        self.udp_socket: Union[socket.socket, None] = None

        self.queue_listener = Thread(target=self.__queue_listener__)
        self.queue_listener.start()

        self.listener = Thread(target=self.__listen__)
        self.listener.start()

    def __queue_listener__(self) -> None:

        logger.debug("Listening to queue...")

        while not self.shutdown.is_set():

            event = self.membership_admin_queue.get(block=True, timeout=None)

            if event is None:
                if self.udp_socket is not None:
                    try:
                        self.udp_socket.close()
                        logger.info("Queue Listener SHUTDOWN")
                    except Exception as e:
                        logger.error("Queue Listener SHUTDOWN: %s", e)
                return

            elif event["type"] == QueueMessageType.NEW_NAME_NODE:
                self.introducer = socket.gethostbyname(event["new_name_node"])

    def __handle__(self, _bytes: bytes) -> None:
        """ Expects an integer from bytes corresponding to MembershipAdminCommand. """

        try:
            msg_type = int.from_bytes(_bytes, byteorder="big")

            if msg_type == MembershipAdminCommand.LIST_MEM.value:
                divider = "-" * 80
                output = ""
                count = 0
                for node_id, entry in self.membership_list.items():
                    output += f"{node_id}:\n{entry}\n"
                    count += 1
                logger.info("\nMembership List (%s)\n%s\n%s%s", count, divider, output, divider)

            elif msg_type == MembershipAdminCommand.LIST_SELF.value:
                for node_id, entry in self.membership_list.items():
                    if HOST_IP_ADDRESS == socket.gethostbyname(entry.member.address):
                        logger.info("List Self:\n%s", entry.member)

            elif msg_type == MembershipAdminCommand.JOIN.value:
                self.heartbeats.set()

                # Add introducer back
                name_node_address = socket.gethostbyname(self.introducer)
                name_node = message_pb2.Member()
                name_node.address = name_node_address
                name_node.port = MEMBERSHIP_PORT
                name_node.version_number = 0
                name_node.heartbeat_counter = 0

                if self.suspicion.is_set():
                    name_node.status = message_pb2.Status.ALIVE
                    name_node.incarnation_number = 0

                self.membership_list_queue.put({
                    "type": QueueMessageType.ADD_MEMBER,
                    "member": name_node
                })

                logger.info("Enabled heartbeats: heartbeats=%s", self.heartbeats.is_set())

            elif msg_type == MembershipAdminCommand.LEAVE.value:
                self.heartbeats.clear()
                logger.info("Disabled heartbeats: heartbeats=%s", self.heartbeats.is_set())

            elif msg_type == MembershipAdminCommand.ENABLE_SUSPICION.value:
                self.suspicion.set()
                logger.info("Enabled suspicion: suspicion=%s", self.suspicion.is_set())

            elif msg_type == MembershipAdminCommand.DISABLE_SUSPICION.value:
                self.suspicion.clear()
                logger.info("Disabled suspicion: suspicion=%s", self.suspicion.is_set())

        except Exception as ex:
            logger.error(ex)

    def __listen__(self) -> None:
        """ Start a socket server that spawns a thread per new connection """

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:

            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            self.udp_socket = s
            self.udp_socket.bind((HOST_IP_ADDRESS, MEMBERSHIP_ADMIN_PORT))

            while not self.shutdown.is_set():
                try:
                    _bytes, _ = self.udp_socket.recvfrom(MAX_ADMIN_COMMAND_SIZE)
                    (Thread(target=self.__handle__, args=(_bytes,))).start()
                except Exception as e:
                    logger.error(e)

        logger.debug("Membership Admin Worker SHUTDOWN")
