import json
import logging
import socket
from enum import Enum
from threading import Event
from threading import Lock
from threading import Thread
from threading import Timer
from typing import Union

from type_hints import Events
from type_hints import Queues
from utils import DATA_NODE_QUEUE
from utils import ELECTION_PORT
from utils import ELECTION_QUEUE
from utils import HOST_ID
from utils import HOST_IP_ADDRESS
from utils import ID_TO_NAME_MAP
from utils import IP_TO_NAME_MAP
from utils import LOG_DIR
from utils import LOG_FORMATTER
from utils import MEMBERSHIP_ADMIN_QUEUE
from utils import NAME_NODE_QUEUE
from utils import QueueMessageType
from utils import SCHEDULER_QUEUE
from utils import SHUTDOWN_EVENT


VM_IDS = list(ID_TO_NAME_MAP.keys())
MAX_ID = max(VM_IDS)

logger = logging.getLogger("Bully Election")
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(f"{LOG_DIR}/election.log", "w")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(LOG_FORMATTER)
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(LOG_FORMATTER)
logger.addHandler(stream_handler)


def is_higher_id(item: int, threshold: int) -> bool:
    return item > threshold


def is_lower_id(item: int, threshold: int) -> bool:
    return threshold > item


class ElectionMessageType(Enum):
    ELECTION = 0
    OK = 1
    COORDINATOR = 2


# TODO: Make singleton
class BullyElectionService:

    def __init__(self, initial_name_node_address: str, queues: Queues, events: Events) -> None:
        self.name_node_address = initial_name_node_address
        self.data_node_queue = queues[DATA_NODE_QUEUE]
        self.election_queue = queues[ELECTION_QUEUE]
        self.name_node_queue = queues[NAME_NODE_QUEUE]
        self.membership_admin_queue = queues[MEMBERSHIP_ADMIN_QUEUE]
        self.scheduler_queue = queues[SCHEDULER_QUEUE]

        self.id = HOST_ID
        self.higher_ids = list(filter(lambda x: is_higher_id(x, self.id), VM_IDS))
        self.lower_ids = list(filter(lambda x: is_lower_id(x, self.id), VM_IDS))

        self.election_period = 14
        self.election_max_message_size = 4096
        self.generation = 0
        self.ready_for_election = Event()
        self.ready_for_election.set()
        self.election_timer = None
        self.election_timer_lock = Lock()
        self.coordinator_timer = None
        self.coordinator_timer_lock = Lock()

        self.shutdown = events[SHUTDOWN_EVENT]

        self.udp_socket: Union[socket.socket, None] = None

        self.queue_listener = Thread(target=self.__queue_listener__)
        self.queue_listener.start()

        self.election_listener = Thread(target=self.__election_listener__)
        self.election_listener.start()

    def __queue_listener__(self) -> None:

        logger.debug("Listening for events...")

        while not self.shutdown.is_set():

            event = self.election_queue.get(block=True, timeout=None)

            if event is None:
                if self.udp_socket is not None:
                    try:
                        self.udp_socket.close()
                        logger.debug("Queue Listener SHUTDOWN")
                    except Exception as e:
                        logger.error("Queue Listener SHUTDOWN: %s", e)
                return

            # Just in case new leader failed before self.name_node_address was updated
            self.ready_for_election.wait()

            if event["type"] == QueueMessageType.NODE_FAILURE.value and event["address"] == self.name_node_address:
                self.ready_for_election.clear()
                (Thread(target=self.__start_election__, args=(self.generation,))).start()
                logger.debug(
                    "Queue Listener started a new election from failure: %s",
                    IP_TO_NAME_MAP.get(event["address"], event["address"])
                )

    def __start_election__(self, generation: int) -> None:
        """ Starts a new bully-styled election """

        with self.election_timer_lock, self.coordinator_timer_lock:

            if self.ready_for_election.is_set():
                # Impossible since self.ready_for_election must be set before this function is called
                logger.error(
                    "Called __start_election__ without clearing self.ready_for_election[%s]",
                    self.ready_for_election.is_set()
                )
                return

            if generation < self.generation:
                logger.warning(
                    "Caught stale generation in __start_election__ generation[%s] < self.generation[%s]",
                    generation,
                    self.generation
                )
                return

            if self.id == MAX_ID:
                for node_id in self.lower_ids:
                    self.udp_socket.sendto(
                        json.dumps({
                            "type": ElectionMessageType.COORDINATOR.value,
                            "address": HOST_IP_ADDRESS,
                            "generation": generation
                        }).encode(),
                        (ID_TO_NAME_MAP[node_id], ELECTION_PORT)
                    )
                logger.debug("Preemptive Victory! Multicasted COORDINATOR message to lower ids: %s", self.lower_ids)
            else:
                for node_id in self.higher_ids:
                    self.udp_socket.sendto(
                        json.dumps({
                            "type": ElectionMessageType.ELECTION.value,
                            "vm_id": self.id,
                            "generation": generation
                        }).encode(),
                        (ID_TO_NAME_MAP[node_id], ELECTION_PORT)
                    )
                logger.debug("Multicasted ELECTION message to higher ids: %s", self.higher_ids)

                # Start election timer to wait for responses
                self.__start_election_timer__(generation)
                logger.debug("Election round timer started for %d seconds", self.election_period)

    def __declare_victory__(self, generation: int) -> None:
        """
        Wins election on Election Timer timeout

        Scenario 1: Election Timer expires *AFTER* Coordinator Message Received which is impossible since the
                    Election Timer is cancelled when a COORDINATOR message is received.

        Scenario 2: Election Timer expires *BEFORE* Coordinator Message Received:

            a. Election Timer gets Locks first, results in multiple leaders. Must increase self.election_period

            b. Coordinator Message Received gets Locks first
                1. Increment generation number
                2. Cancel Election Timer
                3. Cancel Coordinator Timer
                4. Let other processes know of the new name node
                5. set self.ready_for_election == True
                6. Election Timer's generation number is stale and returns immediately
        """

        with self.election_timer_lock, self.coordinator_timer_lock:

            if self.ready_for_election.is_set():
                # Impossible since the Election Timer is cancelled when a COORDINATOR message is received
                logger.error("Caught race condition in __declare_victory__")
                return

            if generation < self.generation:
                logger.warning(
                    "Caught stale generation in __declare_victory__ generation[%s] < self.generation[%s]",
                    generation,
                    self.generation
                )
                return

            self.name_node_address = HOST_IP_ADDRESS
            self.generation += 1

            self.__cancel_coordinator_timer__()
            self.__cancel_election_timer__()

            for node_id in self.lower_ids:
                self.udp_socket.sendto(
                    json.dumps({
                        "type": ElectionMessageType.COORDINATOR.value,
                        "address": HOST_IP_ADDRESS,
                        "generation": self.generation
                    }).encode(),
                    (ID_TO_NAME_MAP[node_id], ELECTION_PORT)
                )
            logger.info(
                "Election Timer declared victory! Multicasted COORDINATOR message to lower ids: %s",
                self.lower_ids
            )

            self.data_node_queue.put(
                {"type": QueueMessageType.NEW_NAME_NODE.value, "new_name_node": HOST_IP_ADDRESS}
            )
            self.name_node_queue.put(
                {"type": QueueMessageType.NEW_NAME_NODE.value, "new_name_node": HOST_IP_ADDRESS}
            )
            self.membership_admin_queue.put(
                {"type": QueueMessageType.NEW_NAME_NODE.value, "new_name_node": HOST_IP_ADDRESS}
            )
            self.scheduler_queue.put(
                {"type": QueueMessageType.NEW_NAME_NODE.value, "new_name_node": HOST_IP_ADDRESS}
            )

            self.ready_for_election.set()

    def __coordinator_over__(self, generation: int) -> None:
        """
        Starts a new election

        Scenario 1: Coordinator Timer expires *AFTER* Coordinator Message Received which is impossible since the
                    Coordinator Timer is cancelled when a COORDINATOR message is received.

        Scenario 2: Coordinator Timer expires *BEFORE* Coordinator Message Received:

            a. Coordinator Timer gets Locks first
                1. Coordinator starts new election
                2. New election multicasts ELECTION messages
                3. Receives OK messages:
                    i. if not in an election (self.ready_for_election == True), then drop OK messages
                    ii. if in a new election (self.ready_for_election == False), drop lower generation numbers

            b. Coordinator Message Received gets Locks first
                1. Increment generation number
                2. Cancel Election Timer
                3. Cancel Coordinator Timer
                4. Let other processes know of the new name node
                5. set self.ready_for_election == True
                6. Coordinator Timer's generation number is stale and returns immediately
        """

        with self.election_timer_lock, self.coordinator_timer_lock:

            if self.ready_for_election.is_set():
                # Impossible since the Election Timer is cancelled when a COORDINATOR message is received
                logger.error("Caught race condition in __coordinator_over__")
                return

            if generation < self.generation:
                logger.warning(
                    "Caught stale generation in __coordinator_over__ generation[%s] < self.generation[%s]",
                    generation,
                    self.generation
                )
                return

            self.__cancel_election_timer__()
            self.__cancel_coordinator_timer__()

            (Thread(target=self.__start_election__, args=(generation,))).start()
            logger.debug("Coordinator Timer started a new election")

    def __handle_election_msg__(self, ip: str, msg: dict) -> None:
        """ Handles message during election """

        if msg["type"] == ElectionMessageType.ELECTION.value:
            if self.id > msg["vm_id"]:
                self.udp_socket.sendto(
                    json.dumps({
                        "type": ElectionMessageType.OK.value,
                        "generation": msg["generation"]
                    }).encode(),
                    (ip, ELECTION_PORT)
                )
            else:
                logger.error("Received election from higher id node: %s", msg)

        elif msg["type"] == ElectionMessageType.OK.value:

            with self.election_timer_lock, self.coordinator_timer_lock:
                if not self.ready_for_election.is_set() and msg["generation"] >= self.generation:
                    self.__cancel_election_timer__()
                    self.__start_coordinator_timer__(msg["generation"])

        elif msg["type"] == ElectionMessageType.COORDINATOR.value:

            logger.info(
                "Received COORDINATOR msg with new leader (%s)!",
                IP_TO_NAME_MAP.get(msg["address"], msg["address"])
            )

            with self.election_timer_lock, self.coordinator_timer_lock:

                self.name_node_address = msg["address"]
                self.generation = max(self.generation + 1, msg["generation"] + 1)

                self.__cancel_election_timer__()
                self.__cancel_coordinator_timer__()

                self.data_node_queue.put(
                    {"type": QueueMessageType.NEW_NAME_NODE.value, "new_name_node": msg["address"]}
                )
                self.name_node_queue.put(
                    {"type": QueueMessageType.NEW_NAME_NODE.value, "new_name_node": msg["address"]}
                )
                self.membership_admin_queue.put(
                    {"type": QueueMessageType.NEW_NAME_NODE.value, "new_name_node": HOST_IP_ADDRESS}
                )
                self.scheduler_queue.put(
                    {"type": QueueMessageType.NEW_NAME_NODE.value, "new_name_node": HOST_IP_ADDRESS}
                )

                self.ready_for_election.set()

    def __election_listener__(self) -> None:
        """ Listens for messages during election """

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:

            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.udp_socket = s
            self.udp_socket.bind((HOST_IP_ADDRESS, ELECTION_PORT))
            logger.debug(f"Server is listening...")

            while not self.shutdown.is_set():
                try:
                    _bytes, (ip, port) = self.udp_socket.recvfrom(self.election_max_message_size)
                    msg = json.loads(_bytes)
                    (Thread(target=self.__handle_election_msg__, args=(ip, msg))).start()
                except Exception as e:
                    logger.error(e)

        logger.debug(f"Server Listener SHUTDOWN")

    def __start_coordinator_timer__(self, generation: int):
        """
        Replaces the current Coordinator Timer with a new Coordinator Timer (cancels old timer).

        Note: NOT THREAD SAFE!
        """

        if self.coordinator_timer is None:
            self.coordinator_timer = Timer(self.election_period, self.__coordinator_over__, args=(generation,))
            self.coordinator_timer.start()

    def __cancel_coordinator_timer__(self):
        """
        Cancels the current Coordinator Timer.

        Note: NOT THREAD SAFE!
        """

        if self.coordinator_timer is not None:
            self.coordinator_timer.cancel()
            self.coordinator_timer = None

    def __start_election_timer__(self, generation: int):
        """
        Replaces the current Election Timer with a new Election Timer (cancels old timer).

        Note: NOT THREAD SAFE!
        """

        self.__cancel_election_timer__()
        self.election_timer = Timer(self.election_period, self.__declare_victory__, args=(generation,))
        self.election_timer.start()

    def __cancel_election_timer__(self):
        """
        Cancels the current Election Timer.

        Note: NOT THREAD SAFE!
        """

        if self.election_timer is not None:
            self.election_timer.cancel()
            self.election_timer = None
