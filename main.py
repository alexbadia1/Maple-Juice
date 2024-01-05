import argparse
import os
import shutil
import socket
import sys
from multiprocessing import Event
from multiprocessing import Process
from multiprocessing import Manager
from multiprocessing import Queue
from typing import Dict

from rpyc import ThreadPoolServer
from rpyc import ThreadedServer

from utils import (CLIENT_ROOT_DIR, LOG_DIR, MAPLE_INTERMEDIATE_DIR, MAPLE_RESULT_DIR, MAPLE_AGGREGATE_DIR,
                   JUICE_AGGREGATE_DIR, JUICE_INTERMEDIATE_DIR, JUICE_RESULT_DIR, MAPLE_EXE_OUT_DIR, JUICE_EXE_OUT_DIR)

os.makedirs(CLIENT_ROOT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

shutil.rmtree(MAPLE_AGGREGATE_DIR, ignore_errors=True)
os.makedirs(MAPLE_AGGREGATE_DIR, exist_ok=True)
shutil.rmtree(MAPLE_INTERMEDIATE_DIR, ignore_errors=True)
os.makedirs(MAPLE_INTERMEDIATE_DIR, exist_ok=True)
shutil.rmtree(MAPLE_RESULT_DIR, ignore_errors=True)
os.makedirs(MAPLE_RESULT_DIR, exist_ok=True)
shutil.rmtree(MAPLE_EXE_OUT_DIR, ignore_errors=True)
os.makedirs(MAPLE_EXE_OUT_DIR, exist_ok=True)

shutil.rmtree(JUICE_AGGREGATE_DIR, ignore_errors=True)
os.makedirs(JUICE_AGGREGATE_DIR, exist_ok=True)
shutil.rmtree(JUICE_INTERMEDIATE_DIR, ignore_errors=True)
os.makedirs(JUICE_INTERMEDIATE_DIR, exist_ok=True)
shutil.rmtree(JUICE_RESULT_DIR, ignore_errors=True)
os.makedirs(JUICE_RESULT_DIR, exist_ok=True)
shutil.rmtree(JUICE_EXE_OUT_DIR, ignore_errors=True)
os.makedirs(JUICE_EXE_OUT_DIR, exist_ok=True)

from file_server import FileService
from utils import CLIENT_NODE_PORT
from utils import DATA_NODE_PORT
from utils import FILE_NODE_PORT
from utils import MAPLE_NODE_PORT
from utils import NAME_NODE_PORT
from utils import SCHEDULER_NODE_PORT

from maple_juice_map import MapleService
from maple_juice_scheduler import SchedulerService
from membership import LocalMembershipList
from membership import LocalMembershipListEntry
from membership import MembershipAdminWorker
from sdfs_election import BullyElectionService
from sdfs_client_node import ClientNodeService
from sdfs_data_node import DataNodeService
from sdfs_name_node import NameNodeService
from type_hints import Queues
from type_hints import Events
from utils import CLIENT_NODE_QUEUE
from utils import CLIENT_NODE_FINISHED_EVENT
from utils import BULLY_ELECTION_FINISHED_EVENT
from utils import DATA_NODE_FINISHED_EVENT
from utils import DATA_NODE_QUEUE
from utils import DATA_NODE_ROOT_DIR
from utils import ELECTION_QUEUE
from utils import MAPLE_JUICE_FINISHED_EVENT
from utils import MAPLE_SERVICE_QUEUE
from utils import MEMBERSHIP_ADMIN_QUEUE
from utils import MEMBERSHIP_FINISHED_EVENT
from utils import MEMBERSHIP_LIST_QUEUE
from utils import NAME_NODE_FINISHED_EVENT
from utils import NAME_NODE_QUEUE
from utils import SEND_HEARTBEATS_EVENT
from utils import SHUTDOWN_EVENT
from utils import SCHEDULER_QUEUE
from utils import USE_SUSPICION_EVENT


def reset_data_node_directory(directory: str) -> None:
    """ Remove all files from data_node on startup """

    if os.path.exists(directory):
        for item in os.listdir(directory):
            item_path = os.path.join(directory, item)
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
            else:
                os.remove(item_path)
    else:
        os.makedirs(directory)


def membership_service(
        membership_list: Dict[str, LocalMembershipListEntry], initial_name_node_ip: str, queues: Queues, events: Events
) -> None:
    """ Bootstrap Gossip Failure Detection """

    # TODO: Ignore CTRL-C and gracefully shutdown
    #   signal.signal(signal.SIGINT, lambda signum, frame: None)

    # Start gossip failure detection
    lml = LocalMembershipList(membership_list, initial_name_node_ip, queues, events)
    maw = MembershipAdminWorker(membership_list, initial_name_node_ip, queues, events)

    # On CTRL-C, wait for the SHUTDOWN_EVENT to let threads join
    maw.queue_listener.join()
    maw.listener.join()
    lml.queue_listener.join()
    lml.heartbeat_sender.join()
    lml.heartbeat_listener.join()

    # Let other processes know that the membership service finished gracefully
    events[MEMBERSHIP_FINISHED_EVENT].set()


def bully_election_service(name_node_ip: str, queues: Queues, events: Events) -> None:
    """ Bootstrap Bully Election Node """

    # TODO: Ignore CTRL-C and gracefully shutdown
    #   signal.signal(signal.SIGINT, lambda signum, frame: None)

    dnw = BullyElectionService(name_node_ip, queues, events)

    dnw.queue_listener.join()
    dnw.election_listener.join()

    events[BULLY_ELECTION_FINISHED_EVENT].set()


def name_node_service(membership_list: Dict[str, LocalMembershipListEntry], queues: Queues) -> None:
    """ Bootstrap Name Node with RPyC """

    # TODO: Ignore CTRL-C and gracefully shutdown
    #   signal.signal(signal.SIGINT, lambda signum, frame: None)

    nns = NameNodeService(membership_list, queues)

    server = ThreadPoolServer(nns, port=NAME_NODE_PORT, nbThreads=4096)
    server.start()


def data_node_service(
        membership_list: Dict[str, LocalMembershipListEntry], name_node: str, queues: Queues
) -> None:
    """ Bootstrap Data Node Service with RPyC """

    # TODO: Ignore CTRL-C and gracefully shutdown
    #   signal.signal(signal.SIGINT, lambda signum, frame: None)

    dns = DataNodeService(membership_list, name_node, queues)

    server = ThreadPoolServer(dns, port=DATA_NODE_PORT, nbThreads=2600)
    server.start()


def client_node_service(
        membership_list: Dict[str, LocalMembershipListEntry], name_node_ip: str, queues: Queues
) -> None:
    """Bootstrap Client Node Service with RPyC """

    # TODO: Ignore CTRL-C and gracefully shutdown
    #   signal.signal(signal.SIGINT, lambda signum, frame: None)

    cns = ClientNodeService(membership_list, name_node_ip, queues)

    server = ThreadPoolServer(cns, port=CLIENT_NODE_PORT, nbThreads=360)
    server.start()


def maple_service(membership_list: Dict[str, LocalMembershipListEntry]) -> None:
    """ Bootstrap Map Node Service with RPyC """

    # TODO: Ignore CTRL-C and gracefully shutdown
    #   signal.signal(signal.SIGINT, lambda signum, frame: None)

    # Start Map Reduce services
    ms = MapleService(membership_list)

    server = ThreadedServer(ms, port=MAPLE_NODE_PORT)
    server.start()


def scheduler_service(
        membership_list: Dict[str, LocalMembershipListEntry], name_node: str, queues: Queues
) -> None:
    """ Bootstrap Scheduler Node Service with RPyC """

    # TODO: Ignore CTRL-C and gracefully shutdown
    #   signal.signal(signal.SIGINT, lambda signum, frame: None)

    # Start Map Reduce services
    ss = SchedulerService(membership_list, name_node, queues)

    server = ThreadedServer(ss, port=SCHEDULER_NODE_PORT)
    server.start()


def file_service() -> None:
    """ Bootstrap Scheduler Node Service with RPyC """

    # TODO: Ignore CTRL-C and gracefully shutdown
    #   signal.signal(signal.SIGINT, lambda signum, frame: None)

    # Start Map Reduce services
    server = ThreadPoolServer(FileService, port=FILE_NODE_PORT, nbThreads=4096)
    server.start()


def shutdown(signum, frame, queues: Queues, events: Events) -> None:
    """ Gracefully shutdown program """

    try:
        # Send shutdown event to all processes
        events[SHUTDOWN_EVENT].set()

        # Gracefully release all queue listeners
        for k, q in queues.items():
            q.put(None)

        # If Heartbeat Sender was disabled, re-enable so the thread can exit
        events[SEND_HEARTBEATS_EVENT].set()

        # Wait for processes to finish gracefully
        events[NAME_NODE_FINISHED_EVENT].wait()
        events[DATA_NODE_FINISHED_EVENT].wait()
        events[BULLY_ELECTION_FINISHED_EVENT].wait()
        events[MEMBERSHIP_FINISHED_EVENT].wait()
        events[MAPLE_JUICE_FINISHED_EVENT].wait()

        # Close all queues
        for k, q in queues.items():
            q.close()
    except Exception as e:
        print(e)

    sys.exit(1)


def main() -> None:
    # Command line arguments
    parser = argparse.ArgumentParser(description="MapleJuice")
    parser.add_argument("-name_node", type=str, help="Name node")
    args = parser.parse_args()
    name_node: str = args.name_node or "fa23-cs425-7810.cs.illinois.edu"

    # In case name_node command line argument is not ip, convert to ip address
    name_node_ip: str = socket.gethostbyname(name_node)

    try:
        with Manager() as manager:
            # Global membership list
            membership_list: Dict[str, LocalMembershipListEntry] = manager.dict()

            # Queues for communication between processes
            queues = {
                ELECTION_QUEUE: Queue(),
                NAME_NODE_QUEUE: Queue(),
                DATA_NODE_QUEUE: Queue(),
                CLIENT_NODE_QUEUE: Queue(),
                MEMBERSHIP_LIST_QUEUE: Queue(),
                MEMBERSHIP_ADMIN_QUEUE: Queue(),
                SCHEDULER_QUEUE: Queue(),
                MAPLE_SERVICE_QUEUE: Queue(),
            }

            # Events for live configuration changes and graceful shutdown
            events = {
                # Gossip failure detection configuration controls
                SEND_HEARTBEATS_EVENT: Event(),
                USE_SUSPICION_EVENT: Event(),

                # Shutdown program gracefully
                SHUTDOWN_EVENT: Event(),
                NAME_NODE_FINISHED_EVENT: Event(),
                DATA_NODE_FINISHED_EVENT: Event(),
                BULLY_ELECTION_FINISHED_EVENT: Event(),
                CLIENT_NODE_FINISHED_EVENT: Event(),
                MEMBERSHIP_FINISHED_EVENT: Event(),
                MAPLE_JUICE_FINISHED_EVENT: Event()
            }

            events[USE_SUSPICION_EVENT].set()
            events[SEND_HEARTBEATS_EVENT].set()

            # Membership
            (Process(target=membership_service, args=(membership_list, name_node_ip, queues, events))).start()
            (Process(target=bully_election_service, args=(name_node_ip, queues, events))).start()

            # Simple Distributed File System
            reset_data_node_directory(DATA_NODE_ROOT_DIR)
            (Process(target=file_service)).start()
            (Process(target=name_node_service, args=(membership_list, queues))).start()
            (Process(target=data_node_service, args=(membership_list, name_node_ip, queues))).start()
            (Process(target=client_node_service, args=(membership_list, name_node_ip, queues))).start()

            # Map-Reduce
            (Process(target=maple_service, args=(membership_list,))).start()
            (Process(target=scheduler_service, args=(membership_list, name_node_ip, queues))).start()

            # On CTRL-C, stop program gracefully
            # signal.signal(signal.SIGINT, lambda signum, frame: shutdown(signum, frame, queues, events))

            while True:
                pass

    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
