import hashlib
import json
import logging
import re
import socket
from datetime import datetime
from enum import Enum
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

from google.protobuf import text_format

import message_pb2


###############################################################################
# Threading - Client Node (~303 Connections)
###############################################################################

CN_MAX_THREADS_PUT = 50
CN_MAX_THREADS_GET = 50
CN_MAX_THREADS_DELETE = 10

# If the user spams these commands, the Client Node may crash from too many threads...
CN_MAX_THREADS_STORE = 1
CN_MAX_THREADS_LS = 1
CN_MAX_THREADS_SCHEDULE_MAPLE_JUICE_JOB = 1

# Rough estimate of the minimum pool size required
CN_MAX_THREADS = CN_MAX_THREADS_GET + CN_MAX_THREADS_PUT + CN_MAX_THREADS_DELETE + CN_MAX_THREADS_STORE + CN_MAX_THREADS_LS + CN_MAX_THREADS_SCHEDULE_MAPLE_JUICE_JOB


###############################################################################
# Threading - File Server (~3,000 Connections)
###############################################################################

# Worst Case: All Data Nodes perform max simultaneous PUT, GET, and DELETE ops
FS_MAX_THREADS_PUT = CN_MAX_THREADS_PUT * 10
FS_MAX_THREADS_GET = CN_MAX_THREADS_GET * 10
FS_MAX_THREADS_REPLICATION = CN_MAX_THREADS_DELETE * 10

# Rough estimate of the minimum pool size required
FS_MAX_THREADS = FS_MAX_THREADS_PUT + FS_MAX_THREADS_GET + FS_MAX_THREADS_REPLICATION


###############################################################################
# Threading - Name Node (~500 Threads | ~3,020 Connections)
###############################################################################

# Worst Case: All Data Nodes perform max simultaneous PUT, GET, and DELETE ops
NN_MAX_THREADS_PUT = CN_MAX_THREADS_PUT * 10
NN_MAX_THREADS_GET = CN_MAX_THREADS_GET * 10
NN_MAX_THREADS_DELETE = CN_MAX_THREADS_DELETE * 10

# Max number of re-replicate requests that can be sent to data nodes at once
NN_MAX_THREADS_RE_REPLICATE = 250

# Each Data Node will open a single rpc connection with the Name Node where
# each PUT confirmations and REPLICATE confirmations are sent asynchronously
NN_MAX_THREADS_CONFIRM = 10

# Client Node will re-use a single connection to send store commands.
# A user spamming "list" could break the system from too many threads
NN_MAX_THREADS_LIST = 10


###############################################################################
# Threading - Data Node (~100 Threads | ~4,500 Connections)
###############################################################################

# Worst case: All clients choose to PUT, GET, and Delete on 1 Data Node
DN_MAX_THREADS_PUT = CN_MAX_THREADS_PUT * 10
DN_MAX_THREADS_GET = CN_MAX_THREADS_GET * 10
DN_MAX_THREADS_DELETE = CN_MAX_THREADS_DELETE * 10

# Worst case: Name Node sends all re-replicate requests to 1 Data Node
DN_MAX_THREADS_RE_REPLICATION = NN_MAX_THREADS_RE_REPLICATE

# Data Node Pools
DN_MAX_THREADS_REPLICATE_POOL = 50

# Worst case: Every Data Node sends all replicate requests to 1 Data Node
DN_MAX_THREADS_REPLICATE = DN_MAX_THREADS_REPLICATE_POOL * 10

# Rough estimate of pool size to support all worst case scenarios
DN_SERVER_MIN_POOL_SIZE = DN_MAX_THREADS_PUT + DN_MAX_THREADS_PUT + DN_MAX_THREADS_DELETE + DN_MAX_THREADS_REPLICATE + DN_MAX_THREADS_RE_REPLICATION


###############################################################################
# Threading - Scheduler Node (~10 Threads | ~3 Connections)
#
#   Threads are created to distribute tasks to Maple Nodes, but depend on the
#   size of the membership list. For class, there's a hard cap of 10 members
#
#
# Threading - Maple Node (~310 Threads, 3 Connections)
###############################################################################

MN_MAX_THREADS_PUT = 100
MN_MAX_THREADS_GET = 100
MN_MAX_THREADS_MAPLE_EXE = 100
MN_MAX_THREADS_FETCH_KEYS = 10

# Scheduler Node recycles connection Maple Connection
MN_MAX_THREADS_MAP = 1
MN_MAX_THREADS_COMBINE = 1
MN_MAX_THREADS_REDUCE = 1


################################
# Multiprocessing
################################


# Queues
ELECTION_QUEUE = "ELECTION_QUEUE"
NAME_NODE_QUEUE = "NAME_NODE_QUEUE"
DATA_NODE_QUEUE = "DATA_NODE_QUEUE"
MEMBERSHIP_LIST_QUEUE = "MEMBERSHIP_LIST_QUEUE"
MEMBERSHIP_ADMIN_QUEUE = "MEMBERSHIP_ADMIN_QUEUE"
SCHEDULER_QUEUE = "SCHEDULER_QUEUE"
MAPLE_SERVICE_QUEUE = "MAPLE_SERVICE_QUEUE"
CLIENT_NODE_QUEUE = "CLIENT_NODE_QUEUE"

# Events
SEND_HEARTBEATS_EVENT = "SEND_HEARTBEATS"
USE_SUSPICION_EVENT = "USE_SUSPICION_EVENT"
SHUTDOWN_EVENT = "SHUTDOWN_EVENT"
NAME_NODE_FINISHED_EVENT = "NAME_NODE_FINISHED_EVENT"
DATA_NODE_FINISHED_EVENT = "DATA_NODE_FINISHED_EVENT"
MEMBERSHIP_FINISHED_EVENT = "MEMBERSHIP_FINISHED_EVENT"
BULLY_ELECTION_FINISHED_EVENT = "BULLY_ELECTION_FINISHED_EVENT"
CLIENT_NODE_FINISHED_EVENT = "CLIENT_NODE_FINISHED_EVENT"
MAPLE_JUICE_FINISHED_EVENT = "MAPLE_JUICE_FINISHED_EVENT"


################################
# Networking
################################

# Addressing
HOST_IP_ADDRESS = socket.gethostbyname(socket.gethostname())
ID_TO_NAME_MAP = {
    1: "fa23-cs425-7801.cs.illinois.edu",
    2: "fa23-cs425-7802.cs.illinois.edu",
    3: "fa23-cs425-7803.cs.illinois.edu",
    4: "fa23-cs425-7804.cs.illinois.edu",
    5: "fa23-cs425-7805.cs.illinois.edu",
    6: "fa23-cs425-7806.cs.illinois.edu",
    7: "fa23-cs425-7807.cs.illinois.edu",
    8: "fa23-cs425-7808.cs.illinois.edu",
    9: "fa23-cs425-7809.cs.illinois.edu",
    10: "fa23-cs425-7810.cs.illinois.edu"
}
NAME_TO_ID_MAP = {
    "fa23-cs425-7801.cs.illinois.edu": 1,
    "fa23-cs425-7802.cs.illinois.edu": 2,
    "fa23-cs425-7803.cs.illinois.edu": 3,
    "fa23-cs425-7804.cs.illinois.edu": 4,
    "fa23-cs425-7805.cs.illinois.edu": 5,
    "fa23-cs425-7806.cs.illinois.edu": 6,
    "fa23-cs425-7807.cs.illinois.edu": 7,
    "fa23-cs425-7808.cs.illinois.edu": 8,
    "fa23-cs425-7809.cs.illinois.edu": 9,
    "fa23-cs425-7810.cs.illinois.edu": 10
}

IP_TO_NAME_MAP: Dict[str, str] = {
    socket.gethostbyname("fa23-cs425-7801.cs.illinois.edu"): "fa23-cs425-7801.cs.illinois.edu",
    socket.gethostbyname("fa23-cs425-7802.cs.illinois.edu"): "fa23-cs425-7802.cs.illinois.edu",
    socket.gethostbyname("fa23-cs425-7803.cs.illinois.edu"): "fa23-cs425-7803.cs.illinois.edu",
    socket.gethostbyname("fa23-cs425-7804.cs.illinois.edu"): "fa23-cs425-7804.cs.illinois.edu",
    socket.gethostbyname("fa23-cs425-7805.cs.illinois.edu"): "fa23-cs425-7805.cs.illinois.edu",
    socket.gethostbyname("fa23-cs425-7806.cs.illinois.edu"): "fa23-cs425-7806.cs.illinois.edu",
    socket.gethostbyname("fa23-cs425-7807.cs.illinois.edu"): "fa23-cs425-7807.cs.illinois.edu",
    socket.gethostbyname("fa23-cs425-7808.cs.illinois.edu"): "fa23-cs425-7808.cs.illinois.edu",
    socket.gethostbyname("fa23-cs425-7809.cs.illinois.edu"): "fa23-cs425-7809.cs.illinois.edu",
    socket.gethostbyname("fa23-cs425-7810.cs.illinois.edu"): "fa23-cs425-7810.cs.illinois.edu"
}
HOST_NAME = IP_TO_NAME_MAP.get(HOST_IP_ADDRESS, HOST_IP_ADDRESS)

IP_TO_ID_MAP = {
    socket.gethostbyname("fa23-cs425-7801.cs.illinois.edu"): 1,
    socket.gethostbyname("fa23-cs425-7802.cs.illinois.edu"): 2,
    socket.gethostbyname("fa23-cs425-7803.cs.illinois.edu"): 3,
    socket.gethostbyname("fa23-cs425-7804.cs.illinois.edu"): 4,
    socket.gethostbyname("fa23-cs425-7805.cs.illinois.edu"): 5,
    socket.gethostbyname("fa23-cs425-7806.cs.illinois.edu"): 6,
    socket.gethostbyname("fa23-cs425-7807.cs.illinois.edu"): 7,
    socket.gethostbyname("fa23-cs425-7808.cs.illinois.edu"): 8,
    socket.gethostbyname("fa23-cs425-7809.cs.illinois.edu"): 9,
    socket.gethostbyname("fa23-cs425-7810.cs.illinois.edu"): 10
}
HOST_ID = IP_TO_ID_MAP.get(HOST_IP_ADDRESS, -1)

# Ports
CLIENT_NODE_PORT = 420_00
DATA_NODE_PORT = 420_11
ELECTION_PORT = 420_22
MEMBERSHIP_PORT = 420_33
MEMBERSHIP_ADMIN_PORT = 420_44
NAME_NODE_PORT = 420_55
SCHEDULER_NODE_PORT = 420_77
MAPLE_NODE_PORT = 420_88
FILE_NODE_PORT = 420_99

# Sockets
SDFS_MESSAGE_SIZE = 4096
MAPLE_JUICE_MESSAGE_SIZE = 4096
MAX_ADMIN_COMMAND_SIZE = 4096
MAX_HEARTBEAT_SIZE = 4096


BUFFER_SIZE = 16 * 1024 * 1024


################################
# Logging
################################

LOG_DIR = "logs"
LOG_FORMATTER = logging.Formatter(f"[{HOST_NAME} - %(levelname)s - %(name)s] %(message)s")

################################
# Membership (Failure Detection)
################################

T_GOSSIP = .25


class QueueMessageType(Enum):
    NODE_FAILURE = "NODE_FAILURE"
    NEW_NAME_NODE = "NEW_NAME_NODE"
    NEW_MEMBERSHIP_LIST = "NEW_MEMBERSHIP_LIST"
    ADD_MEMBER = "ADD_MEMBER"


class MembershipAdminCommand(Enum):
    LIST_MEM = 1
    LIST_SELF = 2
    JOIN = 3
    LEAVE = 4
    ENABLE_SUSPICION = 5
    DISABLE_SUSPICION = 6


class LocalMembershipListEntry:
    def __init__(self, member: message_pb2.Member, timestamp: datetime):
        self.member = member
        self.timestamp = timestamp

    def __str__(self):
        return (f"  name: {IP_TO_NAME_MAP.get(self.member.address, self.member.address)}\n"
                f"{text_format.MessageToString(self.member, indent=2)}  timestamp: {self.timestamp}")


def get_successor_id(vm_id: int, membership_list: Dict[str, LocalMembershipListEntry], excluded_ids: Set[int]) -> int:

    alive_vms: List[str] = [
        IP_TO_NAME_MAP.get(entry.member.address, entry.member.address) for entry in membership_list.values()
    ]

    for i in range(NODE_COUNT):

        curr_id = (vm_id + i) % 10

        if curr_id == 0:
            curr_id = 10

        if curr_id not in excluded_ids and ID_TO_NAME_MAP[curr_id] in alive_vms:
            return curr_id

    return -1


################################
# Simple Distributed File System
################################

# Client
CLIENT_ROOT_DIR = "client"


# Name Node
NODE_COUNT = 10
REPLICA_COUNT = 4


# Data Node
DATA_NODE_ROOT_DIR = "sdfs"
FILE_STORE_KEY = "FILE_STORE_KEY"


def get_raw_filename(filename: str, version: int) -> str:
    return f"{filename}.{version}"


def get_filehash(filename: str) -> str:
    return hashlib.sha256(str(filename).encode()).hexdigest()


def get_consistent_id(filehash: str) -> int:
    return 1 + (int(filehash, 16) % NODE_COUNT)


################################
# Maple Juice
################################

MAPLE_JOB = "MAPLE_JOB"
JUICE_JOB = "JUICE_JOB"

HASH_PARTITION = "h"
RANGE_PARTITION = "r"

NUM_MAPLE_TASKS_LIMIT = 100

# Scheduler irectories
MAPLE_EXE_DIR = "exe/maple"
JUICE_EXE_DIR = "exe/juice"

MAPLE_AGGREGATE_DIR = "COMBINED_MAPLE_RESULTS"
JUICE_AGGREGATE_DIR = "COMBINED_JUICE_RESULTS"


MAPLE_INTERMEDIATE_DIR = "INTERMEDIATE_MAPLE"
JUICE_INTERMEDIATE_DIR = "INTERMEDIATE_JUICE"

# Maple Directories
MAPLE_EXE_OUT_DIR = "MAPLE_EXE_OUT"
MAPLE_RESULT_DIR = "MAPLE_COMBINED_EXE_OUT"


# Juice Directories
JUICE_EXE_OUT_DIR = "JUICE_EXE_OUT"
JUICE_RESULT_DIR = "JUICE_COMBINED_EXE_OUT"


def sanitize_filename(filename):
    filename = filename.replace(' ', '__')
    filename = re.sub(r'[\\/*?:"<>|]', '__', filename)
    filename.replace("\ufeff", "")
    return filename


def load_schema(schema_json_filename: str) -> Optional[dict]:

    try:
        with open(f"schema/schema-{schema_json_filename}.json") as schema_json_file:
            return json.loads(schema_json_file.read())
    except Exception as e:
        print(e)

    return None
