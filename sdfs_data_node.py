import concurrent.futures
import json
import logging
import multiprocessing
import os
import socket
import threading
from typing import Dict
from typing import Set
from typing import Union

import rpyc

from file_server import download_remote_file
from type_hints import Queues

from utils import DATA_NODE_ROOT_DIR, DATA_NODE_PORT, IP_TO_NAME_MAP, NAME_NODE_PORT, QueueMessageType, DATA_NODE_QUEUE, \
    DN_MAX_THREADS_REPLICATE_POOL
from utils import HOST_ID
from utils import HOST_IP_ADDRESS
from utils import HOST_NAME
from utils import ID_TO_NAME_MAP
from utils import LocalMembershipListEntry
from utils import LOG_DIR
from utils import LOG_FORMATTER
from utils import NODE_COUNT
from utils import get_raw_filename
from utils import get_successor_id


logger = logging.getLogger("SDFS Data Node")
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(f"{LOG_DIR}/data-node.log", "w")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(LOG_FORMATTER)
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(LOG_FORMATTER)
logger.addHandler(stream_handler)


def get_filepath(filename: str) -> str:
    return f"{DATA_NODE_ROOT_DIR}/{filename}"


class DataNodeService(rpyc.Service):
    def __init__(
            self, membership_list: Dict[str, LocalMembershipListEntry], initial_name_node_ip: str, queues: Queues
    ) -> None:

        # Current leader in the simple distributed file system (may or may not be this node)
        self.name_node_address: str = initial_name_node_ip
        self.name_node_name: str = IP_TO_NAME_MAP.get(initial_name_node_ip, initial_name_node_ip)

        # All nodes in the cluster
        self.membership_list: Dict[str, LocalMembershipListEntry] = membership_list

        self.replication_pool = concurrent.futures.ThreadPoolExecutor(DN_MAX_THREADS_REPLICATE_POOL)

        # Allows communication with other processes
        self.data_node_queue: multiprocessing.Queue = queues[DATA_NODE_QUEUE]

        self.queue_listener = threading.Thread(target=self.__queue_listener__)
        self.queue_listener.start()

    def __queue_listener__(self) -> None:
        """ Listen to other processes for events happening on this node """

        logger.debug("Listening to queue...")

        while True:

            event = self.data_node_queue.get(block=True, timeout=None)

            if event is None:
                return

            elif event["type"] == QueueMessageType.NEW_NAME_NODE.value:
                self.name_node_address = event["new_name_node"]
                self.name_node_name = IP_TO_NAME_MAP.get(event["new_name_node"], event["new_name_node"])
                logger.debug("Received new Name Node: %s", self.name_node_name)

    def exposed_replicate(self, source_vm_name: str, str_req: str):
        self.replication_pool.submit(self.__replicate__, source_vm_name, str_req)

    def __replicate__(self, source_vm_name: str, str_req: str) -> None:
        """
        Request format:
        {
            "filehash": str,
            "filename": str,
            "version": int,
            "origin": str,
            "ttl": 3 | 2 | 1
        }

        Forward Request format (decrements ttl and updates source to this node):
        {
            "filehash": str,
            "filename": str,
            "version": int,
            "origin": str,
            "ttl": ttl - 1
        }
        """

        req: Dict[str, Union[str, int]] = json.loads(str_req)

        filehash: str = req["filehash"]
        sdfs_filename: str = req["filename"]
        version: int = req["version"]
        origin: str = req["origin"]
        ttl: int = req["ttl"]

        if ttl <= 0:
            # This should never happen
            logger.debug("Dropped expired REPLICATION Request from %s: %s", source_vm_name, req)
            return

        if origin == HOST_NAME and source_vm_name != HOST_NAME:
            # This might happen if the REPLICATION FACTOR is greater than the number of nodes in the cluster
            logger.debug("Dropped cyclical REPLICATION Request from %s: %s", source_vm_name, req)
            return

        logger.debug("Received REPLICATION Request from %s: %s", source_vm_name, req)

        source_vm_ip = socket.gethostbyname(source_vm_name)
        raw_filename = get_raw_filename(sdfs_filename, version)

        if source_vm_ip != HOST_IP_ADDRESS and not os.path.exists(get_filepath(raw_filename)):

            local_path = f"{DATA_NODE_ROOT_DIR}/{raw_filename}"
            remote_path = f"{DATA_NODE_ROOT_DIR}/{raw_filename}"

            try:

                success = download_remote_file(
                    target_vm=source_vm_name,
                    local_path=local_path,
                    remote_path=remote_path
                )

                if not success:
                    return

                # Confirmation
                with rpyc.connect(self.name_node_address, NAME_NODE_PORT, config={"sync_request_timeout": None}) as con:
                    con.root.exposed_confirm_put(
                        HOST_NAME,
                        json.dumps({
                            "filehash": filehash,
                            "filename": sdfs_filename,
                            "version": version,
                            "data_node_name": HOST_NAME
                        })
                    )

                logger.info(
                    "Finished REPLICATE from %s: downloaded %s to %s",
                    source_vm_name, remote_path, raw_filename
                )

            except Exception as e:
                logger.error(e)

        # Forward replication request to successor
        req["ttl"] -= 1

        excluded_ids: Set[int] = {HOST_ID}

        for i in range(NODE_COUNT):

            successor_id: int = get_successor_id(HOST_ID, self.membership_list, excluded_ids)

            if successor_id == -1:
                logger.debug("Failed to find REPLICATION successor for consistent vm_id %s: %s", HOST_ID, req)
                break

            successor_name: str = ID_TO_NAME_MAP[successor_id]

            try:

                with rpyc.connect(successor_name, DATA_NODE_PORT, config={"sync_request_timeout": None}) as dn_conn:

                    logger.debug("Forward REPLICATION to %s: %s", successor_name, req)
                    dn_conn.root.exposed_replicate(HOST_NAME, json.dumps(req))

                break

            except Exception as e:
                excluded_ids.add(successor_id)
                logger.warning("Failed to find REPLICATE successor %s for %s: %s", successor_name, req, e)
