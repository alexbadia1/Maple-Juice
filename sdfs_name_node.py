import concurrent.futures
import json
import logging
import pprint
from datetime import datetime
from threading import Lock
from threading import Thread
from typing import Dict, Optional
from typing import List
from typing import Set
from typing import Union

import rpyc
from type_hints import Queues

from utils import NAME_NODE_QUEUE, QueueMessageType, NN_MAX_THREADS_RE_REPLICATE, FILE_NODE_PORT, get_raw_filename
from utils import DATA_NODE_PORT
from utils import get_consistent_id
from utils import get_filehash
from utils import get_successor_id
from utils import ID_TO_NAME_MAP
from utils import IP_TO_NAME_MAP
from utils import LocalMembershipListEntry
from utils import LOG_DIR
from utils import LOG_FORMATTER
from utils import NODE_COUNT
from utils import REPLICA_COUNT

logger = logging.getLogger("SDFS Name Node")
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(f"{LOG_DIR}/name-node.log", "w")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(LOG_FORMATTER)
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(LOG_FORMATTER)
logger.addHandler(stream_handler)


class FileTableEntry:
    def __init__(self, filehash: str, versions: Dict[int, Set[str]]):
        self.filehash: str = filehash
        self.versions: Dict[int, Set[str]] = versions

    def __repr__(self):
        output = [f"\n\t{self.filehash},"]
        for item in self.versions.items():
            output.append(f"\n\t{item}")
        return "".join(output)


class NameNodeService(rpyc.Service):
    def __init__(self, membership_list: Dict[str, LocalMembershipListEntry], queues: Queues) -> None:

        # All nodes in the cluster
        self.membership_list = membership_list

        # Allows communication with other processes
        self.name_node_queue = queues[NAME_NODE_QUEUE]

        # Lookup table of files and their storage locations
        self.file_table: Dict[str, FileTableEntry] = {}
        self.file_table_lock = Lock()

        # Remember deleted files, so replication's can be rejected (this could grow big rather quickly)
        #
        # Passive replication in an asynchronous system results in a race condition with active deletion.
        # Turns out this is a pretty complicated problem with a non-trivial solution...
        self.delete_cache = set()

        self.re_replication_pool = concurrent.futures.ThreadPoolExecutor(NN_MAX_THREADS_RE_REPLICATE)

        self.queue_listener = Thread(target=self.__queue_listener__)
        self.queue_listener.start()

    def __queue_listener__(self) -> None:
        """ Listens to other processes for events """

        logger.debug("Listening to queue...")

        while True:

            event = self.name_node_queue.get(block=True, timeout=None)

            if event is None:
                return

            elif event["type"] == QueueMessageType.NODE_FAILURE.value:
                (Thread(target=self.__re_replicate__, args=(event["address"],))).start()

    def __re_replicate__(self, failed_node_address: str) -> None:
        """ Sends re-replication requests for the files stored on failed data nodes """

        data_node_name = IP_TO_NAME_MAP.get(failed_node_address, failed_node_address)

        logger.debug("Started RE_REPLICATION from Data Node Failure: %s", data_node_name)

        # Send re-replication requests for all versions of files that were stored on the failed node
        for filename, entry in self.file_table.items():
            failed_versions: Dict[int, Set[str]] = {
                v: l for v, l in entry.versions.items() if data_node_name in l or len(l) < REPLICA_COUNT
            }
            self.re_replication_pool.submit(self.__send_re_replicate__, filename, entry, failed_versions)

        # Remove failed node from all versions of files
        with self.file_table_lock:
            for filename, entry in self.file_table.items():
                for version, locations in entry.versions.items():
                    locations.discard(data_node_name)

    def __send_re_replicate__(
            self, filename: str, entry: FileTableEntry, failed_versions: Dict[int, Set[str]]
    ) -> None:
        """ Sends a DATA_NODE_REPLICATION_REQUEST to the data node """

        consistent_vm_id: int = get_consistent_id(entry.filehash)

        for version, location_set in failed_versions.items():

            excluded_ids: Set[int] = set()

            for i in range(NODE_COUNT):

                successor_id: int = get_successor_id(consistent_vm_id, self.membership_list, excluded_ids)

                if successor_id == -1:
                    logger.debug("Can't find RE_REPLICATION successor for vm_id: %s", consistent_vm_id)
                    break

                vm_name: str = ID_TO_NAME_MAP[successor_id]

                body = {
                    "filehash": entry.filehash,
                    "filename": filename,
                    "version": version,
                    "origin": vm_name,
                    "ttl": REPLICA_COUNT
                }

                try:
                    with rpyc.connect(vm_name, DATA_NODE_PORT, config={"sync_request_timeout": None}) as dn_conn:

                        logger.debug("Sent RE_REPLICATION to %s: %s", vm_name, body)
                        dn_conn.root.exposed_replicate(vm_name, json.dumps(body))

                        break

                except Exception as e:
                    excluded_ids.add(successor_id)
                    logger.warning("Failed RE_REPLICATION of %s to %s: %s", body, vm_name, e)

    def exposed_get(self, source_vm_name: str, sdfs_filename: str) -> str:
        """
        Response format:
            {
                "filename": str,
                "version": int,
                "data_node_names": List[str]
            }
        """

        logger.debug("Received GET Request from %s: sdfs_filename=%s", source_vm_name, sdfs_filename)

        latest_version: Optional[int] = None
        data_node_names: List[str] = []
        filehash: Optional[str] = None

        with self.file_table_lock:
            if sdfs_filename in self.file_table:
                entry = self.file_table[sdfs_filename]
                filehash = entry.filehash
                if list(entry.versions.keys()):
                    latest_version = max(entry.versions.keys())
                    data_node_names = list(entry.versions[latest_version])

        resp = {
            "filehash": filehash,
            "filename": sdfs_filename,
            "version": latest_version,
            "data_node_names": data_node_names
        }
        logger.debug("Sent GET Response to %s: %s", source_vm_name, resp)
        return json.dumps(resp)

    def exposed_put(self, source_vm_name: str, sdfs_filename: str) -> str:
        """
        Response format:
            {
                "filehash": str,
                "filename": str,
                "version": int,
                "data_node_name": str
            }
        """

        logger.debug("Received PUT Request from %s: sdfs_filename=%s", source_vm_name, sdfs_filename)

        version: int = int(round(datetime.now().timestamp()))

        # Consistent hash to determine which node the file should be placed
        filehash: str = get_filehash(sdfs_filename)
        consistent_id: int = get_consistent_id(filehash)
        data_node_name: str = ID_TO_NAME_MAP[consistent_id]

        # Return data node address the client should upload the file too
        resp = {"filehash": filehash, "filename": sdfs_filename, "version": version, "data_node_name": data_node_name}
        logger.debug("Sent PUT Response to %s: %s", source_vm_name, resp)
        return json.dumps(resp)

    def exposed_confirm_put(self, source_vm_name: str, str_req: str) -> None:
        """
        Request format:
            {
                "filehash": str,
                "filename": str,
                "version": int,
                "data_node_name": str
            }
        """

        req: Dict[str, Union[str, int]] = json.loads(str_req)

        logger.debug("Received CONFIRM_PUT Request from %s: %s", source_vm_name, req)

        filehash: str = req["filehash"]
        filename: str = req["filename"]
        version: int = req["version"]
        data_node_name: str = req["data_node_name"]

        with self.file_table_lock:

            if get_raw_filename(filename, version) in self.delete_cache:
                # Timestamp based versioning allows for permanent deletion, since time only moves forwards
                return

            if filename in self.file_table:
                if version in self.file_table[filename].versions:
                    # New location
                    self.file_table[filename].versions[version].add(data_node_name)
                else:
                    # New version
                    self.file_table[filename].versions.update({version: {data_node_name}})
            else:
                # New file
                self.file_table.update({filename: FileTableEntry(filehash, {version: {data_node_name}})})

    def exposed_delete(self, source_vm_name: str, sdfs_filename: str) -> str:
        """ Deletes all replicas of all versions of the file from all data nodes """

        logger.debug("Received DELETE Request from %s: filename=%s", source_vm_name, sdfs_filename)

        target_data_nodes = set()

        with self.file_table_lock:
            if sdfs_filename in self.file_table:
                for version, locations in self.file_table[sdfs_filename].versions.items():
                    self.delete_cache.add(get_raw_filename(sdfs_filename, version))
                    for data_node_name in locations:
                        target_data_nodes.add(data_node_name)

            self.file_table.pop(sdfs_filename, None)

        delete_responses: List[str] = []
        for data_node_name in target_data_nodes:
            try:
                with rpyc.connect(data_node_name, FILE_NODE_PORT, config={"sync_request_timeout": None}) as fs_conn:

                    logger.debug("Sent DELETE to File Node %s: %s", data_node_name, sdfs_filename)
                    resp = fs_conn.root.exposed_delete(sdfs_filename)
                    logger.debug("Received DELETE to File Node %s: %s", data_node_name, resp)
                    delete_responses.append(resp)
            except Exception as e:
                logger.warning("Failed to send delete to File Node. If the Node failed, then is ok: %s", e)
                delete_responses.append(str(e))

        return json.dumps(delete_responses, indent=2)

    def exposed_ls(self, source_vm_name: str) -> str:

        logger.debug("Received LIST Request from %s", source_vm_name)
        return pprint.pformat(self.file_table)

    def exposed_dir(self, source_vm_name: str, sdfs_directory: str) -> str:

        logger.debug("Received DIR Request from %s: sdfs_directory=%s", source_vm_name, sdfs_directory)

        result: Dict[str, Dict[str, Union[str, int]]] = {}

        with self.file_table_lock:
            for filename, e in self.file_table.items():
                if filename.startswith(sdfs_directory):
                    if list(e.versions.keys()):
                        result.update({
                            filename: {
                                "filehash": e.filehash,
                                "version": max(self.file_table[filename].versions.keys())
                            }

                        })

        return json.dumps(result, indent=2, sort_keys=True)
