import json
import logging
import multiprocessing
import threading
from typing import Dict
from typing import Optional
from typing import List
from typing import Set

import rpyc

from file_server import upload_remote_file, download_remote_file
from type_hints import Queues
from utils import CLIENT_NODE_QUEUE, SCHEDULER_NODE_PORT, DATA_NODE_ROOT_DIR, get_raw_filename, DATA_NODE_PORT, \
    REPLICA_COUNT
from utils import HOST_NAME
from utils import ID_TO_NAME_MAP
from utils import IP_TO_NAME_MAP
from utils import LOG_DIR
from utils import LOG_FORMATTER
from utils import NAME_NODE_PORT
from utils import NAME_TO_ID_MAP
from utils import NODE_COUNT
from utils import LocalMembershipListEntry
from utils import QueueMessageType
from utils import get_successor_id


logger = logging.getLogger("SDFS Client Node")
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(f"{LOG_DIR}/client-node.log", "w")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(LOG_FORMATTER)
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(LOG_FORMATTER)
logger.addHandler(stream_handler)


class ClientNodeService(rpyc.Service):
    def __init__(
            self, membership_list: Dict[str, LocalMembershipListEntry], initial_name_node_address: str, queues: Queues
    ) -> None:

        # Current leader in the simple distributed file system (may or may not be this node)
        self.name_node_address: str = initial_name_node_address
        self.name_node_name: str = IP_TO_NAME_MAP.get(initial_name_node_address, initial_name_node_address)
        self.membership_list: Dict[str, LocalMembershipListEntry] = membership_list

        # Allows communication with other processes
        self.client_node_queue: multiprocessing.Queue = queues[CLIENT_NODE_QUEUE]

        self.queue_listener = threading.Thread(target=self.__queue_listener__)
        self.queue_listener.start()

    def __queue_listener__(self) -> None:
        """ Listen to other processes for events happening on this node """

        logger.debug("Listening to queue...")

        while True:

            event = self.client_node_queue.get(block=True, timeout=None)

            if event is None:
                return

            elif event["type"] == QueueMessageType.NEW_NAME_NODE.value:
                self.name_node_address = event["new_name_node"]
                self.name_node_name = IP_TO_NAME_MAP.get(event["new_name_node"], event["new_name_node"])
                logger.debug("Received new Name Node: %s", self.name_node_name)

    def exposed_put(self, source_vm_name: str, str_req: str) -> str:
        """
        Writes or Updates a file in the Simple Distributed File System

        Request format:
            {
                "local_filename": str,
                "sdfs_filename": str,
                "base_dir": str
            }

        Example:
            {
                "local_filename": "illiad.txt",
                "sdfs_filename": "foo.txt",
                "base_dir": "../" or "../data"
            }
        """

        req: Dict[str, str] = json.loads(str_req)

        logger.debug("Received PUT from %s: %s", source_vm_name, req)

        local_filename: str = req["local_filename"]
        sdfs_filename: str = req["sdfs_filename"]
        base_dir: str = req["base_dir"]

        with rpyc.connect(self.name_node_address, NAME_NODE_PORT, config={"sync_request_timeout": None}) as nn_conn:

            logger.debug("Sent PUT Request to Name Node (%s) for %s", self.name_node_name, sdfs_filename)
            str_metadata = nn_conn.root.exposed_put(HOST_NAME, sdfs_filename)
            metadata = json.loads(str_metadata)
            logger.debug("Received PUT Response from Name Node (%s): %s", self.name_node_name, metadata)

            filehash: str = metadata["filehash"]
            version: int = metadata["version"]
            data_node_name: str = metadata["data_node_name"]

            if data_node_name not in NAME_TO_ID_MAP:
                logger.error("Received an invalid node from Name Node (%s): %s", self.name_node_name, metadata)
                return f"Received an invalid node from Name Node ({self.name_node_name}): {metadata}"

            raw_filename: str = get_raw_filename(sdfs_filename, version)
            local_path = f"{base_dir}/{local_filename}"
            remote_path = f"{DATA_NODE_ROOT_DIR}/{raw_filename}"

            consistent_vm_id: int = NAME_TO_ID_MAP[data_node_name]

            excluded_ids: Set[int] = set()

            vm_name = None
            success = False
            for i in range(NODE_COUNT):

                successor_id: int = get_successor_id(consistent_vm_id, self.membership_list, excluded_ids)

                if successor_id == -1:
                    logger.debug("Failed to find PUT successor for vm_id (%s): %s", consistent_vm_id, req)
                    return f"Failed to find PUT successor for vm_id ({consistent_vm_id}): {req}"

                vm_name = ID_TO_NAME_MAP[successor_id]

                try:

                    success = upload_remote_file(
                        target_vm=vm_name,
                        local_path=local_path,
                        remote_path=remote_path
                    )

                    if success:
                        logger.debug("Successfully uploaded %s to %s as %s", local_path, vm_name, remote_path)
                        break
                    else:
                        excluded_ids.add(successor_id)
                        logger.debug("Failed uploaded %s to %s as %s", local_path, vm_name, remote_path)

                except Exception as e:
                    excluded_ids.add(successor_id)
                    logger.warning("Failed to PUT file to %s with %s: %s", vm_name, req, e)

            if vm_name is None or not success:
                logger.error("Failed to PUT file to any successor: %s", req)
                return f"Failed to PUT file to any successor: {req}"

            confirm_body = {
                "filehash": filehash,
                "filename": sdfs_filename,
                "version": version,
                "data_node_name": vm_name
            }
            try:
                logger.debug("Sending CONFIRM PUT to %s: %s", self.name_node_name, confirm_body)
                nn_conn.root.exposed_confirm_put(HOST_NAME, json.dumps(confirm_body))
            except Exception as e:
                logger.error("Failed to send CONFIRM PUT to %s with %s:", self.name_node_name, confirm_body, e)
                return f"Failed to send CONFIRM PUT to Name Node ({self.name_node_name}) with {confirm_body}: {e}"

            replicate_body = {
                "filehash": filehash,
                "filename": sdfs_filename,
                "version": version,
                "origin": data_node_name,
                "ttl": REPLICA_COUNT
            }
            try:
                with rpyc.connect(vm_name, DATA_NODE_PORT, config={"sync_request_timeout": None}) as dn_conn:
                    logger.debug("Sending REPLICATION to %s: %s", vm_name, replicate_body)
                    dn_conn.root.exposed_replicate(vm_name, json.dumps(replicate_body))
            except Exception as e:
                logger.error("Failed to send REPLICATION to %s: %s", vm_name, replicate_body)
                return f"Failed to send REPLICATION to {vm_name} with {replicate_body}: {e}"

    def exposed_get(self, source_vm_name: str, str_req: str) -> Optional[str]:
        """
        Retrieves a file from the Simple Distributed File System

        Request format:
            {
                "local_filename": str,
                "sdfs_filename": str,
                "destination_dir": str
            }

        Example:
            {
                "local_filename": "illiad.txt",
                "sdfs_filename": "foo.txt",
                "destination_dir": "sdfs" or "client"
            }
        """

        req: Dict[str, str] = json.loads(str_req)

        logger.debug("Received GET from %s: %s", source_vm_name, req)

        local_filename: str = req["local_filename"]
        sdfs_filename: str = req["sdfs_filename"]
        download_dir: str = req["destination_dir"]

        with rpyc.connect(self.name_node_address, NAME_NODE_PORT, config={"sync_request_timeout": None}) as nn_conn:

            logger.debug(
                "Sent GET Request to Name Node (%s): sdfs_filename=%s",
                self.name_node_name, sdfs_filename
            )
            str_metadata = nn_conn.root.exposed_get(HOST_NAME, sdfs_filename)
            metadata = json.loads(str_metadata)
            logger.debug("Received GET Response from Name Node (%s): %s", self.name_node_name, metadata)

            filename: str = metadata["filename"]
            filehash: str = metadata["filehash"]
            version: int = metadata["version"]
            data_node_names: List[str] = metadata["data_node_names"]

            raw_filename: str = get_raw_filename(sdfs_filename, version)

            local_path = f"{download_dir}/{local_filename}"
            remote_path = f"{DATA_NODE_ROOT_DIR}/{raw_filename}"

            for data_node_name in data_node_names:

                success = download_remote_file(
                    target_vm=data_node_name,
                    local_path=local_path,
                    remote_path=remote_path
                )

                if success and download_dir == DATA_NODE_ROOT_DIR:

                    # This file is technically "replicated" to this node now
                    confirm_body = {
                        "filehash": filehash,
                        "filename": filename,
                        "version": version,
                        "data_node_name": HOST_NAME
                    }
                    try:
                        logger.debug("Sending CONFIRM GET to %s: %s", self.name_node_name, confirm_body)
                        nn_conn.root.exposed_confirm_put(HOST_NAME, json.dumps(confirm_body, indent=2))
                    except Exception as e:
                        logger.warning(
                            "Failed to send CONFIRM GET to %s with %s:",
                            self.name_node_name, confirm_body, e
                        )

                    return str_metadata

        return None

    def exposed_delete(self, source_vm_name: str,  sdfs_filename: str) -> str:

        logger.debug("Received DELETE from %s: sdfs_filename=%s", source_vm_name, sdfs_filename)

        try:
            with rpyc.connect(self.name_node_address, NAME_NODE_PORT, config={"sync_request_timeout": None}) as nn_conn:

                logger.debug(
                    "Sent DELETE Request to Name Node (%s): sdfs_filename=%s",
                    self.name_node_name, sdfs_filename
                )
                resp: str = nn_conn.root.exposed_delete(HOST_NAME, sdfs_filename)
                logger.debug("Received DELETE Response from Name Node (%s): %s", self.name_node_name, resp)

                return resp

        except Exception as e:
            logger.warning("Failed DELETE to Name Node (%s) with %s: %s", self.name_node_name, sdfs_filename, e)
            return f"Failed DELETE to Name Node ({self.name_node_name}) with {sdfs_filename}: {e}"

    def exposed_ls(self, source_vm_name: str) -> str:
        """
        Response Format:
            {
                "name_node": str,
                "data": str
            }
        """

        logger.debug("Received LS from %s", source_vm_name)

        with rpyc.connect(self.name_node_address, NAME_NODE_PORT, config={"sync_request_timeout": None}) as nn_conn:
            logger.debug(f"Sent LS Request to Name Node (%s)", self.name_node_address)
            data: str = nn_conn.root.exposed_ls(HOST_NAME)
            return json.dumps({
                "name_node": self.name_node_address,
                "data": data
            })

    def exposed_schedule_maplejuice_job(self, vm_name_source: str, task: str, str_req: str) -> int:

        logger.debug("Received task %s from %s: %s", task, vm_name_source, str_req)

        with rpyc.connect(self.name_node_address, SCHEDULER_NODE_PORT, config={"sync_request_timeout": None}) as conn:
            return conn.root.exposed_schedule_task(HOST_NAME, task, str_req)


