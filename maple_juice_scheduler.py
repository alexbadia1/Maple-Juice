import concurrent.futures
import csv
import json
import logging
import multiprocessing
import os
import queue
import shutil
import socket
import threading
import timeit
from datetime import datetime
from enum import Enum
from functools import partial
from itertools import islice
from typing import Dict, TextIO, BinaryIO
from typing import List
from typing import Set
from typing import Union

import rpyc
from sdfs_client import put, delete

from file_server import download_remote_file
from type_hints import Queues
from utils import HOST_NAME, MAPLE_JOB, JUICE_JOB, HASH_PARTITION, MAPLE_INTERMEDIATE_DIR, \
    CN_MAX_THREADS_PUT, MAPLE_RESULT_DIR, MAPLE_AGGREGATE_DIR, sanitize_filename, JUICE_RESULT_DIR, JUICE_AGGREGATE_DIR, \
    JUICE_INTERMEDIATE_DIR, CN_MAX_THREADS_DELETE
from utils import IP_TO_NAME_MAP
from utils import LocalMembershipListEntry
from utils import LOG_DIR
from utils import LOG_FORMATTER
from utils import MAPLE_NODE_PORT
from utils import NAME_NODE_PORT
from utils import QueueMessageType
from utils import SCHEDULER_QUEUE


logger = logging.getLogger("Scheduler")
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(f"{LOG_DIR}/scheduler.log", "w")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(LOG_FORMATTER)
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(LOG_FORMATTER)
logger.addHandler(stream_handler)


class MapleJuiceTaskStatus(Enum):
    READY = 0
    IN_PROGRESS = 1
    SUCCESS = 2
    FAILED = 3


class MapleJuiceTask:
    def __init__(
            self,
            task_id: int,
            status: MapleJuiceTaskStatus,
            assigned_vm_name: str,
            request: dict = None
    ) -> None:
        self.task_id: int = task_id
        self.status: MapleJuiceTaskStatus = status
        self.assigned_vm_name = assigned_vm_name
        self.request: dict = request or {}
        self.result_file: Union[str, None] = None


class MapleJuiceJob:
    def __init__(self) -> None:
        self.job_id: str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.tasks: Dict[int, MapleJuiceTask] = {}


# TODO: Make singleton!
class SchedulerService(rpyc.Service):
    def __init__(
            self, membership_list: Dict[str, LocalMembershipListEntry], initial_leader: str, queues: Queues
    ) -> None:

        # Current leader in cluster (may or may not be this node)
        self.leader: str = socket.gethostbyname(initial_leader)

        # Leader's VM name for logging clarity
        self.leader_name: str = IP_TO_NAME_MAP.get(self.leader, self.leader)

        # Potential members to distribute MAPLE Tasks and JUICE Tasks too
        self.membership_list: Dict[str, LocalMembershipListEntry] = membership_list

        # Allows the Membership Service to notify Scheduler of new leaders
        self.scheduler_queue: multiprocessing.Queue = queues[SCHEDULER_QUEUE]

        # Re-distributed failed map tasks to other nodes
        self.busy_vm_names: Set[str] = set()

        # Only 1 MAPLE Job or JUICE Job at a time
        self.job_queue: queue.Queue = queue.Queue()
        self.ready: threading.Event = threading.Event()
        self.ready.set()

        # Current MAPLE Job
        self.maple_task_count: int = 0
        self.juice_task_count: int = 0
        self.current_job: Union[MapleJuiceJob, None] = None

        # Aggregation
        self.open_intermediate_files: Dict[str, Dict[str, Union[TextIO, any]]] = {}

        (threading.Thread(target=self.__queue_listener__)).start()
        (threading.Thread(target=self.__worker__)).start()

    def __queue_listener__(self) -> None:
        """ Listen for new leaders """

        logger.debug("Listening to queue...")

        while True:

            event: dict = self.scheduler_queue.get(block=True, timeout=None)

            if event is None:
                return

            if event["type"] == QueueMessageType.NEW_NAME_NODE.value:
                self.leader = event["new_name_node"]
                self.leader_name = IP_TO_NAME_MAP.get(self.leader, self.leader)
                logger.debug("Received new Name Node: %s", IP_TO_NAME_MAP.get(self.leader, self.leader))

    def __sdfs_dir__(self, sdfs_directory: str) -> Dict[str, Dict[str, Union[str, int]]]:
        """ Retrieves all filenames in a directory from the Simple Distributed File System """

        resp: Union[str, None] = None

        try:
            with rpyc.connect(self.leader, NAME_NODE_PORT, config={"sync_request_timeout": None}) as nn_conn:

                logger.debug("Sent DIR Request to Name Node (%s): %s:", self.leader_name, sdfs_directory)
                resp = nn_conn.root.exposed_dir(HOST_NAME, sdfs_directory)
                logger.debug("Received DIR Response from Name Node (%s):\n%s", self.leader_name, resp)

                return json.loads(resp)

        except json.JSONDecodeError as e:
            logger.error("Failed to parse DIR Response from Name Node (%s) because %s: %s", self.leader_name, e, resp)

        except Exception as ex:
            logger.error("Failed DIR Request to Name Node (%s) because %s: %s", self.leader_name, ex, sdfs_directory)

        return {}

    def __partition_tasks__(self, partitions: List[dict], num_nodes: int, num_maples: int) -> None:

        maples_per_node = num_maples // num_nodes

        for p in partitions:
            p.update({"num_tasks": maples_per_node})

        remainder = num_maples % num_nodes

        for i in range(remainder):
            partitions[i].update({"num_tasks": partitions[i]["num_tasks"] + 1})

    def __hash_partition_tasks__(self, partitions: List[dict], sdfs_input_files: dict, num_nodes: int) -> None:
        for filename, details in sdfs_input_files.items():
            relative_node_id = int(details["filehash"], 16) % num_nodes
            partitions[relative_node_id]["input_files"].update({filename: sdfs_input_files[filename]["version"]})

    def __range_partition_tasks__(self, partitions: List[dict], sdfs_input_files: dict, num_nodes: int) -> None:

        num_filenames = len(sdfs_input_files)

        # Keys per node without counting the remainder
        min_keys_per_node = num_filenames // num_nodes
        num_filenames_per_node_lookup = [min_keys_per_node] * num_nodes

        # Spread remainder among nodes
        remainder = num_filenames % num_nodes
        for i in range(remainder):
            num_filenames_per_node_lookup[i] += 1

        sorted_filenames = sorted(sdfs_input_files.keys())

        current_index = 0
        filename_partitions = []
        for num_filenames_per_node in num_filenames_per_node_lookup:
            filename_partitions.append(sorted_filenames[current_index:current_index + num_filenames_per_node])
            current_index += num_filenames_per_node

        for i, filename_partition in enumerate(filename_partitions):
            for filename in filename_partition:
                partitions[i]["input_files"].update({filename: sdfs_input_files[filename]["version"]})

    def __send_maple_task_request__(self, maple_task: MapleJuiceTask) -> None:

        str_resp: Union[str, None] = None

        try:
            with rpyc.connect(maple_task.assigned_vm_name, MAPLE_NODE_PORT, config={"sync_request_timeout": None}) as mn_conn:

                logger.debug("Sent MAPLE Map Request to %s: %s", maple_task.assigned_vm_name, maple_task.request)
                str_resp: str = mn_conn.root.exposed_map(HOST_NAME, json.dumps(maple_task.request, indent=2))
                logger.debug("Received MAPLE Map Response from %s: %s", maple_task.assigned_vm_name, str_resp)

        except Exception as e:
            logger.debug(
                "MAPLE Map Request %s to %s failed: %s",
                maple_task.request, maple_task.assigned_vm_name, e
            )
            self.__reschedule_maple_task__(maple_task)
            return

        resp = json.loads(str_resp)
        result_filename = resp["filename"]
        if resp["success"] and result_filename is None:
            logger.debug("MAPLE Task has no output from %s", maple_task.assigned_vm_name)
            self.busy_vm_names.discard(maple_task.assigned_vm_name)
            return

        # Download map results
        logger.debug("Downloading MAPLE Task result from %s: %s", maple_task.assigned_vm_name, result_filename)
        success = download_remote_file(
            target_vm=maple_task.assigned_vm_name,
            local_path=f"{MAPLE_AGGREGATE_DIR}/{result_filename}",
            remote_path=f"{MAPLE_RESULT_DIR}/{result_filename}"
        )

        if not success:
            logger.error(
                "Failed to download MAPLE Task result from %s: %s",
                maple_task.assigned_vm_name, result_filename
            )
            self.__reschedule_maple_task__(maple_task)
            return

        logger.debug(
            "Successfully downloaded MAPLE output file from %s: %s",
            maple_task.assigned_vm_name, result_filename
        )
        self.busy_vm_names.discard(maple_task.assigned_vm_name)

    def __reschedule_maple_task__(self, maple_task: MapleJuiceTask):
        # Re-assign (infinite loop occurs if all node's failed. In that case, this node would've crashed too)
        while True:
            vm_names: List[str] = [
                IP_TO_NAME_MAP.get(entry.member.address, entry.member.address) for entry in
                self.membership_list.values()
            ]

            free_vm_name = next((vm_name for vm_name in vm_names if vm_name not in self.busy_vm_names), None)

            if free_vm_name is not None:

                # In theory a node will eventually be free.
                maple_task.assigned_vm_name = free_vm_name

                logger.info("Re-assigning MAPLE Task to %s: %s", maple_task.assigned_vm_name, maple_task.request)
                self.busy_vm_names.add(maple_task.assigned_vm_name)
                self.__send_maple_task_request__(maple_task)

                return

    def __append_to_intermediate_files__(self, result_filename: str, prefix: str) -> None:

        aggregate_filepath = f"{MAPLE_AGGREGATE_DIR}/{result_filename}"
        logger.debug("Aggregating %s to %s", result_filename, aggregate_filepath)

        with open(aggregate_filepath, "r") as file:

            csv_reader = csv.reader(file)

            while True:

                chunk = list(islice(csv_reader, 100))

                if not chunk:
                    # Empty chunk is end of file
                    break

                for row in chunk:

                    key = row[0]

                    clean_filename = sanitize_filename(f"{prefix}-{key}")
                    filename = f"{MAPLE_INTERMEDIATE_DIR}/{clean_filename}"

                    if filename not in self.open_intermediate_files:
                        new_text_io = open(filename, "a")
                        new_csv_writer = csv.writer(new_text_io)
                        self.open_intermediate_files.update({
                            filename: {
                                "text_io": new_text_io,
                                "csv_writer": new_csv_writer
                            }
                        })

                    self.open_intermediate_files[filename]["csv_writer"].writerow(row)

    def __maple__(self, req: dict) -> None:
        """
        Request format:
            {
                "exe": str,
                "num_maples": int,
                "sdfs_intermediate_filename_prefix": str,
                "sdfs_src_directory": str
            }
        """

        exe: str = req["exe"]
        num_maples: int = req["num_maples"]
        sdfs_src_directory: str = req["sdfs_src_directory"]
        prefix: str = req["sdfs_intermediate_filename_prefix"]
        partition_type: str = req["partition_type"]
        maple_cmd_args: List[str] = req["maple_cmd_args"]

        self.current_job = MapleJuiceJob()

        if num_maples <= 0:
            logger.error("Expected num_maples > 0, but got num_maples = %s", num_maples)
            return

        # 1. Map membership list to VM Names (i.e. "fa23-cs425-7801.cs.illinois.edu") for logging clarity
        vm_names: List[str] = [
            IP_TO_NAME_MAP.get(entry.member.address, entry.member.address) for entry in self.membership_list.values()
        ]

        if len(vm_names) <= 0:
            logger.error("No nodes in the membership list to schedule MAPLE Tasks: %s", vm_names)
            return

        # 2. Sort so the list index becomes the relative "id" for the node
        vm_names.sort()
        logger.info("Schedule based on snapshot of membership list: %s", json.dumps(vm_names, indent=2))

        # 3. Get input files from sdfs_src_directory (Requested from the Name Node):
        #
        #      {
        #        filename: {
        #          "filehash": str,
        #          "version": int
        #        },
        #        ...
        #      }
        #
        sdfs_input_files: Dict[str, Dict[str, Union[str, int]]] = self.__sdfs_dir__(sdfs_src_directory)

        if not any(sdfs_input_files.keys()):
            logger.error("No input files found in sdfs_src_directory: %s", sdfs_src_directory)
            return

        # 4. Partition input files among nodes (Use all nodes if num_maples exceeds the number of available nodes)
        num_nodes = min(num_maples, len(vm_names))

        #  "partitions" is a list where index "i" has the input files assigned to node with relative id "i":
        #    [
        #      {
        #        "num_tasks": int,
        #        "filenames": {
        #          filename1: int,
        #          filename2: int
        #        }
        #      },
        #      ...
        #    ]
        #
        partitions: List[Dict[str, Union[int, Dict[str, int]]]] = [{"input_files": {}} for _ in range(num_nodes)]
        self.__partition_tasks__(partitions, num_nodes, num_maples)

        if partition_type == HASH_PARTITION:
            self.__hash_partition_tasks__(partitions, sdfs_input_files, num_nodes)
            logger.debug("Hash Partitions:\n%s", json.dumps(partitions, indent=2))
        else:
            self.__range_partition_tasks__(partitions, sdfs_input_files, num_nodes)
            logger.debug("Range Partitions:\n%s", json.dumps(partitions, indent=2))

        # 5. Distribute MAPLE Tasks to nodes
        target_vm_names: List[str] = vm_names[:num_nodes]
        for i in range(num_nodes):
            maple_task = MapleJuiceTask(
                    task_id=self.maple_task_count,
                    status=MapleJuiceTaskStatus.READY,
                    assigned_vm_name=target_vm_names[i],
                    request={
                        "exe": exe,
                        "task_id": self.maple_task_count,
                        "data": partitions[i],
                        "maple_cmd_args": maple_cmd_args
                    }
                )
            self.current_job.tasks.update({self.maple_task_count: maple_task})
            self.maple_task_count += 1

        logger.info("Sending MAPLE Tasks targets: %s", json.dumps(target_vm_names, indent=2))
        self.busy_vm_names.update(target_vm_names)
        with concurrent.futures.ThreadPoolExecutor(num_nodes) as pool:
            list(pool.map(self.__send_maple_task_request__, self.current_job.tasks.values()))

        if not any(os.listdir(MAPLE_AGGREGATE_DIR)):
            logger.error("No output files found in MAPLE_AGGREGATE_DIR: %s", MAPLE_AGGREGATE_DIR)
            return

        # 6. Aggregate
        for maple_result_file in os.listdir(MAPLE_AGGREGATE_DIR):
            self.__append_to_intermediate_files__(maple_result_file, prefix)

        for entry in self.open_intermediate_files.values():
            entry["text_io"].close()

        if not any(os.listdir(MAPLE_INTERMEDIATE_DIR)):
            logger.error("No aggregate files found in MAPLE_INTERMEDIATE_DIR: %s", MAPLE_INTERMEDIATE_DIR)
            return

        # 7. Upload
        maple_intermediate_files = os.listdir(MAPLE_INTERMEDIATE_DIR)
        logger.info("Uploading MAPLE intermediate files: %s", json.dumps(maple_intermediate_files, indent=2))
        with concurrent.futures.ThreadPoolExecutor(CN_MAX_THREADS_PUT) as pool:
            list(
                pool.map(
                    lambda args: put(*args),
                    [(f, f, MAPLE_INTERMEDIATE_DIR) for f in maple_intermediate_files]
                )
            )

        return

    def __send_juice_task_request__(self, juice_task: MapleJuiceTask) -> None:

        str_resp: Union[str, None] = None

        try:
            with rpyc.connect(juice_task.assigned_vm_name, MAPLE_NODE_PORT, config={"sync_request_timeout": None}) as mn_conn:
                logger.debug("Sent JUICE Reduce Request to %s: %s", juice_task.assigned_vm_name, juice_task.request)
                str_resp: str = mn_conn.root.exposed_reduce(HOST_NAME, json.dumps(juice_task.request, indent=2))
                logger.debug("Received JUICE Reduce Response from %s: %s", juice_task.assigned_vm_name, str_resp)
        except Exception as e:
            logger.debug(
                "JUICE Reduce Request %s to %s failed: %s",
                juice_task.request, juice_task.assigned_vm_name, e
            )
            self.__reschedule_juice_task__(juice_task)
            return

        resp = json.loads(str_resp)
        result_filename = resp["filename"]
        if resp["success"] and result_filename is None:
            logger.debug("JUICE Task has no output from %s", juice_task.assigned_vm_name)
            self.busy_vm_names.discard(juice_task.assigned_vm_name)
            return

        # Download map results
        logger.debug(
            "Downloading JUICE Task output from %s: %s",
            juice_task.assigned_vm_name, result_filename
        )
        success = download_remote_file(
            target_vm=juice_task.assigned_vm_name,
            local_path=f"{JUICE_AGGREGATE_DIR}/{result_filename}",
            remote_path=f"{JUICE_RESULT_DIR}/{result_filename}"
        )

        if not success:
            logger.error(
                "Failed to download JUICE Task result from %s: %s",
                juice_task.assigned_vm_name, result_filename
            )
            self.__reschedule_juice_task__(juice_task)
            return

        logger.debug(
            "Successfully downloaded JUICE output from %s: %s",
            juice_task.assigned_vm_name, result_filename
        )
        self.busy_vm_names.discard(juice_task.assigned_vm_name)

    def __reschedule_juice_task__(self, juice_task: MapleJuiceTask):

        # Re-assign (infinite loop occurs if all node's failed. In that case, this node would've crashed too)
        while True:
            vm_names: List[str] = [
                IP_TO_NAME_MAP.get(entry.member.address, entry.member.address) for entry in
                self.membership_list.values()
            ]

            free_vm_name = next((vm_name for vm_name in vm_names if vm_name not in self.busy_vm_names), None)

            if free_vm_name is not None:

                # In theory a node will eventually be free.
                juice_task.assigned_vm_name = free_vm_name

                logger.info("Re-assigning JUICE Task to %s: %s", juice_task.assigned_vm_name, juice_task.request)
                self.busy_vm_names.add(juice_task.assigned_vm_name)
                self.__send_juice_task_request__(juice_task)

                return

    def __merge_output_files__(self, result_filename: str, sdfs_dest_file: BinaryIO) -> None:

        logger.info("Merging JUICE results for %s...", result_filename)

        with open(f"{JUICE_AGGREGATE_DIR}/{result_filename}", "rb") as file:
            for byte_chunk in iter(lambda: file.read(4096), b""):
                sdfs_dest_file.write(byte_chunk)

    def __juice__(self, req: dict) -> str:
        """
        Request format:
            {
                "exe": str,
                "num_juices": int,
                "sdfs_intermediate_filename_prefix": str,
                "sdfs_dest_filename": str,
                "delete_input": int,
                "juice_cmd_args": List[str],
                "partition_type": str
            }
        """

        exe: str = req["exe"]
        num_juices: int = req["num_juices"]
        sdfs_dest_filename: str = req["sdfs_dest_filename"]
        prefix: str = req["sdfs_intermediate_filename_prefix"]
        partition_type: str = req["partition_type"]
        juice_cmd_args: List[str] = req["juice_cmd_args"]
        delete_input: int = req["delete_input"]

        self.current_job = MapleJuiceJob()

        if num_juices <= 0:
            return f"Expected num_juices > 0, but got num_juices = {num_juices}"

        # 1. Map membership list to VM Names (i.e. "fa23-cs425-7801.cs.illinois.edu") for logging clarity
        vm_names: List[str] = [
            IP_TO_NAME_MAP.get(entry.member.address, entry.member.address) for entry in self.membership_list.values()
        ]

        if len(vm_names) <= 0:
            return f"No nodes from the membership list to schedule JUICE tasks: {vm_names}"

        # 2. Sort so the list index becomes the relative "id" for the node
        vm_names.sort()

        # 3. Get input files from sdfs_intermediate_filename_prefix (Requested from the Name Node):
        #
        #      {
        #        filename: {
        #          "filehash": str,
        #          "version": int
        #        },
        #        ...
        #      }
        #
        sdfs_input_files: Dict[str, Dict[str, Union[str, int]]] = self.__sdfs_dir__(prefix)

        if not any(sdfs_input_files.keys()):
            return f"No input files found in sdfs_intermediate_filename_prefix: {prefix}"

        # 4. Partition input files among nodes (Use all nodes if num_maples exceeds the number of available nodes)
        num_nodes = min(num_juices, len(vm_names))

        #  "partitions" is a list where index "i" has the input files assigned to node with relative id "i":
        #    [
        #      {
        #        "num_tasks": int,
        #        "filenames": {
        #          filename1: int,
        #          filename2: int
        #        }
        #      },
        #      ...
        #    ]
        #
        partitions: List[Dict[str, Union[int, Dict[str, int]]]] = [{"input_files": {}} for _ in range(num_nodes)]
        self.__partition_tasks__(partitions, num_nodes, num_juices)

        if partition_type == HASH_PARTITION:
            self.__hash_partition_tasks__(partitions, sdfs_input_files, num_nodes)
            logger.info("Hash Partitions:\n%s", json.dumps(partitions, indent=2))
        else:
            self.__range_partition_tasks__(partitions, sdfs_input_files, num_nodes)
            logger.info("Range Partitions:\n%s", json.dumps(partitions, indent=2))

        # 5. Distribute JUICE Tasks to nodes
        target_vm_names: List[str] = vm_names[:num_nodes]
        for i in range(num_nodes):
            maple_task = MapleJuiceTask(
                    task_id=self.juice_task_count,
                    status=MapleJuiceTaskStatus.READY,
                    assigned_vm_name=target_vm_names[i],
                    request={
                        "exe": exe,
                        "task_id": self.juice_task_count,
                        "data": partitions[i],
                        "juice_cmd_args": juice_cmd_args
                    }
                )
            self.current_job.tasks.update({self.juice_task_count: maple_task})
            self.juice_task_count += 1

        logger.info("Sending JUICE Tasks targets %s", target_vm_names)
        self.busy_vm_names.update(target_vm_names)
        with concurrent.futures.ThreadPoolExecutor(num_nodes) as pool:
            list(pool.map(self.__send_juice_task_request__, self.current_job.tasks.values()))

        if not any(os.listdir(JUICE_AGGREGATE_DIR)):
            return f"No juice-out files found in JUICE_RESULT_DIR: {JUICE_AGGREGATE_DIR}"

        # 6. Aggregate
        with open(f"{JUICE_INTERMEDIATE_DIR}/{sdfs_dest_filename}", "wb") as sdfs_dest_file:
            for filename in os.listdir(JUICE_AGGREGATE_DIR):
                self.__merge_output_files__(filename, sdfs_dest_file)

        merged_juice_files = os.listdir(JUICE_INTERMEDIATE_DIR)
        if sdfs_dest_filename not in merged_juice_files:
            return f"File Not Found! {sdfs_dest_filename} not in JUICE_INTERMEDIATE_DIR: {merged_juice_files}"

        # 7. Upload
        put(sdfs_dest_filename, sdfs_dest_filename, JUICE_INTERMEDIATE_DIR)

        # 8. Delete [Optional]
        map_intermediate_files = sdfs_input_files.keys()
        try:
            if delete_input == 1:
                with concurrent.futures.ThreadPoolExecutor(CN_MAX_THREADS_DELETE) as pool:
                    list(pool.map(delete, [filename for filename in map_intermediate_files]))
        except Exception as e:
            logger.warning("JUICE failed to delete intermediate files %s: %s", e, map_intermediate_files)

        return "Finished Maple Job"

    def __worker__(self):
        """ Executes 1 MAPLE Job or JUICE Job at a time """

        while True:

            event: Dict[str, Union[str, dict]] = self.job_queue.get(block=True, timeout=None)

            # Wait for current MAPLE Job or JUICE Job to complete
            self.ready.wait()

            # Prevent other MAPLE Jobs or JUICE Jobs to complete
            self.ready.clear()

            if event["task"] == MAPLE_JOB:

                # Clear directories
                shutil.rmtree(MAPLE_INTERMEDIATE_DIR, ignore_errors=True)
                os.makedirs(MAPLE_INTERMEDIATE_DIR, exist_ok=True)
                shutil.rmtree(MAPLE_AGGREGATE_DIR, ignore_errors=True)
                os.makedirs(MAPLE_AGGREGATE_DIR, exist_ok=True)
                shutil.rmtree(MAPLE_RESULT_DIR, ignore_errors=True)
                os.makedirs(MAPLE_RESULT_DIR, exist_ok=True)

                req = event["req"]
                logger.info("Started MAPLE job: %s", req)
                job_time = timeit.timeit(partial(self.__maple__, req), number=1)
                logger.info("Finished MAPLE Job in %s seconds", job_time)

                # Cleanup for next task
                self.open_intermediate_files.clear()
                self.current_job = None

                # Allow another task to begin
                self.ready.set()

            elif event["task"] == JUICE_JOB:

                # Clear directories
                shutil.rmtree(JUICE_INTERMEDIATE_DIR, ignore_errors=True)
                os.makedirs(JUICE_INTERMEDIATE_DIR, exist_ok=True)
                shutil.rmtree(JUICE_AGGREGATE_DIR, ignore_errors=True)
                os.makedirs(JUICE_AGGREGATE_DIR, exist_ok=True)
                shutil.rmtree(JUICE_RESULT_DIR, ignore_errors=True)
                os.makedirs(JUICE_RESULT_DIR, exist_ok=True)

                req = event["req"]
                logger.info("Started JUICE job: %s", req)
                job_time = timeit.timeit(partial(self.__juice__, req), number=1)
                logger.info("Finished JUICE Job in %s seconds", job_time)

                # Cleanup for next task
                self.current_job = None

                # Allow another task to begin
                self.ready.set()

    def exposed_schedule_task(self, vm_name_source: str, task: str, str_req: str) -> int:
        """ Schedules MAPLE Job or JUICE Job in Job Queue """

        try:

            req = json.loads(str_req)
            logger.info("Received %s Job from %s: %s", task, vm_name_source, req)

            self.job_queue.put({"task": task, "req": req})
            position = self.job_queue.qsize()
            logger.info("Queued %s Job from %s: %s", task, vm_name_source, position)

            return position

        except Exception as e:
            logger.error("Failed to schedule %s Job from %s with %s: %s", task, vm_name_source, str_req, e)

