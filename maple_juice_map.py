import concurrent.futures
import json
import logging
import os
import shutil
import subprocess
from typing import BinaryIO
from typing import Dict
from typing import List

import rpyc
from filelock import FileLock

from sdfs_client import get

from utils import CN_MAX_THREADS_GET
from utils import DATA_NODE_ROOT_DIR
from utils import JUICE_EXE_DIR
from utils import JUICE_EXE_OUT_DIR
from utils import JUICE_RESULT_DIR
from utils import LOG_DIR
from utils import LOG_FORMATTER
from utils import LocalMembershipListEntry
from utils import MAPLE_EXE_DIR
from utils import MAPLE_EXE_OUT_DIR
from utils import MAPLE_RESULT_DIR
from utils import NUM_MAPLE_TASKS_LIMIT
from utils import get_raw_filename


logger = logging.getLogger("Maple")
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(f"{LOG_DIR}/maple.log", "w")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(LOG_FORMATTER)
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(LOG_FORMATTER)
logger.addHandler(stream_handler)


def exe_out_filename(exe_input_filename: str) -> str:
    return f"{exe_input_filename}-out"


class MapleService(rpyc.Service):
    def __init__(self, membership_list: Dict[str, LocalMembershipListEntry]):
        self.membership_list: Dict[str, LocalMembershipListEntry] = membership_list

    def __sdfs_get__(self, sdfs_filename: str, version: int) -> None:
        """ Retrieves a file from the SDFS if the file does not exist on this node """

        try:

            raw_filename = get_raw_filename(sdfs_filename, version)

            if raw_filename in os.listdir(DATA_NODE_ROOT_DIR):
                return None

            get(
                local_filename=raw_filename,
                sdfs_filename=sdfs_filename,
                destination_dir=DATA_NODE_ROOT_DIR
            )

        except Exception as e:
            logger.error(e)

        return None

    def __merge_exe_results__(self, output_file: BinaryIO, exe_result_filename: str, exe_result_dir: str):
        """ Merges an exe result file to a single MAPLE output file or JUICE output file """

        with open(f"{exe_result_dir}/{exe_result_filename}", "rb") as exe_result_file:
            for byte_chunk in iter(lambda: exe_result_file.read(4096), b""):
                output_file.write(byte_chunk)

    def __execute__(self, input_filename: str, exe: str, cmd_args: List[str], exe_dir: str, exe_out_dir: str):
        """ Runs maple_exe on target file """

        logger.info("Executing exe (%s) on %s input file", exe, input_filename)

        filepath = f"{DATA_NODE_ROOT_DIR}/{input_filename}"

        lock = FileLock(f"{filepath}.lock")

        with lock:

            exe_out_file = exe_out_filename(input_filename)
            cmd = ["python3", f"{exe_dir}/{exe}", filepath, f"{exe_out_dir}/{exe_out_file}"]

            if any(cmd_args):
                cmd.extend(cmd_args)

            logger.debug("Running: %s", " ".join(cmd))
            exe_result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            logger.debug(exe_result.stdout.decode())
            logger.error(exe_result.stderr.decode())

            logger.debug("Finished: %s", " ".join(cmd))

    def exposed_map(self, vm_source_name: str, str_req: str) -> str:
        """
        Maple Task Request format:
        {
            "exe": str,
            "data": {
                "num_tasks": int,
                "filenames": {
                    filename1: <version [int]>,
                    filename2: <version [int]>,
                    ...
                }
            }.
            "maple_cmd_args": ["-interconne X"]
        }
        """

        req: dict = json.loads(str_req)

        logger.info("Received MAPLE Map Request from %s: %s", vm_source_name, req)

        exe: str = req["exe"]
        task_id: int = req["task_id"]
        num_tasks: int = min(req["data"]["num_tasks"], NUM_MAPLE_TASKS_LIMIT)
        input_files: Dict[str, int] = req["data"]["input_files"]
        maple_cmd_args: List[str] = req["maple_cmd_args"]

        if not any(input_files.keys()):
            return json.dumps({"filename": None, "success": True}, indent=2)

        try:

            # Clear directories
            shutil.rmtree(MAPLE_RESULT_DIR, ignore_errors=True)
            os.makedirs(MAPLE_RESULT_DIR, exist_ok=True)
            shutil.rmtree(MAPLE_EXE_OUT_DIR, ignore_errors=True)
            os.makedirs(MAPLE_EXE_OUT_DIR, exist_ok=True)

            logger.info("Retrieving missing input files...")

            with concurrent.futures.ThreadPoolExecutor(CN_MAX_THREADS_GET) as pool:
                list(
                    pool.map(
                        lambda args: self.__sdfs_get__(*args),
                        [(filename, version) for filename, version in input_files.items()]
                    )
                )
            logger.info("Retrieved missing input files")

            with concurrent.futures.ThreadPoolExecutor(num_tasks) as pool:
                list(
                    pool.map(
                        lambda args: self.__execute__(*args),
                        [
                                    (
                                        get_raw_filename(filename, version),
                                        exe,
                                        maple_cmd_args,
                                        MAPLE_EXE_DIR,
                                        MAPLE_EXE_OUT_DIR
                                    ) for filename, version in input_files.items()
                        ]
                    )
                )

            # Merge exe outputs
            result_filename = f"maple-result-{task_id}"
            with open(f"{MAPLE_RESULT_DIR}/{result_filename}", "wb") as maple_out:
                for maple_result_filename in os.listdir(MAPLE_EXE_OUT_DIR):
                    self.__merge_exe_results__(
                        maple_out,
                        maple_result_filename,
                        MAPLE_EXE_OUT_DIR
                    )

            logger.info("Finished MAPLE %s Task(s): %s", num_tasks, result_filename)

            return json.dumps({"filename": result_filename, "success": True}, indent=2)

        except Exception as e:
            logger.error(e)
            return json.dumps({"filename": None, "success": False}, indent=2)

    def exposed_reduce(self, vm_source_name: str, str_req: str) -> str:
        """
        Juice Task Request format:
        {
            "exe": str,
            "data": {
                "num_tasks": int,
                "filenames": {
                    filename1: <version [int]>,
                    filename2: <version [int]>,
                    ...
                }
            }.
            "juice_cmd_args": ["-interconne X"]
        }
        """

        req: dict = json.loads(str_req)

        logger.info("Received JUICE Map Request from %s: %s", vm_source_name, req)

        exe: str = req["exe"]
        task_id: int = req["task_id"]
        num_tasks: int = min(req["data"]["num_tasks"], NUM_MAPLE_TASKS_LIMIT)
        input_files: Dict[str, int] = req["data"]["input_files"]
        juice_cmd_args: List[str] = req["juice_cmd_args"]

        if not any(input_files.keys()):
            return json.dumps({"filename": None, "success": True}, indent=2)

        try:

            # Clear directories
            shutil.rmtree(JUICE_RESULT_DIR, ignore_errors=True)
            os.makedirs(JUICE_RESULT_DIR, exist_ok=True)
            shutil.rmtree(JUICE_EXE_OUT_DIR, ignore_errors=True)
            os.makedirs(JUICE_EXE_OUT_DIR, exist_ok=True)

            logger.info("Retrieving missing intermediate files...")

            with concurrent.futures.ThreadPoolExecutor(CN_MAX_THREADS_GET) as pool:
                list(
                    pool.map(
                        lambda args: self.__sdfs_get__(*args),
                        [(filename, version) for filename, version in input_files.items()]
                    )
                )
            logger.info("Retrieved missing input files")

            logger.info("Executing juice_exe (%s) on %s intermediate files...", exe, len(input_files))

            with concurrent.futures.ThreadPoolExecutor(num_tasks) as pool:
                list(
                    pool.map(
                        lambda args: self.__execute__(*args),
                        [
                                    (
                                        get_raw_filename(filename, version),
                                        exe,
                                        juice_cmd_args,
                                        JUICE_EXE_DIR,
                                        JUICE_EXE_OUT_DIR
                                    ) for filename, version in input_files.items()
                        ]
                    )
                )

            result_filename = f"juice-result-{task_id}"
            with open(f"{JUICE_RESULT_DIR}/{result_filename}", "wb") as juice_out:
                for juice_result_filename in os.listdir(JUICE_EXE_OUT_DIR):
                    self.__merge_exe_results__(
                        juice_out,
                        juice_result_filename,
                        JUICE_EXE_OUT_DIR
                    )

            logger.info("Finished JUICE %s Task(s): %s", num_tasks, result_filename)

            return json.dumps({"filename": result_filename, "success": True}, indent=2)

        except Exception as e:
            logger.error(e)
            return json.dumps({"filename": None, "success": False}, indent=2)
