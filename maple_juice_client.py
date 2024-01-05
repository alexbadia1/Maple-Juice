import argparse
import json
import timeit
from functools import partial
from typing import List

import rpyc

from utils import HOST_NAME, CLIENT_NODE_PORT, MAPLE_JOB, HOST_IP_ADDRESS, RANGE_PARTITION, HASH_PARTITION, JUICE_JOB


def maple(
        maple_exe: str,
        num_maples: int,
        sdfs_intermediate_filename_prefix: str,
        sdfs_src_directory: str,
        partition_type: str,
        maple_cmd_args: List[str]
) -> None:

    split_cmd_args = []
    if any(maple_cmd_args):
        for maple_cmd_arg in maple_cmd_args:
            split_cmd_args.extend(maple_cmd_arg.split(" "))

    str_req = json.dumps({
        "exe": maple_exe,
        "num_maples": num_maples,
        "sdfs_intermediate_filename_prefix": sdfs_intermediate_filename_prefix,
        "sdfs_src_directory": sdfs_src_directory,
        "partition_type": partition_type,
        "maple_cmd_args": split_cmd_args,
    }, indent=2)

    with rpyc.connect(HOST_IP_ADDRESS, CLIENT_NODE_PORT, config={"sync_request_timeout": None}) as cn_conn:

        print(f"Sent MAPLE JOB Request to {HOST_NAME}: {str_req}")
        resp: str = cn_conn.root.exposed_schedule_maplejuice_job(HOST_NAME, MAPLE_JOB, str_req)
        print(f"Received MAPLE JOB Response from {HOST_NAME}: {resp}")


def main_maple() -> None:

    partition_types = [HASH_PARTITION, RANGE_PARTITION]

    parser = argparse.ArgumentParser(description="Maple Juice Client")
    parser.add_argument("-maple_exe", type=str)
    parser.add_argument("-num_maples", type=int, help="Defaults to 10.")
    parser.add_argument("-sdfs_intermediate_filename_prefix", type=str)
    parser.add_argument("-sdfs_src_directory", type=str)
    parser.add_argument("-partition_type", choices=partition_types, type=str)
    parser.add_argument("-maple_cmd_args", nargs="*", type=str)

    args = parser.parse_args()

    maple_exe = args.maple_exe or None
    num_maples = args.num_maples or 10
    sdfs_intermediate_filename_prefix = args.sdfs_intermediate_filename_prefix or None
    sdfs_src_directory = args.sdfs_src_directory or None
    partition_type = args.partition_type or RANGE_PARTITION
    maple_cmd_args = args.maple_cmd_args or []

    if maple_exe is None:
        raise Exception("Expected -maple_exe <string> in command: maple")

    elif sdfs_src_directory is None:
        raise Exception("Expected -sdfs_src_directory <string> in command: maple")

    elif sdfs_intermediate_filename_prefix is None:
        raise Exception("Expected -sdfs_intermediate_filename_prefix <string> in command: maple")

    m = partial(
        maple,
        maple_exe=maple_exe,
        num_maples=num_maples,
        sdfs_intermediate_filename_prefix=sdfs_intermediate_filename_prefix,
        sdfs_src_directory=sdfs_src_directory,
        partition_type=partition_type,
        maple_cmd_args=maple_cmd_args
    )

    print(f"Finished in {timeit.timeit(m, number=1)} seconds.")


def juice(
        juice_exe: str,
        num_juices: int,
        sdfs_intermediate_filename_prefix: str,
        sdfs_dest_filename: str,
        partition_type: str,
        juice_cmd_args: List[str],
        delete_input: bool
) -> None:

    split_cmd_args = []
    if any(juice_cmd_args):
        for juice_cmd_arg in juice_cmd_args:
            split_cmd_args.extend(juice_cmd_arg.split(" "))

    str_req = json.dumps({
        "exe": juice_exe,
        "num_juices": num_juices,
        "sdfs_intermediate_filename_prefix": sdfs_intermediate_filename_prefix,
        "sdfs_dest_filename": sdfs_dest_filename,
        "partition_type": partition_type,
        "juice_cmd_args": split_cmd_args,
        "delete_input": 1 if delete_input else 0
    }, indent=2)

    with rpyc.connect(HOST_IP_ADDRESS, CLIENT_NODE_PORT, config={"sync_request_timeout": None}) as cn_conn:

        print(f"Sent JUICE JOB Request to {HOST_NAME}: {str_req}")
        resp: str = cn_conn.root.exposed_schedule_maplejuice_job(HOST_NAME, JUICE_JOB, str_req)
        print(f"Received JUICE JOB Response from {HOST_NAME}: {resp}")


def main_juice() -> None:

    partition_types = [HASH_PARTITION, RANGE_PARTITION]

    parser = argparse.ArgumentParser(description="Maple Juice Client")
    parser.add_argument("-juice_exe", type=str)
    parser.add_argument("-num_juices", type=int, help="Defaults to 10.")
    parser.add_argument("-sdfs_intermediate_filename_prefix", type=str)
    parser.add_argument("-sdfs_dest_filename", type=str)
    parser.add_argument("-partition_type", choices=partition_types, type=str)
    parser.add_argument("-juice_cmd_args", nargs="*", type=str)
    parser.add_argument("-delete_input", "--delete_input", action="store_true")

    args = parser.parse_args()

    juice_exe = args.juice_exe or None
    num_juices = args.num_juices or 10
    sdfs_intermediate_filename_prefix = args.sdfs_intermediate_filename_prefix or None
    sdfs_dest_filename = args.sdfs_dest_filename or None
    partition_type = args.partition_type or RANGE_PARTITION
    juice_cmd_args = args.juice_cmd_args or []
    delete_input = args.delete_input or False

    if juice_exe is None:
        raise Exception("Expected -juice_exe <string> in command: juice")

    elif sdfs_dest_filename is None:
        raise Exception("Expected -sdfs_dest_filename <string> in command: juice")

    elif sdfs_intermediate_filename_prefix is None:
        raise Exception("Expected -sdfs_intermediate_filename_prefix <string> in command: juice")

    j = partial(
        juice,
        juice_exe=juice_exe,
        num_juices=num_juices,
        sdfs_intermediate_filename_prefix=sdfs_intermediate_filename_prefix,
        sdfs_dest_filename=sdfs_dest_filename,
        partition_type=partition_type,
        juice_cmd_args=juice_cmd_args,
        delete_input=delete_input
    )

    print(f"Finished in {timeit.timeit(j, number=1)} seconds.")
