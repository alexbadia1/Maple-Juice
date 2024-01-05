import argparse
import concurrent.futures
import json
import os
import socket
import timeit
from functools import partial
from typing import Optional

import rpyc

from utils import CLIENT_NODE_PORT, CN_MAX_THREADS_PUT, FILE_NODE_PORT, DATA_NODE_ROOT_DIR
from utils import CLIENT_ROOT_DIR
from utils import HOST_NAME
from utils import HOST_IP_ADDRESS
from utils import ID_TO_NAME_MAP


def put(local_filename: str, sdfs_filename: str, base_dir: str = DATA_NODE_ROOT_DIR) -> None:

    req = {"local_filename": local_filename, "sdfs_filename": sdfs_filename, "base_dir": base_dir}

    with rpyc.connect(HOST_IP_ADDRESS, CLIENT_NODE_PORT, config={"sync_request_timeout": None}) as cn_conn:

        print(f"Sent PUT Request to Client Node ({HOST_NAME}): {req}")
        resp: str = cn_conn.root.exposed_put(HOST_NAME, json.dumps(req))
        print(f"Received PUT Response to Client Node ({HOST_NAME}): {resp}")


def put_dir(directory: str) -> None:

    filenames = os.listdir(directory)
    print("Uploading %s files to SDFS...", len(filenames))

    with concurrent.futures.ThreadPoolExecutor(CN_MAX_THREADS_PUT) as pool:
        list(
            pool.map(
                lambda args: put(*args),
                [(filename, filename, directory) for filename in filenames]
            )
        )


def get(local_filename: str, sdfs_filename: str, destination_dir: str = CLIENT_ROOT_DIR) -> Optional[str]:

    req = {"local_filename": local_filename, "sdfs_filename": sdfs_filename, "destination_dir": destination_dir}

    with rpyc.connect(HOST_IP_ADDRESS, CLIENT_NODE_PORT, config={"sync_request_timeout": None}) as cn_conn:

        print(f"Sent GET Request to Client Node ({HOST_NAME}): {req}")
        resp: Optional[str] = cn_conn.root.exposed_get(HOST_NAME, json.dumps(req))
        print(f"Received GET Response to Client Node ({HOST_NAME}): {resp}")

        return resp


def delete(sdfs_filename: str) -> None:

    with rpyc.connect(HOST_IP_ADDRESS, CLIENT_NODE_PORT, config={"sync_request_timeout": None}) as cn_conn:

        print(f"Sent DELETE Request to Client Node ({HOST_NAME}): {sdfs_filename}")
        resp: str = cn_conn.root.exposed_delete(HOST_NAME, sdfs_filename)
        print(f"Received DELETE Response to Client Node ({HOST_NAME}): {resp}")


def store(vm_id: int) -> None:
    """ Retrieves the Data Node's File Table """

    data_node_name = ID_TO_NAME_MAP[vm_id]
    data_node_ip = socket.gethostbyname(data_node_name)

    with rpyc.connect(data_node_ip, FILE_NODE_PORT, config={"sync_request_timeout": None}) as fs_conn:

        print(f"Sent STORED Request to Data Node ({data_node_name})")
        str_resp: str = fs_conn.root.exposed_store()
        resp: dict = json.loads(str_resp)
        print(f"Data Node ({data_node_name}) \n{'-' * 80} \n{resp['filenames']}")


def ls() -> None:
    """ Retrieves the Name Node's File Table """

    with rpyc.connect(HOST_IP_ADDRESS, CLIENT_NODE_PORT, config={"sync_request_timeout": None}) as cn_conn:

        print(f"Sent LS Request to Client Node ({HOST_IP_ADDRESS})")
        str_json: str = cn_conn.root.exposed_ls(HOST_NAME)
        data = json.loads(str_json)
        print(f"Name Node ({data['name_node']}) \n{'-' * 80} \n{data['data']}")


def main() -> None:

    actions = ["put", "get", "delete", "store", "ls", "put-dir"]
    vm_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    parser = argparse.ArgumentParser(description="SDFS Client")
    parser.add_argument("action", type=str, choices=actions)
    parser.add_argument("-dn", type=int, choices=vm_ids, help="Defaults to 10.")
    parser.add_argument("-local_filename", type=str, help=f"")
    parser.add_argument("-sdfs_filename", type=str, help=f"")
    parser.add_argument("-base_dir", type=str, help=f"")
    parser.add_argument("-dir", type=str, help=f"")

    args = parser.parse_args()

    if args.action == "put-dir":

        directory = args.dir or None

        if directory is None:
            raise Exception("Expected -directory <string> in command: sdfs put-dir")

        pd = partial(put_dir, directory)
        print(f"\n\n\nFinished PUT in {timeit.timeit(pd, number=1)} seconds.")

    elif args.action == "put":
        base_dir = args.base_dir or DATA_NODE_ROOT_DIR

        local_filename = args.local_filename or None
        sdfs_filename = args.sdfs_filename or None

        if local_filename is None:
            raise Exception("Expected -local_filename <string> in command: sdfs put")

        elif sdfs_filename is None:
            raise Exception("Expected -sdfs_filename <string> in command: sdfs put")

        p = partial(
            put,
            local_filename=local_filename,
            sdfs_filename=sdfs_filename,
            base_dir=base_dir
        )
        print(f"\n\n\nFinished PUT in {timeit.timeit(p, number=1)} seconds.")

    elif args.action == "get":

        sdfs_filename = args.sdfs_filename or None
        local_filename = args.local_filename or None

        if sdfs_filename is None:
            raise Exception("Expected -sdfs_filename <string> in command: sdfs get")

        elif local_filename is None:
            raise Exception("Expected -local_filename <string> in command: sdfs get")

        g = partial(
            get,
            local_filename=local_filename,
            sdfs_filename=sdfs_filename,
        )

        print(f"\n\n\nFinished GET in {timeit.timeit(g, number=1)} seconds.")

    elif args.action == "delete":

        sdfs_filename = args.sdfs_filename or None

        if sdfs_filename is None:
            raise Exception("Expected -sdfs_filename <string> in command: sdfs delete")

        d = partial(delete, sdfs_filename)

        print(f"\n\n\nFinished DELETE in {timeit.timeit(d, number=1)} seconds.")

    elif args.action == "store":

        s = partial(store, args.dn or 10)
        print(f"\n\n\nFinished STORE in {timeit.timeit(s, number=1)} seconds.")

    elif args.action == "ls":
        print(f"\n\n\nFinished LS in {timeit.timeit(ls, number=1)} seconds.")
