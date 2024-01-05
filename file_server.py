import json
import os

import rpyc
from filelock import FileLock

from utils import DATA_NODE_ROOT_DIR
from utils import FILE_NODE_PORT


def download_remote_file(target_vm: str, local_path: str, remote_path: str) -> bool:
    try:
        with rpyc.connect(target_vm, FILE_NODE_PORT, config={"sync_request_timeout": None}) as fs_conn:
            lock = FileLock(f"{local_path}.lock")
            with lock:
                with open(local_path, "wb") as local_file:
                    size = 0
                    while True:
                        remote_bytes = fs_conn.root.exposed_read_bytes(remote_path, size)
                        if not remote_bytes:
                            break
                        local_file.write(remote_bytes)
                        size += len(remote_bytes)
        return True
    except Exception as e:
        print(e)
        try:
            os.remove(local_path)
        except Exception as ex:
            print(ex)
    return False


def upload_remote_file(target_vm: str, local_path: str, remote_path: str) -> bool:
    try:
        with rpyc.connect(target_vm, FILE_NODE_PORT, config={"sync_request_timeout": None}) as fs_conn:
            with open(local_path, "rb") as local_file:
                for byte_chunk in iter(lambda: local_file.read(16_777_226), b""):
                    fs_conn.root.exposed_append_bytes(remote_path, byte_chunk)
        return True
    except Exception as e:
        print(e)
    return False


class FileService(rpyc.Service):
    def exposed_read_bytes(self, filepath: str, offset: int) -> bytes:
        with open(filepath, mode="rb") as file:
            file.seek(offset)
            return file.read(16_77_226)

    def exposed_append_bytes(self, filepath: str, byte_chunk: bytes) -> None:
        with open(filepath, mode="ab") as file:
            file.write(byte_chunk)

    def exposed_delete(self, sdfs_filename: str) -> str:
        deleted_files = []
        try:
            for raw_filename in os.listdir(DATA_NODE_ROOT_DIR):

                if raw_filename.startswith(sdfs_filename):
                    os.remove(f"{DATA_NODE_ROOT_DIR}/{raw_filename}")
                    deleted_files.append(raw_filename)

            return f"Deleted: {deleted_files}"
        except Exception as e:
            print(e)
            return f"Failed to delete {sdfs_filename}: {e}"

    def exposed_store(self) -> str:
        try:
            return json.dumps({"filenames": os.listdir(DATA_NODE_ROOT_DIR)})
        except Exception as e:
            print(e)
            return json.dumps({"filenames": []})
