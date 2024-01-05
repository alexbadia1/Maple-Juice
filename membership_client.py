import socket
from multiprocessing import Process

from utils import MembershipAdminCommand
from utils import MEMBERSHIP_ADMIN_PORT

_TARGETS = [
    "fa23-cs425-7801.cs.illinois.edu",
    "fa23-cs425-7802.cs.illinois.edu",
    "fa23-cs425-7803.cs.illinois.edu",
    "fa23-cs425-7804.cs.illinois.edu",
    "fa23-cs425-7805.cs.illinois.edu",
    "fa23-cs425-7806.cs.illinois.edu",
    "fa23-cs425-7807.cs.illinois.edu",
    "fa23-cs425-7808.cs.illinois.edu",
    "fa23-cs425-7809.cs.illinois.edu",
    "fa23-cs425-7810.cs.illinois.edu"
]
TIMEOUT = 3


def handle_command(target: str, message_type: MembershipAdminCommand) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_socket:
        udp_socket.sendto(
            message_type.value.to_bytes(1, byteorder='big'),
            (socket.gethostbyname(target), MEMBERSHIP_ADMIN_PORT)
        )


def execute_command(message_type: MembershipAdminCommand) -> None:
    target_vms = input("List the node ids are you targeting with commas in between (enter for all): ")
    try:
        if target_vms == "":
            target_vms = _TARGETS
        else:
            target_vms = [_TARGETS[int(target.strip()) - 1] for target in target_vms.split(",")]
        print(target_vms)
        confirm = input(
            f"Are these the machines are you targeting '{str(message_type).replace('MembershipAdminCommand.', '')}'? (yes/no): "
        ).lower()
        if confirm == "yes":
            processes = [Process(target=handle_command, args=(target, message_type)) for target in target_vms]
            for process in processes:
                process.start()
            for process in processes:
                process.join()
    except Exception as e:
        print(
            f"Invalid input: {target_vms}. Expected a comma separated list ranging from 1 to 10 inclusive such as: 1,"
            f"2,3,4,5"
        )


def main_menu() -> None:
    while True:
        print("\nMain Menu:")
        for message_type in MembershipAdminCommand:
            print(f"\t{message_type.value}. {str(message_type).replace('MessageType.', '')}")
        choice = input("Enter your choice ('quit' to exit menu): ")
        if choice == 'quit':
            exit()
        for message_type in MembershipAdminCommand:
            if str(message_type.value) == choice:
                execute_command(message_type)


if __name__ == "__main__":
    main_menu()
