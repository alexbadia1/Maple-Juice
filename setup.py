from setuptools import setup, find_packages

setup(
    name="maplejuice",
    version="0.0.1",
    author="Alex Badia",
    author_email="abadia2@illinois.cedu",
    packages=find_packages(),
    py_modules=[
        "file_server",
        "main",
        "maple_juice_client",
        "maple_juice_map",
        "maple_juice_scheduler",
        "membership",
        "membership_client",
        "sdfs_client",
        "sdfs_client_node",
        "sdfs_data_node",
        "sdfs_election",
        "sdfs_name_node",
        "sql",
        "message_pb2",
        "type_hints",
        "utils"
    ],
    entry_points={
        "console_scripts": [
            "maplejuice = main:main",
            "juice = maple_juice_client:main_juice",
            "maple = maple_juice_client:main_maple",
            "sql = sql:main",
            "sdfs = sdfs_client:main",
            "membership = membership_client:main_menu"
        ],
    },
    install_requires=[
        "bcrypt==4.0.1",
        "cffi==1.15.1",
        "cryptography~=40.0.2",
        "protobuf==4.21.0",
        "pycparser==2.21",
        "PyNaCl==1.5.0",
        "cachetools~=4.2.4",
        "rpyc==5.0.1",
        "filelock==3.4.1",
        "pandas==1.1.5",
        "sqlparse==0.4.4"
    ]
)
