import argparse
import datetime
import json
import re
import timeit
from functools import partial
from typing import List
from typing import Match
from typing import Optional
from typing import Pattern

import rpyc
import sqlparse

from utils import HASH_PARTITION, load_schema, HOST_NAME, MAPLE_JOB, JUICE_JOB, CLIENT_NODE_PORT, HOST_IP_ADDRESS
from utils import RANGE_PARTITION


def parse_sql_joins(table1: str, table2: str, condition: str) -> Optional[dict]:

    condition_pattern: Pattern[str] = re.compile(r'\bWHERE\b\s*(\w+\.\w+)\s*=\s*(\w+\.\w+)$', re.IGNORECASE)
    match: Optional[Match[str]] = condition_pattern.search(condition)

    if match:
        left_condition: List[str] = match.group(1).split(".")
        right_condition: List[str] = match.group(2).split(".")
        table1_attribute: str = left_condition[1] if table1 == left_condition[0] else right_condition[1]
        table2_attribute: str = right_condition[1] if table2 == right_condition[0] else left_condition[1]

        return {
            "type": "joins",
            "table1": table1,
            "table1_attribute": table1_attribute,
            "table2": table2,
            "table2_attribute": table2_attribute
        }
    else:
        raise Exception("Invalid SQL: Expected condition (i.e. WHERE d1.Name = d2.Name)")


def parse_sql(sql_query):
    parsed = sqlparse.parse(sql_query)

    if parsed:
        stmt = parsed[0]

        # Extract tables and conditions
        tables = [d.replace(",", "") for t in stmt.tokens if isinstance(t, sqlparse.sql.IdentifierList) for d in t.value.split()]
        conditions = [str(t) for t in stmt.tokens if isinstance(t, sqlparse.sql.Where)]

        if len(conditions) != 1:
            print(f"Invalid SQL Statement: Expected only one condition: {conditions}")

        if len(tables) == 2:
            return parse_sql_joins(tables[0], tables[1], conditions[0])
    else:
        return None


def parse_sql_file(file_path: str) -> Optional[dict]:
    with open(file_path, 'r') as file:
        sql_query = file.read()

        if file_path.endswith(".sqlx"):
            return
        else:
            return parse_sql(sql_query)


def sql_joins(sql: dict, partition_type: str, delete_input: str, sql_joins_out_filename: str) -> None:

    sql_join_data_dir = f"sql_joins_data_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"

    d1_name: str = sql["table1"]
    d1_col_name: str = sql["table1_attribute"]

    d1_schema = load_schema(d1_name)

    if d1_schema is None:
        print(f"Schema Not Found: Schema[{d1_schema}]")
        return

    if d1_col_name not in d1_schema:
        print(f"Attribute Not Found: Attribute[{d1_col_name}] does not exist in Schema[{d1_schema}]")
        return

    d1_join_col_id = d1_schema[d1_col_name]
    d1_sdfs_intermediate_filename_prefix = f"{d1_name}-int-{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    d1_sdfs_dest_filename = f"{sql_join_data_dir}-{d1_name}"

    maple1 = json.dumps({
        "exe": "maple_sql_joins.py",
        "num_maples": 10,
        "sdfs_intermediate_filename_prefix": d1_sdfs_intermediate_filename_prefix,
        "sdfs_src_directory": d1_name,
        "partition_type": partition_type,
        "maple_cmd_args": ["-dataset", str(d1_name), "-join_col_id", str(d1_join_col_id)]
    }, indent=2)

    juice1 = json.dumps({
        "exe": "juice_identity.py",
        "num_juices": 10,
        "sdfs_intermediate_filename_prefix": d1_sdfs_intermediate_filename_prefix,
        "sdfs_dest_filename": d1_sdfs_dest_filename,
        "partition_type": partition_type,
        "juice_cmd_args": [],
        "delete_input": 1 if delete_input else 0
    }, indent=2)

    d2_name: str = sql["table2"]
    d2_col_name: str = sql["table2_attribute"]

    d2_schema = load_schema(d2_name)

    if d2_schema is None:
        print(f"Schema Not Found: Schema[{d2_schema}]")
        return

    if d2_col_name not in d2_schema:
        print(f"Attribute Not Found: Attribute[{d2_col_name}] does not exist in Schema[{d2_schema}]")
        return

    d2_join_col_id = d2_schema[d2_col_name]
    d2_sdfs_intermediate_filename_prefix = f"{d2_name}-int-{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    d2_sdfs_dest_filename = f"{sql_join_data_dir}-{d2_name}"

    maple2 = json.dumps({
        "exe": "maple_sql_joins.py",
        "num_maples": 10,
        "sdfs_intermediate_filename_prefix": d2_sdfs_intermediate_filename_prefix,
        "sdfs_src_directory": d2_name,
        "partition_type": partition_type,
        "maple_cmd_args": ["-dataset", str(d2_name), "-join_col_id", str(d2_join_col_id)]
    }, indent=2)

    juice2 = json.dumps({
        "exe": "juice_identity.py",
        "num_juices": 10,
        "sdfs_intermediate_filename_prefix": d2_sdfs_intermediate_filename_prefix,
        "sdfs_dest_filename": d2_sdfs_dest_filename,
        "partition_type": partition_type,
        "juice_cmd_args": [],
        "delete_input": 1 if delete_input else 0
    }, indent=2)

    sql_joins_sdfs_intermediate_filename_prefix = f"sql_joins-int-{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"

    maple3 = json.dumps({
        "exe": "maple_identity.py",
        "num_maples": 10,
        "sdfs_intermediate_filename_prefix": sql_joins_sdfs_intermediate_filename_prefix,
        "sdfs_src_directory": sql_join_data_dir,
        "partition_type": partition_type,
        "maple_cmd_args": []
    }, indent=2)

    juice3 = json.dumps({
        "exe": "juice_sql_joins.py",
        "num_juices": 10,
        "sdfs_intermediate_filename_prefix": sql_joins_sdfs_intermediate_filename_prefix,
        "sdfs_dest_filename": sql_joins_out_filename,
        "partition_type": partition_type,
        "juice_cmd_args": ["-d1", d1_name, "-d2", d2_name],
        "delete_input": 1 if delete_input else 0
    }, indent=2)

    with rpyc.connect(HOST_IP_ADDRESS, CLIENT_NODE_PORT, config={"sync_request_timeout": None}) as cn_conn:

        print(f"Sent MAPLE Job Request to {HOST_NAME}: {maple1}")
        resp: str = cn_conn.root.exposed_schedule_maplejuice_job(HOST_NAME, MAPLE_JOB, maple1)
        print(f"Received JUICE Job Response from {HOST_NAME}: {resp}")

        print(f"Sent JUICE Job Request to {HOST_NAME}: {juice1}")
        resp: str = cn_conn.root.exposed_schedule_maplejuice_job(HOST_NAME, JUICE_JOB, juice1)
        print(f"Received JUICE Job Response from {HOST_NAME}: {resp}")

        print(f"Sent MAPLE Job Request to {HOST_NAME}: {maple2}")
        resp: str = cn_conn.root.exposed_schedule_maplejuice_job(HOST_NAME, MAPLE_JOB, maple2)
        print(f"Received JUICE Job Response from {HOST_NAME}: {resp}")

        print(f"Sent JUICE Job Request to {HOST_NAME}: {juice2}")
        resp: str = cn_conn.root.exposed_schedule_maplejuice_job(HOST_NAME, JUICE_JOB, juice2)
        print(f"Received JUICE Job Response from {HOST_NAME}: {resp}")

        print(f"Sent MAPLE Job Request to {HOST_NAME}: {maple3}")
        resp: str = cn_conn.root.exposed_schedule_maplejuice_job(HOST_NAME, MAPLE_JOB, maple3)
        print(f"Received JUICE Job Response from {HOST_NAME}: {resp}")

        print(f"Sent JUICE Job Request to {HOST_NAME}: {juice3}")
        resp: str = cn_conn.root.exposed_schedule_maplejuice_job(HOST_NAME, JUICE_JOB, juice3)
        print(f"Received JUICE Job Response from {HOST_NAME}: {resp}")


def sql_filter(dataset: str, query: str, sdfs_dest_filename: str, partition_type: str, delete_input: str) -> None:

    sdfs_intermediate_filename_prefix = f"sql-filter-{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"

    maple_req = json.dumps({
        "exe": "maple_sql_filter.py",
        "num_maples": 10,
        "sdfs_intermediate_filename_prefix": sdfs_intermediate_filename_prefix,
        "sdfs_src_directory": dataset,
        "partition_type": partition_type,
        "maple_cmd_args": ["-regex", query],
    }, indent=2)

    juice_req = json.dumps({
        "exe": "juice_sql_filter.py",
        "num_juices": 10,
        "sdfs_intermediate_filename_prefix": sdfs_intermediate_filename_prefix,
        "sdfs_dest_filename": sdfs_dest_filename,
        "partition_type": partition_type,
        "juice_cmd_args": [],
        "delete_input": 1 if delete_input else 0
    }, indent=2)

    with rpyc.connect(HOST_IP_ADDRESS, CLIENT_NODE_PORT, config={"sync_request_timeout": None}) as cn_conn:

        print(f"Sent MAPLE Job Request to {HOST_NAME}: {maple_req}")
        resp: str = cn_conn.root.exposed_schedule_maplejuice_job(HOST_NAME, MAPLE_JOB, maple_req)
        print(f"Received JUICE Job Response from {HOST_NAME}: {resp}")

        print(f"Sent JUICE Job Request to {HOST_NAME}: {juice_req}")
        resp: str = cn_conn.root.exposed_schedule_maplejuice_job(HOST_NAME, JUICE_JOB, juice_req)
        print(f"Received JUICE Job Response from {HOST_NAME}: {resp}")


def main() -> None:

    partition_types = [HASH_PARTITION, RANGE_PARTITION]

    parser = argparse.ArgumentParser(description="Maple Juice Client")
    parser.add_argument("sql_filepath", type=str)
    parser.add_argument("-sdfs_dest_filename", "--sdfs_dest_filename", type=str)
    parser.add_argument("-partition_type", "--partition_type", choices=partition_types, type=str)
    parser.add_argument("-delete_input", "--delete_input", action="store_true")

    args = parser.parse_args()

    # Required
    sql_filepath: str = args.sql_filepath

    # Optional
    partition_type = args.partition_type or RANGE_PARTITION
    delete_input = args.delete_input or False

    if sql_filepath.endswith(".sql"):

        print("Parsing SQL...")
        sql = parse_sql_file(file_path=sql_filepath)

        if sql is None:
            print("Invalid SQL query format")
            return

        print(f"Running SQL Inner Joins Query:\n{json.dumps(sql, indent=2)}")
        sdfs_dest_filename = args.sdfs_dest_filename or f"sql_joins-out-{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
        joins = partial(
            sql_joins,
            sql=sql,
            partition_type=partition_type,
            delete_input=delete_input,
            sql_joins_out_filename=sdfs_dest_filename
        )

        print(f"Submitted in {timeit.timeit(joins, number=1)} seconds.")

    elif sql_filepath.endswith(".sqlx"):
        print("Parsing SQLX...")

        sql_query = None
        try:
            with open(sql_filepath, 'r') as file:
                sql_query = file.read()
        except Exception as e:
            print(f"Failed to extract SQLX from {sql_filepath}: {e}")
            return

        if sql_query is not None:

            pattern = r"\s*SELECT\s+\*\s+FROM\s+(\w+)\s+WHERE\s+(.+)\s*"
            match = re.search(pattern, sql_query)

            if not match:
                print(f"Invalid SQLX syntax format: {sql_query}")
                return

            print(f"Running SQL Filter on Dataset[{match.group(1)}] with RegexCondition[{match.group(2)}]")
            sdfs_dest_filename = args.sdfs_dest_filename or f"sql_filter-out-{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
            filters = partial(
                sql_filter,
                dataset=match.group(1),
                query=match.group(2),
                sdfs_dest_filename=sdfs_dest_filename,
                partition_type=partition_type,
                delete_input=delete_input
            )

            print(f"Submitted in {timeit.timeit(filters, number=1)} seconds.")


if __name__ == "__main__":
    main()
