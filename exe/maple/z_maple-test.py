import subprocess


if __name__ == '__main__':

    # Percent Composition (Map 1 Test)
    # cmd = ["python", f"maple1.py", f"maple1-test-in.txt", 'maple1-test-out.txt', '-interconne', "Fiber/Radio"]
    # cmd = ["python", f"maple1.py", f"maple1-test-in.txt", 'maple1-test-out.txt', '-interconne', "None"]
    # cmd = ["python", f"maple1.py", f"maple1-test-in.txt", 'maple1-test-out.txt', '-interconne', "Radio"]
    # cmd = ["python", f"maple1.py", f"maple1-test-in.txt", 'maple1-test-out.txt', '-interconne', "(BLANK)"]
    # cmd = ["python", f"maple1.py", f"maple1-test-in.txt", 'maple1-test-out.txt', '-interconne', "Fiber"]

    # Percent Composition (Map 2 Test)
    # cmd = ["python", f"maple2.py", f"maple2-test-in.txt", 'maple2-test-out.txt']

    # SQL Filter (Map 1: Simple Regex)
    # cmd = [
    #     "python", "maple_sql_filter.py", "maple_sql_filter-test-in.txt", "maple_sql_filter-test-out-champaign.txt",
    #     "-regex", r"^Champaign"
    # ]

    # SQL Filter (Map 1: Complex Regex)
    cmd = [
        "python", "maple_sql_filter.py", "maple_sql_filter-test-in.txt", "maple_sql_filter-test-out.txt",
        "-regex", r"Video|Radio"
    ]

    # Maple Identity
    # cmd = ["python", "maple_identity.py", "maple_identity-test-in.txt", "maple_identity-test-out.txt"]

    # SQL Joins (Map 1 Test)
    # cmd = [
    #     "python", "maple_sql_joins.py", "maple_sql_joins-test-in.txt", "maple_sql_joins-test-out.txt",
    #     "-dataset", "pokestats", "-join_col_id", "0"
    # ]

    # Run
    exe_result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(exe_result.stdout.decode())
    print(exe_result.stderr)
