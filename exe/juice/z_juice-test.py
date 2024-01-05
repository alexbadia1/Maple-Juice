import subprocess


if __name__ == '__main__':

    # Percent Composition (Juice 1)
    # cmd = ["python", "juice1.py", "juice1-test-in.txt", "juice1-test-out.txt"]

    # Percent Composition (Juice 2)
    # cmd = ["python", "juice2.py", "juice2-test-in.txt", "juice2-test-out.txt"]

    # SQL Filter
    # cmd = ["python", "juice_sql_filter.py", "juice_sql_filter-test-in.txt", "juice_sql_filter-test-out.txt"]

    # Juice Identity
    # cmd = ["python", "juice_identity.py", "juice_identity-test-in.txt", "juice_identity-test-out.txt"]

    # SQL Joins
    cmd = [
        "python", "juice_sql_joins.py", "juice_sql_joins-test-in.txt", "juice_sql_joins-test-out.txt",
        "-d1", "pokestats", "-d2", "pokegen"
    ]

    # Run
    exe_result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(exe_result.stdout.decode())
    print(exe_result.stderr)
