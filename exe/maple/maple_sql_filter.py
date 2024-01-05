import argparse
import csv
import re
from itertools import islice


def maple_sql_filter(input_filename: str, output_filename: str, regex: str) -> None:

    with open(input_filename, "r") as input_file, open(output_filename, "w") as output_file:

        csv_reader = csv.reader(input_file)
        csv_writer = csv.writer(output_file, lineterminator='\n')

        while True:

            output_rows = []
            chunk = list(islice(csv_reader, 100))

            print(chunk)

            if not chunk:
                # Empty chunk is end of file
                break

            for row in chunk:
                output = ",".join(row)
                if re.search(regex, output):
                    csv_writer.writerow([1, output])

            csv_writer.writerows(output_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Maple Juice EXE")
    parser.add_argument("input_filename", type=str)
    parser.add_argument("output_filename", type=str)
    parser.add_argument("-regex", "--regex", type=str)
    args = parser.parse_args()

    maple_sql_filter(args.input_filename, args.output_filename, args.regex)


if __name__ == "__main__":
    main()
