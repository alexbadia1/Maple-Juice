import argparse
import csv
from itertools import islice


def juice_sql_filter(input_filename: str, output_filename: str) -> None:
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
                output_rows.append([row[0], row[1]])

            csv_writer.writerows(output_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Maple Juice EXE")
    parser.add_argument("input_filename", type=str)
    parser.add_argument("output_filename", type=str)
    args = parser.parse_args()

    juice_sql_filter(args.input_filename, args.output_filename)


if __name__ == "__main__":
    main()
