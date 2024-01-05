import argparse
import csv
import re
from itertools import islice


def maple1(input_filename: str, output_filename: str, interconne_type: str) -> None:

    pattern = re.compile(r"^$")

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

                detection_ = row[9]
                interconne = row[10]

                if interconne == interconne_type:
                    output_rows.append([detection_, 1])

            csv_writer.writerows(output_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Maple Juice EXE")
    parser.add_argument("input_filename", type=str)
    parser.add_argument("output_filename", type=str)
    parser.add_argument("-interconne", "--interconne", type=str)
    args = parser.parse_args()

    maple1(args.input_filename, args.output_filename, args.interconne)


if __name__ == "__main__":
    main()
