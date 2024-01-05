import argparse
import csv
from itertools import islice


def juice(input_filename: str, output_filename: str) -> None:
    """ Expects: key \t 1 """

    with open(input_filename, "r") as input_file, open(output_filename, "w") as output_file:

        csv_reader = csv.reader(input_file)
        csv_writer = csv.writer(output_file, lineterminator='\n')

        count = 0
        key = None

        while True:

            chunk = list(islice(csv_reader, 100))

            print(chunk)

            if not chunk:
                # Empty chunk is end of file
                break

            for row in chunk:

                # Loop/Video
                key = row[0]
                count += 1

        csv_writer.writerow([key, count])


def main() -> None:
    parser = argparse.ArgumentParser(description="Maple Juice EXE")
    parser.add_argument("input_file", type=str)
    parser.add_argument("output_file", type=str)
    args = parser.parse_args()

    juice(args.input_file, args.output_file)


if __name__ == "__main__":
    main()
