import argparse
import csv
from itertools import islice


def juice(input_filename: str, output_filename: str) -> None:
    """ Expects: key \t 1 """

    with open(input_filename, "r") as input_file, open(output_filename, "w") as output_file:

        csv_reader = csv.reader(input_file)
        csv_writer = csv.writer(output_file, lineterminator='\n')

        total = 0

        while True:

            chunk = list(islice(csv_reader, 100))

            if not chunk:
                # Empty chunk is end of file
                break

            for row in chunk:
                total += int(row[1].split(",")[1])

        input_file.seek(0)

        while True:

            output_rows = []
            chunk = list(islice(csv_reader, 100))

            print(chunk)

            if not chunk:
                # Empty chunk is end of file
                break

            for row in chunk:
                tmp = row[1].split(",")
                key = tmp[0]
                value = int(tmp[1])

                if total != 0:
                    percent = value / total * 100
                    output_rows.append([key, percent])
                else:
                    output_rows.append([key, "NaN"])

            csv_writer.writerows(output_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Maple Juice EXE")
    parser.add_argument("input_file", type=str)
    parser.add_argument("output_file", type=str)
    args = parser.parse_args()

    juice(args.input_file, args.output_file)


if __name__ == "__main__":
    main()
