import argparse
import csv
from itertools import islice


def juice_sql_joins(input_filename: str, output_filename: str, d1: str, d2: str) -> None:

    with open(input_filename, "r") as input_file, open(output_filename, "w") as output_file:

        csv_reader = csv.reader(input_file)
        csv_writer = csv.writer(output_file, lineterminator='\n')

        while True:

            chunk = list(islice(csv_reader, 100))

            if not chunk:
                # Empty chunk is end of file
                break

            d1_records = [r[1].split(",")[1:] for r in chunk if r[1].split(",")[0] == d1]
            d2_records = [r[1].split(",")[1:] for r in chunk if r[1].split(",")[0] == d2]

            # join_key, "dataset,col1,col2,col3"
            for d1_row in d1_records:
                print(d1_row)
                d1_record = ",".join(d1_row[1:])
                for d2_row in d2_records:
                    print(d2_row)
                    d2_record = ",".join(d2_row[1:])
                    csv_writer.writerow([d1_row[0], ",".join([d1_record, d2_record])])


def main() -> None:
    parser = argparse.ArgumentParser(description="Maple Juice EXE")
    parser.add_argument("input_filename", type=str)
    parser.add_argument("output_filename", type=str)
    parser.add_argument("-d1", "--d1", type=str)
    parser.add_argument("-d2", "--d2", type=str)
    args = parser.parse_args()

    juice_sql_joins(
        input_filename=args.input_filename,
        output_filename=args.output_filename,
        d1=args.d1,
        d2=args.d2,
    )


if __name__ == "__main__":
    main()
