import argparse
import csv
from itertools import islice


def maple_sql_filter(input_filename: str, output_filename: str, dataset: str, join_col_id: int) -> None:

    with open(input_filename, "r") as input_file, open(output_filename, "w") as output_file:

        csv_reader = csv.reader(input_file)
        csv_writer = csv.writer(output_file, lineterminator='\n')

        while True:

            output_rows = []
            chunk = list(islice(csv_reader, 100))

            if not chunk:
                # Empty chunk is end of file
                break

            for row in chunk:
                output_rows.append([row[join_col_id],  f"{dataset},{','.join(row)}"])

            csv_writer.writerows(output_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Maple Juice EXE")
    parser.add_argument("input_filename", type=str)
    parser.add_argument("output_filename", type=str)
    parser.add_argument("-dataset", "--dataset", type=str)
    parser.add_argument("-join_col_id", "--join_col_id", type=int)
    args = parser.parse_args()

    maple_sql_filter(
        input_filename=args.input_filename,
        output_filename=args.output_filename,
        dataset=args.dataset,
        join_col_id=args.join_col_id
    )


if __name__ == "__main__":
    main()
