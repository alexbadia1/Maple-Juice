import csv
import json

if __name__ == "__main__":

    dataset = "demo"

    with open(f"{dataset}.csv", 'r') as file, open(f"schema-{dataset}.json", 'w') as json_file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)
        json_file.write(json.dumps({value: int(index) for index, value in enumerate(header)}, indent=2))
