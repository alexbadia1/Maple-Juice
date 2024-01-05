import argparse
import shutil


def juice_identity(input_filename: str, output_filename: str) -> None:
    shutil.copyfile(input_filename, output_filename)


def main() -> None:
    parser = argparse.ArgumentParser(description="Maple Juice EXE")
    parser.add_argument("input_filename", type=str)
    parser.add_argument("output_filename", type=str)
    args = parser.parse_args()

    juice_identity(args.input_filename, args.output_filename)


if __name__ == "__main__":
    main()
