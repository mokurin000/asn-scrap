from os import listdir

import polars as pl


def main():
    csv_files = [file for file in listdir(".") if file.endswith(".csv")]
    for csv_file in csv_files:
        pl.read_csv(csv_file).write_excel(csv_file.replace(".csv", ".xlsx"))


if __name__ == "__main__":
    main()
