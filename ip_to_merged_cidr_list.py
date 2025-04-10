import polars as pl

from ip_to_cidr_list import ip_to_cidr_list


def merge_cidr_list(input_csv: str, output_csv: str):
    mid_file = input_csv.replace(".csv", "-cidr.csv")
    ip_to_cidr_list(input_csv, mid_file)

    df = pl.read_csv(mid_file)
    df = df.select([pl.col("CIDR列表", "运营商")])
    df = df.with_columns(
        pl.col("运营商").map_elements(
            lambda s: "广电" if s.endswith("广电") else s, return_dtype=pl.String
        )
    )
    df = df.group_by(pl.col("运营商")).agg(pl.col("CIDR列表").str.join(", "))
    df.write_csv(output_csv)


if __name__ == "__main__":
    from sys import argv

    input_csv = argv[1]
    output_csv = argv[2]

    merge_cidr_list(input_csv, output_csv)
