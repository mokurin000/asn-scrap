import polars as pl

from ip_to_cidr_list import ip_to_cidr_list


def merge_cidr_list(input_csv: str, output_csv: str):
    mid_file = input_csv.replace(".csv", "-cidr.csv")
    ip_to_cidr_list(input_csv, mid_file)

    df = pl.read_csv(mid_file)
    df = df.select([pl.col("CIDR列表", "运营商")])
    df = df.with_columns(
        pl.col("运营商")
        .str.replace_all(".*广电$", "广电")
        .str.replace_all(".*大学$", "教育网")
    )

    df = df.group_by(pl.col("运营商")).agg(pl.col("CIDR列表").str.join(" "))
    df.write_csv(output_csv, include_bom=True)

    output_csv_selected = output_csv.replace(".csv", "-isp.csv")
    ISP_IN_CHINA = [
        "教育网",
        "电信",
        "联通",
        "移动",
        "广电",
        "方正宽带",
    ]
    df.filter(pl.col("运营商").is_in(ISP_IN_CHINA)).sort(
        by=pl.col("运营商").map_elements(ISP_IN_CHINA.index, return_dtype=pl.UInt32)
    ).write_csv(output_csv_selected, include_bom=True)


if __name__ == "__main__":
    from sys import argv

    input_csv = argv[1]
    output_csv = argv[2]

    merge_cidr_list(input_csv, output_csv)
