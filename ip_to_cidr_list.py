import polars as pl
from bitarray import bitarray

from ipaddress import IPv4Address


def ip_to_int(ip: str) -> int:
    return int(IPv4Address(ip))


def cidr_expand(ip_len: tuple[int, int]) -> str:
    current_ip, amount = ip_len
    amount = bitarray(f"{amount:032b}", endian="big")
    cidr_list = []
    for idx, bit in enumerate(amount, start=1):
        bit_base = 32 - idx
        if not bit:
            continue
        cidr_list.append(f"{IPv4Address(current_ip)}/{idx}")
        current_ip += idx << bit_base
    return ", ".join(cidr_list)


def ip_to_cidr_list(input_csv: str, output_csv: str):
    df = pl.read_csv(input_csv)
    df = df.with_columns(
        pl.concat_list(
            pl.col("IP开始").map_elements(ip_to_int, return_dtype=pl.UInt32),
            pl.col("IP数量"),
        )
        .map_elements(cidr_expand, return_dtype=pl.String)
        .alias("CIDR列表")
    )
    df.write_csv(output_csv)
