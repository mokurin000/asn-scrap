import asyncio
from io import StringIO

import pandas as pd
import polars as pl
from aiohttp import ClientSession

from ip_to_cidr_list import ip_to_cidr_list


async def fetch_data(session: ClientSession):
    results = []
    for page in range(1, 1000):
        print(f"fetching page: {page}")
        async with session.get(f"/ip-block?page={page}") as resp:
            s = await resp.text()
            tables = pd.read_html(StringIO(s))
            df = pl.from_pandas(tables.pop())
            if not len(df):
                break
            results.append(df)

    return results


async def main():
    async with ClientSession("https://www.22tool.com/") as session:
        results = await fetch_data(session)

    result: pl.DataFrame = pl.concat(results)
    result.write_csv("output.csv")
    ip_to_cidr_list("output.csv", "output-cidr.csv")


if __name__ == "__main__":
    asyncio.run(main())
