import asyncio
import aiohttp
import aiofiles
from aiolimiter import AsyncLimiter
import os
import pandas as pd


async def _main():
    df = pd.read_csv("data.csv")
    if not "path" in df.columns:
        df["path"] = ""
    df["path"] = df["path"].fillna("")

    timeout = aiohttp.ClientTimeout(total=10)
    connector = aiohttp.TCPConnector(limit=10)
    os.makedirs("articles", exist_ok=True)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = []
        for _, row in df.iterrows():
            path = row.get("path")
            link = row.get("link")
            if path:
                continue
            if not link:
                continue
            tasks.append(asyncio.create_task(_fetch_and_write(session, link, df)))
        results = await asyncio.gather(*tasks)

    df.to_csv("data.csv", index=False)
    new_successful = sum(results)
    new_failed = len(results) - new_successful
    total = len(df)

    print(
        f"Total articles: {total}, Added: {new_successful}, Failed to fetch: {new_failed}"
    )


async def fetch_and_write_all(urls: list[str]):
    timeout = aiohttp.ClientTimeout(total=10)
    connector = aiohttp.TCPConnector(limit=10)
    limiter = AsyncLimiter(10, 3)
    os.makedirs("articles", exist_ok=True)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = [
            asyncio.create_task(fetch_and_write(session, url, limiter)) for url in urls
        ]
        results = await asyncio.gather(*tasks)
    return results


async def fetch_and_write(
    session: aiohttp.ClientSession, url: str, limiter: AsyncLimiter, verbose=False
):
    filepath = os.path.join("articles", "".join(url.split("/")[-3:]))
    if not filepath.endswith(".html"):
        filepath += ".html"
    try:
        async with limiter:
            async with session.get(url) as response:
                async with aiofiles.open(filepath, "w") as f:
                    await f.write(await response.text())
                if verbose:
                    print(f"Fetched and saved: {filepath}")
                return filepath
    except Exception as e:
        if verbose:
            print(f"failed to fetch {filepath} at {url} error:{e}")
        return None


async def _fetch_and_write(session: aiohttp.ClientSession, url: str, df: pd.DataFrame):
    filename = os.path.join("articles", url.split("/")[-1])
    if not filename.endswith(".html"):
        filename += ".html"
    try:
        async with session.get(url) as response:
            async with aiofiles.open(filename, "w") as f:
                await f.write(await response.text())
            df.loc[df["link"] == url, "path"] = filename
            print(f"successfully fetched and wrote: {filename}")
            return True
    except Exception as e:
        print(f"failed {filename} at {url} error:{e}")
        return False


if __name__ == "__main__":
    asyncio.run(_main())
