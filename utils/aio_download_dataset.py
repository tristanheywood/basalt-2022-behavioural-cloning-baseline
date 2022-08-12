# A script to download OpenAI contractor data or BASALT datasets
# using the shared .json files (index file).
#
# Json files are in format:
# {"basedir": <prefix>, "relpaths": [<relpath>, ...]}
#
# The script will download all files in the relpaths list,
# or maximum of set number of demonstrations,
# to a specified directory.

import argparse
import asyncio
import math
import os

import aiofiles
import aiohttp

parser = argparse.ArgumentParser(description="Download OpenAI contractor datasets")
parser.add_argument(
    "--json-file", type=str, required=True, help="Path to the index .json file"
)
parser.add_argument(
    "--output-dir", type=str, required=True, help="Path to the output directory"
)
parser.add_argument(
    "--num-demos",
    type=int,
    default=None,
    help="Maximum number of demonstrations to download",
)


async def download(
    basedir: str, relpath: str, output_dir: str, session: aiohttp.ClientSession
):
    url = basedir + relpath
    filename = os.path.basename(relpath)
    outpath = os.path.join(output_dir, filename)

    async with session.get(url) as response:
        async with aiofiles.open(outpath, "wb") as f:
            await f.write(await response.read())

    jsonl_url = url.replace(".mp4", ".jsonl")
    jsonl_filename = filename.replace(".mp4", ".jsonl")
    jsonl_outpath = os.path.join(output_dir, jsonl_filename)

    async with session.get(jsonl_url) as response:
        async with aiofiles.open(jsonl_outpath, "wb") as f:
            await f.write(await response.read())


async def main():
    args = parser.parse_args()

    with open(args.json_file, "r") as f:
        data = f.read()

    data = eval(data)
    basedir = data["basedir"]
    relpaths = data["relpaths"]
    if args.num_demos is not None:
        relpaths = relpaths[: args.num_demos]

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    # Colab kills the cell if we try to download too many simultaneously.
    BLOCK_SIZE = 100
    for i in range(math.ceil(len(relpaths) / BLOCK_SIZE)):

        block = relpaths[i * BLOCK_SIZE : min(len(relpaths), (i + 1) * BLOCK_SIZE)]

        async with aiohttp.ClientSession() as session:
            await asyncio.gather(
                *[
                    download(basedir, relpath, args.output_dir, session)
                    for relpath in block
                ],
            )

        print(f"Downloaded blocks {i+1} of {math.ceil(len(relpaths) / BLOCK_SIZE)}")


if __name__ == "__main__":
    asyncio.run(main())
