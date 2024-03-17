import argparse
import asyncio
import aiopath
import aioshutil
import logging


parser = argparse.ArgumentParser(description="Sorting files")
parser.add_argument("--from", "-f", required=True, help="From dir")
parser.add_argument("--to", "-t", help="To dir", default="Output_dir")
args = vars(parser.parse_args())

from_dir = aiopath.AsyncPath(args["from"])
to_dir = aiopath.AsyncPath(args["to"])


async def read_folder(path: aiopath.AsyncPath):
    async for file in path.iterdir():
        if await file.is_dir():
            await read_folder(file)
        else:
            await copy_file(file)


async def copy_file(file: aiopath.AsyncPath):
    folder = to_dir / file.suffix[1:]
    try:
        await folder.mkdir(parents=True, exist_ok=True)
        await aioshutil.copyfile(file, folder / file.name)
    except OSError as e:
        logging.error(e)


if __name__ == "__main__":
    format = "%(threadName)s %(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
    asyncio.run(read_folder(from_dir))
    print(f"Files have been copied from directory '{from_dir}' to '{to_dir}'.")
