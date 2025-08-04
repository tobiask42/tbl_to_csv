import os
import sys
import time
import functools
from enum import IntEnum
from loguru import logger
from typing import Callable, TypeVar, ParamSpec
from argparse import ArgumentParser, Namespace

P = ParamSpec("P")
R = TypeVar("R")

def time_it(message: str) -> Callable[[Callable[P,R]],Callable[P,R]]:
    def decorator(func: Callable[P,R]) -> Callable[P,R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            start: float = time.perf_counter()
            result = func(*args, **kwargs)
            duration: float = time.perf_counter() - start
            logger.info(f"{message} {duration:.2f} seconds.")
            return result
        return wrapper
    return decorator

class ExitCode(IntEnum):
    SUCCESS = 0
    ERROR = 1
    NO_INPUT_FILES = 2
    NOTHING_WRITTEN = 3

class Result(IntEnum):
    FAILURE = 0
    SUCCESS = 1


TBL_SUFFIX: str = ".tbl"
CSV_SUFFIX: str = ".csv"

@time_it("File written in")
def convert_to_csv(tbl_name:str,csv_list:list[str],overwrite:bool) -> int:
    base_name, _ = os.path.splitext(tbl_name)
    base_name: str
    csv_name: str = base_name + CSV_SUFFIX
    if csv_name in csv_list and not overwrite:
        logger.info(f"{csv_name} already exists. Skipping file...")
        return Result.FAILURE
    elif csv_name in csv_list and overwrite:
        logger.warning(f"{csv_name} already exists. Overwriting file...")
    else:
        print(f"Writing {csv_name}...")
    with open(tbl_name, 'r') as tbl_file, open(csv_name, 'w') as csv_file:
        for line in tbl_file:
            csv_line: str = line[:-2].replace('|', ',') + "\n"
            csv_file.write(csv_line)
    return Result.SUCCESS

@time_it("Total runtime:")
def main(args: Namespace) -> int:
    written_files: int = 0
    overwrite: bool = args.overwrite
    names: list[str] = os.listdir()
    tbl: list[str] = [x for x in names if x.endswith(TBL_SUFFIX)]
    csv: list[str] = [x for x in names if x.endswith(CSV_SUFFIX)]
    if len(tbl) == 0:
        logger.warning("No tbl files found.")
        logger.info("Exiting...")
        return ExitCode.NO_INPUT_FILES
    for tbl_name in tbl:
        written_files += convert_to_csv(tbl_name,csv,overwrite)
    if written_files == 0:
        return ExitCode.NOTHING_WRITTEN
    return ExitCode.SUCCESS

if __name__ == '__main__':
    parser = ArgumentParser(
        description="Script for turning tbl into csv for PostgreSQL",
        epilog="""Exit-Codes:
        0: Successfully written CSVs
        2: No .tbl files found
        3: No new files written""")
    parser.add_argument("-o", "--overwrite", action="store_true", help="Overwrite existing files")
    args: Namespace = parser.parse_args()
    sys.exit(main(args))