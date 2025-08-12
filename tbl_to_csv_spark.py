import os
import sys
import time
import functools
import psutil
import polars as pl
from enum import IntEnum
from tqdm import tqdm
from loguru import logger
from typing import Callable, TypeVar, ParamSpec
from argparse import ArgumentParser, Namespace, ArgumentTypeError
from pathlib import Path
import shutil
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import split, size, col
from py4j.protocol import Py4JJavaError, Py4JNetworkError, Py4JError
from pyspark.errors import PySparkRuntimeError, AnalysisException

# === Decorator for measuring runtime of functions
P = ParamSpec("P")
R = TypeVar("R")

def time_it(message: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            start: float = time.perf_counter()
            result = func(*args, **kwargs)
            duration: float = time.perf_counter() - start
            logger.info(f"{message} {duration:.2f} seconds.")
            return result
        return wrapper
    return decorator


# === Enums and Constants ===
class ExitCode(IntEnum):
    SUCCESS = 0
    ERROR = 1
    NO_INPUT_FILES = 2
    NOTHING_WRITTEN = 3

class Result(IntEnum):
    FAILURE = 0
    SUCCESS = 1

TBL_SUFFIX = ".tbl"
CSV_SUFFIX = ".csv"


# === Custom Error class ===
class SparkErrorHandler:
    @staticmethod
    def handle_initialization(e: Exception) -> None:
        if isinstance(e, (Py4JJavaError, Py4JNetworkError)):
            logger.error(f"Spark Error while initializing JVM: {e}")        
        elif isinstance(e, Py4JError):
            logger.error(f"py4j-Protocol Error: {e}")
        elif isinstance(e, RuntimeError):
            logger.error(f"Generic Error while initializing Spark: {e}")
        else:
            logger.error(f"Unexpected Error while Initializing Spark: {e}")
        
    @staticmethod
    def handle_runtime(e: Exception) -> None:
        if isinstance(e, Py4JJavaError):
            logger.error(f"Java-Error in Spark: {e}")
        elif isinstance(e, Py4JNetworkError):
            logger.error(f"Network Error to Spark JVM: {e}")
        elif isinstance(e, Py4JError):
            logger.error(f"py4j-protocol error: {e}")
        elif isinstance(e, PySparkRuntimeError):
            logger.error(f"Spark Runtime Error: {e}")
        elif isinstance(e, AnalysisException):
            logger.error(f"Spark analysis error: {e}")
        else:
            logger.error(f"Unexpected Error in Spark: {e}")



# === Helper functions
def check_ratio(ratio: float) -> None:
    if ratio <= 0:
        raise ArgumentTypeError("Ratio must be greater than 0.")
    if ratio >= 1.0:
        logger.critical(f"Ratio set to {ratio:.2f}: This may exceed available RAM and could lead to a system crash.")
    elif ratio >= 0.9:
        logger.warning(f"Ratio set to {ratio:.2f}: This may significantly impact System performance")
    elif ratio >= 0.75:
        logger.info(f"Ratio set to {ratio:.2f}: High memory usage expected")
    else:
        logger.info(f"Ratio set to {ratio:.2f}")

def format_file_size(size_in_bytes: int) -> str:
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    size = float(size_in_bytes)
    for unit in units:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"

@time_it("Files merged in")
def merge_csv_parts(tmp_dir: Path, final_file: Path) -> bool:
    """merges part-CSV-files"""
    if final_file.is_dir():
        raise IsADirectoryError(f"Expected a file, but '{final_file}' is a directory.")
    part_files = sorted(tmp_dir.glob("part-*"))
    if not part_files:
        logger.warning(f"No part-* files found in {tmp_dir}")
        return False
    total_size = sum(f.stat().st_size for f in part_files)
    logger.info(f"Merging {len(part_files)} part files into {final_file}...")
    with open(final_file, "wb") as fout:
        with tqdm(total=total_size, unit="B", unit_scale=True, desc="Merging", ncols=80) as pbar:
            for part_file in part_files:
                with open(part_file, "rb") as fin:
                    while True:
                        buf = fin.read(100 * 1024 * 1024)
                        if not buf:
                            break
                        fout.write(buf)
                        pbar.update(len(buf))
    return True

def safe_first_int(df: DataFrame) -> int:
    row: Row | None = df.first()
    if row is None:
        raise ValueError("DataFrame is empty")
    value = row[0]
    if not isinstance(value, int):
        raise TypeError(f"Expected int, got {type(value)}")
    return value

def str_for_spark(path: Path) -> str:
    """Spark requires string paths"""
    return str(path)

def get_spark_threshold(ratio: float) -> int:
    """
    provides a memory threshold based on a ratio and
    the available memory for deciding between Spark and polars 
    """
    mem = psutil.virtual_memory()
    logger.info(f"Available memory: {format_file_size(mem.available)}")
    return int(mem.available * ratio)

@time_it("Spark initialized in")
def initialize_spark() -> SparkSession|None:
    try:
        return SparkSession.builder.appName("tbl2csv_spark").getOrCreate()
    except Exception as e:
        SparkErrorHandler.handle_initialization(e)
    return None

def cleanup_spark_data(tmp_path: Path) -> None:
    try:
        shutil.rmtree(tmp_path)
    except FileNotFoundError:
        logger.warning(f"{tmp_path} already deleted.")
    except PermissionError as e:
        logger.error(f"Permission denied while deleting {tmp_path}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error while deleting {tmp_path}: {e}")

# === conversion functions ===
def convert_with_spark(
    folder: str,
    spark: SparkSession,
    tbl_name: str,
    overwrite: bool
) -> int:
    tbl_path = Path(folder) / tbl_name
    base_name: str = tbl_path.stem
    tmp_path = Path(folder) / f"{base_name}_out"
    csv_path = str_for_spark(tmp_path)
    csv_file_path = Path(folder) / (base_name + CSV_SUFFIX)
    tmp_path = Path(csv_path)

    if tmp_path.exists() and overwrite:
        cleanup_spark_data(tmp_path)

    tbl_path_str: str = str_for_spark(tbl_path)
    try:
        # 1. Datei als Text einlesen
        df_raw: DataFrame = spark.read.text(tbl_path_str)

        # 2. Anzahl der Spalten (| als Trennzeichen, letzte Spalte ist leer)
        num_cols: int = safe_first_int(df_raw.select(size(split(col("value"), r"\|")))) - 1

        # 3. Splitten und letzte Spalte ignorieren
        df = df_raw.select([
            split(df_raw['value'], r'\|').getItem(i).alias(f'col_{i}')
            for i in range(num_cols)
        ])

        # 4. Als CSV speichern
        df.write.mode("overwrite" if overwrite else "error") \
            .option("header", "false") \
            .option("delimiter", ",") \
            .csv(csv_path)
    except Exception as e:
        SparkErrorHandler.handle_runtime(e)
        logger.info(f"Cleaning up after Error")
        cleanup_spark_data(tmp_path)
        return Result.FAILURE
    
    # 5. part-*.csv -> *.csv

    logger.info("Merging temporary files...")
    success: bool = False
    try:
        success = merge_csv_parts(tmp_path, csv_file_path)
    except Exception as e:
        logger.warning(f"Failed to merge: {e}")

    # 6. Verzeichnis löschen
    logger.info("Cleaning up...")
    cleanup_spark_data(tmp_path)

    if not success:
        return Result.FAILURE
    artifacts_folder = Path("artifacts")
    if artifacts_folder.is_dir():
        shutil.rmtree("artifacts")
    return Result.SUCCESS


@time_it("File written in")
def convert_with_polars(
    folder: str,
    spark: SparkSession,
    tbl_name: str,
    csv_list: list[str],
    overwrite: bool,
    ratio: float
) -> int:
    tbl_path = Path(folder) / tbl_name
    base_name: str = tbl_path.stem
    csv_name: str = base_name + CSV_SUFFIX
    csv_path = Path(folder) / csv_name

    file_size = tbl_path.stat().st_size
    logger.info(f"Size of {tbl_name}: {format_file_size(file_size)} ({file_size} bytes)")

    # === Schwelle für Spark-Fallback ===
    SPARK_THRESHOLD: int = get_spark_threshold(ratio)
    logger.info(f"Current Spark threshold: {format_file_size(SPARK_THRESHOLD)} ({SPARK_THRESHOLD} bytes)")

    if csv_name in csv_list and not overwrite:
        logger.warning(f"{csv_name} already exists. Skipping file...")
        return Result.FAILURE
    elif csv_name in csv_list and overwrite:
        logger.warning(f"{csv_name} already exists. Overwriting file...")
    else:
        logger.info(f"Writing {csv_name}...")

    if file_size > SPARK_THRESHOLD:
        logger.info("Using Spark due to large file size...")
        return convert_with_spark(folder, spark,tbl_name,overwrite)
    else:
        logger.info("Using Polars eager mode...")
        df = pl.read_csv(
            tbl_path,
            separator="|",
            has_header=False,
            ignore_errors=True
        )
        df = df[:, :-1]
        df.write_csv(
            csv_path,
            include_header=False,
            separator=","
        )

    return Result.SUCCESS

# === main function ===

@time_it("Total runtime:")
def main(args: Namespace) -> int:
    written_files: int = 0
    overwrite: bool = args.overwrite
    folder: str = args.folder
    ratio: float = args.ratio
    try:
        check_ratio(ratio)
    except ArgumentTypeError as ate:
        logger.error(ate)
        return ExitCode.ERROR
    try:
        names: list[str] = os.listdir(folder)
    except FileNotFoundError as fnf:
        logger.error(fnf)
        return ExitCode.ERROR
    
    tbl_files: list[str] = [x for x in names if x.endswith(TBL_SUFFIX)]
    csv_files: list[str] = [x for x in names if x.endswith(CSV_SUFFIX)]

    if len(tbl_files) == 0:
        logger.warning("No tbl files found.")
        logger.info("Exiting...")
        return ExitCode.NO_INPUT_FILES
    
    spark: SparkSession|None = None
    try:
        spark = initialize_spark()
        if spark is None:
            return ExitCode.ERROR
        for tbl in tbl_files:
            written_files += convert_with_polars(folder, spark, tbl, csv_files,overwrite,ratio)
        if written_files == 0:
            return ExitCode.NOTHING_WRITTEN
        return ExitCode.SUCCESS
    finally:
        logger.info("Stopping spark...")
        if spark is not None:
            try:
                spark.stop()
            except Exception as e:
                logger.warning(f"Couldn't stop spark cleanly: {e}")

if __name__ == "__main__":
    parser = ArgumentParser(
        description="Script for turning tbl into csv using pyspark and polars",
        epilog="""Exit-Codes:
        0: Successfully written CSVs
        1: Error within Script
        2: No .tbl files found
        3: No new files written""")
    parser.add_argument("-o", "--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("-r", "--ratio", type=float, default=0.5, help="Fraction of available RAM used to determine Spark threshold (e.g. 0.5)")
    parser.add_argument("-f", "--folder", type=str, default="1GB", help="Subfolder for .tbl data")
    args: Namespace = parser.parse_args()
    sys.exit(main(args))
