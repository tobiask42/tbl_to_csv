import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
import polars as pl
from tbl_to_csv_spark import convert_with_polars, Result
from loguru import logger


@pytest.fixture
def mock_spark():
    return MagicMock

def test_convert_with_polars_SUCCESS(tmp_path: Path, mock_spark: MagicMock):
    folder:Path = tmp_path
    tbl_name:str = "test.tbl"
    tbl_path:Path = folder / tbl_name
    tbl_path.write_text("1|2|3|4|5|6|")  # einfache Dummy-Daten

    # Patch: Größe so setzen, dass Polars verwendet wird
    with patch("tbl_to_csv_spark.get_spark_threshold", return_value=1_000_000), \
         patch("polars.read_csv") as mock_read_csv:

        mock_df = MagicMock(spec=pl.DataFrame)
        mock_df.__getitem__.return_value = mock_df
        mock_read_csv.return_value = mock_df

        csv_list:list[str] = []
        result = convert_with_polars(str(folder), mock_spark, tbl_name, csv_list, overwrite=False, ratio=0.5)

        assert result == Result.SUCCESS
        mock_read_csv.assert_called_once_with(
            Path(folder) / tbl_name,
            separator="|",
            has_header=False,
            ignore_errors=True
        )
        mock_df.__getitem__.assert_called()  # Kürzen der Spalten
        mock_df.write_csv.assert_called()

def test_convert_with_polars_SUCCESS_overwrite(tmp_path: Path, mock_spark: MagicMock):
    folder:Path = tmp_path
    tbl_name:str = "test.tbl"
    csv_name: str = "test.csv"
    tbl_path: Path = folder / tbl_name
    tbl_path.write_text("1|2|3|4|5|6|")  # Dummy-Daten

    # Patch: Größe so setzen, dass Polars verwendet wird
    with patch("tbl_to_csv_spark.get_spark_threshold", return_value=1_000_000), \
         patch("polars.read_csv") as mock_read_csv:

        mock_df = MagicMock(spec=pl.DataFrame)
        mock_df.__getitem__.return_value = mock_df
        mock_read_csv.return_value = mock_df

        csv_list:list[str] = [csv_name]
        result = convert_with_polars(str(folder), mock_spark, tbl_name, csv_list, overwrite=True, ratio=0.5)

        assert result == Result.SUCCESS
        mock_read_csv.assert_called_once_with(
            Path(folder) / tbl_name,
            separator="|",
            has_header=False,
            ignore_errors=True
        )
        mock_df.__getitem__.assert_called()  # Kürzen der Spalten
        mock_df.write_csv.assert_called()

def test_convert_with_polars_FAILURE(tmp_path: Path, mock_spark: MagicMock):
    folder:Path = tmp_path
    tbl_name:str = "test.tbl"
    csv_name: str = "test.csv"
    tbl_path: Path = folder / tbl_name
    tbl_path.write_text("1|2|3|4|5|6|")  # Dummy-Daten

    # Patch: Größe so setzen, dass Polars verwendet wird
    with patch("tbl_to_csv_spark.get_spark_threshold", return_value=1_000_000), \
         patch("polars.read_csv") as mock_read_csv:

        mock_df = MagicMock(spec=pl.DataFrame)
        mock_df.__getitem__.return_value = mock_df
        mock_read_csv.return_value = mock_df

        csv_list:list[str] = [csv_name]
        result = convert_with_polars(str(folder), mock_spark, tbl_name, csv_list, overwrite=False, ratio=0.5)

        assert result == Result.FAILURE

@pytest.mark.parametrize(
    "csv_already_exists,overwrite,expected_result,expected_log_substring",
    [
        (False, False, Result.SUCCESS, "Writing test.csv..."),
        (True, False, Result.FAILURE, "already exists. Skipping"),
        (True, True, Result.SUCCESS, "already exists. Overwriting"),
    ]
)
def test_convert_with_polars_logging_varianten(tmp_path: Path, csv_already_exists: bool, overwrite: bool, expected_result: Result, expected_log_substring: str):
    folder: Path = tmp_path
    tbl_name = "test.tbl"
    base_name = Path(tbl_name).stem
    csv_name = base_name + ".csv"
    tbl_path = folder / tbl_name
    tbl_path.write_text("1|2|3|\n")

    csv_list = [csv_name] if csv_already_exists else []

    logs: list[str] = []
    def sink(msg: str):
        logs.append(msg)
    sink_id = logger.add(sink, level="INFO")

    with patch("tbl_to_csv_spark.get_spark_threshold", return_value=1_000_000), \
         patch("polars.read_csv") as mock_read_csv:

        mock_df = MagicMock(spec=pl.DataFrame)
        mock_df.__getitem__.return_value = mock_df
        mock_read_csv.return_value = mock_df

        result = convert_with_polars(str(folder), MagicMock(), tbl_name, csv_list, overwrite, ratio=0.5)

    logger.remove(sink_id)

    assert result == expected_result
    assert any(expected_log_substring in msg for msg in logs)

def test_convert_with_polars_uses_spark(tmp_path: Path):
    folder: Path = tmp_path
    tbl_name = "large.tbl"
    tbl_path = folder / tbl_name
    tbl_path.write_bytes(b"X" * 2_000_000)  # große Datei erzeugen

    with patch("tbl_to_csv_spark.get_spark_threshold", return_value=1), \
         patch("tbl_to_csv_spark.convert_with_spark") as mock_convert_spark, \
         patch("polars.read_csv") as mock_read_csv:

        mock_convert_spark.return_value = Result.SUCCESS  # wenn aufgerufen, soll es Erfolg liefern

        result = convert_with_polars(
            str(folder),
            MagicMock(),       # fake SparkSession
            tbl_name,
            csv_list=[],       # Datei ist neu
            overwrite=False,
            ratio=0.5
        )

        # prüfen, ob Spark-Pfad verwendet wurde
        mock_convert_spark.assert_called_once()
        args, _ = mock_convert_spark.call_args
        assert args[0] == str(folder)
        assert args[2] == tbl_name
        assert args[3] is False

        # sicherstellen, dass Polars nicht verwendet wurde
        mock_read_csv.assert_not_called()

        # die Funktion soll SUCCESS zurückgeben
        assert result == Result.SUCCESS
