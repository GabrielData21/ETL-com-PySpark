"""
Extract Module: Raw data ingestion into the Landing Zone.
Downloads the official INEP microdata or stages sample data,
extracting the CSV to the Landing Zone directory.
"""
import shutil
import ssl
import sys
import urllib.request
import zipfile
from pathlib import Path
from typing import Optional

from config.settings import (
    LANDING_DIR,
    SAMPLE_DATA_PATH,
    INEP_ENEM_2020_URL,
)
from src.common.logger import get_logger

logger = get_logger("Extract-LandingZone")

# Optional progress bar support via tqdm
try:
    from tqdm import tqdm
except ImportError:
    class DummyProgressBar:
        def __init__(self, *args, **kwargs): pass
        def update(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
    tqdm = DummyProgressBar


def stage_sample_data(
    sample_source: Path = SAMPLE_DATA_PATH,
    target_landing_dir: Path = LANDING_DIR / "enem_2020",
) -> Path:
    """
    Copies the sample CSV to the Landing Zone.
    Allows running the full pipeline locally without downloading ~3GB.
    """
    target_landing_dir.mkdir(parents=True, exist_ok=True)
    destination_file = target_landing_dir / "MICRODADOS_ENEM_2020.csv"

    if not sample_source.exists():
        raise FileNotFoundError(f"Sample file not found: {sample_source}")

    logger.info(f"Copying sample file to Landing Zone: {destination_file}")
    shutil.copyfile(sample_source, destination_file)
    logger.info(f"Sample file loaded successfully in Landing Zone: {destination_file}")
    return destination_file


def download_enem_zip(
    url: str = INEP_ENEM_2020_URL,
    dest_path: Optional[Path] = None,
    verify_ssl: bool = False,
) -> Path:
    """
    Downloads the official ENEM microdata ZIP file with streaming support.
    """
    if dest_path is None:
        dest_path = LANDING_DIR / "microdados_enem_2020.zip"

    dest_path.parent.mkdir(parents=True, exist_ok=True)

    if dest_path.exists() and dest_path.stat().st_size > 0:
        logger.info(f"ZIP file already exists in Landing Zone: {dest_path}. Skipping download.")
        return dest_path

    logger.info(f"Starting download of ENEM microdata from: {url}")
    logger.warning("Note: The full dataset is ~3 GB and may take several minutes.")

    # SSL context (INEP certificates sometimes have incomplete chains)
    ctx = ssl.create_default_context()
    if not verify_ssl:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, context=ctx) as response, open(dest_path, "wb") as out_file:
        total_size = int(response.headers.get("content-length", 0))
        block_size = 1024 * 1024  # 1MB chunk
        downloaded = 0

        with tqdm(
            desc="Downloading ENEM 2020",
            total=total_size,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
        ) as progress_bar:
            while True:
                buffer = response.read(block_size)
                if not buffer:
                    break
                out_file.write(buffer)
                downloaded += len(buffer)
                progress_bar.update(len(buffer))

    logger.info(f"Download completed successfully: {dest_path}")
    return dest_path


def extract_enem_csv(
    zip_path: Path,
    target_dir: Path = LANDING_DIR / "enem_2020",
    remove_zip: bool = False,
) -> Path:
    """
    Extracts the main MICRODADOS_ENEM_2020.csv file from the ZIP into the Landing Zone.
    """
    target_dir.mkdir(parents=True, exist_ok=True)
    destination_csv = target_dir / "MICRODADOS_ENEM_2020.csv"

    logger.info(f"Extracting {zip_path}...")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        csv_member = None
        for member in zip_ref.namelist():
            if member.upper().endswith("MICRODADOS_ENEM_2020.CSV"):
                csv_member = member
                break

        if not csv_member:
            raise ValueError(
                f"File MICRODADOS_ENEM_2020.csv not found inside {zip_path}"
            )

        logger.info(f"Extracting {csv_member} to {destination_csv}...")
        with zip_ref.open(csv_member) as source_stream, open(destination_csv, "wb") as target_stream:
            shutil.copyfileobj(source_stream, target_stream)

    logger.info(f"Extraction completed successfully: {destination_csv}")

    if remove_zip and zip_path.exists():
        logger.info(f"Removing temporary ZIP file: {zip_path}")
        zip_path.unlink()

    return destination_csv


def extract_pipeline(use_sample: bool = False, remove_zip: bool = False) -> Path:
    """
    Executes the extraction step to populate the Landing Zone.

    Args:
        use_sample: If True, uses the sample dataset (fast and offline).
        remove_zip: If True, deletes the downloaded ZIP file after extraction.

    Returns:
        Path to the CSV file ready in the Landing Zone.
    """
    logger.info(f"=== STARTING EXTRACTION PHASE (LANDING ZONE) [Sample Mode: {use_sample}] ===")
    if use_sample:
        csv_path = stage_sample_data()
    else:
        zip_path = download_enem_zip()
        csv_path = extract_enem_csv(zip_path, remove_zip=remove_zip)

    logger.info(f"Extraction phase finished. File ready in Landing Zone: {csv_path}")
    return csv_path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extract ENEM data to the Landing Zone.")
    parser.add_argument("--sample", action="store_true", help="Use local sample dataset.")
    parser.add_argument("--remove-zip", action="store_true", help="Remove ZIP after extraction.")
    args = parser.parse_args()

    extract_pipeline(use_sample=args.sample, remove_zip=args.remove_zip)
