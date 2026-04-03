"""
Download the Kaggle airline dataset and land the raw source files in S3.

This module is importable from Airflow and also runnable as a standalone script.
"""

import argparse
import os
from pathlib import Path
import tempfile
from typing import Dict, Sequence

import boto3


DEFAULT_DATASET_REF = "bulter22/airline-data"
DEFAULT_DATASET_SUBDIR = "airline_data"


def join_s3_key(*parts: str) -> str:
    return "/".join(part.strip("/") for part in parts if part)


def raw_dataset_exists(s3_client, bucket: str, raw_prefix: str) -> bool:
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=f"{raw_prefix.rstrip('/')}/",
        MaxKeys=1,
    )
    return response.get("KeyCount", 0) > 0


def download_kaggle_to_s3_raw(
    bucket: str,
    region: str,
    raw_prefix: str,
    dataset_ref: str = DEFAULT_DATASET_REF,
    download_dir: str | None = None,
    force_refresh: bool = False,
) -> Dict[str, object]:
    s3_client = boto3.client("s3", region_name=region)

    if not force_refresh and raw_dataset_exists(s3_client, bucket, raw_prefix):
        raw_path = f"s3://{bucket}/{raw_prefix.rstrip('/')}/"
        print(f"Raw dataset already exists at {raw_path}; skipping Kaggle download.")
        return {"skipped": True, "raw_path": raw_path, "uploaded_files": 0}

    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()

    with tempfile.TemporaryDirectory(prefix="airline-raw-", dir=download_dir or None) as temp_dir:
        api.dataset_download_files(
            dataset_ref,
            path=temp_dir,
            unzip=True,
            quiet=False,
        )

        source_files = sorted(
            path
            for path in Path(temp_dir).rglob("*")
            if path.is_file()
            and (
                path.name.lower().endswith(".csv")
                or path.name.lower().endswith(".csv.shuffle")
            )
        )
        if not source_files:
            raise FileNotFoundError(
                f"No CSV source files found after downloading {dataset_ref}."
            )

        for local_file in source_files:
            s3_key = join_s3_key(raw_prefix, local_file.name)
            s3_client.upload_file(str(local_file), bucket, s3_key)

        raw_path = f"s3://{bucket}/{raw_prefix.rstrip('/')}/"
        print(
            f"Uploaded {len(source_files)} raw files from {dataset_ref} "
            f"to {raw_path}"
        )
        return {"skipped": False, "raw_path": raw_path, "uploaded_files": len(source_files)}


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download a Kaggle dataset and upload the raw files to S3."
    )
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET"))
    parser.add_argument(
        "--region",
        default=os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1")),
    )
    parser.add_argument(
        "--dataset-ref",
        default=os.getenv("KAGGLE_DATASET_REF", DEFAULT_DATASET_REF),
    )
    parser.add_argument(
        "--raw-prefix",
        default=join_s3_key(
            "raw",
            os.getenv("DATASET_SUBDIR", DEFAULT_DATASET_SUBDIR),
        ),
    )
    parser.add_argument(
        "--download-dir",
        default=os.getenv("KAGGLE_DOWNLOAD_DIR"),
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Redownload and reupload even when the raw prefix already has data.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    if not args.bucket:
        raise ValueError("S3 bucket is required. Pass --bucket or set S3_BUCKET.")

    download_kaggle_to_s3_raw(
        bucket=args.bucket,
        region=args.region,
        raw_prefix=args.raw_prefix,
        dataset_ref=args.dataset_ref,
        download_dir=args.download_dir,
        force_refresh=args.force_refresh,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(os.sys.argv[1:]))
