import sys
import json
import time
import schedule
import pandas as pd
from os import environ, remove
from pathlib import Path
from ftplib import FTP_TLS


def get_ftp() -> FTP_TLS:
    try:
        # Get FTP details
        FTPHOST = environ["FTPHOST"]
        FTPUSER = environ["FTPUSER"]
        FTPPASS = environ["FTPPASS"]
        FTPPORT = environ["FTPPORT"]

        # Return authenticated FTP
        ftp = FTP_TLS(FTPHOST, FTPUSER, FTPPASS)
        ftp.prot_p()
        return ftp
    except KeyError as e:
        print(f"Error: {e} environment variable is not set.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: Failed to establish FTP connection - {e}")
        sys.exit(1)


def upload_to_ftp(ftp: FTP_TLS, file_source: Path):
    try:
        with open(file_source, "rb") as fp:
            ftp.storbinary(f"STOR {file_source.name}", fp)
    except Exception as e:
        print(f"Error: Failed to upload {file_source.name} to FTP - {e}")


def delete_file(file_source: Path):
    try:
        remove(file_source)
    except Exception as e:
        print(f"Error: Failed to delete {file_source.name} - {e}")


def read_csv(config: dict) -> pd.DataFrame:
    try:
        url = config["URL"]
        params = config["PARAMS"]
        return pd.read_csv(url, **params)
    except Exception as e:
        print(f"Error: Failed to read CSV from {url} - {e}")


def pipeline():
    try:
        # Load source configuration
        with open("config.json", "r") as fp:
            config = json.load(fp)

        # Get FTP connection outside the loop
        ftp = get_ftp()

        # Loop through each configuration to get the source_name and its corresponding configuration
        for source_name, source_config in config.items():
            file_name = Path(
                source_name + ".csv"
            )  # Use lowercase extension for consistency
            df = read_csv(source_config)
            if df is not None:
                df.to_csv(file_name, index=False)

                print(f"File {file_name} has been downloaded.")

                upload_to_ftp(ftp, file_name)
                print(f"File {file_name} has been uploaded to FTP.")

                delete_file(file_name)
                print(f"File {file_name} has been deleted.")
    except Exception as e:
        print(f"Error: Pipeline execution failed - {e}")


if __name__ == "__main__":
    param = sys.argv[1] if len(sys.argv) > 1 else None  # Check if argument provided

    if param == "manual":
        pipeline()
    elif param == "schedule":
        schedule.every().day.at("21:34").do(pipeline)

        while True:
            schedule.run_pending()
            time.sleep(1)
    else:
        print(
            "Invalid parameter. The parameter should be either 'manual' or 'schedule'"
        )
