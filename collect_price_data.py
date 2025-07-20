import os
import logging
import zipfile
import requests
import pandas as pd


class PriceDataDownloader:
    def __init__(self, state: str, year: int):
        self.state = state.upper()
        self.year = year
        self.temp_dir = os.path.join(os.getcwd(), "temp", str(year))
        self.output_filename = f"{self.state}_{self.year}.csv"
        self.output_path = os.path.join(self.temp_dir, self.output_filename)
        os.makedirs(self.temp_dir, exist_ok=True)
        logging.debug(f"Download and extraction directory: {self.temp_dir}")

    def generate_urls(self) -> list[str]:
        cutoff_year = 2024
        cutoff_month = 7
        urls = []

        for month in range(1, 13):
            yyyymm = f"{self.year}{month:02d}010000"
            if self.year < cutoff_year or (self.year == cutoff_year and month <= cutoff_month):
                url = (
                    f"https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{self.year}/"
                    f"MMSDM_{self.year}_{month:02d}/MMSDM_Historical_Data_SQLLoader/DATA/"
                    f"PUBLIC_DVD_DISPATCHPRICE_{yyyymm}.zip"
                )
            else:
                url = (
                    f"https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{self.year}/"
                    f"MMSDM_{self.year}_{month:02d}/MMSDM_Historical_Data_SQLLoader/DATA/"
                    f"PUBLIC_ARCHIVE%23DISPATCHPRICE%23FILE01%23{yyyymm}.zip"
                )
            urls.append(url)
        return urls

    def download_zips(self, urls: list[str]) -> None:
        for url in urls:
            zip_filename = os.path.join(self.temp_dir, os.path.basename(url))
            logging.info(f"Downloading: {url}")
            try:
                if not os.path.exists(zip_filename):
                    response = requests.get(url, timeout=30)
                    response.raise_for_status()
                    with open(zip_filename, "wb") as f:
                        f.write(response.content)
                else:
                    logging.debug(f"ZIP already exists: {zip_filename}")
            except requests.RequestException as e:
                logging.warning(f"Download failed: {url} — {e}")

    def extract_zips(self) -> None:
        for item in os.listdir(self.temp_dir):
            if item.endswith(".zip"):
                file_path = os.path.join(self.temp_dir, item)
                try:
                    with zipfile.ZipFile(file_path, 'r') as zip_ref:
                        zip_ref.extractall(self.temp_dir)
                    logging.info(f"Extracted: {file_path}")
                except zipfile.BadZipFile as e:
                    logging.error(f"Invalid ZIP file: {item} — {e}")

    def delete_zips(self) -> None:
        deleted = 0
        for item in os.listdir(self.temp_dir):
            if item.lower().endswith(".zip"):
                try:
                    os.remove(os.path.join(self.temp_dir, item))
                    deleted += 1
                except Exception as e:
                    logging.warning(f"Could not delete ZIP: {item} — {e}")
        logging.info(f"Deleted {deleted} ZIP files")

    def load_csvs(self) -> list[pd.DataFrame]:
        all_dfs = []
        keep_columns = ["SETTLEMENTDATE", "REGIONID", "RRP"]
        csv_files = [
            f for f in os.listdir(self.temp_dir)
            if f.lower().endswith(".csv")
        ]
        logging.info(f"Found {len(csv_files)} CSV files")

        for fname in csv_files:
            try:
                df = pd.read_csv(os.path.join(self.temp_dir, fname), skiprows=1)
                df = df[keep_columns]
                all_dfs.append(df)
                logging.debug(f"Loaded: {fname}")
            except Exception as e:
                logging.warning(f"Failed to load {fname}: {e}")
        return all_dfs

    def delete_csvs(self) -> None:
        deleted = 0
        for fname in os.listdir(self.temp_dir):
            if fname.lower().endswith(".csv"):
                try:
                    os.remove(os.path.join(self.temp_dir, fname))
                    deleted += 1
                except Exception as e:
                    logging.warning(f"Failed to delete CSV {fname}: {e}")
        logging.info(f"Deleted {deleted} CSV files")

    def save_combined_df(self, df: pd.DataFrame) -> None:
        try:
            df.to_csv(self.output_path, index=False)
            logging.info(f"Saved final CSV: {self.output_path}")
        except Exception as e:
            logging.error(f"Failed to save CSV: {e}")

    def get_price_data(self) -> pd.DataFrame:
        # Step 0: Check if combined CSV already exists
        if os.path.exists(self.output_path):
            logging.info(f"Found existing CSV: {self.output_path}, loading...")
            try:
                full_df = pd.read_csv(self.output_path, parse_dates=["SETTLEMENTDATE"])
                logging.info("Loaded data from existing CSV")
                return full_df
            except Exception as e:
                logging.warning(f"Failed to load existing CSV: {e}, will reprocess")

        # Step 1–6: Full download and processing pipeline
        urls = self.generate_urls()
        self.download_zips(urls)
        self.extract_zips()
        self.delete_zips()

        all_dfs = self.load_csvs()
        if not all_dfs:
            logging.error("No data found after loading CSVs.")
            return pd.DataFrame()

        full_df = pd.concat(all_dfs, ignore_index=True)

        if "REGIONID" in full_df.columns:
            full_df = full_df[full_df["REGIONID"] == self.state]

        if "SETTLEMENTDATE" in full_df.columns:
            full_df["SETTLEMENTDATE"] = pd.to_datetime(full_df["SETTLEMENTDATE"])
            full_df = full_df.sort_values("SETTLEMENTDATE").reset_index(drop=True)

        self.delete_csvs()
        self.save_combined_df(full_df)

        return full_df
