import logging
import argparse

from import_battery_properties import BatteryLoader
from collect_price_data import PriceDataDownloader

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    parser = argparse.ArgumentParser(description="Battery performance simulator")

    parser.add_argument(
        "--state",
        type=str,
        default="QLD1",
        help="Australian NEM region (default: NSW1)"
    )

    parser.add_argument(
        "--year",
        type=int,
        default=2024,
        help="Year to simulate (default: 2023)"
    )

    args = parser.parse_args()

    state = args.state.upper()
    year = args.year

    # Load batteries
    loader = BatteryLoader("batteries.xml")
    batteries = loader.load_batteries()

    for battery in batteries:
        logging.info(f"Battery loaded: {battery}")

    # Download and process price data
    downloader = PriceDataDownloader(state, year)
    price_df = downloader.get_price_data()

    if price_df.empty:
        print("No data was downloaded or processed.")
    else:
        print(f"Downloaded and processed price data for {state} in {year}.")
        print(price_df.head())

    # TODO: Run simulation logic here


if __name__ == "__main__":
    main()
