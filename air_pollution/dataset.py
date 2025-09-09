from pathlib import Path

from loguru import logger
from tqdm import tqdm
import typer
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

from air_pollution.config import PROCESSED_DATA_DIR, RAW_DATA_DIR

app = typer.Typer()

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)


@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    input_path: Path = RAW_DATA_DIR / "dataset.csv",
    output_path: Path = PROCESSED_DATA_DIR / "dataset.csv",
    # ----------------------------------------------
):
    logger.info("Processing dataset...")

    # Open-Meteo API request parameters
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    params = {
        "latitude": 52.0908,
        "longitude": 5.1222,
        "hourly": "nitrogen_dioxide",
        "start_date": "2021-01-01",
        "end_date": "2024-12-31",
    }

    # Fetch data from Open-Meteo API
    responses = openmeteo.weather_api(url, params=params)

    # Process the first location (add a loop for multiple locations if needed)
    response = responses[0]
    logger.info(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
    logger.info(f"Elevation: {response.Elevation()} m asl")
    logger.info(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

    # Process hourly data
    hourly = response.Hourly()
    hourly_nitrogen_dioxide = hourly.Variables(0).ValuesAsNumpy()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )
    }

    hourly_data["nitrogen_dioxide"] = hourly_nitrogen_dioxide
    hourly_dataframe = pd.DataFrame(data=hourly_data)

    # Save the processed data to the output path
    hourly_dataframe.to_csv(output_path, index=False)
    logger.success(f"Processing dataset complete. Data saved to {output_path}")


if __name__ == "__main__":
    app()
