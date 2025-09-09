from pathlib import Path

from loguru import logger
import matplotlib.pyplot as plt
import pandas as pd
import typer

from air_pollution.config import FIGURES_DIR, PROCESSED_DATA_DIR

app = typer.Typer()


@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    input_path: Path = PROCESSED_DATA_DIR / "dataset.csv",
    output_path: Path = FIGURES_DIR / "plot.png",
    # -----------------------------------------
):
    # ---- REPLACE THIS WITH YOUR OWN CODE ----
    logger.info("Generating plot from data...")

    df = pd.read_csv(input_path)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df["rolling_avg"] = df["nitrogen_dioxide"].rolling("7D").mean()

    _, ax = plt.subplots()
    ax.plot(df.index, df["rolling_avg"], label="1-Week Rolling Average", color="orange")
    ax.set_xlabel("Date")
    ax.set_ylabel("Concentration ($\mu g/m$)")
    ax.set_title("1-Week Rolling Average of Nitrogen Dioxide Levels")
    ax.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

    logger.success("Plot generation complete.")
    # -----------------------------------------


if __name__ == "__main__":
    app()
