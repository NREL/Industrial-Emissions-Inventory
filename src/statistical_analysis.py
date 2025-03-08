import os
import logging
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from decimal import Decimal
from sklearn.ensemble import IsolationForest
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set up result folder using current date
RESULT_FOLDER = datetime.today().strftime('%Y-%m-%d')
OUTPUT_PATH = os.path.join("..", RESULT_FOLDER)

# Ensure output directory exists
os.makedirs(OUTPUT_PATH, exist_ok=True)


def load_inventory_data(sector: str) -> pd.DataFrame:
    """
    Load facility-level inventory data for a given sector.

    :param sector: Industrial sector name (e.g., cement, hydrogen)
    :return: DataFrame containing the inventory data
    :raises FileNotFoundError: If the required file is missing
    """
    file_path = os.path.join(OUTPUT_PATH, sector, f"{sector}_inventory_facility_level.csv")
    if not os.path.exists(file_path):
        logging.error(f"Missing inventory file: {file_path}")
        raise FileNotFoundError(f"Inventory file not found: {file_path}")
    logging.info(f"Loading inventory data from {file_path}")
    return pd.read_csv(file_path)


def plot_histogram(data: pd.DataFrame, sector: str, pollutant: str, filename_suffix: str):
    """
    Generate and save a histogram plot for pollutant flow amounts.

    :param data: DataFrame containing the pollutant data
    :param sector: Industrial sector name
    :param pollutant: Pollutant name
    :param filename_suffix: Suffix for the output filename
    """
    fig, ax = plt.subplots()
    sns.histplot(data["FlowAmount"], bins=50, ax=ax)
    ax.set_title(f'{pollutant} Distribution (Kg)')
    ax.set_xlabel('Flow Amount (Kg)')
    ax.set_ylabel('Facility Count')
    output_file = os.path.join(OUTPUT_PATH, sector, f"{sector}_{filename_suffix}.pdf")
    fig.savefig(output_file, bbox_inches='tight')
    logging.info(f"Histogram saved: {output_file}")


def detect_outliers(data: pd.DataFrame, column: str) -> list:
    """
    Identify outliers in a dataset using the Isolation Forest algorithm.

    :param data: DataFrame containing the data
    :param column: Column name to analyze for outliers
    :return: List of facility IDs classified as outliers
    """
    if column not in data.columns:
        logging.warning(f"Column {column} not found in dataset. Skipping outlier detection.")
        return []
    
    data = data.dropna(subset=[column])
    clf = IsolationForest(contamination=0.01, random_state=1)
    clf.fit(data[[column]])
    data['Outlier'] = clf.predict(data[[column]])
    outliers = data[data['Outlier'] == -1]['FRS_ID'].tolist()
    logging.info(f"Detected {len(outliers)} outliers in {column}")
    return outliers


def remove_outliers(data: pd.DataFrame, outlier_list: list) -> (pd.DataFrame, pd.DataFrame):
    """
    Remove outliers from the dataset.

    :param data: DataFrame containing the facility data
    :param outlier_list: List of outlier facility IDs
    :return: Tuple (cleaned data, outliers data)
    """
    data["FRS_ID"] = data["FRS_ID"].astype(str)
    cleaned_data = data[~data["FRS_ID"].isin(outlier_list)]
    outlier_data = data[data["FRS_ID"].isin(outlier_list)]
    logging.info(f"Removed {len(outlier_list)} outliers from dataset")
    return cleaned_data, outlier_data


def save_data(data: pd.DataFrame, sector: str, filename: str):
    """
    Save DataFrame to a CSV file.

    :param data: DataFrame to save
    :param sector: Industrial sector name
    :param filename: Name of the output file
    """
    output_file = os.path.join(OUTPUT_PATH, sector, filename)
    data.to_csv(output_file, index=False)
    logging.info(f"Data saved: {output_file}")


def stat_analysis(sector: str):
    """
    Perform statistical analysis and outlier detection on facility-level emissions data.

    :param sector: Industrial sector name
    """
    try:
        # Load data
        data = load_inventory_data(sector)

        # Define emissions of interest
        emissions = ["Volatile Organic Compounds", "Sulfur Dioxide", "Nitrogen Oxides", "PM10-PM2.5"]

        # Generate histograms
        for pollutant in emissions:
            plot_histogram(data, sector, pollutant, f"{pollutant.lower().replace(' ', '_')}_histogram")

        # Detect outliers
        outlier_ids = []
        for pollutant in emissions:
            outlier_ids.extend(detect_outliers(data, "Concentration"))

        # Remove outliers
        cleaned_data, outlier_data = remove_outliers(data, outlier_ids)

        # Save results
        save_data(cleaned_data, sector, f"{sector}_cleaned.csv")
        save_data(outlier_data, sector, f"{sector}_outliers.csv")

    except FileNotFoundError as e:
        logging.error(e)
    except Exception as e:
        logging.exception("An unexpected error occurred")



