"""
This script runs the FECM inventory for specified sectors and creates output files
in folders named with the current date. The FECM inventory builder consists of three main parts:

1. Running Stewi to obtain emissions inventories.
2. Compiling the data to create emissions inventories for a sector, handling errors, and generating facility- and process-level results.
3. Performing statistical analysis on the compiled inventory.
"""

import logging
from datetime import datetime
from aegis import main
from statistical_analysis import stat_analysis

# Configure logging
logging.basicConfig(
    filename=f"fecm_inventory_{datetime.today().strftime('%Y-%m-%d')}.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def run_inventory_for_sector(sector: str, use_flight_tool: bool, naics_code: int, corrected_parquet: bool, run_stewi: bool, year: str):
    """
    Runs the emissions inventory process for a given industrial sector.
    
    Parameters
    ----------
    sector : str
        Name of the industrial sector (e.g., 'cement', 'steel').
    use_flight_tool : bool
        Whether to use GHGRP Flight Tool exported file for NAICS code.
    naics_code : int
        NAICS code for the sector; required if `use_flight_tool` is False.
    corrected_parquet : bool
        If True, use a corrected parquet file with manual corrections.
    run_stewi : bool
        Whether to run Stewi for emissions inventory retrieval.
    year : str
        Year for which the inventory is being built.
    """
    try:
        logging.info(f"Starting inventory creation for sector: {sector}")
        main(sector, use_flight_tool, naics_code, corrected_parquet, run_stewi, year)
        logging.info(f"Successfully created inventory for sector: {sector}")
    except Exception as e:
        logging.error(f"Error processing sector {sector}: {e}", exc_info=True)

def run_statistical_analysis(sector: str):
    """
    Runs statistical analysis on the compiled emissions inventory for a sector.
    
    Parameters
    ----------
    sector : str
        Name of the industrial sector.
    """
    try:
        logging.info(f"Starting statistical analysis for sector: {sector}")
        stat_analysis(sector)
        logging.info(f"Statistical analysis completed for sector: {sector}")
    except Exception as e:
        logging.error(f"Error during statistical analysis for {sector}: {e}", exc_info=True)

if __name__ == "__main__":
    # Define sector configurations
    flag_for_running_stewi = False
    corrected_parquet = True
    year = "2017"
    
    # Sectors that require GHGRP Flight Tool exported files
    ghgrp_sectors = ['cement']
    
    for idx, sector in enumerate(ghgrp_sectors):
        run_inventory_for_sector(sector, True, 0, corrected_parquet, flag_for_running_stewi if idx == 0 else False, year)
    
    # Run statistical analysis for all sectors
    all_sectors = ghgrp_sectors 
    for sector in all_sectors:
        run_statistical_analysis(sector)
