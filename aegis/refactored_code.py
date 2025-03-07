#!/usr/bin/env python3
"""
Air Emissions Grouped By Industrial Sectors (AEGIS)
=====================================================

The AEGIS Inventory Builder achieves multiple objectives:
1. Runs STEWI to download and obtain databases from three sources - GHGRP, NEI, and TRI.
2. Compiles the databases and builds emissions inventories for individual sectors.
3. Compiles emissions inventories at the facility and process levels.
4. Compiles stack parameter information for facilities in the NEI.
"""

import os
import sys
import logging
import warnings
from datetime import datetime

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

import stewi
import stewicombo

# Configure logging to output messages to stdout.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
warnings.filterwarnings("ignore")

# Set up result folder based on the current date.
dt = datetime.today().strftime('%Y-%m-%d')
result_folder = dt
out_path1 = os.path.join("..", dt)
try:
    os.makedirs(out_path1, exist_ok=True)
    logging.info(f"Results directory created or exists: {out_path1}")
except Exception as e:
    logging.error(f"Error creating results directory: {e}")

def main(sector, filename_flag, naics, corrected_parquet_available_flag, flag_for_running_stewi, year):
    """
    Overarching function to compile the emissions inventories.

    This function is called by the run file to create the FECM inventory. It:
      1. Retrieves data using STEWI.
      2. Compiles facility and process-level inventories.
      3. Produces issue files and stack parameter information files.

    Parameters
    ----------
    sector : str
        Industrial sector being analyzed.
    filename_flag : bool
        Flag indicating if the GHGRP Flight file is present.
    naics : int or str
        NAICS code for the sector.
    corrected_parquet_available_flag : bool
        Flag indicating if a corrected STEWI parquet file is available.
    flag_for_running_stewi : bool
        Flag to determine whether to run the STEWI process.
    year : int or str
        Year being analyzed.

    Returns
    -------
    None
    """
    # Option flag to determine inventory preference
    option = "GHGRP Not Preferred"
    
    # Dictionary with information about GHGRP flight tool file names and literature obtained CO2 concentration
    data = {
        'cement': {'filename': "flight_cement.xls", 'co2concentration': 0.3183},
        'steel': {'filename': "flight_steel.xls", 'co2concentration': 0.3515},
        'ethanol': {'filename': "flight_ethanol.xls", 'co2concentration': 0.999999},
        'ammonia': {'filename': "flight_ammonia.xls", 'co2concentration': 0.987887169},
        'hydrogen': {'filename': "flight_hydrogen.xls", 'co2concentration': 0.999999},
        'pulp': {'filename': "flight_pulp.xls", 'co2concentration': 0.9999999},
        'refining': {'filename': "flight_refining.xls", 'co2concentration': 0.9999999},
        'natural_gas_processing': {'filename': "flight_natural_gas_processing_agr.xlsx", 'co2concentration': 0.995884774},
    }
    
    # Create output subdirectories for the sector and issue files.
    out_path2 = os.path.join(out_path1, sector)
    out_path3 = os.path.join(out_path2, "issue_files")
    try:
        os.makedirs(out_path2, exist_ok=True)
        os.makedirs(out_path3, exist_ok=True)
        logging.info(f"Created subdirectories: {out_path2} and {out_path3}")
    except Exception as e:
        logging.error(f"Error creating subdirectories: {e}")
    
    # NAICS codes for sectors when codes are not available from the FLIGHT Tool.
    naics1 = naics
    naics2 = ""
    naics3 = ""
    naics4 = ""
    
    # Filename for reading GHGRP data from the FLIGHT Tool.
    ghgrpfilename = os.path.join("..", "Data", data.get(sector, {}).get('filename', ''))
    
    def save_data(inventory, year):
        """
        Saves individual inventory information from STEWI.

        It saves both facility-level and, when available, process-level database information,
        as well as facility details required for the final database.

        Parameters
        ----------
        inventory : str
            Name of the inventory (e.g., 'GHGRP', 'NEI', or 'TRI').
        year : int or str
            Year being analyzed.

        Returns
        -------
        None
        """
        try:
            # Running STEWI to get inventory by facility.
            flow_by_facility = stewi.getInventory(inventory, year, 'flowbyfacility', filters=['filter_for_LCI'])
            facility_csv = os.path.join("..", "Data", f"flow_by_facility_{inventory}.csv")
            flow_by_facility.to_csv(facility_csv, index=False)
            logging.info(f"Saved facility-level inventory for {inventory} to {facility_csv}")
        except Exception as e:
            logging.error(f"Error retrieving/saving facility-level inventory for {inventory}: {e}")
        
        try:
            # Process level inventory may not be available for all databases.
            flow_by_process = stewi.getInventory(inventory, year, 'flowbyprocess', filters=['filter_for_LCI'])
            flow_by_process['Year'] = year
            process_csv = os.path.join("..", "Data", f"flow_by_process_{inventory}.csv")
            flow_by_process.to_csv(process_csv, index=False)
            logging.info(f"Saved process-level inventory for {inventory} to {process_csv}")
        except Exception as e:
            logging.warning(f"Process-level data not available for {inventory}: {e}")
        
        try:
            facility = stewi.getInventoryFacilities(inventory, year)
            facility_file = os.path.join("..", "Data", f"facility_{inventory}.csv")
            facility.to_csv(facility_file, index=False)
            logging.info(f"Saved facility information for {inventory} to {facility_file}")
        except Exception as e:
            logging.error(f"Error retrieving/saving facility information for {inventory}: {e}")
    
    def combine_facility_name_inv(a, b, c):
        """
        Combines facility names from STEWI for debugging purposes.

        Parameters
        ----------
        a : str
            Name of the first inventory.
        b : str
            Name of the second inventory.
        c : str
            Name of the third inventory.

        Returns
        -------
        None
        """
        try:
            d1 = pd.read_csv(os.path.join("..", "Data", f"facility_{a}.csv"))
            d2 = pd.read_csv(os.path.join("..", "Data", f"facility_{b}.csv"))
            d3 = pd.read_csv(os.path.join("..", "Data", f"facility_{c}.csv"))
            d4 = pd.concat([d1, d2, d3])
            combined_file = os.path.join("..", "Data", "FacilityNamesFromStewicombined.csv")
            d4.to_csv(combined_file, index=False)
            logging.info(f"Combined facility names saved to {combined_file}")
        except Exception as e:
            logging.error(f"Error combining facility names: {e}")
    
    def run_stewi(option):
        """
        Saves combined inventory information from STEWI.

        It retrieves data for GHGRP, NEI, and TRI inventories and then combines them.

        Parameters
        ----------
        option : str
            Inventory preference option. If not 'GHGRP Preferred', a full combination is performed.

        Returns
        -------
        None
        """
        # Process each inventory individually.
        for inv in ['GHGRP', 'NEI', 'TRI']:
            save_data(inv, year)
        
        # Obtain and save combined inventory from STEWI with and without overlapping filters.
        inventory_dict = {"GHGRP": year, "NEI": year, "TRI": year}
        try:
            if option != "GHGRP Preferred":
                logging.info("Combining full inventories")
                df = stewicombo.combineFullInventories(inventory_dict, filter_for_LCI=True, remove_overlap=True, compartments=None)
            else:
                logging.info("Using GHGRP as the preferred inventory")
                df = stewicombo.combineInventoriesforFacilitiesinBaseInventory("GHGRP", inventory_dict)
            combined_file = os.path.join("..", "Data", "combinedinventories_NEI_TRI_GHGRP.csv")
            df.to_csv(combined_file, index=False)
            logging.info(f"Combined inventories saved to {combined_file}")
        except Exception as e:
            logging.error(f"Error combining inventories: {e}")
        
        combine_facility_name_inv('NEI', 'TRI', 'GHGRP')
    
    if flag_for_running_stewi:
        logging.info("Running STEWI process as flagged.")
        run_stewi(option)
    
    def add_cas_number(df_em):
        """
        Assigns Chemical Abstracts Service (CAS) numbers to the emissions.

        Parameters
        ----------
        df_em : pandas.DataFrame
            Emissions inventory DataFrame.

        Returns
        -------
        pandas.DataFrame
            Emissions inventory with CAS numbers assigned to every pollutant.
        """
        try:
            pollutants_all = pd.read_csv(os.path.join("..", "Data", "pollutants_to_cas.csv"))
            df_em = pd.merge(df_em, pollutants_all, on="FlowName", how='left')
            logging.info("CAS numbers successfully added to emissions data.")
        except Exception as e:
            logging.error(f"Error adding CAS numbers: {e}")
        return df_em
    
    def naics_code_checker(df1, frs_id_list):
        """
        Checks for NAICS code inconsistencies for facilities.

        Parameters
        ----------
        df1 : pandas.DataFrame
            DataFrame containing facility and emissions data.
        frs_id_list : list of str
            List of FRS IDs to check.

        Returns
        -------
        None
        """
        fac_with_naics_code_issue = pd.DataFrame()
        for ide in frs_id_list:
            df2 = df1[df1['FRS_ID'] == ide]
            naics_values = list(pd.unique(df2['NAICS']))
            if len(naics_values) > 1:
                df2 = df2[['FRS_ID', 'FacilityName', 'Address', 'City', 'State', 'Zip',
                           'Latitude', 'Longitude', 'County', 'NAICS', 'SIC', 'FacilityID', 'Source']].drop_duplicates()
                fac_with_naics_code_issue = pd.concat([fac_with_naics_code_issue, df2])
        issues_file = os.path.join(out_path3, f"{sector}_facilities_with_naics_code_issues.csv")
        try:
            fac_with_naics_code_issue.to_csv(issues_file, index=False)
            logging.info(f"NAICS code issues saved to {issues_file}")
        except Exception as e:
            logging.error(f"Error saving NAICS code issues file: {e}")
    
    def manipulate_databases(naics_code1, naics_code2, naics_code3, naics_code4):
        """
        Reads and manipulates combined inventories and facility data.

        It merges the combined inventories from NEI, TRI, and GHGRP with facility information,
        performs a concentration calculation (only for reported emissions),
        and filters data based on NAICS codes or Flight file availability.

        Parameters
        ----------
        naics_code1 : int or str
            Primary NAICS code for the industrial sector.
        naics_code2 : int or str
            Secondary NAICS code.
        naics_code3 : int or str
            Tertiary NAICS code.
        naics_code4 : int or str
            Quaternary NAICS code.

        Returns
        -------
        tuple
            (df1_sector, naics_list, frs_id_list)
            - df1_sector: pandas.DataFrame containing the sector's inventory.
            - naics_list: list of NAICS codes extracted.
            - frs_id_list: list of FRS IDs used for filtering.
        """
        try:
            combined_df = pd.read_csv(os.path.join("..", "Data", "combinedinventories_NEI_TRI_GHGRP.csv"))
            nei_df = pd.read_csv(os.path.join("..", "Data", "facility_NEI.csv"))
            tri_df = pd.read_csv(os.path.join("..", "Data", "facility_TRI.csv"))
            ghgrp_df = pd.read_csv(os.path.join("..", "Data", "facility_GHGRP.csv"))
            logging.info("Successfully loaded combined inventories and facility data.")
        except Exception as e:
            logging.error(f"Error loading data files: {e}")
            sys.exit(1)
    
        # Process combined inventory
        combined_df = combined_df[['FacilityID', 'FlowName', 'Compartment', 'FlowAmount', 'Unit', 
                                     'DataReliability', 'Source', 'Year', 'FRS_ID']]
        combined_df[['FacilityID', 'Source']] = combined_df[['FacilityID', 'Source']].astype('str')
    
        # Tag facility data with their respective sources
        nei_df['Source'] = "NEI"
        tri_df['Source'] = "TRI"
        ghgrp_df['Source'] = "GHGRP"
    
        df_facility = pd.concat([ghgrp_df, nei_df, tri_df])
        naics_list = []
    
        df_facility[['FacilityID', 'Source']] = df_facility[['FacilityID', 'Source']].astype('str')
    
        # Merge facility information with emissions inventory
        df1 = combined_df.merge(df_facility, on=['FacilityID', 'Source'], indicator=True, how='outer').drop_duplicates()
    
        missing_in_flow = df1[df1['_merge'] == 'right_only']
        missing_file = os.path.join(out_path3, "missing_in_combined_flow_but_not_facility_level_inventory.csv")
        try:
            missing_in_flow.to_csv(missing_file, index=False)
            logging.info(f"Missing flow data saved to {missing_file}")
        except Exception as e:
            logging.error(f"Error saving missing flow data file: {e}")
    
        df1 = df1[df1['_merge'] == 'both']
    
        # Prepare facility-level DataFrame
        frs_fac_df = df1[['FacilityID', 'FRS_ID']].drop_duplicates()
        df1 = df1[['FlowName', 'Compartment', 'FlowAmount', 'Unit', 'DataReliability', 'Source', 'Year', 
                   'FRS_ID', 'FacilityName', 'Address', 'City', 'State', 'Zip', 'Latitude', 'Longitude', 
                   'County', 'NAICS', 'SIC', 'FacilityID']].drop_duplicates()
        df_ghgrp = df1[df1['Source'] == "GHGRP"]
        df_nei_tri = df1[(df1['Source'] == "NEI") | (df1['Source'] == "TRI")]
        df1_air = df1[df1['Compartment'] == "air"]
    
        # If the Flight file is available, extract NAICS codes from it.
        if filename_flag:
            try:
                df_ghgrp_flight = pd.read_excel(ghgrpfilename)
                df_ghgrp_flight['GHGRP ID'] = df_ghgrp_flight['GHGRP ID'].astype('str')
                ghgrp_raw_data = pd.read_excel(os.path.join("..", "Data", f"GHGRP_epa_{year}.xlsx"))
                ghgrp_raw_data = ghgrp_raw_data[['Facility Id', 'FRS Id', 'Facility Name', 'City', 'State', 'Zip Code',
                                                 'Address', 'County', 'Latitude', 'Longitude', 'Primary NAICS Code']]
                ghgrp_raw_data[['Facility Id', 'FRS Id']] = ghgrp_raw_data[['Facility Id', 'FRS Id']].astype('str')
                df_ghgrp_flight = df_ghgrp_flight.merge(ghgrp_raw_data, left_on=['GHGRP ID'], right_on=['Facility Id'])
                naics_list = list(pd.unique(df_ghgrp_flight['Primary NAICS Code']))
                logging.info(f"Extracted NAICS list from GHGRP Flight file: {naics_list}")
            except Exception as e:
                logging.error(f"Error processing GHGRP Flight file: {e}")
                naics_list = []
    
            df3 = df_ghgrp[df_ghgrp['NAICS'].isin(naics_list)]
            df4 = df_nei_tri[df_nei_tri['NAICS'].isin(naics_list)]
    
            df1_sector = pd.concat([df3, df4])
            df1_sector['FRS_ID'] = df1_sector['FRS_ID'].astype('str')
            frs_id_list_2 = list(pd.unique(df1_sector['FRS_ID']))
            df1_air['FRS_ID'] = df1_air['FRS_ID'].astype('str')
            df1_sector = df1_air[df1_air['FRS_ID'].isin(frs_id_list_2)]
            naics_code_checker(df1_sector, frs_id_list_2)
    
            try:
                fac_ids_from_ghgrp = list(pd.unique(df_ghgrp_flight['Facility Id']))
                df_ghgrp[['FacilityID', 'FRS_ID']] = df_ghgrp[['FacilityID', 'FRS_ID']].astype('str')
                ghgrp_chosen = df_ghgrp[df_ghgrp['FacilityID'].isin(fac_ids_from_ghgrp)]
                frs_id_list = list(pd.unique(ghgrp_chosen['FRS_ID']))
            except Exception as e:
                logging.error(f"Error processing GHGRP facility IDs: {e}")
                frs_id_list = []
    
            df1_sector = add_cas_number(df1_sector)
            sector_inventory_file = os.path.join(out_path2, f"{sector}_inventory_facility_level_including_all_facilities_from_all_db.csv")
            try:
                df1_sector.to_csv(sector_inventory_file, index=False)
                logging.info(f"Sector facility-level inventory (all DB) saved to {sector_inventory_file}")
            except Exception as e:
                logging.error(f"Error saving sector inventory file: {e}")
    
            # If corrected parquet is available, filter to include only GHGRP facilities.
            if corrected_parquet_available_flag:
                df1_sector = df1_sector[df1_sector['FRS_ID'].isin(frs_id_list)]
    
        else:
            # Use user-provided NAICS code if the Flight file is not available.
            df_facility[['FacilityID', 'Source']] = df_facility[['FacilityID', 'Source']].astype('str')
            df1 = combined_df.merge(df_facility, on=['FacilityID', 'Source'], indicator=True, how='left').drop_duplicates()
            frs_fac_df = df1[['FacilityID', 'FRS_ID']].drop_duplicates()
            df1 = df1[['FlowName', 'Compartment', 'FlowAmount', 'Unit', 'DataReliability', 'Source', 'Year', 
                       'FRS_ID', 'FacilityName', 'Address', 'City', 'State', 'Zip', 'Latitude', 'Longitude', 
                       'County', 'NAICS', 'SIC', 'FacilityID']].drop_duplicates()
            df1_air = df1[df1['Compartment'] == "air"]
            df1_air['FRS_ID'] = df1_air['FRS_ID'].astype('str')
            df1_sector = df1_air[(df1_air['NAICS'] == naics_code1) | (df1_air['NAICS'] == naics_code2)]
            frs_id_list_all_db = list(pd.unique(df1_sector['FRS_ID']))
            df1_sector = df1_air[df1_air['FRS_ID'].isin(frs_id_list_all_db)]
            naics_code_checker(df1_sector, frs_id_list_all_db)
    
            df_ghgrp = df1_sector[df1_sector['Source'] == "GHGRP"].drop_duplicates()
            df_ghgrp['FRS_ID'] = df_ghgrp['FRS_ID'].astype('str')
            frs_id_list = list(pd.unique(df_ghgrp['FRS_ID']))
            df1_sector['FRS_ID'] = df1_sector['FRS_ID'].astype('str')
            if corrected_parquet_available_flag:
                df1_sector = df1_sector[df1_sector['FRS_ID'].isin(frs_id_list)]
    
        df1_sector['FRS_ID'] = df1_sector['FRS_ID'].astype('str')
        df1_sector = add_cas_number(df1_sector)
        final_inventory_file = os.path.join(out_path2, f"{sector}_inventory_facility_level.csv")
        try:
            df1_sector.to_csv(final_inventory_file, index=False)
            logging.info(f"Final sector facility-level inventory saved to {final_inventory_file}")
        except Exception as e:
            logging.error(f"Error saving final sector inventory file: {e}")
    
        return df1_sector, naics_list, frs_id_list
    
    df1_sector, naics_list, frs_id_list = manipulate_databases(naics1, naics2, naics3, naics4)

    def facility_stack_parameters(df1_sector):
        """
        Combines facility information with available stack parameters from the NEI database.
        Facilities not reporting to the NEI database do not have any stack data.
    
        Parameters
        ----------
        df1_sector : pandas.DataFrame
            DataFrame containing facility-level inventory data.
    
        Outputs
        -------
        Writes CSV files with stack parameter information for available facilities.
        The output file contains columns for emissions, facility ID, facility name, and stack data.
    
        Returns
        -------
        None
        """
        try:
            # Read the NEI stack parameter data file.
            nei_flat_file = pd.read_csv(
                os.path.join("..", "Data", "reduced_stack_file.csv"), low_memory=False)
            nei_flat_file['FACILITY_ID'] = nei_flat_file['FACILITY_ID'].astype('str')
            nei_flat_file = nei_flat_file[['FACILITY_ID', 'UNIT_ID', 'PROCESS_ID', 'STKHGT',
                                           'STKDIAM', 'STKTEMP', 'STKFLOW', 'STKVEL']]
            df_inv = df1_sector.copy()
            df_inv['FacilityID'] = df_inv['FacilityID'].astype('str')
            nei_flat_file_cem = df_inv.merge(nei_flat_file,
                                             left_on=['FacilityID'],
                                             right_on=['FACILITY_ID'])
            nei_flat_file_cem['FACILITY_ID'] = nei_flat_file_cem['FACILITY_ID'].astype('str')
            nei_flat_file_cem['PROCESS_ID'] = nei_flat_file_cem['PROCESS_ID'].astype('str')
            nei_flat_file_cem['STKFLOW'] = pd.to_numeric(nei_flat_file_cem['STKFLOW'], errors='coerce')
    
            # Read facility information from NEI.
            facility_info = pd.read_csv(os.path.join("..", "Data", "facility_NEI.csv"))
            facility_info['FacilityID'] = facility_info['FacilityID'].astype('str')
    
            # Create and save flat facility file with stack parameters.
            flat_fac = nei_flat_file_cem[['FACILITY_ID', 'STKHGT', 'STKDIAM', 'STKTEMP', 'STKFLOW', 'STKVEL']].drop_duplicates()
            flat_fac = flat_fac.merge(facility_info, left_on=['FACILITY_ID'], right_on=['FacilityID'])
            stack_file_fac = os.path.join(out_path2, f"{sector}_nei_fac_level_info_for_stack_parameters.csv")
            flat_fac.to_csv(stack_file_fac, index=False)
            logging.info(f"NEI facility-level stack parameters saved to {stack_file_fac}")
    
            # Create and save flat unit file with stack parameters.
            flat_unit = nei_flat_file_cem[['FACILITY_ID', 'STKHGT', 'STKDIAM', 'STKTEMP', 'STKFLOW', 'STKVEL', 'UNIT_ID', 'PROCESS_ID']].drop_duplicates()
            nei_flow_by_process = pd.read_csv(os.path.join("..", "Data", "flow_by_process_NEI.csv"))
            nei_flow_by_process['FacilityID'] = nei_flow_by_process['FacilityID'].astype('str')
            nei_flow_by_process['Process'] = pd.to_numeric(nei_flow_by_process['Process'], errors='coerce')
            flat_unit = flat_unit.merge(facility_info, left_on=['FACILITY_ID'], right_on=['FacilityID'])
            stack_file_unit = os.path.join(out_path2, f"{sector}_nei_unit_level_info_for_stack_parameters.csv")
            flat_unit.to_csv(stack_file_unit, index=False)
            logging.info(f"NEI unit-level stack parameters saved to {stack_file_unit}")
        except Exception as e:
            logging.error("File not found: reduced_stack_file.csv > 300 MB. Please ask authors for the file to create inventory with stack parameters. Error: %s", e)
    
    
    df_stack_data = facility_stack_parameters(df1_sector)
    
    
    def overlap_explorer(df1_sector):
        """
        Explores overlap between databases and facilities by identifying the number of databases
        that report for each facility (or FRS ID) and listing the associated emission flows.
    
        Parameters
        ----------
        df1_sector : pandas.DataFrame
            DataFrame containing facility-level emissions data.
    
        Outputs
        -------
        Writes CSV files with:
          - Overlap information with facility names.
          - Overlap information without facility names (to reduce duplicates).
    
        Returns
        -------
        None
        """
        try:
            frs_id_list = list(pd.unique(df1_sector['FRS_ID']))
            frs_id_df = df1_sector[['FRS_ID', 'Source', 'FlowName']].drop_duplicates()
            name_df = df1_sector[['FRS_ID', 'FacilityName', 'City', 'State', 'Latitude', 'Longitude']].drop_duplicates()
    
            source_list_all = []
            frs_a = []
            flows_all = []
            co2_missing_flags = []
    
            for frs in frs_id_list:
                chk = frs_id_df[frs_id_df['FRS_ID'] == frs]
                sources = sorted(list(pd.unique(chk['Source'])))
                flows = sorted(list(pd.unique(chk['FlowName'])))
                source_list_all.append(' '.join(sources))
                flows_all.append(flows)
                # Flag as True if "Carbon Dioxide" is missing from the flow list.
                co2_missing_flags.append("Carbon Dioxide" not in flows)
                frs_a.append(frs)
    
            overlap_df = pd.DataFrame({
                'FRS_ID': frs_a,
                'Source': source_list_all,
                'Flows': flows_all,
                'CO2 missing': co2_missing_flags
            })
            overlap_df = overlap_df.merge(name_df, on='FRS_ID')
            overlap_df['City'] = overlap_df['City'].astype('str')
            overlap_df['city'] = overlap_df['City'].str.lower()
            overlap_df = overlap_df.drop_duplicates(subset=['FRS_ID', 'city'], keep='first')
    
            overlap_file_with_names = os.path.join(out_path3, f"{sector}_overlap_explorer_with_facility_names.csv")
            overlap_df.to_csv(overlap_file_with_names, index=False)
            logging.info(f"Overlap explorer file (with facility names) saved to {overlap_file_with_names}")
    
            overlap_df_corr = pd.DataFrame()
            cities = sorted(list(pd.unique(overlap_df['city'])))
            for c in cities:
                d = overlap_df[overlap_df['city'] == c]
                if len(d) > 1:
                    sources_names = ' '.join(pd.unique(d['Source'])).split()
                    # Remove duplicates while preserving order.
                    sources_names_1 = " ".join(sorted(set(sources_names), key=sources_names.index))
                    # If only NEI and TRI are reported, then skip.
                    if sources_names_1 in ["NEI", "TRI", "NEI TRI", "TRI NEI"]:
                        continue
                    else:
                        overlap_df_corr = pd.concat([overlap_df_corr, d])
    
            if not overlap_df_corr.empty:
                overlap_file_corr = os.path.join(out_path3, f"{sector}_overlap_explorer_with_facility_names.csv")
                overlap_df_corr.to_csv(overlap_file_corr, index=False)
                logging.info(f"Overlap explorer corrected file saved to {overlap_file_corr}")
                # Drop facility names to avoid duplicates.
                if 'FacilityName' in overlap_df_corr.columns:
                    overlap_df_corr = overlap_df_corr.drop(columns=['FacilityName'])
                overlap_file_no_names = os.path.join(out_path3, f"{sector}_overlap_explorer_without_facility_names.csv")
                overlap_df_corr.to_csv(overlap_file_no_names, index=False)
                logging.info(f"Overlap explorer file (without facility names) saved to {overlap_file_no_names}")
        except Exception as e:
            logging.error("Error during overlap exploration: %s", e)
    
    
    overlap_explorer(df1_sector)
    
    
    def emission_concentration_calculation(df1_sector):
        """
        Calculates emission concentration by summing up all reported emissions per facility and then computing
        the concentration of each individual emission relative to the total. It further extracts facilities that
        report CO2 and focuses analysis on those facilities.
    
        Parameters
        ----------
        df1_sector : pandas.DataFrame
            DataFrame of sector facilities with emission data.
    
        Outputs
        -------
        Writes CSV file with concentration calculations for facility-level emissions, including CO2 flows.
    
        Returns
        -------
        pandas.DataFrame
            DataFrame of sector facilities that report CO2 emissions along with all other emissions.
        """
        try:
            co2_df = df1_sector[df1_sector['FlowName'] == "Carbon Dioxide"]
            co2_reporting_facilities = list(pd.unique(co2_df['FRS_ID']))
            sector_ids = list(pd.unique(df1_sector['FRS_ID']))
            sector_df1 = pd.DataFrame()
    
            for fid in sector_ids:
                df_fac = df1_sector[df1_sector['FRS_ID'] == fid]
                total_flow = df_fac['FlowAmount'].sum()
                df_fac = df_fac.copy()
                df_fac['FlowSum'] = total_flow
                df_fac['Concentration kg/kg'] = df_fac['FlowAmount'] / total_flow if total_flow != 0 else 0
                sector_df1 = pd.concat([sector_df1, df_fac])
    
            sector_df1 = sector_df1[['FRS_ID', 'FlowName', 'Compartment', 'FlowAmount', 'FlowSum', 'Unit',
                                     'Concentration kg/kg', 'Source', 'Year', 'FacilityName', 'State',
                                     'Latitude', 'Longitude', 'County', 'NAICS', 'SIC', 'FacilityID']]
    
            # Filter to include only facilities that report CO2.
            sector_facilities_with_co2 = sector_df1[sector_df1['FlowName'] == 'Carbon Dioxide'][['FRS_ID']].drop_duplicates()
            sector_df2 = sector_df1.merge(sector_facilities_with_co2, on='FRS_ID')
            sector_df3 = sector_df2.copy()  # Keeping a copy for further use.
            sector_df3 = add_cas_number(sector_df3)
            final_inventory_file = os.path.join(out_path2, f"{sector}_inventory_facility_level_emissions_with_co2_conc_calculated.csv")
            sector_df3.to_csv(final_inventory_file, index=False)
            logging.info(f"Emission concentration calculations saved to {final_inventory_file}")
            return sector_df2
        except Exception as e:
            logging.error("Error during emission concentration calculation: %s", e)
            return None
    
    
    sector_df2 = emission_concentration_calculation(df1_sector)
    
    
    def plots_and_exploration1(sector_df2):
        """
        Generates exploratory plots for specified cap emissions by visualizing concentration and flow amount.
        Saves scatter plots for each emission type and outputs a CSV file containing facility-level data.
    
        Parameters
        ----------
        sector_df2 : pandas.DataFrame
            DataFrame for sector facilities with CO2 emission data.
    
        Returns
        -------
        None
        """
        try:
            cap_emissions = ['Carbon Dioxide', 'Methane', 'Nitrogen Oxides', 'Nitrous Oxide',
                             'Sulfur Dioxide', 'Volatile Organic Compounds', 'PM10-PM2.5',
                             'PM2.5 Primary (Filt + Cond)']
            sector_inv_for_cap_em = pd.DataFrame()
    
            for em in cap_emissions:
                em_df = sector_df2[sector_df2['FlowName'] == em]
                sector_inv_for_cap_em = pd.concat([sector_inv_for_cap_em, em_df])
                # Plot concentration scatter plot.
                fig1 = plt.figure()
                sns.scatterplot(x=em_df['FRS_ID'], y=em_df['Concentration kg/kg'])
                plt.tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
                plt.title(f"{em}_concentration_kg/kg")
                plot_file1 = os.path.join("..", "Data", f"{em}_concentrationkgperkg.png")
                fig1.savefig(plot_file1, dpi=200)
                plt.close(fig1)
                logging.info(f"Saved plot: {plot_file1}")
    
                # Plot flow amount scatter plot.
                fig2 = plt.figure()
                sns.scatterplot(x=em_df['FRS_ID'], y=em_df['FlowAmount'])
                plt.tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
                plt.title(f"{em}_flowamount")
                plot_file2 = os.path.join("..", "Data", f"{em}_flowamount.png")
                fig2.savefig(plot_file2, dpi=200)
                plt.close(fig2)
                logging.info(f"Saved plot: {plot_file2}")
    
            cap_emissions_file = os.path.join(out_path2, f"{sector}_facility_level_cap_emissions.csv")
            sector_inv_for_cap_em.to_csv(cap_emissions_file, index=False)
            logging.info(f"Cap emissions data saved to {cap_emissions_file}")
        except Exception as e:
            logging.error("Error in plots_and_exploration1: %s", e)
    
    
    plots_and_exploration1(sector_df2)
    
    
    def concentration_calculation(df1_sector):
        """
        Calculates the concentration of emissions based on a CO2 reference.
        It first isolates the CO2 flow for each facility, computes total emissions excluding CO2,
        then calculates the total flue gas amount. Finally, it computes the concentration of each emission.
    
        Parameters
        ----------
        df1_sector : pandas.DataFrame
            DataFrame containing flow emissions data.
    
        Returns
        -------
        None
        """
        try:
            # Extract CO2 data.
            data_co2_only = df1_sector.loc[df1_sector["FlowName"] == "Carbon Dioxide", ["FRS_ID", "FlowAmount", "Source"]]
            data_co2_only = data_co2_only.rename(columns={"FlowAmount": "CO2Amount", "Source": "CO2Source"})
    
            # Total emissions minus CO2.
            data_all_emissions_minus_CO2 = df1_sector.loc[df1_sector["FlowName"] != "Carbon Dioxide", ["FRS_ID", "FlowAmount"]]
            data_all_emissions_minus_CO2 = data_all_emissions_minus_CO2.rename(columns={"FlowAmount": "TotalFlowAmountminusCO2"})
            data_sum_all_emissions_minus_CO2 = data_all_emissions_minus_CO2.groupby("FRS_ID")["TotalFlowAmountminusCO2"].agg('sum').reset_index()
    
            # Merge CO2 and non-CO2 data with the main DataFrame.
            df1_sector = pd.merge(df1_sector, data_co2_only, how="left", on="FRS_ID")
            df1_sector = pd.merge(df1_sector, data_sum_all_emissions_minus_CO2, how="left", on="FRS_ID")
    
            # Calculate total flue gas using CO2 concentration from the 'data' dictionary.
            df1_sector["Total_Flue_Gas_Amount"] = df1_sector["CO2Amount"] / data[sector]['co2concentration'] + df1_sector["TotalFlowAmountminusCO2"]
            df1_sector["Concentration"] = df1_sector["FlowAmount"] / df1_sector["Total_Flue_Gas_Amount"]
    
            df1_sector = add_cas_number(df1_sector)
            concentration_file = os.path.join(out_path2, f"{sector}_facility_level_with_concentration_via_co2_reference.csv")
            df1_sector.to_csv(concentration_file, index=False)
            logging.info(f"Concentration calculation results saved to {concentration_file}")
        except Exception as e:
            logging.error("Error during concentration calculation: %s", e)
    
    
    concentration_calculation(sector_df2)
    
    
    def plots_and_exploration2(sector_df3):
        """
        Generates exploratory plots for back-calculated emission concentrations.
        It uses a literature-based CO2 concentration reference to back-calculate the total flue gas amount,
        and then recalculates pollutant concentrations for each facility.
    
        Parameters
        ----------
        sector_df3 : pandas.DataFrame
            DataFrame for sector facilities with back-calculated emission concentration data.
    
        Returns
        -------
        None
        """
        try:
            cap_emissions = ['Carbon Dioxide', 'Methane', 'Nitrogen Oxides', 'Nitrous Oxide',
                             'Sulfur Dioxide', 'Volatile Organic Compounds', 'PM10-PM2.5',
                             'PM2.5 Primary (Filt + Cond)']
            sector_inv_for_cap_em = pd.DataFrame()
    
            for em in cap_emissions:
                em_df = sector_df3[sector_df3['FlowName'] == em]
                logging.info("For emission %s, total number of facilities obtained is %d", em, len(em_df['FRS_ID'].unique()))
                sector_inv_for_cap_em = pd.concat([sector_inv_for_cap_em, em_df])
    
                fig = plt.figure()
                ax = sns.scatterplot(x=em_df['FRS_ID'], y=em_df['Concentration'])
                plt.tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
                ax.set(ylabel=None)
                plt.title(f"{em}_concentration_kg/kg")
                plot_file = os.path.join("..", "Data", f"{em}_concentrationkgperkg_backcalculated.png")
                fig.savefig(plot_file, dpi=200)
                plt.close(fig)
                logging.info(f"Saved plot: {plot_file}")
    
            backcalc_file = os.path.join(out_path2, f"{sector}_facility_level_cap_emissions_backcalculated.csv")
            sector_inv_for_cap_em.to_csv(backcalc_file, index=False)
            logging.info(f"Back-calculated cap emissions data saved to {backcalc_file}")
        except Exception as e:
            logging.error("Error in plots_and_exploration2: %s", e)
            
    def nei_by_process_exploration(naics1, naics2, naics3, naics4):
        """
        Explores the NEI dataset by process emissions from STEWI. Combines the SCC codes with NEI provided codes
        and obtains fuel and unit information.
    
        Parameters
        ----------
        naics1 : int or str
            Primary NAICS code.
        naics2 : int or str
            Secondary NAICS code.
        naics3 : int or str
            Tertiary NAICS code.
        naics4 : int or str
            Quaternary NAICS code.
    
        Returns
        -------
        None
        """
        try:
            flow_by_process = pd.read_csv(os.path.join("..", "Data", "flow_by_process_NEI.csv"))
            flow_by_process['Source'] = "NEI"
    
            nei_df = pd.read_csv(os.path.join("..", "Data", "facility_NEI.csv"))
            tri_df = pd.read_csv(os.path.join("..", "Data", "facility_TRI.csv"))
            ghgrp_df = pd.read_csv(os.path.join("..", "Data", "facility_GHGRP.csv"))
    
            nei_df['Source'] = "NEI"
            tri_df['Source'] = "TRI"
            ghgrp_df['Source'] = "GHGRP"
    
            # Combine facility data from different inventories.
            df2 = pd.concat([ghgrp_df, nei_df, tri_df])
    
            # Convert FacilityID and Source to strings.
            flow_by_process[['FacilityID', 'Source']] = flow_by_process[['FacilityID', 'Source']].astype('str')
            df2[['FacilityID', 'Source']] = df2[['FacilityID', 'Source']].astype('str')
    
            # Merge flow data with facility data.
            df1 = flow_by_process.merge(df2, on=['FacilityID', 'Source'], how='left').drop_duplicates()
    
            # Select relevant columns.
            df1 = df1[['FacilityID', 'FlowName', 'Compartment', 'FlowAmount', 'Unit',
                       'DataReliability', 'Process', 'ProcessType', 'Source', 'FacilityName', 'Address',
                       'City', 'Year', 'State', 'Zip', 'Latitude', 'Longitude', 'County', 'NAICS', 'SIC']]
    
            # Keep only air emissions.
            df1_air = df1[df1['Compartment'] == "air"]
    
            # Filter by NAICS codes.
            if filename_flag:
                df1_sector = df1_air.loc[df1_air['NAICS'].isin(naics_list)]
            else:
                df1_sector = df1_air[(df1_air['NAICS'] == naics1) | (df1_air['NAICS'] == naics2) |
                                    (df1_air['NAICS'] == naics3) | (df1_air['NAICS'] == naics4)]
    
            # Merge with SCC map to obtain additional process information.
            scc_map = pd.read_csv(os.path.join("..", "Data", "SCCmap.csv"))
            scc_map['code'] = scc_map['SCC'].astype('str')
            df1_sector['Process'] = df1_sector['Process'].astype('str')
            df1_sector_process = df1_sector.merge(scc_map, left_on=['Process'], right_on=['SCC'], how='left')
    
            df1_sector_process = df1_sector_process[['FacilityID', 'FlowName', 'Compartment', 'FlowAmount', 'Unit',
                                                     'DataReliability', 'Process', 'ProcessType', 'Source', 'FacilityName',
                                                     'Address', 'City', 'Year', 'State', 'Zip', 'Latitude', 'Longitude',
                                                     'County', 'NAICS', 'SIC', 'SCC', 'code description', 'data category',
                                                     'scc level one', 'scc level two', 'scc level three', 'scc level four',
                                                     'sector', 'short name', 'status', 'tier 1 code', 'tier 1 description',
                                                     'tier 2 code', 'tier 2 description', 'tier 3 code', 'tier 3 description']].drop_duplicates()
    
            # Merge with FRS ID from the combined inventories.
            frs_id_df = pd.read_csv(os.path.join("..", "Data", "combinedinventories_NEI_TRI_GHGRP.csv"))
            frs_id_df = frs_id_df[['FacilityID', 'FRS_ID']].drop_duplicates()
            frs_id_df[['FacilityID', 'FRS_ID']] = frs_id_df[['FacilityID', 'FRS_ID']].astype('str')
            df1_sector_process = df1_sector_process.merge(frs_id_df, on=['FacilityID'], how='left')
    
            # Ensure FRS IDs in the global frs_id_list are strings.
            for i in range(len(frs_id_list)):
                frs_id_list[i] = str(float(frs_id_list[i]))
    
            if corrected_parquet_available_flag:
                df1_sector_process = df1_sector_process.loc[df1_sector_process['FRS_ID'].isin(frs_id_list)]
    
            df1_sector_process = df1_sector_process.drop_duplicates()
            df1_sector_process = add_cas_number(df1_sector_process)
    
            output_file = os.path.join(out_path2, f"{sector}_inventory_with_process_data_nei.csv")
            df1_sector_process.to_csv(output_file, index=False)
            logging.info(f"NEI process data exploration saved to {output_file}")
        except Exception as e:
            logging.error("Error in nei_by_process_exploration: %s", e)
    
    
    def ghgrp_process_exploration_unit(naics1, naics2, naics3, naics4):
        """
        Explores process-level emissions reported by the GHGRP database at the unit level.
        Combines process-level information from supplementary GHGRP files with STEWI data.
    
        Parameters
        ----------
        naics1 : int or str
            Primary NAICS code.
        naics2 : int or str
            Secondary NAICS code.
        naics3 : int or str
            Tertiary NAICS code.
        naics4 : int or str
            Quaternary NAICS code.
    
        Returns
        -------
        None
        """
        try:
            process_info_ghgrp = pd.read_csv(os.path.join("..", "Data", "Unit_process_info_GHGRP.csv"))
            process_info_ghgrp[['Facility Id', 'Industry Type (subparts)']] = process_info_ghgrp[['Facility Id', 'Industry Type (subparts)']].astype('str')
            process_info_ghgrp = process_info_ghgrp[['Facility Id', 'FRS Id', 'Primary NAICS Code', 'Industry Type (subparts)',
                                                     'Industry Type (sectors)', 'Unit Name', 'Unit Type', 'Unit Reporting Method',
                                                     'Unit Maximum Rated Heat Input Capacity (mmBTU/hr)', 'Unit CO2 emissions (non-biogenic) ',
                                                     'Unit Methane (CH4) emissions ', 'Unit Nitrous Oxide (N2O) emissions ',
                                                     'Unit Biogenic CO2 emissions (metric tons)']]
    
            process_info_ghgrp['FRS Id'] = pd.to_numeric(process_info_ghgrp['FRS Id'], errors='coerce')
            process_info_ghgrp.dropna(subset=['FRS Id'], inplace=True)
    
            if filename_flag:
                df1_sector = process_info_ghgrp.loc[process_info_ghgrp['Primary NAICS Code'].isin(naics_list)]
            else:
                df1_sector = process_info_ghgrp[(process_info_ghgrp['Primary NAICS Code'] == naics1) |
                                                (process_info_ghgrp['Primary NAICS Code'] == naics2) |
                                                (process_info_ghgrp['Primary NAICS Code'] == naics3) |
                                                (process_info_ghgrp['Primary NAICS Code'] == naics4)]
    
            df1_sector['FRS Id'] = df1_sector['FRS Id'].astype('float').astype('str')
            ghgrp_process_df = df1_sector.copy()
    
            if corrected_parquet_available_flag:
                ghgrp_process_df = ghgrp_process_df.loc[ghgrp_process_df['FRS Id'].isin(frs_id_list)]
    
            ghgrp_process_df['Facility Id'] = ghgrp_process_df['Facility Id'].astype('str')
            ghgrp_df = pd.read_csv(os.path.join("..", "Data", "facility_GHGRP.csv"))
            ghgrp_df[['FacilityID']] = ghgrp_df[['FacilityID']].astype('str')
            ghgrp_process_df = ghgrp_process_df.merge(ghgrp_df, left_on=['Facility Id'], right_on=['FacilityID'], how='left').drop_duplicates()
    
            output_file = os.path.join(out_path2, f"{sector}_inventory_with_process_data_ghgrp_unit_level.csv")
            ghgrp_process_df.to_csv(output_file, index=False)
            logging.info(f"GHGRP unit-level process data saved to {output_file}")
        except Exception as e:
            logging.error("Error in ghgrp_process_exploration_unit: %s", e)
    
    
    def ghgrp_process_exploration_fuel(naics1, naics2, naics3, naics4):
        """
        Explores process-level emissions reported by the GHGRP database at the fuel level.
        Combines process-level information from supplementary GHGRP fuel files with STEWI data.
    
        Parameters
        ----------
        naics1 : int or str
            Primary NAICS code.
        naics2 : int or str
            Secondary NAICS code.
        naics3 : int or str
            Tertiary NAICS code.
        naics4 : int or str
            Quaternary NAICS code.
    
        Returns
        -------
        None
        """
        try:
            process_info_ghgrp = pd.read_csv(os.path.join("..", "Data", "Fuel_process_info_GHGRP.csv"))
            process_info_ghgrp[['Facility Id', 'Industry Type (subparts)']] = process_info_ghgrp[['Facility Id', 'Industry Type (subparts)']].astype('str')
            process_info_ghgrp = process_info_ghgrp[['Facility Id', 'FRS Id', 'Primary NAICS Code', 'Industry Type (subparts)',
                                                     'Industry Type (sectors)', 'Unit Name', 'General Fuel Type',
                                                     'Specific Fuel Type', 'Other Fuel Name', 'Blend Fuel Name',
                                                     'Fuel Methane (CH4) emissions (mt CO2e)',
                                                     'Fuel Nitrous Oxide (N2O) emissions (mt CO2e)']]
    
            process_info_ghgrp['FRS Id'] = pd.to_numeric(process_info_ghgrp['FRS Id'], errors='coerce')
            process_info_ghgrp.dropna(subset=['FRS Id'], inplace=True)
    
            if filename_flag:
                df1_sector = process_info_ghgrp.loc[process_info_ghgrp['Primary NAICS Code'].isin(naics_list)]
            else:
                df1_sector = process_info_ghgrp[(process_info_ghgrp['Primary NAICS Code'] == naics1) |
                                                (process_info_ghgrp['Primary NAICS Code'] == naics2) |
                                                (process_info_ghgrp['Primary NAICS Code'] == naics3) |
                                                (process_info_ghgrp['Primary NAICS Code'] == naics4)]
    
            df1_sector['FRS Id'] = df1_sector['FRS Id'].astype('float').astype('str')
            ghgrp_process_df = df1_sector.copy()
    
            if corrected_parquet_available_flag:
                ghgrp_process_df = ghgrp_process_df.loc[ghgrp_process_df['FRS Id'].isin(frs_id_list)]
    
            ghgrp_process_df['Facility Id'] = ghgrp_process_df['Facility Id'].astype('str')
            ghgrp_df = pd.read_csv(os.path.join("..", "Data", "facility_GHGRP.csv"))
            ghgrp_df[['FacilityID']] = ghgrp_df[['FacilityID']].astype('str')
            ghgrp_process_df = ghgrp_process_df.merge(ghgrp_df, left_on=['Facility Id'], right_on=['FacilityID'], how='left').drop_duplicates()
    
            output_file = os.path.join(out_path2, f"{sector}_inventory_with_process_data_ghgrp_fuel_level.csv")
            ghgrp_process_df.to_csv(output_file, index=False)
            logging.info(f"GHGRP fuel-level process data saved to {output_file}")
        except Exception as e:
            logging.error("Error in ghgrp_process_exploration_fuel: %s", e)
    
    
    def ghgrp():
        """
        Reads GHGRP data files, compares them with the created facility-level inventory, and creates issue files listing various issues,
        including missing facilities and FRS mismatches.
    
        Returns
        -------
        list
            A list of NAICS codes extracted from the GHGRP flight file (if available).
        """
        try:
            ghgrp_raw_data = pd.read_excel(os.path.join("..", "Data", f"GHGRP_epa_{year}.xlsx"))
            ghgrp_raw_data = ghgrp_raw_data[['Facility Id', 'FRS Id', 'Facility Name', 'City', 'State', 'Zip Code',
                                             'Address', 'County', 'Latitude', 'Longitude', 'Primary NAICS Code']]
            ghgrp_raw_data[['Facility Id', 'FRS Id']] = ghgrp_raw_data[['Facility Id', 'FRS Id']].astype('str')
    
            naics_list_local = []
            ghgrp_raw_output = os.path.join("..", "Data", "GHGRP_facility_information_file.xlsx")
            ghgrp_raw_data.to_excel(ghgrp_raw_output, index=False)
            logging.info(f"GHGRP raw facility information saved to {ghgrp_raw_output}")
    
            if filename_flag:
                df_ghgrp = pd.read_excel(ghgrpfilename)
                df_ghgrp['GHGRP ID'] = df_ghgrp['GHGRP ID'].astype('str')
                df_ghgrp = df_ghgrp.merge(ghgrp_raw_data, left_on=['GHGRP ID'], right_on=['Facility Id'])
                naics_list_local = list(pd.unique(df_ghgrp['Primary NAICS Code']))
                logging.info("Extracted NAICS list from GHGRP Flight file.")
            else:
                df_ghgrp = ghgrp_raw_data[ghgrp_raw_data['Primary NAICS Code'] == naics1]
    
            f_sector_production = df_ghgrp.copy()
    
            df1_f_sector = pd.read_csv(os.path.join(out_path2, f"{sector}_inventory_facility_level.csv"))
            df1_f_sector = df1_f_sector.astype(str)
            df1_f_sector = df1_f_sector[['FRS_ID', 'FacilityName', 'Address', 'City', 'State',
                                         'Zip', 'Latitude', 'Longitude', 'County', 'NAICS', 'SIC', 'FacilityID', 'Source']]
            df1_f_sector = df1_f_sector[df1_f_sector['Source'] == 'GHGRP']
    
            f_sector_production = f_sector_production.astype(str)
    
            # Merge GHGRP facility production with the inventory.
            missing_facilities = f_sector_production.merge(df1_f_sector, left_on=['Facility Id'], right_on=['FacilityID']).drop_duplicates()
            missing_facilities['FRS_ID_Issues'] = np.where(missing_facilities['FRS Id'] == missing_facilities['FRS_ID'], False, True)
            missing_facilities = missing_facilities[missing_facilities['FRS_ID_Issues'] == True]
    
            issues_file_frs = os.path.join(out_path3, f"{sector}_ghgrp_issue_frs_mismatch.csv")
            missing_facilities.to_csv(issues_file_frs, index=False)
            logging.info(f"GHGRP FRS mismatch issues saved to {issues_file_frs}")
    
            missing_facilities = f_sector_production.merge(df1_f_sector, left_on=['Facility Id'], right_on=['FacilityID'], how='outer').drop_duplicates()
            missing_facilities = missing_facilities.fillna('issues')
            issues_with_ghgrp = missing_facilities[(missing_facilities['FRS Id'] == "issues") |
                                                   (missing_facilities['FRS_ID'] == "NaN") |
                                                   (missing_facilities['FRS_ID'].isna()) |
                                                   (missing_facilities['FRS_ID'] == 'nan') |
                                                   (missing_facilities['FRS_ID'] == "") |
                                                   (missing_facilities['FacilityID'] == None) |
                                                   (missing_facilities['FacilityID'] == 'issues') |
                                                   (missing_facilities['FRS_ID'] == 'issues')]
    
            issues_file_missing = os.path.join(out_path3, f"{sector}_ghgrp_issue_missing_facility.csv")
            issues_with_ghgrp.to_csv(issues_file_missing, index=False)
            logging.info(f"GHGRP missing facility issues saved to {issues_file_missing}")
    
            d = issues_with_ghgrp
            ids = list(pd.unique(issues_with_ghgrp['Facility Id']))
    
            dff1 = pd.read_csv(os.path.join("..", "Data", "flow_by_facility_GHGRP.csv"))
            dff3 = pd.read_csv(os.path.join("..", "Data", "combinedinventories_NEI_TRI_GHGRP.csv"))
    
            ghgrp_missing_in_stewi = pd.DataFrame()
            ghgrp_missing_in_combination_frs_fac_id_issue = pd.DataFrame()
            ghgrp_missing_other_issues = pd.DataFrame()
    
            for ide in ids:
                df2 = dff1[dff1['FacilityID'] == ide]
                if df2.empty:
                    df2 = d[d['Facility Id'] == ide]
                    ghgrp_missing_in_stewi = pd.concat([ghgrp_missing_in_stewi, df2])
                else:
                    df3_temp = dff3[dff3['FacilityID'] == ide]
                    if df3_temp.empty:
                        df3_temp = d[d['Facility Id'] == ide]
                        ghgrp_missing_in_combination_frs_fac_id_issue = pd.concat([ghgrp_missing_in_combination_frs_fac_id_issue, df3_temp])
                    else:
                        df4 = d[d['Facility Id'] == ide]
                        ghgrp_missing_other_issues = pd.concat([ghgrp_missing_other_issues, df4])
    
            ghgrp_missing_in_stewi.to_csv(os.path.join(out_path3, f"{sector}_ghgrp_missing_in_stewi.csv"), index=False)
    
            facility_ghgrp = pd.read_csv(os.path.join("..", "Data", "facility_GHGRP.csv"))
            if not ghgrp_missing_in_combination_frs_fac_id_issue.empty:
                ghgrp_missing_in_combination_frs_fac_id_issue = ghgrp_missing_in_combination_frs_fac_id_issue.merge(facility_ghgrp, left_on=['Facility Id'], right_on=['FacilityID'])
            ghgrp_missing_in_combination_frs_fac_id_issue.to_csv(os.path.join(out_path3, f"{sector}_ghgrp_missing_in_combination_frs_fac_id_issue.csv"), index=False)
    
            ghgrp_missing_other_issues.to_csv(os.path.join(out_path3, f"{sector}_ghgrp_missing_other_issues.csv"), index=False)
    
            return naics_list_local
        except Exception as e:
            logging.error("Error in ghgrp: %s", e)
            return []
    
    
    def nei(naics_list):
        """
        Reads the NEI data files, compares them with the facility-level inventory, and creates issue files listing various issues,
        including missing facilities and FRS mismatches.
    
        Parameters
        ----------
        naics_list : list
            List of NAICS codes for the industrial sector.
    
        Returns
        -------
        None
        """
        try:
            nei_raw_data = pd.read_excel(os.path.join("..", "Data", "NEI_facility_information_file.xlsx"))
            nei_raw_data['naics code'] = nei_raw_data['naics code'].astype('str')
            naics_list2 = [str(i) for i in naics_list]
            if filename_flag:
                f_sector = nei_raw_data.loc[nei_raw_data['naics code'].isin(naics_list2)]
            else:
                f_sector = nei_raw_data[(nei_raw_data['naics code'] == str(naics1)) | (nei_raw_data['naics code'] == str(naics2)) |
                                        (nei_raw_data['naics code'] == naics3) | (nei_raw_data['naics code'] == naics4)].drop_duplicates()
    
            nei_raw_data['naics description'] = nei_raw_data['naics description'].astype('str').str.lower()
    
            df1_f_sector = pd.read_csv(os.path.join(out_path2, f"{sector}_inventory_facility_level.csv"))
            df1_f_sector = df1_f_sector.astype(str)
            df1_f_sector = df1_f_sector[['FRS_ID', 'FacilityName', 'Address', 'City', 'State',
                                         'Zip', 'Latitude', 'Longitude', 'County', 'NAICS', 'SIC', 'FacilityID', 'Source']].drop_duplicates()
            df1_f_sector = df1_f_sector[df1_f_sector['Source'] == "NEI"]
    
            f_sector['eis facility id'] = f_sector['eis facility id'].astype('str')
            f_sector = f_sector.drop_duplicates()
    
            issues_with_nei_r = f_sector.merge(df1_f_sector, left_on='eis facility id', right_on='FacilityID', how="outer")
            issues_with_nei_r = issues_with_nei_r.fillna('issues')
            issues_with_nei = issues_with_nei_r[(issues_with_nei_r['eis facility id'] == "issues") |
                                                (issues_with_nei_r['FRS_ID'] == "NaN") |
                                                (issues_with_nei_r['FRS_ID'].isna()) |
                                                (issues_with_nei_r['FRS_ID'] == 'nan') |
                                                (issues_with_nei_r['FRS_ID'] == "") |
                                                (issues_with_nei_r['FacilityID'] == None) |
                                                (issues_with_nei_r['FacilityID'] == 'issues') |
                                                (issues_with_nei_r['FRS_ID'] == 'issues')]
    
            missing_facilities2 = issues_with_nei[['eis facility id', 'site name', 'FRS_ID', 'FacilityName', 'FacilityID', 'Source']].drop_duplicates()
            missing_facilities_file = os.path.join(out_path3, f"{sector}_nei_issue_missing_facility_using_naics_codes.csv")
            missing_facilities2.to_csv(missing_facilities_file, index=False)
            logging.info(f"NEI missing facility issues saved to {missing_facilities_file}")
    
            d = missing_facilities2
            ids = list(pd.unique(d['eis facility id']))
    
            dff1 = pd.read_csv(os.path.join("..", "Data", "flow_by_facility_NEI.csv"))
            dff3 = pd.read_csv(os.path.join("..", "Data", "combinedinventories_NEI_TRI_GHGRP.csv"))
    
            nei_missing_in_stewi = pd.DataFrame()
            nei_missing_in_combination_frs_fac_id_issue = pd.DataFrame()
            nei_missing_other_issues = pd.DataFrame()
    
            for ide in ids:
                df2 = dff1[dff1['FacilityID'] == ide]
                if df2.empty:
                    df2 = d[d['eis facility id'] == ide]
                    nei_missing_in_stewi = pd.concat([nei_missing_in_stewi, df2])
                else:
                    df3_temp = dff3[dff3['FacilityID'] == ide]
                    if df3_temp.empty:
                        df3_temp = d[d['eis facility id'] == ide]
                        nei_missing_in_combination_frs_fac_id_issue = pd.concat([nei_missing_in_combination_frs_fac_id_issue, df3_temp])
                    else:
                        df4 = d[d['eis facility id'] == ide]
                        nei_missing_other_issues = pd.concat([nei_missing_other_issues, df4])
    
            nei_missing_in_stewi.to_csv(os.path.join(out_path3, f"{sector}_nei_missing_in_stewi.csv"), index=False)
            facility_nei = pd.read_csv(os.path.join("..", "Data", "facility_NEI.csv"))
            if not nei_missing_in_combination_frs_fac_id_issue.empty:
                nei_missing_in_combination_frs_fac_id_issue = nei_missing_in_combination_frs_fac_id_issue.merge(facility_nei, left_on=['eis facility id'], right_on=['FacilityID'])
            nei_missing_in_combination_frs_fac_id_issue.to_csv(os.path.join(out_path3, f"{sector}_nei_missing_in_combination_frs_fac_id_issue.csv"), index=False)
            nei_missing_other_issues.to_csv(os.path.join(out_path3, f"{sector}_nei_missing_other_issues.csv"), index=False)
        except Exception as e:
            logging.error("Error in nei: %s", e)
    
    
    def tri(naics_list):
        """
        Reads TRI data files, compares them with the facility-level inventory, and creates issue files listing various issues,
        including missing facilities and FRS mismatches.
    
        Parameters
        ----------
        naics_list : list
            List of NAICS codes for the industrial sector.
    
        Returns
        -------
        None
        """
        try:
            tri_data = pd.read_excel(os.path.join("..", "Data", "TRI_Facility_information_file.xlsx"))
            tri_data['PRIMARY NAICS'] = tri_data['PRIMARY NAICS'].astype('str')
            naics_list2 = [str(i) for i in naics_list]
    
            if filename_flag:
                f_sector1 = tri_data.loc[tri_data['PRIMARY NAICS'].isin(naics_list2)]
            else:
                f_sector1 = tri_data[(tri_data['PRIMARY NAICS'] == str(naics1)) | (tri_data['PRIMARY NAICS'] == str(naics2)) |
                                    (tri_data['PRIMARY NAICS'] == str(naics3)) | (tri_data['PRIMARY NAICS'] == str(naics4))].drop_duplicates()
    
            df1_f_sector = pd.read_csv(os.path.join(out_path2, f"{sector}_inventory_facility_level.csv"))
            df1_f_sector = df1_f_sector.astype(str)
            df1_f_sector = df1_f_sector[['FRS_ID', 'FacilityName', 'Address', 'City', 'State',
                                         'Zip', 'Latitude', 'Longitude', 'County', 'NAICS', 'SIC', 'FacilityID', 'Source']].drop_duplicates()
            df1_f_sector = df1_f_sector[df1_f_sector['Source'] == "TRI"]
            df1_f_sector['FacilityID'] = df1_f_sector['FacilityID'].astype('str')
            f_sector1['TRIFD'] = f_sector1['TRIFD'].astype('str')
    
            issues_with_tri_r = f_sector1.merge(df1_f_sector, left_on=["TRIFD"], right_on=["FacilityID"], how="outer", indicator=True)
            issues_with_tri_r = issues_with_tri_r.fillna('issues').drop_duplicates()
            issues_with_tri = issues_with_tri_r[(issues_with_tri_r['TRIFD'] == "issues") | (issues_with_tri_r['FACILITY NAME'] == "issues") |
                                                (issues_with_tri_r['FRS_ID'] == "NaN") | (issues_with_tri_r['FRS_ID'].isna()) |
                                                (issues_with_tri_r['FRS_ID'] == 'nan') | (issues_with_tri_r['FRS_ID'] == "") |
                                                (issues_with_tri_r['FacilityID'] == None) | (issues_with_tri_r['FacilityID'] == 'issues') |
                                                (issues_with_tri_r['FRS_ID'] == 'issues') | (issues_with_tri_r['Source'] == 'issues')]
    
            issues_file_tri = os.path.join(out_path3, f"{sector}_tri_issue_missing_facility_all_using_naics_code.csv")
            issues_with_tri.to_csv(issues_file_tri, index=False)
            logging.info(f"TRI missing facility issues saved to {issues_file_tri}")
    
            if corrected_parquet_available_flag:
                issues_with_tri_ghgrp_only = issues_with_tri.loc[issues_with_tri['FRS_ID'].isin(frs_id_list)]
            else:
                issues_with_tri_ghgrp_only = issues_with_tri
    
            issues_with_tri_frs_issues = issues_with_tri_ghgrp_only.copy()
            issues_with_tri_frs_issues['FRS_ID_Issues'] = np.where(issues_with_tri_frs_issues['FRS ID'] == issues_with_tri_frs_issues['FRS_ID'], True, False)
            issues_with_tri_frs_issues = issues_with_tri_frs_issues[issues_with_tri_frs_issues['FRS_ID_Issues'] == True]
    
            issues_file_tri_frs = os.path.join(out_path3, f"{sector}_tri_issue_frs_mismatch.csv")
            issues_with_tri_frs_issues.to_csv(issues_file_tri_frs, index=False)
            logging.info(f"TRI FRS mismatch issues saved to {issues_file_tri_frs}")
    
            d = issues_with_tri
            ids = list(pd.unique(d['TRIFD']))
            dff1 = pd.read_csv(os.path.join("..", "Data", "flow_by_facility_TRI.csv"))
            dff3 = pd.read_csv(os.path.join("..", "Data", "combinedinventories_NEI_TRI_GHGRP.csv"))
    
            tri_missing_in_stewi = pd.DataFrame()
            tri_missing_in_combination_frs_fac_id_issue = pd.DataFrame()
            tri_missing_other_issues = pd.DataFrame()
    
            for ide in ids:
                df2 = dff1[dff1['FacilityID'] == ide]
                if df2.empty:
                    df2 = d[d['TRIFD'] == ide]
                    tri_missing_in_stewi = pd.concat([tri_missing_in_stewi, df2])
                else:
                    df3_temp = dff3[dff3['FacilityID'] == ide]
                    if df3_temp.empty:
                        df3_temp = d[d['TRIFD'] == ide]
                        tri_missing_in_combination_frs_fac_id_issue = pd.concat([tri_missing_in_combination_frs_fac_id_issue, df3_temp])
                    else:
                        df4 = d[d['TRIFD'] == ide]
                        tri_missing_other_issues = pd.concat([tri_missing_other_issues, df4])
    
            issues_file_tri_ghgrp = os.path.join(out_path3, f"{sector}_tri_issue_missing_facility_ghgrp_only.csv")
            issues_with_tri_ghgrp_only.to_csv(issues_file_tri_ghgrp, index=False)
            logging.info(f"TRI missing facility (GHGRP only) issues saved to {issues_file_tri_ghgrp}")
    
            tri_missing_in_stewi.to_csv(os.path.join(out_path3, f"{sector}_tri_missing_in_stewi.csv"), index=False)
            tri_missing_in_combination_frs_fac_id_issue.to_csv(os.path.join(out_path3, f"{sector}_tri_missing_in_combination_frs_fac_id_issue.csv"), index=False)
            tri_missing_other_issues.to_csv(os.path.join(out_path3, f"{sector}_tri_missing_other_issues.csv"), index=False)
        except Exception as e:
            logging.error("Error in tri: %s", e)
    
    
    # Execute the exploration functions.
    try:
        naics_list = ghgrp()
        tri(naics_list)
        nei(naics_list)
        nei_by_process_exploration(naics1, naics2, naics3, naics4)
        ghgrp_process_exploration_unit(naics1, naics2, naics3, naics4)
        ghgrp_process_exploration_fuel(naics1, naics2, naics3, naics4)
    except Exception as e:
        logging.error("Error during NEI/TRI/GHGRP explorations: %s", e)
    
