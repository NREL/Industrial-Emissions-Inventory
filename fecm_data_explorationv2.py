# FECM data exploration v2.0

"""
The FECM Inventory Builder achieves three objectives
1. Runs STEWI to download and obtain databases from three sources - GHGRP, NEI and TRI
2. Compiles the databases and builds emissions inventories for individual sectors
3. Compiles emissions inventories at the facility and the process level
4. Compiles stack parameter information for facilities in the NEI
"""


import pandas as pd
import seaborn as sns
import sys
import numpy as np
import matplotlib.pyplot as plt
import warnings
# importing os module
import os
warnings.filterwarnings("ignore")
from datetime import datetime
dt = datetime.today().strftime('%Y-%m-%d')


"Using current date as results folder name"
result_folder = dt
out_path1 = "../"+dt
try:
    os.mkdir(out_path1)
except:
    pass

def main(sector,filename_flag,naics,corrected_parquet_available_flag,flag_for_running_stewi):    
    '''
    Overarching function that contains all the invidual functions for compiling the emission inventories
    Its called by the run file to create the FECM inventory

    Parameters 
    ----------
    sector: string
    filename_flag: boolean
    nacis code: int
    corrected parquet available flag: boolean
    

    Output
    ------
    
    csv files with
    1. facility level inventory
    2. process level inventory
    3. issues files with errors
    4. files with stack parameter information
    

    Returns
    -------
    None.
    '''
    
    # Flags
    option = "GHGRP Not Preferred"
    
    """
    Dictionary with information about GHGRP flight tool exported file names and literature obtained CO2 concentration of flue gas from NETL
    """
    data={}
    data['cement'] = {}
    data['cement']['filename'] = "flight_cement.xls"
    data['cement']['co2concentration'] = 0.3183
    
    data['steel'] = {}
    data['steel']['filename'] = "flight_steel.xls"
    data['steel']['co2concentration'] = 0.3515
    
    data['ethanol'] = {}
    data['ethanol']['filename'] = "flight_ethanol.xls"
    data['ethanol']['co2concentration'] = 0.999999
    
    data['ammonia'] = {}
    data['ammonia']['filename'] = "flight_ammonia.xls"
    data['ammonia']['co2concentration'] = 0.987887169
    
    data['hydrogen'] = {}
    data['hydrogen']['filename'] = "flight_hydrogen.xls"
    data['hydrogen']['co2concentration'] = 0.999999
    
    data['pulp'] = {}
    data['pulp']['filename'] = "flight_pulp.xls"
    data['pulp']['co2concentration'] = 0.9999999
    
    data['refining'] = {}
    data['refining']['filename'] = "flight_refining.xls"
    data['refining']['co2concentration'] = 0.9999999
    
    data['natural_gas_processing'] = {}
    data['natural_gas_processing']['filename'] = "flight_natural_gas_processing_agr.xlsx"
    data['natural_gas_processing']['co2concentration'] = 0.995884774
    
    out_path2 = out_path1 + "/" + sector + "/"
    try:
        os.mkdir(out_path2)
    except:
        pass

    """
    naics codes for sectors when codes are not available from the FLIGHT Tool
    multiple naics codes can be provided if required although not necessary for inventories under study
    """
    naics1 = naics
    naics2 = ""
    naics3 = ""
    naics4 = ""
    """
    filename for reading GHGRP data from the FLIGHT Tool
    """
    ghgrpfilename = "../Data/"+data[sector]['filename']
 
    def save_data(inventory, year):
        '''
        Saves individual inventory information from STEWI. It can save both process and facility level database info
        Saves facility information required for compiling final database. 
    
        Parameters 
        ----------
        inventory: str
            Name of the inventory.
        year:int
            Year being analyzed.
    
        Output
        ------
        writes csv files for every inventory and year requested to the function.
        writes csv files for storing facility information for every inventory. 
    
        Returns
        -------
        None.
        '''
    
        # Running STEWI to get inventory by facility.
        flow_by_facility = stewi.getInventory(
            inventory, year, 'flowbyfacility', filters=['filter_for_LCI'])
        flow_by_facility.to_csv('../Data/flow_by_facility_' +
                                inventory+'.csv', index=False)
        try:
            # Not all databases have process level information.
            flow_by_process = stewi.getInventory(
                inventory, year, 'flowbyprocess', filters=['filter_for_LCI'])
            flow_by_process['Year'] = year
            flow_by_process.to_csv(
                '../Data/flow_by_process_'+inventory+'.csv', index=False)
        except:
            print('not available for')
            print(inventory)
    
        facility = stewi.getInventoryFacilities(inventory, year)
        facility.to_csv('../Data/facility_'+inventory+'.csv', index=False)
    
    
    def combine_facility_name_inv(a, b, c):

        '''
        Combines facility names from Stewi for debugging purposes

        Parameters 
        ----------
        a: str
            Name of the inventory.
        b: str
            Name of the inventory.
        c: str
            Name of the inventory.

    
        Output
        ------
        Writes csv files with all facility names combined with source of facility
  
        Returns
        -------
        None.
        '''       
        d1 = pd.read_csv('../Data/facility_'+a+'.csv')
        d2 = pd.read_csv('../Data/facility_'+b+'.csv')
        d3 = pd.read_csv('../Data/facility_'+c+'.csv')
    
        d4 = pd.concat([d1, d2, d3])
        d4.to_csv('../Data/FacilityNamesFromStewicombined.csv', index=False)
    
    
    def run_stewi(option):
        '''
        Saves combined inventory information from STEWI. It can save only facility level database info
    
        Parameters 
        ----------
        option: str
        GHGRP as the primary inventory option for the main stewi run
    
        Output
        ------
        writes csv files for combined inventory and year requested to the function. 
    
        Returns
        -------
        None.
        '''
    
        inventory = 'GHGRP'
        year = '2017'
        # Run this for individual inventory
        save_data(inventory, year)
    
        inventory = 'NEI'
        year = '2017'
        # Run this for individual inventory
        save_data(inventory, year)
    
        inventory = 'TRI'
        year = '2017'
        # Run this for individual inventory
        save_data(inventory, year)
    
        # Obtain and save combined inventory from STEWI with and without overlapping and filters.
        inventory_dict = {"GHGRP": "2017", "NEI": "2017", "TRI": "2017"}
    
        if option != "GHGRP Preferred":
    
            print('Combining full inventories')
            df = stewicombo.combineFullInventories(
                inventory_dict, filter_for_LCI=True, remove_overlap=True, compartments=None)
    
        else:
            print('GHGRP preferred')
            df = stewicombo.combineInventoriesforFacilitiesinBaseInventory("GHGRP",
                                                                           inventory_dict,
                                                                           filter_for_LCI=True,
                                                                           remove_overlap=True)
    
        df.to_csv('../Data/combinedinventories_NEI_TRI_GHGRP.csv', index=False)
    
        combine_facility_name_inv('NEI', 'TRI', 'GHGRP')
    
    
    if flag_for_running_stewi:
        # Obtain information from stewi and save the stewi files. If stewi files are present no need to run.
        run_stewi(option)

    
    def add_cas_number(df_em):


        pollutants_all = pd.read_csv('../Data/pollutants_to_cas.csv')
        df_em = pd.merge(df_em, pollutants_all, on = "FlowName", how = 'left')       
        return df_em 


        

    def manipulate_databases(naics_code1, naics_code2, naics_code3, naics_code4):
        '''
        Reads the combined inventories file from NEI TRI and GHGRP. Then manipulates the data, combines it with NEI, TRI and GHGRP facility files
        Reads the facility information files for every inventory.
        Performs simple concentration calculation.
        The concentration calculation is done only for reported emissions. 
    
        Parameters 
        ----------
        naics_code1: int
            NAICS code for requested industrial sector.
    
        Output
        ------
            writes csv files for combined inventory and year requested to the function. 

    
        Returns
        -------
            df1_sector: pandas DataFrame
            sector facilities with emisison information and facility information obtained from Stewi
            naics list: list of ints
            list of naics codes selected for a sector
            frs_id_list: list of strs
            
            
        '''
    
        # Read all the necessary files generated from previous functions.
        combined_df = pd.read_csv('../Data/combinedinventories_NEI_TRI_GHGRP.csv')
        nei_df = pd.read_csv('../Data/facility_NEI.csv')
        tri_df = pd.read_csv('../Data/facility_TRI.csv')
        ghgrp_df = pd.read_csv('../Data/facility_GHGRP.csv')
    
    
        # Combining emissions and facility information databases.
        combined_df = combined_df[['FacilityID', 'FlowName', 'Compartment',
                                   'FlowAmount', 'Unit', 'DataReliability', 'Source', 'Year', 'FRS_ID']]
        combined_df[['FacilityID', 'Source']] = combined_df[[
            'FacilityID', 'Source']].astype('str')
        
        # Providing database source information
        nei_df['Source'] = "NEI"
        tri_df['Source'] = "TRI"
        ghgrp_df['Source'] = "GHGRP"
    
        
        df_facility = pd.concat([ghgrp_df, nei_df, tri_df])
        naics_list = []
    
        df_facility[['FacilityID', 'Source']] = df_facility[[
            'FacilityID', 'Source']].astype('str')
        
        #Combining the facility information with the emissions inventory
        df1 = combined_df.merge(df_facility, on=[
                                'FacilityID', 'Source'], indicator=True, how='left').drop_duplicates()
        
        frs_fac_df = df1[['FacilityID', 'FRS_ID']].drop_duplicates()
        
        
        df1 = df1[['FlowName', 'Compartment', 'FlowAmount', 'Unit',
                   'DataReliability', 'Source', 'Year', 'FRS_ID', 'FacilityName',
                   'Address', 'City', 'State', 'Zip', 'Latitude', 'Longitude', 'County',
                   'NAICS', 'SIC', 'FacilityID']].drop_duplicates()
    
        df_nei_tri = df1[(df1['Source'] == "NEI") | (df1['Source'] == "TRI")]
    
        
        """
        If the file name flag is true that means that GHGRP Flight file is present. THus the NAICS code are extracted from the Flight file for the corressponding sector
        These NAICS code are then used to extract out sector wise facility from the combined STEWI inventory
        """
        if filename_flag:
    
            dfghgrp2 = df1[df1['Source'] == 'GHGRP']
            # Need to merge on Facility ID
            dfghgrp2[['FacilityID', 'Source']] = dfghgrp2[['FacilityID', 'Source']].astype('str')
            # keeping only air emissions.
            df1_air = df1[df1['Compartment'] == "air"]
            df_ghgrp = pd.read_excel(ghgrpfilename)
            df_ghgrp['GHGRP ID'] = df_ghgrp['GHGRP ID'].astype('str')
            df_ghgrp = df_ghgrp.merge(
                df1_air, left_on=['GHGRP ID'], right_on=['FacilityID'])
            
            df_ghgrp = df_ghgrp[['FlowName', 'Compartment', 'FlowAmount', 'Unit',
                                 'DataReliability', 'Source', 'Year', 'FRS_ID', 'FacilityName',
                                 'Address', 'City', 'State', 'Zip', 'Latitude', 'Longitude', 'County',
                                 'NAICS', 'SIC', 'FacilityID']]
            
            naics_list = list(pd.unique(df_ghgrp['NAICS']))
            print(naics_list,flush=True)
            df_ghgrp['FRS_ID'] = df_ghgrp['FRS_ID'].astype('str')
            
            df4 = df_nei_tri.loc[df_nei_tri['NAICS'].isin(naics_list)]
            df1_sector = pd.concat([df_ghgrp, df4])
            df1_sector['FRS_ID'] = df1_sector['FRS_ID'].astype('str')
            df_ghgrp_frs = df1_sector[df1_sector['Source'] == 'GHGRP']
            frs_id_list = list(pd.unique(df_ghgrp_frs['FRS_ID']))
            df1_sector = add_cas_number(df1_sector)
            df1_sector.to_csv(
            out_path2+sector+'_'+'inventory_facility_level_including_all_facilities_from_all_db.csv', index=False)
            # Including only GHGRP facilities if correcred parquet is available
            """
            If all necessary corrections to facilities, IDS, inventories have been performed and a corrected STEWI parquet file is available, the code needs to drop all facilities that do not report exclusively to GHGRP. 
            We keep the facilities from other inventories before the corrections so that facilities with ID issues do not get dropped and are properly combined across inventories via our debugger correction method. 
            """
            if corrected_parquet_available_flag:
                df1_sector = df1_sector.loc[df1_sector['FRS_ID'].isin(frs_id_list)]
    
        else:
            """
            NAICS code chosen from User inputs
            """
    
            # Need to merge on Facility ID
            df_facility[['FacilityID', 'Source']] = df_facility[[
                'FacilityID', 'Source']].astype('str')
            df1 = combined_df.merge(df_facility, on=[
                                    'FacilityID', 'Source'], indicator=True, how='left').drop_duplicates()
            frs_fac_df = df1[['FacilityID', 'FRS_ID']].drop_duplicates()
            #frs_fac_df.to_csv('../Data/FRS_ID_FAC_ID.csv', index=False)
            df1 = df1[['FlowName', 'Compartment', 'FlowAmount', 'Unit',
                       'DataReliability', 'Source', 'Year', 'FRS_ID', 'FacilityName',
                       'Address', 'City', 'State', 'Zip', 'Latitude', 'Longitude', 'County',
                       'NAICS', 'SIC', 'FacilityID']].drop_duplicates()
    
            # keeping only air emissions.
            df1_air = df1[df1['Compartment'] == "air"]

            df1_sector = df1_air[(df1_air['NAICS'] == naics_code1) | (df1_air['NAICS'] == naics_code2)]
            
            # Including only GHGRP facilities if correcred parquet is available
            df_ghgrp = df1_sector[df1_sector['Source']
                                  == "GHGRP"].drop_duplicates()
            df_ghgrp['FRS_ID'] = df_ghgrp['FRS_ID'].astype('str')
            frs_id_list = list(pd.unique(df_ghgrp['FRS_ID']))
            df1_sector['FRS_ID'] = df1_sector['FRS_ID'].astype('str')
            if corrected_parquet_available_flag:
                df1_sector = df1_sector.loc[df1_sector['FRS_ID'].isin(frs_id_list)]
    
        
        df1_sector['FRS_ID'] = df1_sector['FRS_ID'].astype('str')
        df1_sector = add_cas_number(df1_sector)
        df1_sector.to_csv(
            out_path2+sector+'_'+'inventory_facility_level.csv', index=False)
    
        return df1_sector, naics_list, frs_id_list
        # saved inventory for sector facilities with facility data
    
    
    df1_sector, naics_list, frs_id_list = manipulate_databases(naics1, naics2, naics3, naics4)
    
    
    def facility_stack_parameters(df1_sector):
        '''
        This function combines facility information with available stack parameters from the NEI database. Facilities not reporting to the NEI database
        do not have any stack data. 

        Parameters
        ----------
        df1_sector:  pandas DataFrame

        Outputs
        -------
        writes csv files with stack parameter information for available facilities
        Dataframe with columns - Emissions, facility ID, Facility name and Other information and Stack data.

        Returns
        -------
        df_stack_data:  pandas DataFrame
        Dataframe with columns - Emissions, facility ID, Facility name and Other information and Stack data.

        '''

        # Reading the NEI stack parameter data file and writing the results file with stack parameters

        nei_flat_file = pd.read_csv('../Data/reduced_stack_file.csv',low_memory=False)
        nei_flat_file['FACILITY_ID'] = nei_flat_file['FACILITY_ID'].astype('str')
        nei_flat_file = nei_flat_file[['FACILITY_ID','UNIT_ID', 'PROCESS_ID','STKHGT',
                               'STKDIAM',                   'STKTEMP',
                               'STKFLOW',                    'STKVEL',
                                 ]]                       
        df_inv = df1_sector
        df_inv['FacilityID'] = df_inv['FacilityID'].astype('str')
        nei_flat_file_cem = df_inv.merge(nei_flat_file,left_on = ['FacilityID'], right_on = ['FACILITY_ID'])
        nei_flat_file_cem['FACILITY_ID'] = nei_flat_file_cem['FACILITY_ID'].astype('str')
        nei_flat_file_cem['PROCESS_ID'] = nei_flat_file_cem['PROCESS_ID'].astype('str')
        nei_flat_file_cem['STKFLOW'] = pd.to_numeric(nei_flat_file_cem['STKFLOW'], errors = 'coerce')


        facility_info = pd.read_csv("../Data/facility_NEI.csv")
        facility_info['FacilityID'] = facility_info['FacilityID'].astype('str')
        flat_fac = nei_flat_file_cem[['FACILITY_ID','STKHGT',
                               'STKDIAM',                   'STKTEMP',
                               'STKFLOW',                    'STKVEL',
                                 ]].drop_duplicates()

        flat_fac = flat_fac.merge(facility_info, left_on = ['FACILITY_ID'], right_on = ['FacilityID'])
        flat_fac.to_csv(out_path2+sector+'_'+'nei_flat_fac_level_info_for_stack_parameters.csv', index = False)


        flat_unit = nei_flat_file_cem[['FACILITY_ID','STKHGT',
                               'STKDIAM',                   'STKTEMP',
                               'STKFLOW',                    'STKVEL',
                                'UNIT_ID','PROCESS_ID']].drop_duplicates()
        nei_flow_by_process = pd.read_csv('../Data/flow_by_process_NEI.csv')
        nei_flow_by_process['FacilityID'] = nei_flow_by_process['FacilityID'].astype('str')
        nei_flow_by_process['Process'] = pd.to_numeric(nei_flow_by_process['Process'], errors = 'coerce')
        flat_unit = flat_unit.merge(facility_info, left_on = ['FACILITY_ID'], right_on = ['FacilityID'])
        flat_unit.to_csv(out_path2+sector+'_'+'nei_flat_process_unit_level_info_for_stack_parameters.csv', index = False)
        return None


    df_stack_data = facility_stack_parameters(df1_sector)
    

    def overlap_explorer(df1_sector):
        '''
        This function tries to find overlap between databases and facilities. It tries to find the number of databases that report for each
        facility or FRS ID and prints it in a column. Along with that, it also prints a column with the total number of emissions showing up for every facility
    
        Having a dataframe with names creates duplicates due to name mismatches. A separate df is created without names. 
    
        Parameters
        ----------
        df1_sector:  pandas DataFrame
    
        Outputs
        -------
        Dataframe with columns - FRS ID, Source databases, emission flow list, CO2 missing flag and Name of facility
        Dataframe with same columns but no Names to prevent duplicates. 
    
        Returns
        -------
        None.
    
    
        '''
    
        frs_id = list(pd.unique(df1_sector['FRS_ID']))
        frs_id_df = df1_sector[['FRS_ID', 'Source', 'FlowName']].drop_duplicates()
        name_df = df1_sector[['FRS_ID', 'FacilityName', 'City',
                              'State', 'Latitude', 'Longitude']].drop_duplicates()
    
        source = []
        frs_a = []
        flw = []
        co2 = []
    
        # Creating a list of FRS Ids to scroll through and extract out data.
        for frs in frs_id:
            chk = frs_id_df[frs_id_df['FRS_ID'] == frs]
    
            source_list = list(pd.unique(chk['Source']))
            flw_list = list(pd.unique(chk['FlowName']))
            source_list.sort()
            flw_list.sort()
            source.append(' '.join(source_list))
            flw.append(flw_list)
            if "Carbon Dioxide" in list(pd.unique(chk['FlowName'])):
                co2.append(False)
            else:
                co2.append(True)
            frs_a.append(frs)
    
        overlap_df = pd.DataFrame()
        overlap_df['FRS_ID'] = frs_a
        overlap_df['Source'] = source
        overlap_df['Flows'] = flw
        overlap_df['CO2 missing'] = co2
        overlap_df = overlap_df.merge(name_df, on='FRS_ID')
    
        overlap_df['city'] = overlap_df['City'].str.lower()
        overlap_df = overlap_df.drop_duplicates(
            subset=['FRS_ID', 'city'], keep='first')
        overlap_df.to_csv(out_path2+sector+'_'+'overlap_explorer_with_facility_names.csv', index = False)
    
        overlap_df_corr = pd.DataFrame()
        cities = list(pd.unique(overlap_df['city']))
        cities.sort()
        
        for c in cities:
            d = overlap_df[overlap_df['city'] == c]
            if len(d) > 1:
                sources_names = ' '.join(pd.unique(d['Source']))   
                sources_names = sources_names.split()
                sources_names_1 = " ".join(
                    sorted(set(sources_names), key=sources_names.index))
                #This drop is assuming that GHGRP is mandatory here. 
                if (sources_names_1 == "NEI") | (sources_names_1 == "TRI") | (sources_names_1 == "NEI TRI") | (sources_names_1 == "TRI NEI"):
                    print(sources_names_1)
                    pass
                else:
                    
                    overlap_df_corr = pd.concat([overlap_df_corr, d])
        
        if not overlap_df_corr.empty:
            overlap_df_corr.to_csv(out_path2+sector+'_'+'overlap_explorer_with_facility_names.csv', index=False)
        
            del overlap_df_corr['FacilityName']
            overlap_df_w_name = overlap_df_corr
            overlap_df_w_name.to_csv(
            out_path2+sector+'_'+'overlap_explorer_without_facility_names.csv', index=False)
    
    
    overlap_explorer(df1_sector)
    
    # %%
    def emission_concentration_calculation(df1_sector):
        '''
        This function focussed on specific emissions of interest and tries some exploratory analysis.
        Calculates emission concentration by summing up all the reported emissions and then calculate
        concentration by dividing individual emissions by total emission. It also extracts out facilities
        which report CO2 (along with any other emission) and just focusses analysis on those facilities.
    
        Parameters
        ----------
        df1_sector: pandas DataFrame
    
        Outputs
        -------
    
        data frame with concentration calculated
        data frame with concentration calculated that mandatorily reports CO2.
    
        Returns
        -------
        sector_df2: pandas DataFrame
            Dataframe of sector facilities which report CO2 emissions along with all others.
    
        '''
    
        co2 = df1_sector[df1_sector['FlowName'] == "Carbon Dioxide"]
    
        co2_reporting_facilities = list(pd.unique(co2['FRS_ID']))
    
        sector_id = list(pd.unique(df1_sector['FRS_ID']))
        sector_df1 = pd.DataFrame()
        for ide in sector_id:
            df1 = df1_sector[df1_sector['FRS_ID'] == ide]
            total = sum(df1['FlowAmount'])
            df1['FlowSum'] = total
            df1['Concentration kg/kg'] = df1['FlowAmount']/total
            sector_df1 = pd.concat([sector_df1, df1])
    
        sector_df1 = sector_df1[['FRS_ID', 'FlowName', 'Compartment', 'FlowAmount', 'FlowSum', 'Unit', 'Concentration kg/kg',
                                 'Source', 'Year', 'FacilityName', 'State', 'Latitude', 'Longitude', 'County', 'NAICS', 'SIC', 'FacilityID']]
        #sector_df1.to_csv('../Data/sector_inventory_with_concentration.csv', index = False)
    
        # Extracting out only CO2 flows and then remerging it back with the total dataset to only include those facilities
        # that mandatorily include CO2 emission
        sector_facilities_with_co2 = sector_df1[sector_df1['FlowName']
                                                == 'Carbon Dioxide']
        sector_facilities_with_co2 = sector_facilities_with_co2[[
            'FRS_ID']].drop_duplicates()
    
        sector_df2 = sector_df1.merge(sector_facilities_with_co2, on=['FRS_ID'])

        sector_df3 = sector_df2[['FRS_ID', 'FlowName', 'Compartment', 'FlowAmount', 'FlowSum', 'Unit',
                                                       'Source', 'Year', 'FacilityName', 'State', 'Latitude', 'Longitude', 'County', 'NAICS', 'SIC', 'FacilityID','Concentration kg/kg']]

        sector_df3 = add_cas_number(sector_df3)
        sector_df3.to_csv(out_path2+sector+'_'+'inventory_facility_level_emissions_with_co2_conc_calculated.csv', index=False)

        return sector_df2
    
    
    sector_df2 = emission_concentration_calculation(df1_sector)
    
    def plots_and_exploration1(sector_df2):
        '''
        This function plots and epxlores cap emissions in details. it explores the concentration 
        calculated from the reported emissions.
    
        Parameters
        ----------
        sector_df2 : pandas Dataframe
            Dataframe for sector facilities with CO2 emission
    
        Returns
        -------
        None.
    
        '''
    
        cap_emissions = ['Carbon Dioxide', 'Methane', 'Nitrogen Oxides', 'Nitrous Oxide',
                         'Sulfur Dioxide', 'Volatile Organic Compounds', 'PM10-PM2.5', 'PM2.5 Primary (Filt + Cond)']
        sector_inv_for_cap_em = pd.DataFrame()
        for em in cap_emissions:
    
            co2 = sector_df2[sector_df2['FlowName'] == em]
            sector_inv_for_cap_em = pd.concat([sector_inv_for_cap_em, co2])
            #print("For emission "+em+" total number of facilities obtained is "+str(len(co2['FRS_ID'])))
            fig = plt.figure()
            sns.scatterplot(x=co2['FRS_ID'], y=co2['Concentration kg/kg'])
            plt.tick_params(
                axis='x',          # changes apply to the x-axis
                which='both',      # both major and minor ticks are affected
                bottom=False,      # ticks along the bottom edge are off
                top=False,         # ticks along the top edge are off
                labelbottom=False)
            plt.title(em+"_concentrationkg/kg")
            #fig.savefig("../Data/"+em+"_concentrationkgperkg.png", dpi = 200)
    
            fig = plt.figure()
            sns.scatterplot(x=co2['FRS_ID'], y=co2['FlowAmount'])
            plt.tick_params(
                axis='x',          # changes apply to the x-axis
                which='both',      # both major and minor ticks are affected
                bottom=False,      # ticks along the bottom edge are off
                top=False,         # ticks along the top edge are off
                labelbottom=False)
            plt.title(em)
            plt.title(em+"_flowamount_.png")
            '''
            fig.savefig("../Data/"+em+"_flowamount_.png", dpi = 200)
            '''
    
    
    plots_and_exploration1(sector_df2)
    
    # %%
    def concentration_calculation(df1_sector):
        '''
        This function accepts a file with flow emissions data. It adds a column containing the CO2 emissions flow data for each facility, divides it by xx (for f_sector) to 
        calculate the total amount of flue gas.
        This total flue gas amount is further used to calculated the emissions for all other pollutants. These back calculated concentrations are present in the output.
    
        Parameters
        ----------
        df1_sector: pandas DataFrame
    
        Outputs
        -------
    
    
        Returns
        -------
    
    
        '''
        # create df of facilities which have CO2 data, and store only the amount of CO2 being emittes
        data_co2_only = df1_sector.loc[df1_sector["FlowName"] == "Carbon Dioxide", [
            "FRS_ID", "FlowAmount", "Source"]]
        data_co2_only = data_co2_only.rename(
            columns={"FlowAmount": "CO2Amount", "Source": "CO2Source"})
    
        ############################THIS IS NOT NEEDED NOW THAT WE HAVE SOLVED THE DUPLIUCATE IDS ISSUE######################
        # create list of FRS IDs of facilities which have a CO2 flow from the GHGRP database
        #data_co2_only_ghgrp_FRS_IDs = list(data_co2_only[data_co2_only["CO2Source"] == "GHGRP"]["FRS_ID"])
        # drop facilities from CO2 data which have a source of NEI or TRI
        #data_co2_only.drop(data_co2_only[(data_co2_only["CO2Source"] == "NEI") & (data_co2_only["FRS_ID"].isin(data_co2_only_ghgrp_FRS_IDs))].index, inplace = True)
        #data_co2_only.drop(data_co2_only[(data_co2_only["CO2Source"] == "TRI") & (data_co2_only["FRS_ID"].isin(data_co2_only_ghgrp_FRS_IDs))].index, inplace = True)
        ########################################################################################################################
    
        # Create df with of total facility emissions minus CO2 flow for each facility
        data_all_emissions_minus_CO2 = df1_sector.loc[df1_sector["FlowName"] != "Carbon Dioxide", [
            "FRS_ID", "FlowAmount"]]  # check if this not equal sign works
        data_all_emissions_minus_CO2 = data_all_emissions_minus_CO2.rename(
            columns={"FlowAmount": "TotalFlowAmountminusCO2"})
        data_sum_all_emissions_minus_CO2 = data_all_emissions_minus_CO2.groupby(
            ["FRS_ID"])["TotalFlowAmountminusCO2"].agg('sum').reset_index()
        ###
        # merge CO2 flow amount and total flow minus CO2 flow amount for each facility with the main df
        df1_sector = pd.merge(left=df1_sector, right=data_co2_only,
                              how="left", left_on="FRS_ID", right_on="FRS_ID")
        df1_sector = pd.merge(left=df1_sector, right=data_sum_all_emissions_minus_CO2,
                              how="left", left_on="FRS_ID", right_on="FRS_ID")
        # calculate total flue gas. conc of CO2 in flue gas consisting of CO2, O2, N2 and H2O only. Thus, we first calculate the flue gas containing these 4 elements, then add the other flow pollutants to it
        df1_sector["Total_Flue_Gas_Amount"] = df1_sector["CO2Amount"] / \
           data[sector]['co2concentration']  + df1_sector["TotalFlowAmountminusCO2"]
        df1_sector["Concentration"] = df1_sector["FlowAmount"] / \
            df1_sector["Total_Flue_Gas_Amount"]

        df1_sector = add_cas_number(df1_sector)  
        df1_sector.to_csv(
            out_path2+sector+'_'+'facility_level_with_concentration_via_co2_reference.csv', index=False)
    
    
    concentration_calculation(sector_df2)
    

    def plots_and_exploration2(sector_df3):
        '''
        This function plots and explores cap emission concenrtations calculated using the back calculation method. We use a literature review to determine that
        co2 concentrations in sector plants is generally around 0.2. Using that we calculate the total flue gas and then recalculate the concentrations of all species. 
        CO2 concentrations thus become constant for all plants.  
    
        Parameters
        ----------
        sector_df2 : pandas Dataframe
            Dataframe for sector facilities with CO2 emission
        Returns
        -------
        None.
    
        '''
    
        cap_emissions = ['Carbon Dioxide', 'Methane', 'Nitrogen Oxides', 'Nitrous Oxide',
                         'Sulfur Dioxide', 'Volatile Organic Compounds', 'PM10-PM2.5', 'PM2.5 Primary (Filt + Cond)']
        sector_inv_for_cap_em = pd.DataFrame()
        for em in cap_emissions:
    
            co2 = sector_df3[sector_df3['FlowName'] == em]
    
            print("For emission "+em +
                  " total number of facilities obtained is "+str(len(co2['FRS_ID'])))
    
            sector_inv_for_cap_em = pd.concat([sector_inv_for_cap_em, co2])
            fig = plt.figure()
            ax = sns.scatterplot(x=co2['FRS_ID'], y=co2['Concentration'])
            plt.tick_params(
                axis='x',          # changes apply to the x-axis
                which='both',      # both major and minor ticks are affected
                bottom=False,      # ticks along the bottom edge are off
                top=False,         # ticks along the top edge are off
                labelbottom=False)
            ax.set(ylabel=None)
            plt.title(em+"_concentration_kg/kg")
            fig.savefig("../Data/"+em +
                        "_concentrationkgperkg_backcalculated.png", dpi=200)
    


    # NEI exploration_by_facility()
    def nei_by_process_exploration(naics1, naics2, naics3, naics4):
        '''
        This function explores the NEI dataset by process emissions from STEWI. IT combines the SCC Codes with NEI provided codes and 
        obtains the fuel and units information. They are done separately as the information obtained is different. 
    
    
        Parameters
        ----------
        naics: int
            NAICS Sector code for exploration
    
        Returns
        -------
        None.
    
        '''
    
        # run_stewi()
        flow_by_process = pd.read_csv('../Data/flow_by_process_NEI.csv')
        flow_by_process['Source'] = "NEI"
    
        nei_df = pd.read_csv('../Data/facility_NEI.csv')
        tri_df = pd.read_csv('../Data/facility_TRI.csv')
        ghgrp_df = pd.read_csv('../Data/facility_GHGRP.csv')
    
        nei_df['Source'] = "NEI"
        tri_df['Source'] = "TRI"
        ghgrp_df['Source'] = "GHGRP"
    
        df2 = pd.concat([nei_df, tri_df, ghgrp_df])
    
        '''
        #Sanity Check for Facility IDs
        fac_id_from_raw_data = df2[['FacilityID','Source', 'NAICS', 'SIC']].drop_duplicates()
        fac_id_from_stewi_data = flow_by_process[['FacilityID', 'Source']].drop_duplicates()
        
        check_fac_id = fac_id_from_stewi_data.merge(fac_id_from_raw_data, on = ['FacilityID','Source'], how = 'left').drop_duplicates()
        check_fac_id['ind'] = check_fac_id.duplicated(subset = ['FacilityID','Source'], keep = False)
        
        problems = check_fac_id[check_fac_id['ind'] == True]
        fac_ids_to_merge = check_fac_id[check_fac_id['ind'] == False]
        '''
    
        df2 = pd.concat([ghgrp_df, nei_df, tri_df])
        flow_by_process[['FacilityID', 'Source']] = flow_by_process[[
            'FacilityID', 'Source']].astype('str')
        df2[['FacilityID', 'Source']] = df2[['FacilityID', 'Source']].astype('str')
        df1 = flow_by_process.merge(
            df2, on=['FacilityID', 'Source'], how='left').drop_duplicates()
    
        df1 = df1[['FacilityID', 'FlowName', 'Compartment', 'FlowAmount', 'Unit',
                   'DataReliability',  'Process', 'ProcessType', 'Source', 'FacilityName', 'Address', 'City', 'Year',
                   'State', 'Zip', 'Latitude', 'Longitude', 'County', 'NAICS', 'SIC']]
    
        fac_id_from_stewi_data_1 = df1[['FacilityID', 'Source']].drop_duplicates()
    
        # keeping only air
        df1_air = df1[df1['Compartment'] == "air"]
    
        if filename_flag:
            df1_sector = df1_air.loc[df1_air['NAICS'].isin(naics_list)]
        else:
            df1_sector = df1_air[(df1_air['NAICS'] == naics1) | (df1_air['NAICS'] == naics2) | (
                df1_air['NAICS'] == naics3) | (df1_air['NAICS'] == naics4)]
    
        scc_map = pd.read_csv("../Data/SCCmap.csv")
        scc_map['code'] = scc_map['SCC'].astype('str')
        df1_sector['Process'] = df1_sector['Process'].astype('str')
        df1_sector_process = df1_sector.merge(
            scc_map, left_on=['Process'], right_on=['SCC'], how='left')
    
        df1_sector_process = df1_sector_process[['FacilityID', 'FlowName', 'Compartment', 'FlowAmount', 'Unit',
                                                 'DataReliability', 'Process', 'ProcessType', 'Source', 'FacilityName',
                                                 'Address', 'City', 'Year', 'State', 'Zip', 'Latitude', 'Longitude',
                                                 'County', 'NAICS', 'SIC', 'SCC', 'code description', 'data category',
                                                 'scc level one', 'scc level two', 'scc level three', 'scc level four',
                                                 'sector', 'short name', 'status', 'tier 1 code', 'tier 1 description',
                                                 'tier 2 code', 'tier 2 description', 'tier 3 code',
                                                 'tier 3 description']].drop_duplicates()
    
        frs_id_df = pd.read_csv('../Data/combinedinventories_NEI_TRI_GHGRP.csv')
        frs_id_df = frs_id_df[['FacilityID', 'FRS_ID']].drop_duplicates()
        frs_id_df[['FacilityID', 'FRS_ID']] = frs_id_df[[
            'FacilityID', 'FRS_ID']].astype('str')
    
        df1_sector_process = df1_sector_process.merge(
            frs_id_df, on=['FacilityID'], how='left')
    
        for i in range(0, len(frs_id_list)):
            frs_id_list[i] = str(float(frs_id_list[i]))
    
        if corrected_parquet_available_flag:
                df1_sector_process = df1_sector_process.loc[df1_sector_process['FRS_ID'].isin(frs_id_list)]
        
        df1_sector_process = df1_sector_process.drop_duplicates()
        df1_sector_process = add_cas_number(df1_sector_process)
        df1_sector_process.to_csv(
            out_path2+sector+'_'+'inventory_with_process_data_NEI.csv', index=False)
    
    
    nei_by_process_exploration(naics1, naics2, naics3, naics4)
    

    def ghgrp_process_exploration_unit(naics1, naics2, naics3, naics4):
        '''
        This function explores the process level emissions reported by the GHGRP database. 
        We obtain the process level GHGRP file from STEWI. 
        The process information needs to be obtained from supplementary files provided by GHGRP. 
        Combining these two information, we obtain the process level GHGRP emissions. 
    
    
        Parameters
        ----------
        naics: int
            NAICS Sector code for exploration
    
        Returns
        -------
        None.
    
        Output
        ------
        GHGRP Process level emission file: pandas Dataframe
    
        '''
    
    
        process_info_ghgrp = pd.read_csv('../Data/Unit_process_info_GHGRP.csv')
        process_info_ghgrp[['Facility Id', 'Industry Type (subparts)']] = process_info_ghgrp[[
            'Facility Id', 'Industry Type (subparts)']].astype('str')
            
        process_info_ghgrp = process_info_ghgrp[['Facility Id', 'FRS Id',
                                                 'Primary NAICS Code', 'Industry Type (subparts)',
                                                 'Industry Type (sectors)', 'Unit Name', 'Unit Type',
                                                 'Unit Reporting Method',
                                                 'Unit Maximum Rated Heat Input Capacity (mmBTU/hr)',
                                                 'Unit CO2 emissions (non-biogenic) ', 'Unit Methane (CH4) emissions ',
                                                 'Unit Nitrous Oxide (N2O) emissions ',
                                                 'Unit Biogenic CO2 emissions (metric tons)']]
        
        process_info_ghgrp['FRS Id'] = pd.to_numeric(process_info_ghgrp['FRS Id'], errors = 'coerce')
        process_info_ghgrp.dropna(subset = ['FRS Id'], inplace = True)
        #process_info_ghgrp['FRS Id'] = process_info_ghgrp['FRS Id'].astype('float').astype('int').astype('str')
        
        
        if filename_flag:
            df1_sector = process_info_ghgrp.loc[process_info_ghgrp['Primary NAICS Code'].isin(naics_list)]
            
        else:
    
            df1_sector = process_info_ghgrp[(process_info_ghgrp['Primary NAICS Code'] == naics1) | (process_info_ghgrp['Primary NAICS Code'] == naics2) | (
                process_info_ghgrp['Primary NAICS Code'] == naics3) | (process_info_ghgrp['Primary NAICS Code'] == naics4)]
    
    
        #ghgrp_process_df = df1_sector.merge(process_info_ghgrp, left_on=['FacilityID', 'Process'], right_on=['Facility Id', 'Industry Type (subparts)']).drop_duplicates()
        df1_sector['FRS Id'] = df1_sector['FRS Id'].astype('float').astype('str')
        ghgrp_process_df = df1_sector
        
        #print(process_info_ghgrp['FRS Id'])
        if corrected_parquet_available_flag:
                ghgrp_process_df = ghgrp_process_df.loc[ghgrp_process_df['FRS Id'].isin(frs_id_list)]
    
        ghgrp_process_df['Facility Id'] = ghgrp_process_df['Facility Id'].astype('str')
        ghgrp_df = pd.read_csv('../Data/facility_GHGRP.csv')
        ghgrp_df[['FacilityID']] = ghgrp_df[['FacilityID']].astype('str')
        ghgrp_process_df = ghgrp_process_df.merge(ghgrp_df, left_on = ['Facility Id'], right_on = ['FacilityID'], how = 'left').drop_duplicates()
    
    
        ghgrp_process_df.to_csv(
            out_path2+sector+'_'+'inventory_with_process_data_GHGRP_unit.csv', index=False)
    
    ghgrp_process_exploration_unit(naics1, naics2, naics3, naics4)
    

    def ghgrp_process_exploration_fuel(naics1, naics2, naics3, naics4):
        '''
        This function explores the process level emissions reported by the GHGRP database. 
        We obtain the process level GHGRP file from STEWI. 
        The process information needs to be obtained from supplementary files provided by GHGRP. 
        Combining these two information, we obtain the process level GHGRP emissions. 
    
    
        Parameters
        ----------
        naics: int
            NAICS Sector code for exploration
    
        Returns
        -------
        None.
    
        Output
        ------
        GHGRP Process level emission file: pandas Dataframe
    
        '''
    
        process_info_ghgrp = pd.read_csv('../Data/Fuel_process_info_GHGRP.csv')
        process_info_ghgrp[['Facility Id', 'Industry Type (subparts)']] = process_info_ghgrp[[
            'Facility Id', 'Industry Type (subparts)']].astype('str')
        process_info_ghgrp = process_info_ghgrp[['Facility Id', 'FRS Id',
                                                 'Primary NAICS Code', 'Industry Type (subparts)',
                                                 'Industry Type (sectors)', 'Unit Name', 'General Fuel Type',
                                                 'Specific Fuel Type', 'Other Fuel Name', 'Blend Fuel Name',
                                                 'Fuel Methane (CH4) emissions (mt CO2e)',
                                                 'Fuel Nitrous Oxide (N2O) emissions (mt CO2e)']]
        
        process_info_ghgrp['FRS Id'] = pd.to_numeric(process_info_ghgrp['FRS Id'], errors = 'coerce')
        process_info_ghgrp.dropna(subset = ['FRS Id'], inplace = True)
        #process_info_ghgrp['FRS Id'] = process_info_ghgrp['FRS Id'].astype('float').astype('int').astype('str')
        
        
        if filename_flag:
            df1_sector = process_info_ghgrp.loc[process_info_ghgrp['Primary NAICS Code'].isin(naics_list)]
            
        else:
    
            df1_sector = process_info_ghgrp[(process_info_ghgrp['Primary NAICS Code'] == naics1) | (process_info_ghgrp['Primary NAICS Code'] == naics2) | (
                process_info_ghgrp['Primary NAICS Code'] == naics3) | (process_info_ghgrp['Primary NAICS Code'] == naics4)]
    
    
        #ghgrp_process_df = df1_sector.merge(process_info_ghgrp, left_on=['FacilityID', 'Process'], right_on=['Facility Id', 'Industry Type (subparts)']).drop_duplicates()
        df1_sector['FRS Id'] = df1_sector['FRS Id'].astype('float').astype('str')
        ghgrp_process_df = df1_sector
    
        
        #print(process_info_ghgrp['FRS Id'])
        if corrected_parquet_available_flag:
                ghgrp_process_df = ghgrp_process_df.loc[ghgrp_process_df['FRS Id'].isin(frs_id_list)]
  
        ghgrp_process_df['Facility Id'] = ghgrp_process_df['Facility Id'].astype('str')
        ghgrp_df = pd.read_csv('../Data/facility_GHGRP.csv')
        ghgrp_df[['FacilityID']] = ghgrp_df[['FacilityID']].astype('str')
        ghgrp_process_df = ghgrp_process_df.merge(ghgrp_df, left_on = ['Facility Id'], right_on = ['FacilityID'], how = 'left').drop_duplicates()
        ghgrp_process_df.to_csv(
            out_path2+sector+'_'+'inventory_with_process_data_GHGRP_fuel.csv', index=False)
    
    ghgrp_process_exploration_fuel(naics1, naics2, naics3, naics4)




    def ghgrp():
    
        ghgrp_raw_data = pd.read_excel('../Data/GHGRP_epa_2017.xlsx')
        ghgrp_raw_data = ghgrp_raw_data[['Facility Id', 'FRS Id', 'Facility Name', 'City', 'State', 'Zip Code',
                                         'Address', 'County', 'Latitude', 'Longitude', 'Primary NAICS Code']]
    
        ghgrp_raw_data[['Facility Id', 'FRS Id']] = ghgrp_raw_data[[
            'Facility Id', 'FRS Id']].astype('str')
        naics_list = []
        ghgrp_raw_data.to_excel("../Data/GHGRP_facility_information_file.xlsx",index=False)
        if filename_flag:
            df_ghgrp = pd.read_excel(ghgrpfilename)
            df_ghgrp['GHGRP ID'] = df_ghgrp['GHGRP ID'].astype('str')
            df_ghgrp = df_ghgrp.merge(ghgrp_raw_data, left_on=[
                                      'GHGRP ID'], right_on=['Facility Id'])
            naics_list = list(pd.unique(df_ghgrp['Primary NAICS Code']))
        else:
            df_ghgrp = ghgrp_raw_data[ghgrp_raw_data['Primary NAICS Code'] == naics1]
        
        f_sector_production = df_ghgrp

        df1_f_sector = pd.read_csv(out_path2+sector+'_'+'inventory_facility_level.csv')
    
        df1_f_sector[['FRS_ID', 'FacilityName', 'Address', 'City', 'State',
                    'Zip', 'Latitude', 'Longitude', 'County', 'NAICS', 'SIC', 'FacilityID']] = df1_f_sector[['FRS_ID', 'FacilityName', 'Address', 'City', 'State',
                                                                                                           'Zip', 'Latitude', 'Longitude', 'County', 'NAICS', 'SIC', 'FacilityID']].astype('str')
    
        df1_f_sector = df1_f_sector[['FRS_ID', 'FacilityName', 'Address', 'City', 'State',
                                 'Zip', 'Latitude', 'Longitude', 'County', 'NAICS', 'SIC', 'FacilityID', 'Source']].drop_duplicates()
        df1_f_sector = df1_f_sector[df1_f_sector['Source'] == 'GHGRP']
    
        f_sector_production[['Facility Id', 'FRS Id', 'City', 'State', 'Zip Code',
                           'Address', 'County', 'Latitude', 'Longitude', 'Primary NAICS Code']] = f_sector_production[['Facility Id', 'FRS Id', 'City', 'State', 'Zip Code',
                                                                                                                     'Address', 'County', 'Latitude', 'Longitude', 'Primary NAICS Code']].astype('str')
    
        f_sector_production = f_sector_production.drop_duplicates()
    
        missing_facilities = f_sector_production.merge(df1_f_sector, left_on=[
                                                     'Facility Id'], right_on=['FacilityID'], how='left').drop_duplicates()
        missing_facilities['FRS_ID_Issues'] = np.where(
            missing_facilities['FRS Id'] == missing_facilities['FRS_ID'], True, False)
    
        missing_facilities.to_csv(
            out_path2+sector+'_'+'GHGRP_issue_FRS_mismatch.csv', index=False)
        
        
        missing_facilities = missing_facilities.fillna('issues')
        issues_with_ghgrp = missing_facilities[(missing_facilities['FRS_ID'] == "NaN") | (missing_facilities['FRS_ID'] == np.nan)| (missing_facilities['FRS_ID'] == 'nan') | (missing_facilities['FRS_ID'] == "") | (missing_facilities['FacilityID'] == None) | (missing_facilities['FacilityID'] == 'issues') | (missing_facilities['FRS_ID'] == 'issues')]

        #missing_facilities = missing_facilities.dropna()

        issues_with_ghgrp.to_csv(
           out_path2+sector+'_'+'GHGRP_issue_file_missing_facility.csv', index=False)
        return naics_list
    
    
    def nei():
        """
        nei_raw_data1 = pd.read_csv("../Data/point_12345.csv", low_memory=False)
        nei_raw_data2 = pd.read_csv("../Data/point_678910.csv", low_memory=False)
    
        nei_raw_data2.columns = nei_raw_data1.columns
        nei_raw_data = pd.concat([nei_raw_data1, nei_raw_data2])
    
        nei_raw_data = nei_raw_data[['state', 'fips state code', 'tribal name',
                                     'fips code', 'county', 'eis facility id', 'tri facility id', 'company name', 'site name',
                                     'naics code', 'naics description', 'facility source type', 'city', 'address']].drop_duplicates()
        
        nei_raw_data.to_excel("../Data/NEI_facility_information_file.xlsx",index=False)
        """
        nei_raw_data = pd.read_excel("../Data/NEI_facility_information_file.xlsx")
        # If file is available from GHGRP FLIGHT TOOL, we use that to extract out facilities from STEWI and obtain NAICS list from that.
        # If not available, we directly use user defined NAICS codes.
        if filename_flag:
            """
            This part is currently not correct
            Using the NAICS code for extracting NEI facilities will give too many facilities that do not exist in the GHGRP Flight tool files
            Thus we should have two approaches to this
            1. We extract out NEI facilities using the NAICS code list
            2. We extract out NEI facilities and then somehow drop all that do not exist in GHGRP. 
            """
            f_sector = nei_raw_data.loc[nei_raw_data['naics code'].isin(naics_list)]
        else:
            f_sector = nei_raw_data[(nei_raw_data['naics code'] == naics1) | (nei_raw_data['naics code'] == naics2) | (
                nei_raw_data['naics code'] == naics3) | (nei_raw_data['naics code'] == naics4)].drop_duplicates()
    
        nei_raw_data['naics description'] = nei_raw_data['naics description'].astype(
            'str')
        nei_raw_data['naics description'] = nei_raw_data['naics description'].str.lower()
    
        # missing facilies =
        df1_f_sector = pd.read_csv(out_path2+sector+'_'+'inventory_facility_level.csv')
    
        df1_f_sector[['FRS_ID', 'FacilityName', 'Address', 'City', 'State',
                    'Zip', 'Latitude', 'Longitude', 'County', 'NAICS', 'SIC', 'FacilityID']] = df1_f_sector[['FRS_ID', 'FacilityName', 'Address', 'City', 'State',
                                                                                                           'Zip', 'Latitude', 'Longitude', 'County', 'NAICS', 'SIC', 'FacilityID']].astype('str')
    
        df1_f_sector = df1_f_sector[['FRS_ID', 'FacilityName', 'Address', 'City', 'State',
                                 'Zip', 'Latitude', 'Longitude', 'County', 'NAICS', 'SIC', 'FacilityID', 'Source']].drop_duplicates()
    
        f_sector['eis facility id'] = f_sector['eis facility id'].astype('str')
        f_sector = f_sector.drop_duplicates()
        issues_with_nei_r = f_sector.merge(
            df1_f_sector, left_on='eis facility id', right_on='FacilityID', how='left')
        issues_with_nei_r = issues_with_nei_r.fillna('issues')
        issues_with_nei = issues_with_nei_r[(issues_with_nei_r['FRS_ID'] == "NaN") | (issues_with_nei_r['FRS_ID'] == np.nan)| (issues_with_nei_r['FRS_ID'] == 'nan') | (issues_with_nei_r['FRS_ID'] == "") | (issues_with_nei_r['FacilityID'] == None) | (issues_with_nei_r['FacilityID'] == 'issues') | (issues_with_nei_r['FRS_ID'] == 'issues')]
        
    
        missing_facilities2 = issues_with_nei[[
            'eis facility id', 'site name', 'FRS_ID', 'FacilityName', 'FacilityID', 'Source']].drop_duplicates()
        missing_facilities2.to_csv(
            out_path2+sector+'_'+'NEI_issue_file_missing_facility_ALL_using_NAICS_codes.csv', index=False)

        d = missing_facilities2

        """
        List of of all NEI facilities that are missing or have issues
        I think we are uninterested in those IDS that DO NOT occur in GHGRP
        We are assuming the NEI has the same FRS IDs as GHGRP
        Unfortunately NEI DOES not have FRS IDS. So we cannot merge NEI with GHGRP FLight Tool. 
        """



        issues_with_nei.to_csv(out_path2+sector+'_'+'NEI_issue_file_ALL_using_NAICS_codes.csv')

        ids = list(pd.unique(d['eis facility id']))

        #These files are constant. They do not change between sectors
        dff1 = pd.read_csv('../Data/flow_by_facility_NEI.csv')

        dff3 = pd.read_csv('../Data/combinedinventories_NEI_TRI_GHGRP.csv')


        nei_missing_in_stewi = pd.DataFrame()
        nei_missing_in_combination_frs_fac_id_issue = pd.DataFrame()
        nei_missing_other_issues = pd.DataFrame()
        
        for ide in ids:

            df2 = dff1[dff1['FacilityID'] == ide]
            if df2.empty == True:
                df2 = d[d['eis facility id'] == ide]
                nei_missing_in_stewi = pd.concat([nei_missing_in_stewi,df2])
            else:
                df3 = dff3[dff3['FacilityID'] == ide]
                if df3.empty == True:
                    df3 = d[d['eis facility id'] == ide]
                    nei_missing_in_combination_frs_fac_id_issue = pd.concat([nei_missing_in_combination_frs_fac_id_issue,df3])
                else:
                    df4 = d[d['eis facility id'] == ide]
                    nei_missing_other_issues = pd.concat([nei_missing_other_issues,df4])

        
        #All issues of files missing in STEWI
        nei_missing_in_stewi.to_csv(out_path2+sector+'_'+'nei_missing_in_stewi.csv',index = False)
        #All issues due to missing in the combination emissions file
        facility_nei = pd.read_csv('../Data/facility_NEI.csv')
        if not nei_missing_in_combination_frs_fac_id_issue.empty:
            nei_missing_in_combination_frs_fac_id_issue=nei_missing_in_combination_frs_fac_id_issue.merge(facility_nei, left_on = ['eis facility id'], right_on = ['FacilityID'])
        nei_missing_in_combination_frs_fac_id_issue.to_csv(out_path2+sector+'_'+'nei_missing_in_combination_frs_fac_id_issue.csv',index = False)
        #All other issues
        nei_missing_other_issues.to_csv(out_path2+sector+'_'+'nei_missing_other_issues.csv',index= False)

    
    def tri():
        """
        tri = pd.read_csv('../Data/2017_us.csv')
        tri = tri.iloc[:, 0:49]
        cols = ['YEAR', 'TRIFD', 'FRS ID', 'FACILITY NAME', 'STREET ADDRESS', 'CITY', 'COUNTY', 'ST', 'ZIP', 'BIA', 'TRIBE', 'LATITUDE', 'LONGITUDE', 'HORIZONTAL DATUM', 'PARENT CO NAME', 'PARENT CO DB NUM', 'STANDARD PARENT CO NAME', 'FEDERAL FACILITY', 'INDUSTRY SECTOR CODE', 'INDUSTRY SECTOR', 'PRIMARY SIC', 'SIC 2', 'SIC 3', 'SIC 4', 'SIC 5',
                'SIC 6', 'PRIMARY NAICS', 'NAICS 2', 'NAICS 3', 'NAICS 4', 'NAICS 5', 'NAICS 6', 'DOC_CTRL_NUM', 'CHEMICAL', 'ELEMENTAL METAL INCLUDED', 'TRI CHEMICAL/COMPOUND ID', 'CAS#', 'SRS ID', 'CLEAN AIR ACT CHEMICAL', 'CLASSIFICATION', 'METAL', 'METAL CATEGORY', 'CARCINOGEN', 'PBT', 'PFAS', 'FORM TYPE', 'UNIT OF MEASURE', 'FUGITIVE AIR', 'STACK AIR']
        tri.columns = cols
        tri_orig = tri
    
        tri = tri[["TRIFD", "FACILITY NAME", "PRIMARY NAICS", "NAICS 2",
                   "NAICS 3", "NAICS 4", "NAICS 5", "NAICS 6", "PARENT CO NAME", "FRS ID"]]
        tri[["TRIFD", "FACILITY NAME", "PRIMARY NAICS", "NAICS 2", "NAICS 3", "NAICS 4", "NAICS 5", "NAICS 6", "PARENT CO NAME", "FRS ID"]] = tri[[
            "TRIFD", "FACILITY NAME", "PRIMARY NAICS", "NAICS 2", "NAICS 3", "NAICS 4", "NAICS 5", "NAICS 6", "PARENT CO NAME", "FRS ID"]].astype('str')
        tri.to_excel("../Data/TRI_Facility_information_file.xlsx",index=False)
        """ 
        tri = pd.read_excel("../Data/TRI_Facility_information_file.xlsx")
        
        
        
        if filename_flag:
            f_sector1 = tri.loc[tri['PRIMARY NAICS'].isin(naics_list)]
        else:
            f_sector1 = tri[(tri['PRIMARY NAICS'] == str(naics1)) | (tri['PRIMARY NAICS'] == str(naics2)) | (
                tri['PRIMARY NAICS'] == str(naics3)) | (tri['PRIMARY NAICS'] == str(naics4))].drop_duplicates()
    
        # missing facilies =
        df1_f_sector = pd.read_csv(out_path2+sector+'_'+'inventory_facility_level.csv')
        df1_f_sector[['FRS_ID', 'FacilityName', 'Address', 'City', 'State',
                    'Zip', 'Latitude', 'Longitude', 'County', 'NAICS', 'SIC', 'FacilityID']] = df1_f_sector[['FRS_ID', 'FacilityName', 'Address', 'City', 'State',
                                                                                                           'Zip', 'Latitude', 'Longitude', 'County', 'NAICS', 'SIC', 'FacilityID']].astype('str')
        df1_f_sector = df1_f_sector[['FRS_ID', 'FacilityName', 'Address', 'City', 'State',
                                 'Zip', 'Latitude', 'Longitude', 'County', 'NAICS', 'SIC', 'FacilityID', 'Source']].drop_duplicates()
    
        issues_with_tri_r = f_sector1.merge(df1_f_sector, left_on=["TRIFD"], right_on=[
                                           "FacilityID"], how='left')
        
        issues_with_tri_r = issues_with_tri_r.fillna('issues').drop_duplicates()
        issues_with_tri = issues_with_tri_r[(issues_with_tri_r['FRS_ID'] == "NaN") | (issues_with_tri_r['FRS_ID'] == np.nan)| (issues_with_tri_r['FRS_ID'] == 'nan') | (issues_with_tri_r['FRS_ID'] == "") | (issues_with_tri_r['FacilityID'] == None) | (issues_with_tri_r['FacilityID'] == 'issues') | (issues_with_tri_r['FRS_ID'] == 'issues')| (issues_with_tri_r['Source'] == 'issues')]
        #Missing facility
        issues_with_tri.to_csv(out_path2+sector+'_'+'TRI_issue_file_missing_facility_all_using_naics_code.csv', index=False)
        
        

        """
        List of of all TRI facilities that are missing or have issues
        I think we are uninterested in those IDS that DO NOT occur in GHGRP
        We are assuming the TRI has the same FRS IDs as GHGRP
        """        
        ghgrp_raw_data = pd.read_excel('../Data/GHGRP_epa_2017.xlsx')
        ghgrp_raw_data = ghgrp_raw_data[['Facility Id', 'FRS Id', 'Facility Name', 'City', 'State', 'Zip Code',
                                         'Address', 'County', 'Latitude', 'Longitude', 'Primary NAICS Code']]
    
        ghgrp_raw_data[['Facility Id', 'FRS Id']] = ghgrp_raw_data[[
            'Facility Id', 'FRS Id']].astype('str')
        
        frs_id_list = list(pd.unique(ghgrp_raw_data['FRS Id']))
        
        # Including only GHGRP facilities if correcred parquet is available
        """
        If all necessary corrections to facilities, IDS, inventories have been performed and a corrected STEWI parquet file is available, the code needs to drop all facilities that do not report exclusively to GHGRP. 
        We keep the facilities from other inventories before the corrections so that facilities with ID issues do not get dropped and are properly combined across inventories via our debugger correction method. 
        """
        if corrected_parquet_available_flag:
            issues_with_tri_ghgrp_only = issues_with_tri.loc[issues_with_tri['FRS_ID'].isin(frs_id_list)]
        
        
        issues_with_tri_frs_issues = issues_with_tri_ghgrp_only
        issues_with_tri_frs_issues['FRS_ID_Issues'] = np.where(
            issues_with_tri_frs_issues['FRS ID'] == issues_with_tri_frs_issues['FRS_ID'], True, False)
        
        issues_with_tri_frs_issues = issues_with_tri_frs_issues[issues_with_tri_frs_issues['FRS_ID_Issues'] == True]
        issues_with_tri_frs_issues.to_csv(
            out_path2+sector+'_'+'TRI_issue_file_FRS_mismatch.csv', index=False)       

        d = issues_with_tri_ghgrp_only
        
        
        ids = list(pd.unique(d['TRIFD']))
        dff1 = pd.read_csv('../Data/flow_by_facility_TRI.csv')
        dff3 = pd.read_csv('../Data/combinedinventories_NEI_TRI_GHGRP.csv')


        tri_missing_in_stewi = pd.DataFrame()
        tri_missing_in_combination_frs_fac_id_issue = pd.DataFrame()
        tri_missing_other_issues = pd.DataFrame()
        
        for ide in ids:

            df2 = dff1[dff1['FacilityID'] == ide]
            if df2.empty == True:
                df2 = d[d['TRIFD'] == ide]
                tri_missing_in_stewi = pd.concat([tri_missing_in_stewi,df2])
            else:
                df3 = dff3[dff3['FacilityID'] == ide]
                if df3.empty == True:
                    df3 = d[d['TRIFD'] == ide]
                    tri_missing_in_combination_frs_fac_id_issue = pd.concat([tri_missing_in_combination_frs_fac_id_issue,df3])
                else:
                    df4 = d[d['TRIFD'] == ide]
                    tri_missing_other_issues = pd.concat([tri_missing_other_issues,df4])
        #Missing facility
        issues_with_tri_ghgrp_only.to_csv(out_path2+sector+'_'+'TRI_issue_file_missing_facility_ghgrp_only.csv', index=False)
        #Missing completely in STEWI
        tri_missing_in_stewi.to_csv(out_path2+sector+'_'+'tri_missing_in_stewi.csv')
        #Missing only in combined files
        tri_missing_in_combination_frs_fac_id_issue.to_csv(out_path2+sector+'_'+'tri_missing_in_combination_frs_fac_id_issue.csv')
        #Other issues
        tri_missing_other_issues.to_csv(out_path2+sector+'_'+'tri_missing_other_issues.csv')


    naics_list = ghgrp()
    tri()
    nei()  
  