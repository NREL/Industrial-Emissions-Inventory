# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 13:35:55 2023

@author: satnoork
"""

import pandas as pd

parquet_address = "C:/Users/SATNOORK/Downloads/FacilityMatchList_forStEWI_v1.0.5_0a1fab6.xlsx"
parquet_data = pd.read_excel(parquet_address)

natural_gas_issue_mismatch_file_address = "C:/Users/SATNOORK/Box/FECM inventory/2023-03-15/natural_gas_processing/natural_gas_processing_GHGRP_issue_FRS_mismatch.csv"
natural_gas_issue_mismatch_data = pd.read_csv(natural_gas_issue_mismatch_file_address)

natural_gas_issue_mismatch_data = pd.merge(natural_gas_issue_mismatch_data, parquet_data, left_on = "FRS Id", right_on = "FRS_ID", how = "left")



def parquet_file_matcher_for_ghgrp():

    parquet_address = "../Data/FacilityMatchList_forStEWI_v1.0.5_0a1fab6.xlsx"
    parquet_data = pd.read_excel(parquet_address)


    _issue_mismatch_file_address = out_path2+sector+"_GHGRP_issue_FRS_mismatch.csv"
    _issue_mismatch_data = pd.read_csv(natural_gas_issue_mismatch_file_address)
    _issue_mismatch_data = _issue_mismatch_data[['Facility Id', 'FRS Id', 'Facility Name', 'Primary NAICS Code','Source','FacilityID','NAICS','FRS_ID']]
    _issue_mismatch_data['FRS ID from STEWI'] =  _issue_mismatch_data['FRS_ID']
    _issue_mismatch_data['FRS ID from GHGRPwebsite'] = _issue_mismatch_data['FRS_Id']
    del _issue_mismatch_data['FRS_ID']

    _issue_mismatch_data = pd.merge(natural_gas_issue_mismatch_data, parquet_data, left_on = "FRS Id", right_on = "FRS_ID", how = "left")

    _issue_mismatch_data.to_csv(out_path3+sector+'_'+'GHGRP_issue_FRS_mismatch_with_parquet.csv')
