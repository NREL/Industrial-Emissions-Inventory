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
