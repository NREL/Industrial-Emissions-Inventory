'''
This file runs the fecm inventory for sectors listed sequentially and creates output files
in folders with current date as names. 
The fecm inventory builder involves 3 main parts - 
1. Running Stewi and obtaining the inventories
2. Compiling the data to create the emissions inventory for a certain sector, find errors and provide results files at facility and process level
3. Statistical analysis
'''



from fecm_data_explorationv2 import main




'''
The sectors listed here do not have a user defined NAICS code
Instead the NAICS code is obtained from the GHGRP Flight tool sector wise exported files and used for compilation of emission inventories
The two boolean flags refer to 
1. If a GHGRP Flight Tool exported file with NAICS code is available or not
2. If a corrected parquet file is available with manual corrections provided for debugging various inventory issues and errors
If a corrected parquet is available, the inventory builder drops all non GHGRP listed facilities
'''


flag_for_running_stewi = False
corrected_parquet = True

sectors = ['cement']

count = 0
for sec in sectors:
    if count > 0:
        main(sec,True,0,corrected_parquet,False)
    else:
        main(sec,True,0,corrected_parquet,flag_for_running_stewi)
    count = count + 1
    



