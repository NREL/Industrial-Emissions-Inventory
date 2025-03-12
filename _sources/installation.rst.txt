
Installation
--------------------

1. **Clone the Repository:**

   
   ``git clone https://github.com/NREL/AEGIS.git``

   ``cd AEGIS``

2. **Create a Virtual Environment (Recommended):**

   
   ``python3 -m venv venv``

   ``source venv/bin/activate``  
    
   ``# On Windows: venv\Scripts\activate``
   

3. **Install Required Packages:**

   ``pip install -r requirements.txt``

   *(Ensure that the `requirements.txt` file includes all necessary libraries such as pandas, numpy, matplotlib, seaborn, STEWI, etc.)*

4. **Data Files**

Some data files are too large to be stored on GitHub. The following files in the `Data/` folder are **not** supplied in the repository and must be obtained separately:

- `reduced_stack_file.csv`
- `combinedinventories_NEI_TRI_GHGRP.csv`
- `flow_by_process_NEI.csv`

Ensure these files are placed in the `Data/` folder as required by the framework.