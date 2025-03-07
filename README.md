

# AEGIS: Air Emissions Grouped By Industrial Sectors

AEGIS is a robust framework designed to build and compile emissions inventories for various industrial sectors. It integrates multiple emissions databases provided by the USEPA – including GHGRP, NEI, and TRI – and leverages the STEWI and STEWICOMBO tools to retrieve, merge, and process data. The framework produces facility- and process-level inventories, identifies discrepancies (e.g., NAICS or FRS mismatches), and performs exploratory analysis including emission concentration calculations and visualization.

## Features

- **Multi-database Integration:**  
  Retrieves and combines emissions data from GHGRP, NEI, and TRI inventories.

- **Robust Data Processing:**  
  Uses structured processing routines to merge facility and process-level data, apply NAICS code filtering, and address data quality issues.

- **Emission Concentration Calculations:**  
  Computes emission concentrations using CO₂ as a reference to estimate flue gas totals.

- **Exploratory Data Analysis & Visualization:**  
  Generates scatter plots and other visualizations for in-depth analysis of emission flows and concentrations.

- **Error Handling & Logging:**  
  Incorporates detailed logging and robust error handling to ensure smooth execution and easier debugging.

- **Extensible and Documented:**  
  Code is organized into modular functions with Sphinx-compatible docstrings, making it easier to extend and maintain.

## Requirements

- **Python 3.x**  
- **Key Python Libraries:**  
  - [pandas](https://pandas.pydata.org/)
  - [numpy](https://numpy.org/)
  - [matplotlib](https://matplotlib.org/)
  - [seaborn](https://seaborn.pydata.org/)
  - [STEWI](https://github.com/USEPA/Stewi) and [STEWICOMBO](https://github.com/USEPA/Stewicombo) (for data retrieval and processing)

- **Operating System:**  
  Cross-platform (Windows, macOS, Linux)

## Installation

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/YourOrganization/AEGIS.git
   cd AEGIS
   ```

2. **Create a Virtual Environment (Recommended):**

   ```bash
   python3 -m venv venv
   source venv/bin/activate   # On Windows: venv\Scripts\activate
   ```

3. **Install Required Packages:**

   ```bash
   pip install -r requirements.txt
   ```

   *(Ensure that the `requirements.txt` file includes all necessary libraries such as pandas, numpy, matplotlib, seaborn, STEWI, etc.)*

## Data Files

Some data files are too large to be stored on GitHub. The following files in the `Data/` folder are **not** supplied in the repository and must be obtained separately:

- `reduced_stack_file.csv`
- `combinedinventories_NEI_TRI_GHGRP.csv`
- `flow_by_process_NEI.csv`

Ensure these files are placed in the `Data/` folder as required by the framework.

## Usage

AEGIS is designed to be executed as a modular script. You can run the main inventory-building process by executing the primary script (e.g., `run.py`):

```bash
python run.py
```

### Configuration

- **Sector and NAICS Codes:**  
  Configure the target industrial sector and corresponding NAICS codes either via command-line arguments or by editing the configuration section in the main script.

- **File Paths:**  
  Ensure that data files (GHGRP, NEI, TRI source files, and supplementary files) are stored in the correct directories as specified in the code (e.g., the `../Data/` folder).

## Directory Structure

A typical directory structure for AEGIS may look like this:

```
AEGIS/
│
├── Data/                   # Raw data files and intermediate results
│   ├── GHGRP_epa_<year>.xlsx
│   ├── NEI_facility_information_file.xlsx
│   ├── TRI_Facility_information_file.xlsx
│   ├── reduced_stack_file.csv   # Not provided; obtain separately.
│   ├── combinedinventories_NEI_TRI_GHGRP.csv   # Not provided; obtain separately.
│   ├── flow_by_process_NEI.csv   # Not provided; obtain separately.
│   └── ... (other data files)
│
├── src/                    # Source code modules
│   ├── fecm_data_explorationv2.py
│   └── ... (other modules)
│
├── run.py                  # Main execution script
├── requirements.txt        # Python dependencies
└── README.md               # Project documentation (this file)
```

## Documentation

The code is fully documented using Sphinx-compatible docstrings. You can generate additional documentation by running:

```bash
sphinx-build -b html docs/ build/docs
```

*(Ensure you have Sphinx installed and a proper `conf.py` in your `docs/` folder.)*

## Contributing

Contributions to AEGIS are welcome! Please follow these guidelines:

1. Fork the repository and create your branch from `main`.
2. Ensure your code adheres to the project's style and includes proper logging, documentation, and error handling.
3. Submit a pull request with a detailed description of your changes.

## License

AEGIS is released under the [MIT License](LICENSE).

## References

- **Emissions Databases:**  
  - GHGRP, NEI, TRI – USEPA emissions databases.
- **STEWI & STEWICOMBO:**  
  - [STEWI GitHub Repository](https://github.com/USEPA/Stewi)
  - [STEWICOMBO GitHub Repository](https://github.com/USEPA/Stewicombo)

---

This README provides a comprehensive overview of the AEGIS framework, its features, and how to set it up and use it. Please obtain the large data files (`reduced_stack_file.csv`, `combinedinventories_NEI_TRI_GHGRP.csv`, and `flow_by_process_NEI.csv`) separately as they are not included in the repository due to GitHub storage limitations. Feel free to modify or expand the sections based on additional project specifics or future enhancements.
