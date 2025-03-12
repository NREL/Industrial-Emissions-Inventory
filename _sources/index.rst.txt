.. aegis documentation master file, created by
   sphinx-quickstart on Fri Mar  7 17:27:57 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

AEGIS documentation
===================

AEGIS: Air Emissions Grouped By Industrial Sectors
AEGIS is a robust framework designed to build and compile emissions inventories for various industrial sectors. It integrates multiple emissions databases provided by the USEPA – including GHGRP, NEI, and TRI – and leverages the STEWI and STEWICOMBO tools to retrieve, merge, and process data. The framework produces facility- and process-level inventories, identifies discrepancies (e.g., NAICS or FRS mismatches), and performs exploratory analysis including emission concentration calculations and visualization.

Features

Multi-database Integration:
Retrieves and combines emissions data from GHGRP, NEI, and TRI inventories.

Robust Data Processing:
Uses structured processing routines to merge facility and process-level data, apply NAICS code filtering, and address data quality issues.

Emission Concentration Calculations:
Computes emission concentrations using CO₂ as a reference to estimate flue gas totals.

Exploratory Data Analysis & Visualization:
Generates scatter plots and other visualizations for in-depth analysis of emission flows and concentrations.

Error Handling & Logging:
Incorporates detailed logging and robust error handling to ensure smooth execution and easier debugging.

Extensible and Documented:
Code is organized into modular functions with Sphinx-compatible docstrings, making it easier to extend and maintain.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   usage
   run
   aegis
   statistical_analysis



.. autosummary::
   :toctree: _autosummary




