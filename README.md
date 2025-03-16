# Study Data Integration Shiny App

## Description
This project is a **Shiny web application** designed to manage and visualize 
study data, including study, group, animal, and endpoint information, among others. 
The app allows users to upload and process data files (in CSV or Excel formats) and store them in an SQLite database. 
It is important to download the SQLite file before restarting the App, because the database gets overwritten 
with each use, ensuring data integrity and preventing conflicts between different sets of data. 
The dashboard provides an interactive interface for analyzing and visualizing this data, 
including tables, plots, and summary statistics. 

Key features of the app include:
- Upload and process ZIP files containing Excel and CSV files.
- Visualize study data, including tables and graphical summaries.
- View specific datasets such as study data, analytical data, and chemical data.
- Generate dynamic plots based on the data.
- Download the integrated data as a SQLite file

---

## Installation and Setup

Before starting the app, the following software needs to be installed:

- **R**: The programming language used for the Shiny app.
  - Install from [CRAN](https://cran.r-project.org/)
- **Required R Packages**: The following R packages need to be installed before running the app. 
These packages can be installed by running the following in your R console:

    ```r
    install.packages(c(
  "readxl", "DT", "shinydashboard", "plotly", "dplyr", "ggplot2", "DBI", 
  "RSQLite", "stringi", "tidyverse", "here", "tidyxl", "readr", "writexl"
))

    ```
- **Navigate to Your Project Directory**:
   In **Terminal** or **RStudio**, navigate to the folder where you downloaded or cloned the project. For example:
   ```sh
   cd /path/to/your/project
