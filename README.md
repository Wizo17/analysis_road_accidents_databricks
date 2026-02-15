# Road Accident Analysis on Databricks

## Overview

This project implements a complete data pipeline for analyzing road accidents in France using Databricks. The pipeline covers the entire process from data ingestion, transformation, and loading (ETL) to generating analytical reports and dashboards. It leverages Databricks' unified analytics platform to process large datasets efficiently and provide insights into road safety trends.

The project is structured as a multi-module application with separate components for data ingestion, ETL processing, and analytics, following best practices for scalable data engineering.

## Features

- **Data Ingestion**: Automated ingestion of road accident data from various sources including CSV files for accident characteristics, sites, vehicles, victims, and geographic references.
- **ETL Pipeline**: Comprehensive data transformation and cleaning processes to prepare data for analysis.
- **Analytics and Reporting**: Advanced analytics on accident risks, patterns, and trends with interactive dashboards.
- **Databricks Integration**: Full utilization of Databricks features including Delta Lake, MLflow, and Databricks Jobs.
- **Modular Architecture**: Organized into ingestion, ETL, and analytics modules for maintainability and scalability.
- **Testing**: Comprehensive test suite using pytest for ensuring data quality and pipeline reliability.

## Project Structure

```
analysis_road_accidents_databricks/
├── analysis_road_accidents_ingestion/    # Data ingestion module
│   ├── src/
│   │   ├── accidents_ingestion/
│   │   └── references_ingestion/
│   ├── resources/                        # Databricks job configurations
│   ├── tests/
│   └── databricks.yml
├── analysis_road_accidents_etl/          # ETL processing module
│   ├── src/
│   │   ├── dataprep/                     # Data preparation transformations
│   │   └── analytics/                    # Analytics transformations
│   ├── resources/                        # Pipeline configurations
│   ├── tests/
│   └── databricks.yml
├── databricks_common_config/             # Shared configurations
├── data/                                 # Raw data files
│   ├── accidents routes/                 # Accident data CSVs
│   └── referentiel-geographique/         # Geographic reference data
├── reports/                              # Dashboard and report files
├── INSTALL.md                            # Installation instructions
├── Setup for Road Accident Analysis Project.ipynb  # Setup notebook
└── README.md                             # This file
```

## Prerequisites

- Databricks workspace with appropriate permissions
- Python 3.8+
- Databricks CLI
- Access to Azure Blob Storage or equivalent for data storage (if applicable)
- Git for version control

## Installation and Setup

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd analysis_road_accidents_databricks
   ```

2. **Configure Databricks**:
   - Set up your Databricks workspace
   - Configure authentication (personal access token or Azure AD)
   - Update `databricks_common_config/targets.yml` with your workspace details

3. **Install dependencies**:
   - For each module (ingestion, etl), navigate to the directory and install:
     ```bash
     cd analysis_road_accidents_ingestion
     pip install -e .
     cd ../analysis_road_accidents_etl
     pip install -e .
     ```

4. **Data Setup**:
   - Place raw data files in the `data/` directory
   - Ensure data follows the expected schema (refer to schemas in `analysis_road_accidents_etl/src/analytics/schemas/`)

5. **Run Setup Notebook**:
   - Open `Setup for Road Accident Analysis Project.ipynb` in Databricks
   - Execute the notebook to initialize the environment and create necessary databases/tables

For detailed installation steps, refer to [INSTALL.md](INSTALL.md).

## Usage

### Running the Pipeline

1. **Data Ingestion**:
   - Deploy and run the ingestion jobs using Databricks Jobs
   - Jobs are configured in `analysis_road_accidents_ingestion/resources/`

2. **ETL Processing**:
   - Execute the ETL pipelines defined in `analysis_road_accidents_etl/resources/`
   - This includes data cleaning, transformation, and loading into Delta tables

3. **Analytics and Reporting**:
   - Run analytics transformations in `analysis_road_accidents_etl/src/analytics/`
   - Access reports and dashboards in the `reports/` directory

### Key Components

- **Accident Characteristics**: Processing of accident metadata
- **Accident Sites**: Geographic and environmental factors
- **Vehicles**: Vehicle-related accident data
- **Victims**: Casualty and injury information
- **Geographic Reference**: Location mapping and reference data

### Running Tests

```bash
# From each module directory
pytest tests/
```

## Data Sources

The project uses official French road accident data, including:
- Accident characteristics (caractéristiques)
- Accident locations (lieux)
- Vehicle information (véhicules)
- Victim details (usagers)
- Geographic reference system

Data is sourced from French government databases and covers multiple years.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Based on Udemy course: Databricks Data Engineer Associate Certification
- Data provided by French government road safety authorities
- Built using Databricks platform and Apache Spark