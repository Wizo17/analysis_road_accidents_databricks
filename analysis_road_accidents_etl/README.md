# Road Accidents Analysis ETL on Databricks

This project implements an Extract, Transform, Load (ETL) pipeline for analyzing road accidents data using Databricks. It processes raw accident data, cleans and formats it, and performs analytical transformations to derive insights on accident risks and patterns.

## Project Structure

- `src/dataprep/`: Contains modules for data preparation and cleaning
  - `clean_format/`: Scripts for formatting accident characteristics, sites, vehicles, victims, and geographic references
  - `transformations/`: Data transformation logic
  - `utilities/`: Utility functions for data processing
- `src/analytics/`: Contains analytical modules
  - `schemas/`: Data schemas for analytics
  - `transformations/`: Analytical transformations for accident risks and road accidents
  - `utilities/`: Utility functions for analytics
- `resources/`: Databricks configuration files
  - `dataprep_job.job.yml`: Job configuration for data preparation
  - `analytics/`: Analytics pipeline configuration
  - `dataprep/`: Data preparation pipeline configuration
- `tests/`: Unit tests and test configurations
- `fixtures/`: Test data fixtures
- `databricks.yml`: Databricks project configuration
- `pyproject.toml`: Python project configuration

## Prerequisites

- Databricks workspace
- Python 3.8+
- Databricks CLI
- Access to the road accidents dataset

## Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd analysis_road_accidents_etl
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure Databricks:
   - Update `databricks.yml` with your workspace details
   - Set up authentication (e.g., via Databricks CLI or environment variables)

4. Deploy to Databricks:
   ```bash
   databricks bundle deploy
   ```

## Usage

### Data Preparation Pipeline

Run the data preparation pipeline to clean and format raw accident data:

```bash
databricks jobs run-now --job-id <dataprep-job-id>
```

### Analytics Pipeline

Execute the analytics pipeline for insights:

```bash
databricks jobs run-now --job-id <analytics-job-id>
```

## Testing

Run unit tests:

```bash
pytest tests/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

## License

This project is licensed under the MIT License.
