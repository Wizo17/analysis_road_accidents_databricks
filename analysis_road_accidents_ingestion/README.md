# Road Accidents Data Ingestion for Databricks

This project implements data ingestion pipelines for road accidents analysis in Databricks. It processes various datasets including accident characteristics, accident sites, accident vehicles, accident victims, BAAC vehicle registration, and geographic reference systems.

## Project Structure

- `src/`: Source code for ingestion transformations and utilities
  - `accidents_ingestion/`: Transformations for accident-related data
  - `references_ingestion/`: Transformations for reference data
- `resources/`: Databricks job and pipeline configurations
  - `accidents/`: Job and pipeline files for accident data ingestion
  - `references/`: Job and pipeline files for reference data ingestion
- `tests/`: Unit tests for the ingestion logic
- `fixtures/`: Test data fixtures
- `databricks.yml`: Databricks project configuration
- `pyproject.toml`: Python project dependencies and configuration

## Prerequisites

- Databricks workspace with appropriate permissions
- Python 3.8+
- Databricks CLI installed and configured
- Access to the source data files

## Installation

1. Clone this repository:
   ```bash
   git clone <repository-url>
   cd analysis_road_accidents_ingestion
   ```

2. Install dependencies:
   ```bash
   pip install -e .
   ```

3. Configure Databricks connection:
   ```bash
   databricks configure --token
   ```

## Usage

### Running Ingestion Jobs

Use the Databricks CLI to deploy and run the ingestion jobs:

```bash
# Deploy jobs
databricks bundle deploy

# Run a specific job
databricks jobs run-now --job-id <job-id>
```

### Pipeline Execution

Pipelines can be triggered through the Databricks UI or via API calls as defined in the pipeline YAML files.

## Data Sources

- **Accident Characteristics**: Details about accident circumstances
- **Accident Sites**: Geographic information about accident locations
- **Accident Vehicles**: Information about vehicles involved
- **Accident Victims**: Details about individuals affected
- **BAAC Vehicle Registration**: Vehicle registration data
- **Geographic Reference System**: Reference data for locations

## Development

### Running Tests

```bash
pytest tests/
```

### Adding New Transformations

1. Create a new transformation file in `src/accidents_ingestion/transformations/` or `src/references_ingestion/transformations/`
2. Add corresponding job and pipeline configurations in `resources/`
3. Update tests in `tests/`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.