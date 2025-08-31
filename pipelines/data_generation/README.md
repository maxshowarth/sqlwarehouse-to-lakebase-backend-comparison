# Data Generation Pipeline

This pipeline generates synthetic retail data using the functions from `data_generators.py` and writes the data to Delta tables in your configured Databricks Lakehouse catalog.

## Overview

The pipeline generates a complete dataset with the following tables:
- **stores** - Store locations and metadata
- **products** - Product catalog with categories and brands
- **customers** - Customer segments and demographics
- **promotions** - Product promotions and discounts
- **orders** - Customer orders with timestamps
- **order_items** - Individual line items within orders
- **inventory_snapshots** - Daily inventory levels

## Configuration

### Catalog and Schema
The pipeline uses the same catalog and schema configuration as defined in the main `databricks.yml`:

```yaml
variables:
  catalog: max_howarth_demos
  schema: sqlw-to-lakebase-backend
```

### Pipeline Parameters
The pipeline accepts the following parameters:

| Parameter | Description | Default | Options |
|-----------|-------------|---------|---------|
| `catalog` | Target catalog name | From main config | Any valid catalog |
| `schema` | Target schema name | From main config | Any valid schema |
| `scale` | Data volume scale | small | small, medium, large |
| `days` | Days of order history | 14 | 1-365 |
| `overwrite` | Overwrite existing data | true | true, false |
| `seed` | Random seed for reproducibility | 42 | Any integer |

### Data Scales
Each scale generates different volumes of data:

- **small**: 10 stores, 200 products, 2K customers, ~4K orders
- **medium**: 50 stores, 1K products, 25K customers, ~75K orders
- **large**: 200 stores, 5K products, 120K customers, ~500K orders

## Usage

### Deploy the Pipeline
Since this pipeline is part of the main Databricks Asset Bundle, deploy from the project root:

```bash
# Deploy to dev environment
databricks bundle deploy -t dev

# Deploy to prod environment
databricks bundle deploy -t prod
```

### Run the Pipeline
```bash
# Run with default parameters
databricks jobs run-now --job-id <job_id>

# Run with custom parameters
databricks jobs run-now \
  --job-id <job_id> \
  --python-params '{"scale": "medium", "days": 30, "overwrite": false}'
```

### Easy Deployment Script
Use the provided deployment script from the pipeline directory:

```bash
cd pipelines/data_generation
./deploy.sh dev medium 30 false
```

This script will:
1. Change to the project root directory
2. Deploy the bundle to the specified target
3. Find the job ID automatically
4. Run the pipeline with custom parameters

### Monitor Execution
The pipeline provides detailed logging:
- Progress updates for each table generation step
- Row counts for each generated table
- Success/failure status for each write operation
- Final summary with total row counts

## Data Generation Sequence

The pipeline generates data in the correct sequence to maintain referential integrity:

1. **Dimension tables** (stores, products, customers)
2. **Promotions** (depends on products)
3. **Orders and order items** (depend on all dimensions)
4. **Inventory snapshots** (depend on stores and products)

## Error Handling

- **Fail-fast**: If any table generation fails, the entire pipeline fails
- **Data consistency**: All tables are generated together or none are written
- **Detailed logging**: Clear error messages and progress tracking

## Future Enhancements

The pipeline is designed to support future merge operations:
- **Merge mode**: Update existing records and insert new ones
- **Volume matching**: Automatically adjust data volume to match configuration
- **Incremental updates**: Add only new data without overwriting existing

## Local Development

For local development and testing, you can use the CLI wrapper script from the project root:

```bash
# Generate small dataset
python generate_data.py --scale small --days 14 --output-dir sample_data

# Generate medium dataset with custom output
python generate_data.py --scale medium --days 30 --output-dir my_data
```

This generates CSV files locally while the pipeline generates Delta tables in Databricks.

## Project Structure

This pipeline is organized in the `pipelines/data_generation/` folder:

```
sqlwarehouse-to-lakebase-backend-comparison/
├── databricks.yml                    # Main bundle configuration
├── generate_data.py                  # CLI wrapper script (project root)
├── app/                              # Application code
├── pipelines/
│   └── data_generation/              # This pipeline
│       ├── data_generators.py        # Data generation functions
│       ├── generate_data_job.py      # Databricks job script
│       ├── config.json               # Configuration options
│       ├── deploy.sh                 # Deployment script
│       └── README.md                 # This file
└── ...
```

The pipeline resources (job and environment) are defined in the main `databricks.yml` file, ensuring everything is managed as a single bundle.

## File Organization

- **`data_generators.py`** - Contains all the data generation functions (gen_stores, gen_products, etc.)
- **`generate_data_job.py`** - The Databricks job script that imports from data_generators.py and writes to Delta tables
- **CLI wrapper** - The `generate_data.py` in the project root provides command-line access to the functions

This separation allows:
- Clean imports without circular dependencies
- Easy local development via CLI
- Simple pipeline execution in Databricks
- Maintainable code organization
