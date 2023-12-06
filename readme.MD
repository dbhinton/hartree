# Data Processing with Pandas and Apache Beam

## Overview

This repository contains Python scripts to perform data processing tasks using two different frameworks: Pandas and Apache Beam. The task involves joining two input datasets, dataset1 and dataset2, and generating an output file with specific calculations and aggregation. The goal is to calculate metrics for legal entities, counterparties, and tiers based on the provided data.

### Requirements

- Python 3.x
- Pandas (for Pandas framework)
- Apache Beam (for Apache Beam framework)

### Input Data

Two input CSV files are used for this task:

1. **dataset1.csv:** Contains information about invoices, including legal entities, counterparties, ratings, statuses, and values.
2. **dataset2.csv:** Contains information about counterparties and their associated tiers.

### Output

The output file generated by both frameworks will contain the following columns:

- `legal_entity`: Legal entity
- `counterparty`: Counterparty
- `tier`: Tier
- `max(rating by counterparty)`: Maximum rating by counterparty
- `sum(value where status=ARAP)`: Sum of values where status is ARAP
- `sum(value where status=ACCR)`: Sum of values where status is ACCR

Additionally, the output will include new records to represent totals for each of the legal entities, counterparties, and tiers.

## Running the Code

### Pandas Framework

To run the code using the Pandas framework:

1. Ensure you have Python 3.x installed.

2. Install the Pandas library if you don't already have it installed:

pip install pandas


3. Place your `dataset1.csv` and `dataset2.csv` files in the same directory as the script `pandas_version.py`.

4. Open a terminal and navigate to the directory containing the script


5. Run the Pandas script:

`python pandas_version_.py`


6. The script will process the data and generate an output CSV file named `output_pandas.csv` in the same directory.

### Apache Beam Framework

To run the code using the Apache Beam framework:

1. Ensure you have Python 3.x installed.

2. Install the Apache Beam library if you don't already have it installed:

`pip install apache-beam`

3. Place your `dataset1.csv` and `dataset2.csv` files in the same directory as the script `apache_beam_framework.py`.

4. Open a terminal and navigate to the directory containing the script


5. Run the Apache Beam script:

python my_beam_script.py


6. The script will process the data and generate an output text file named `output_beam.txt` in the same directory.

## Output Files

The output files (`output_pandas.csv` and `output_beam.txt`) will contain the calculated metrics and totals for legal entities, counterparties, and tiers based on the input data.

Feel free to reach out if you have any questions or encounter any issues while running the code.
# hartree