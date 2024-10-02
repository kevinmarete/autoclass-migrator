# AutoClass Migrator

## Overview

**AutoClass Migrator** is a Python script designed to automate the migration of GCP buckets to Autoclass and sets the terminal storage class to `ARCHIVE`. This overall helps reduce costs in storage buckets by enable Autoclass which determines automatically sets storage types based on frequency of access.

## Features

- Automates migration of buckets to Autoclass.
- Supports input-file selection.
- Provides logging of migration processes and errors.
- Fast processing through adoption of multi-threading.
- Thread safe implementation with retry and backoff mechanism.

## Requirements

- Python 3.x
- Required packages specified in `requirements.txt`

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/kevinmarete/autoclass-migrator.git
   cd autoclass-migrator
   
2. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. Run the script:
   ```bash
   python main.py -f <input_file>
   ```
    where `<input_file>` is the path to the input file containing the list of GCP buckets to migrate.

2. The script will automatically migrate the buckets to Autoclass and set the terminal storage class to `ARCHIVE`.
3. Output will be generated to a csv file in the same directory as the input file called `<input_file>_output.csv`.
4. The script will also log the migration processes and errors to a file in the same directory as the input file called `<input_file>_output.log`.