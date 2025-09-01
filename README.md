# LoyaltyPipeline

A Python-based data processing pipeline that transforms Excel transaction data into structured CSV files for loyalty program analysis.

## Overview

The LoyaltyPipeline processes raw transaction data from Excel files and generates two main outputs:
- **SLS (Sales Header)**: Transaction-level summary data
- **SDET (Sales Detail)**: Item-level detail data

## Features

- **Multi-sheet Excel Processing**: Reads data from multiple Excel sheets
- **Branch Mapping**: Maps store numbers to branch information
- **Data Transformation**: Converts raw data into structured format
- **Type Conversion**: Applies proper data types for database compatibility
- **Dynamic File Naming**: Generates descriptive output filenames with date, brand, and branch

## Requirements

- Python 3.x
- pandas
- numpy
- openpyxl (for Excel file handling)

## File Structure

```
JumsApp/
├── LoyaltyPipeline.py          # Main pipeline script
├── branch_mapping.xlsx         # Branch/store mapping data
├── output_data/               # Generated CSV files
├── *.xlsx                     # Input Excel transaction files
└── README.md                  # This file
```

## Usage

1. Ensure your Excel transaction file is in the project directory
2. Update the `EXCEL_FILE_PATH` variable in `LoyaltyPipeline.py`
3. Run the pipeline:

```bash
python3 LoyaltyPipeline.py
```

## Input Data Structure

The pipeline expects Excel files with the following sheets:
- **Transaction Header**: Main transaction data
- **Trans. Sales Entry**: Item-level sales data
- **Trans. Payment Entry**: Payment information
- **Trans. Infocode Entry**: Additional transaction codes

## Output Files

- `SLS_[date]_[brand]_[branch].csv`: Sales header data
- `SDET_[date]_[brand]_[branch].csv`: Sales detail data

## Configuration

Key configuration parameters in `LoyaltyPipeline.py`:
- `EXCEL_FILE_PATH`: Path to input Excel file
- `OUTPUT_DIRECTORY`: Output directory for CSV files
- `BRANCH_MAPPING_FILE`: Branch mapping Excel file

## Architecture

The pipeline follows a modular architecture with the following components:

- **ExcelDataReader**: Handles Excel file reading
- **BaseDataFrameTransformer**: Abstract base for data transformations
- **SlsDataFrameTransformer**: Processes sales header data
- **SdetDataFrameTransformer**: Processes sales detail data
- **DataTypeConverter**: Handles data type conversions
- **DataSaver**: Manages CSV file output
- **PipelineRunner**: Orchestrates the entire pipeline

## Example Output

Successfully processed files:
```
output_data/SLS_070925_MANAM_MANAM_SM_CEBU.csv    # 1,834 transactions
output_data/SDET_070925_MANAM_MANAM_SM_CEBU.csv   # 13,664 line items
```

## Error Handling

The pipeline includes comprehensive error handling for:
- Missing files
- Column mapping issues
- Data type conversion errors
- Branch mapping mismatches

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

[Add your license information here]
