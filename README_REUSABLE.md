# LoyaltyPipeline - Reusable Edition

A Python-based data processing pipeline that transforms Excel transaction data into structured CSV files for loyalty program analysis. **Now fully optimized for orchestrator integration.**

## Overview

The LoyaltyPipeline processes raw transaction data from Excel files and generates two main outputs:
- **SLS (Sales Header)**: Transaction-level summary data
- **SDET (Sales Detail)**: Item-level detail data

## 🔥 New Reusable Interface for Orchestrators

### Quick Start for Orchestrators

```python
from LoyaltyPipeline import PipelineRunner

# Create pipeline from files (one-liner)
pipeline = PipelineRunner.create_from_files(
    excel_file_path="your_file.xlsx",
    branch_mapping_file_path="branch_mapping.xlsx",
    output_directory="output_data"
)

# Process only SLS (Sales Header)
sls_file = pipeline.process_sls_only(verbose=False)

# Process only SDET (Sales Detail)  
sdet_file = pipeline.process_sdet_only(verbose=False)

# Process both
both_files = pipeline.process_both(verbose=False)
```

### Available Methods for Orchestrators

| Method | Description | Returns |
|--------|-------------|---------|
| `PipelineRunner.create_from_files()` | Factory method to create pipeline from file paths | PipelineRunner instance |
| `process_sls_only(verbose=False)` | Process SLS (Sales Header) only | String (CSV file path) |
| `process_sdet_only(verbose=False)` | Process SDET (Sales Detail) only | String (CSV file path) |
| `process_both(verbose=False)` | Process both SLS and SDET | Dict with 'sls_file' and 'sdet_file' keys |

### Key Features for Orchestrators

- ✅ **One-liner instantiation** with `create_from_files()`
- ✅ **Selective processing** - choose SLS only, SDET only, or both
- ✅ **Silent mode** with `verbose=False` for clean orchestrator logs
- ✅ **Data caching** - Excel data loaded once, reused for multiple processes
- ✅ **Error handling** - Clean exception propagation
- ✅ **Backward compatibility** - Existing code still works

## Requirements

- Python 3.x
- pandas
- numpy
- openpyxl (for Excel file handling)

## File Structure

```
JumsApp/
├── LoyaltyPipeline.py          # Main pipeline script (now reusable)
├── orchestrator_demo.py        # Example for orchestrators
├── branch_mapping.xlsx         # Branch/store mapping data
├── output_data/               # Generated CSV files
├── *.xlsx                     # Input Excel transaction files
└── README.md                  # This file
```

## Usage Examples

### For Orchestrators

```python
# orchestrator_example.py
from LoyaltyPipeline import PipelineRunner

def process_transaction_file(file_path, branch_mapping, output_dir, process_type="both"):
    """Function that orchestrators can call directly"""
    pipeline = PipelineRunner.create_from_files(file_path, branch_mapping, output_dir)
    
    if process_type == "sls":
        return pipeline.process_sls_only(verbose=False)
    elif process_type == "sdet":
        return pipeline.process_sdet_only(verbose=False)
    else:
        return pipeline.process_both(verbose=False)

# Usage
result = process_transaction_file(
    file_path="transaction_data.xlsx",
    branch_mapping="branch_mapping.xlsx", 
    output_dir="output_data",
    process_type="sls"  # or "sdet" or "both"
)
```

### Standalone Usage (Traditional)

```bash
python3 LoyaltyPipeline.py
```

The script will demonstrate both the new reusable interface and the legacy approach.

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

Key configuration is handled automatically by the `create_from_files()` method:
- Column mappings
- Data type conversions
- Branch mapping integration
- Default value assignments

## Architecture

The pipeline follows a modular architecture optimized for reusability:

```
PipelineRunner (Orchestrator Interface)
├── ExcelDataReader (File Processing)
├── SlsDataFrameTransformer (Sales Header Processing)
├── SdetDataFrameTransformer (Sales Detail Processing)
├── DataTypeConverter (Type Management)
└── DataSaver (Output Management)
```

## Orchestrator Integration

Perfect for integration into larger data processing systems:

1. **Detect transaction files** in your watched directories
2. **Call `PipelineRunner.create_from_files()`** with the file path
3. **Choose processing type** based on your requirements
4. **Get clean file paths** for further processing
5. **Handle errors** gracefully with try/catch

## Example Output

Successfully processed files:
```
output_data/SLS_081725_MANAM_MANAM_GREENHILLS.csv    # 3,261 transactions
output_data/SDET_081725_MANAM_MANAM_GREENHILLS.csv   # 17,738 line items
```

## Branch Information

Current branch: `feature/reusable-pipeline`
- New reusable interface for orchestrators
- Backward compatibility maintained
- Enhanced error handling and logging control

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly with both interfaces
5. Submit a pull request

## License

[Add your license information here]
