import numpy as np
import pandas as pd
import datetime
import os

# Set display options for pandas
pd.set_option('display.max_columns', None)

class ExcelDataReader:
    """
    Handles reading all sheets from an Excel file into a dictionary of DataFrames.
    Applies the specified header row.
    """
    def __init__(self, file_path: str, header_row: int = 2):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Excel file not found at: {file_path}")
        self.file_path = file_path
        self.header_row = header_row

    def read_sheets(self) -> dict[str, pd.DataFrame]:
        """Reads all sheets from the Excel file."""
        print(f"Reading Excel file: {self.file_path}")
        all_sheets_dict = pd.read_excel(self.file_path, sheet_name=None, header=self.header_row)
        for sheet_name, df in all_sheets_dict.items():
            print(f"  Sheet '{sheet_name}' loaded with shape: {df.shape}")
            # Optional: display first few rows for quick verification
            # print(f"  {df.head().to_string()}")
        return all_sheets_dict

class BaseDataFrameTransformer:
    """
    Abstract base class for DataFrame transformers.
    Enforces a common interface for transformation.
    """
    def __init__(self, branch_mapping_df: pd.DataFrame, target_columns: list[str], column_mapping: dict[str, str]):
        self.branch_mapping_df = branch_mapping_df
        self.target_columns = target_columns
        self.column_mapping = column_mapping
        self.default_mapping = {}

    def _set_default_mapping(self, trans_header: pd.DataFrame):
        """
        Dynamically sets default values based on the transaction header and a predefined static map.
        This part is common for both SLS and SDET to derive branch/brand info.
        """
        map_to_use = self.branch_mapping_df.loc[
            self.branch_mapping_df['erp_code'].fillna(-1).astype(int).isin(trans_header['Store No.'])
        ]

        # Common default values
        self.default_mapping = {
            'branch_id': map_to_use['brand_id'].iloc[0] if not map_to_use.empty else None,
            'branch': map_to_use['branch'].iloc[0] if not map_to_use.empty else None,
            'brand': map_to_use['brand'].iloc[0] if not map_to_use.empty else None,
            'posted': False,
            # Specific defaults will be added by subclasses
        }
        print("Base default mapping initialized dynamically.")

    def _apply_column_mapping_and_defaults(self, source_df: pd.DataFrame, target_df: pd.DataFrame):
        """Applies column mapping and then default values to the target DataFrame."""
        for target_col, source_col in self.column_mapping.items():
            if source_col in source_df.columns:
                target_df[target_col] = source_df[source_col]
            else:
                print(f"Warning: Source column '{source_col}' not found in source_df. Column '{target_col}' in target_df will be empty.")

        for default_col, default_value in self.default_mapping.items():
            if default_col in target_df.columns:
                if target_df[default_col].isnull().all() or \
                   (target_df[default_col].isnull().any() and default_value is not None):
                    target_df[default_col] = target_df[default_col].fillna(default_value)
            else:
                print(f"Warning: Default column '{default_col}' not found in target_df.")
        return target_df

    def transform(self, data_sheets: dict[str, pd.DataFrame]) -> pd.DataFrame:
        raise NotImplementedError("Subclasses must implement the 'transform' method.")


class SlsDataFrameTransformer(BaseDataFrameTransformer):
    """
    Responsible for merging, selecting, renaming, and applying default values
    to DataFrames to create the final sales header table (SLS).
    """
    def __init__(self, branch_mapping_df: pd.DataFrame, sls_columns: list[str], column_mapping: dict[str, str]):
        super().__init__(branch_mapping_df, sls_columns, column_mapping)

    def _set_default_mapping(self, trans_header: pd.DataFrame):
        super()._set_default_mapping(trans_header) # Call base method for common defaults
        # SLS-specific default values
        self.default_mapping.update({
            'login_ref': 0,
            'cash_draw': 1,
            'sale_area': 1,
            'acc_posted': False,
            'dollardisc': 0,
            'hidden': False,
            'tax_table': 1,
            'cust_id': 0,
            'phone_id': 0,
            'gratovrpct': -99.99,
            'gratovramt': -10000000000,
            'cash_back': 0,
            'start_stn': 0,
            'settle_stn': 0,
            'waiter0': 0,
            'tray_ref': 1,
            'notaxamt': 0,
            'address_id': 0,
            'addr_mode': 0,
            'advconvert': 0,
            'printed': True
        })
        print("SLS-specific default mapping initialized.")

    def transform(self, data_sheets: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Performs the merging, column mapping, and default value application for SLS.
        """
        trans_header = data_sheets['Transaction Header']
        trans_sales_entry = data_sheets['Trans. Sales Entry']
        trans_payment_entry = data_sheets['Trans. Payment Entry']
        trans_infocode_entry = data_sheets['Trans. Infocode Entry']

        if 'Store No.' not in trans_header.columns:
            raise ValueError("'Store No.' column not found in 'Transaction Header' sheet. Cannot determine branch.")

        self._set_default_mapping(trans_header) # Initialize default mapping

        print("SLS: Filtering transaction header...")
        sls_df = trans_header[
            (trans_header['Transaction No.'].astype(str).isin(trans_sales_entry['Transaction No.'].astype(str))) &
            (trans_header['Transaction Type'].astype(str) == "Sales")
        ].copy()[[
            'Transaction No.', 'Table No.', 'No. of Covers', 'VAT Amount',
            'Income/Exp. Amount', 'Sales Type', 'Discount Amount'
        ]]
        
        # Convert Transaction No. to string in sls_df
        sls_df['Transaction No.'] = sls_df['Transaction No.'].astype(str)

        print("SLS: Processing sales entry data...")
        tse_sls_temp = trans_sales_entry[(trans_sales_entry['Transaction No.'].astype(str).isin(sls_df['Transaction No.'].astype(str)))].copy()
        tse_sls_temp['Price'] = pd.to_numeric(tse_sls_temp['Price'], errors='coerce').fillna(0)

        tse_sls_temp['Transaction No.'] = tse_sls_temp['Transaction No.'].astype(str)
        # Handle potential missing columns gracefully for TSE merge
        for col in ['Staff ID', 'Discount Module Name', 'Time', 'Date']:
            if col not in tse_sls_temp.columns:
                tse_sls_temp[col] = '' # Add empty column if missing
            else:
                tse_sls_temp[col] = tse_sls_temp[col].astype(str)

        tse_sls_grouped = tse_sls_temp.groupby('Transaction No.').agg(
            Staff_ID=('Staff ID', 'first'),
            Discount_Module_Name=('Discount Module Name', lambda x: x.mode()[0] if not x.mode().empty else ''),
            Time=('Time', 'first'),
            Date=('Date', 'first'),
            Price=('Price', 'sum')
        ).reset_index()
        tse_sls_grouped.rename(columns={
            'Staff_ID': 'Staff ID',
            'Discount_Module_Name': 'Discount Module Name'
        }, inplace=True)

        print("SLS: Processing payment entry data...")
        tpe_sls = trans_payment_entry[(trans_payment_entry['Transaction No.'].astype(str).isin(sls_df['Transaction No.'].astype(str)))].copy()
        tpe_sls['Transaction No.'] = tpe_sls['Transaction No.'].astype(str)
        if 'Tender Type' not in tpe_sls.columns:
            tpe_sls['Tender Type'] = ''
        else:
            tpe_sls['Tender Type'] = tpe_sls['Tender Type'].astype(str)
        tpe_sls = tpe_sls.drop_duplicates(subset=['Transaction No.'])

        print("SLS: Processing infocode entry data...")
        tie_sls = trans_infocode_entry[
            (trans_infocode_entry['Transaction No.'].astype(str).isin(sls_df['Transaction No.'].astype(str))) &
            (trans_infocode_entry['Infocode'] == "1MOMENT")
        ].copy()
        tie_sls['Transaction No.'] = tie_sls['Transaction No.'].astype(str)
        if 'Information' not in tie_sls.columns:
            tie_sls['Information'] = ''
        else:
            tie_sls['Information'] = tie_sls['Information'].astype(str)

        print("SLS: Merging dataframes...")
        merged_df = pd.merge(sls_df, tse_sls_grouped, on='Transaction No.', how='left')
        merged_df = pd.merge(merged_df, tpe_sls, on='Transaction No.', how='left', suffixes=('', '_payment'))
        merged_df = pd.merge(merged_df, tie_sls, on='Transaction No.', how='left', suffixes=('', '_infocode'))
        
        # Fix column naming conflicts - prioritize TSE (Sales Entry) Time/Date over others
        if 'Time_infocode' in merged_df.columns and 'Time' in merged_df.columns:
            # Keep TSE Time, rename infocode Time
            merged_df = merged_df.drop(columns=['Time_infocode'])
        if 'Date_infocode' in merged_df.columns and 'Date' in merged_df.columns:
            # Keep TSE Date, rename infocode Date  
            merged_df = merged_df.drop(columns=['Date_infocode'])
            
        # Handle payment entry conflicts
        if 'Time_payment' in merged_df.columns:
            merged_df = merged_df.drop(columns=['Time_payment']) 
        if 'Date_payment' in merged_df.columns:
            merged_df = merged_df.drop(columns=['Date_payment'])

        # Convert all applicable columns to string before mapping to avoid type conflicts during merge.
        # This is a robust approach, then specific types are applied by DataTypeConverter
        for col in merged_df.columns:
            if col in ['Transaction No.', 'Staff ID', 'Discount Module Name', 'Tender Type', 'Sales Type', 'Information']:
                merged_df[col] = merged_df[col].astype(str)
            else:
                merged_df[col] = pd.to_numeric(merged_df[col], errors='ignore')
        merged_df = merged_df.convert_dtypes() # Use convert_dtypes for initial flexible conversion

        print("SLS: Creating sls_table and applying column mapping & defaults...")
        sls_table = pd.DataFrame(index=merged_df.index, columns=self.target_columns)
        sls_table = self._apply_column_mapping_and_defaults(merged_df, sls_table)

        if 'bill_no' in sls_table.columns:
            sls_table['bill_no'] = sls_table['bill_no'].astype(str)

        return sls_table

class SdetDataFrameTransformer(BaseDataFrameTransformer):
    """
    Responsible for selecting, renaming, and applying default values
    to DataFrames to create the final sales detail table (SDET).
    """
    def __init__(self, branch_mapping_df: pd.DataFrame, sdet_columns: list[str], column_mapping: dict[str, str]):
        super().__init__(branch_mapping_df, sdet_columns, column_mapping)

    def _set_default_mapping(self, trans_header: pd.DataFrame):
        super()._set_default_mapping(trans_header) # Call base method for common defaults
        # SDET-specific default values
        self.default_mapping.update({
            'posted': False, # Already in base, but explicit for clarity
            'del_code': False,
            'prc_adj': False,
            'two4one': False,
            'disc_no': '', # Ensure disc_no defaults to empty string instead of NaN
            'prc_lvl': 0,
            'prc_lvl0': 0,
            'iscoupon': False,
            'item_adj': 0,
            'pricemult': 1,
            'invmult': 1,
            'gd_no': 0,
            'adj_no': 0,
            'refundflag': False,
            'cou_rec': 0,
            'coupitem': False,
            'hash_stat': 0
        })
        print("SDET-specific default mapping initialized.")

    def transform(self, data_sheets: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Performs the selection, column mapping, and default value application for SDET.
        """
        trans_sales_entry = data_sheets['Trans. Sales Entry']
        trans_header = data_sheets['Transaction Header'] # Needed for branch mapping

        if 'Store No.' not in trans_header.columns:
            raise ValueError("'Store No.' column not found in 'Transaction Header' sheet. Cannot determine branch for SDET.")

        self._set_default_mapping(trans_header) # Initialize default mapping

        print("SDET: Filtering sales entry data...")
        # Ensure 'Transaction No.' is of comparable type (string)
        trans_sales_entry_filtered = trans_sales_entry[
            trans_sales_entry['Transaction No.'].astype(str).isin(trans_header['Transaction No.'].astype(str))
        ].copy()

        # Select desired columns for sdet_df, ensuring they exist
        required_cols = [
            'Transaction No.', 'Item No.', 'Quantity', 'Price', 'Net Price',
            'Cost Amount', 'Staff ID', 'Discount Module Name', 'VAT Amount',
            'Discount Amount', 'Time', 'Date'
        ]
        sdet_df = trans_sales_entry_filtered[[col for col in required_cols if col in trans_sales_entry_filtered.columns]].copy()

        # Handle missing columns by adding them as empty if not present in source
        for col in required_cols:
            if col not in sdet_df.columns:
                sdet_df[col] = np.nan # Use NaN for numerical, will be filled by default mapping

        # Ensure numeric columns are actually numeric, coercing errors
        sdet_df['Quantity'] = pd.to_numeric(sdet_df['Quantity'], errors='coerce')
        sdet_df['Price'] = pd.to_numeric(sdet_df['Price'], errors='coerce')
        sdet_df['Net Price'] = pd.to_numeric(sdet_df['Net Price'], errors='coerce')
        sdet_df['Cost Amount'] = pd.to_numeric(sdet_df['Cost Amount'], errors='coerce')
        sdet_df['VAT Amount'] = pd.to_numeric(sdet_df['VAT Amount'], errors='coerce')
        sdet_df['Discount Amount'] = pd.to_numeric(sdet_df['Discount Amount'], errors='coerce')


        sdet_df['Quantity'] = sdet_df['Quantity'].abs()
        sdet_df['Cost Amount'] = sdet_df['Cost Amount'].abs()

        # Convert to object type for flexible initial merging, handling NaN properly
        for col in sdet_df.columns:
            # For string/text columns, replace NaN with empty string before converting to string
            if col in ['Discount Module Name', 'Staff ID']:
                sdet_df[col] = sdet_df[col].fillna('').astype(str)
            else:
                sdet_df[col] = sdet_df[col].astype(str)

        print("SDET: Creating sdet_table and applying column mapping & defaults...")
        sdet_table = pd.DataFrame(index=sdet_df.index, columns=self.target_columns)
        sdet_table = self._apply_column_mapping_and_defaults(sdet_df, sdet_table)

        return sdet_table

class DataTypeConverter:
    """
    Manages the conversion of DataFrame columns to the specified SQL-like data types.
    This is a generic converter that can be reused for different dataframes
    by providing the specific dtype map.
    """
    def __init__(self, sql_to_pandas_dtype_map: dict[str, str]):
        self.sql_to_pandas_dtype_map = sql_to_pandas_dtype_map

    def convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applies data type conversions to the DataFrame."""
        print("Applying final data type conversions...")

        df_copy = df.copy() # Work on a copy to avoid SettingWithCopyWarning

        # Separate columns by target dtype for specific handling
        datetime_columns = {col: dtype for col, dtype in self.sql_to_pandas_dtype_map.items() if dtype == 'datetime64[ns]' and col in df_copy.columns}
        integer_columns = {col: dtype for col, dtype in self.sql_to_pandas_dtype_map.items() if dtype == 'int64' and col in df_copy.columns}
        object_columns = {col: dtype for col, dtype in self.sql_to_pandas_dtype_map.items() if dtype == 'object' and col in df_copy.columns}
        float_columns = {col: dtype for col, dtype in self.sql_to_pandas_dtype_map.items() if dtype == 'float64' and col in df_copy.columns}
        boolean_columns = {col: dtype for col, dtype in self.sql_to_pandas_dtype_map.items() if dtype == 'bool' and col in df_copy.columns}

        # Handle datetime columns (conversion to datetime then formatting)
        for col in datetime_columns:
            df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')
            if 'time' in col:
                df_copy[col] = df_copy[col].dt.strftime("%H:%M:%S").replace({pd.NA: ""})
            else:
                df_copy[col] = df_copy[col].dt.strftime("%Y-%m-%d").replace({pd.NA: ""})

        # Handle integer columns
        for col in integer_columns:
            df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0).astype('int64')

        # Handle float columns
        for col in float_columns:
            df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0.0)
            
            # Apply proper rounding for monetary/financial columns to avoid precision errors
            if col in ['total', 'received', 'taxes', 'auto_grat', 'discount', 'taxable', 
                      'disc_pct', 'dollardisc', 'gratovrpct', 'gratovramt', 'cash_back',
                      'notaxamt', 'price_paid', 'raw_price', 'cost', 'vat_adj', 'disc_adj']:
                # Round to 2 decimal places for currency values
                df_copy[col] = df_copy[col].round(2)
            else:
                # Round to 4 decimal places for other float values  
                df_copy[col] = df_copy[col].round(4)
                
            df_copy[col] = df_copy[col].astype('float64')

        # Handle boolean columns
        for col in boolean_columns:
            true_values = [True, 1, 'True', 'true', 'TRUE', 'Y', 'y', 'Yes', 'yes']
            false_values = [False, 0, 'False', 'false', 'FALSE', 'N', 'n', 'No', 'no', '', 'None']
            # Convert to object first to handle mixed types gracefully, then map
            df_copy[col] = df_copy[col].astype(str).apply(
                lambda x: True if x in true_values else (False if x in false_values else pd.NA)
            )
            df_copy[col] = df_copy[col].fillna(False).astype('boolean') # Use 'boolean' for nullable boolean dtype

        # Handle object (string) columns - do this last as other conversions might rely on initial object type
        for col in object_columns:
            # Convert to string and handle 'nan' strings by replacing them with empty strings
            df_copy[col] = df_copy[col].astype(str).replace('nan', '').fillna("")

        print("Data type conversion completed.")
        return df_copy


class DataSaver:
    """
    Handles saving the processed DataFrame to a CSV file.
    Can be configured for different prefixes (SLS, SDET).
    """
    def __init__(self, output_dir: str = "."):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True) # Ensure output directory exists

    def save(self, df: pd.DataFrame, filename_prefix: str) -> str:
        """
        Saves the DataFrame to a CSV file with a dynamic filename.
        """
        # Attempt to get the latest date from 'bill_date' or 'ord_date'
        date_col = None
        if 'bill_date' in df.columns:
            date_col = 'bill_date'
        elif 'ord_date' in df.columns:
            date_col = 'ord_date'

        last_date = "UNKNOWN_DATE"
        if date_col and pd.api.types.is_datetime64_any_dtype(df[date_col]):
            max_date = df[date_col].max()
            if pd.notna(max_date):
                last_date = max_date.strftime("%m%d%y")
        elif date_col: # Try converting if not already datetime
             temp_date_series = pd.to_datetime(df[date_col], errors='coerce')
             max_date = temp_date_series.max()
             if pd.notna(max_date):
                 last_date = max_date.strftime("%m%d%y")


        brand = df['brand'].astype(str).unique()[0].upper() if 'brand' in df.columns and not df['brand'].isnull().all() else "UNKNOWN_BRAND"
        branch = df['branch'].astype(str).unique()[0].upper().replace(" ", "_") if 'branch' in df.columns and not df['branch'].isnull().all() else "UNKNOWN_BRANCH"

        output_filename = os.path.join(self.output_dir, f"{filename_prefix}_{last_date}_{brand}_{branch}.csv")
        print(f"Saving processed data to: {output_filename}")
        df.to_csv(output_filename, index=False)
        print(f"CSV file '{output_filename}' generated successfully.")
        return output_filename

class PipelineRunner:
    """
    Orchestrates multiple data processing pipelines (e.g., SLS and SDET)
    using injected dependencies. Designed for reusability in orchestrator systems.
    """
    def __init__(self,
                 data_reader: ExcelDataReader,
                 sls_transformer: SlsDataFrameTransformer,
                 sdet_transformer: SdetDataFrameTransformer,
                 sls_type_converter: DataTypeConverter, # Reusing generic converter
                 sdet_type_converter: DataTypeConverter, # Reusing generic converter
                 data_saver: DataSaver):
        self.data_reader = data_reader
        self.sls_transformer = sls_transformer
        self.sdet_transformer = sdet_transformer
        self.sls_type_converter = sls_type_converter
        self.sdet_type_converter = sdet_type_converter
        self.data_saver = data_saver
        self._cached_sheets = None  # Cache for loaded sheets

    @classmethod
    def create_from_files(cls, excel_file_path: str, branch_mapping_file_path: str, output_directory: str = "output_data"):
        """
        Factory method to create a PipelineRunner from file paths.
        This is the main entry point for orchestrators.
        
        Args:
            excel_file_path: Path to the Excel transaction file
            branch_mapping_file_path: Path to the branch mapping Excel file
            output_directory: Output directory for CSV files
            
        Returns:
            Configured PipelineRunner instance
        """
        # Load branch mapping
        branch_mapping_df = pd.read_excel(branch_mapping_file_path)
        
        # Create components with configurations
        data_reader = ExcelDataReader(file_path=excel_file_path, header_row=2)
        
        sls_transformer = SlsDataFrameTransformer(
            branch_mapping_df=branch_mapping_df,
            sls_columns=cls._get_sls_columns(),
            column_mapping=cls._get_sls_column_mapping()
        )
        
        sdet_transformer = SdetDataFrameTransformer(
            branch_mapping_df=branch_mapping_df,
            sdet_columns=cls._get_sdet_columns(),
            column_mapping=cls._get_sdet_column_mapping()
        )
        
        sls_type_converter = DataTypeConverter(sql_to_pandas_dtype_map=cls._get_sls_dtype_map())
        sdet_type_converter = DataTypeConverter(sql_to_pandas_dtype_map=cls._get_sdet_dtype_map())
        
        data_saver = DataSaver(output_dir=output_directory)
        
        return cls(data_reader, sls_transformer, sdet_transformer, 
                  sls_type_converter, sdet_type_converter, data_saver)

    def load_data(self) -> dict[str, pd.DataFrame]:
        """
        Load and cache Excel data. Called automatically by pipeline methods.
        
        Returns:
            Dictionary of sheet name to DataFrame
        """
        if self._cached_sheets is None:
            self._cached_sheets = self.data_reader.read_sheets()
        return self._cached_sheets

    def process_sls_only(self, verbose: bool = True) -> str:
        """
        Process SLS (Sales Header) pipeline only.
        Perfect for orchestrators that need selective processing.
        
        Args:
            verbose: Whether to print detailed processing information
            
        Returns:
            Path to the generated SLS CSV file
        """
        all_sheets_dict = self.load_data()
        return self.run_sls_pipeline(all_sheets_dict, verbose=verbose)

    def process_sdet_only(self, verbose: bool = True) -> str:
        """
        Process SDET (Sales Detail) pipeline only.
        Perfect for orchestrators that need selective processing.
        
        Args:
            verbose: Whether to print detailed processing information
            
        Returns:
            Path to the generated SDET CSV file
        """
        all_sheets_dict = self.load_data()
        return self.run_sdet_pipeline(all_sheets_dict, verbose=verbose)

    def process_both(self, verbose: bool = True) -> dict[str, str]:
        """
        Process both SLS and SDET pipelines.
        
        Args:
            verbose: Whether to print detailed processing information
            
        Returns:
            Dictionary with 'sls_file' and 'sdet_file' paths
        """
        all_sheets_dict = self.load_data()
        
        if verbose:
            print("\n--- Starting All Data Pipelines ---")
        
        sls_output = self.run_sls_pipeline(all_sheets_dict, verbose=verbose)
        sdet_output = self.run_sdet_pipeline(all_sheets_dict, verbose=verbose)

        if verbose:
            print("\n--- All Data Pipelines Finished ---")
        return {"sls_file": sls_output, "sdet_file": sdet_output}

    def run_sls_pipeline(self, all_sheets_dict: dict[str, pd.DataFrame], verbose: bool = True) -> str:
        """Executes the SLS data pipeline."""
        if verbose:
            print("\n--- SLS Data Pipeline Started ---")
        
        processed_df = self.sls_transformer.transform(all_sheets_dict)
        
        if verbose:
            print(f"\nSLS DataFrame after transformation (first 5 rows):\n{processed_df.head().to_string()}")
            print(f"\nSLS DataFrame after transformation (info):\n")
            processed_df.info()

        final_df = self.sls_type_converter.convert_types(processed_df)
        
        if verbose:
            print(f"\nSLS DataFrame after type conversion (first 5 rows):\n{final_df.head().to_string()}")
            print(f"\nSLS DataFrame after type conversion (info):\n")
            final_df.info()

        output_file_path = self.data_saver.save(final_df, filename_prefix="SLS")
        
        if verbose:
            print("--- SLS Data Pipeline Finished ---")
        return output_file_path

    def run_sdet_pipeline(self, all_sheets_dict: dict[str, pd.DataFrame], verbose: bool = True) -> str:
        """Executes the SDET data pipeline."""
        if verbose:
            print("\n--- SDET Data Pipeline Started ---")
        
        processed_df = self.sdet_transformer.transform(all_sheets_dict)
        
        if verbose:
            print(f"\nSDET DataFrame after transformation (first 5 rows):\n{processed_df.head().to_string()}")
            print(f"\nSDET DataFrame after transformation (info):\n")
            processed_df.info()

        final_df = self.sdet_type_converter.convert_types(processed_df)
        
        if verbose:
            print(f"\nSDET DataFrame after type conversion (first 5 rows):\n{final_df.head().to_string()}")
            print(f"\nSDET DataFrame after type conversion (info):\n")
            final_df.info()

        output_file_path = self.data_saver.save(final_df, filename_prefix="SDET")
        
        if verbose:
            print("--- SDET Data Pipeline Finished ---")
        return output_file_path

    def run_all_pipelines(self) -> dict[str, str]:
        """Runs all defined pipelines. Legacy method for backward compatibility."""
        return self.process_both(verbose=True)

    @staticmethod
    def _get_sls_columns() -> list[str]:
        """Returns SLS target columns configuration."""
        return [
            'branch_id', 'brand','branch', 'bill_no', 'fact_no', 'session_no',
            'table', 'seat_no', 'waiter', 'people_no', 'pay_type',
            'disc_type', 'open_time', 'date', 'bill_time', 'bill_date',
            'printed', 'posted', 'total', 'received', 'taxes',
            'auto_grat', 'login_ref', 'cash_draw', 'sale_type', 'sale_area',
            'acc_posted', 'discount', 'dollardisc', 'taxable', 'prt_time',
            'prt_date', 'disc_pct', 'hidden', 'tax_table', 'account_no',
            'rev_center', 'phone', 'cust_id', 'phone_id', 'gratovrpct',
            'gratovramt', 'note', 'sls_name', 'cash_back', 'trans_id', 'start_stn',
            'settle_stn', 'waiter0', 'send_time', 'assg_time', 'tray_ref',
            'notaxamt', 'retn_time', 'address_id', 'addr_mode', 'advconvert', 'ready_time', 'dlv_time'
        ]

    @staticmethod
    def _get_sls_column_mapping() -> dict[str, str]:
        """Returns SLS column mapping configuration."""
        return {
            'bill_no': 'Transaction No.',
            'table': 'Table No.',
            'waiter': 'Staff ID',
            'people_no': 'No. of Covers',
            'pay_type': 'Tender Type',
            'disc_type': 'Discount Module Name',
            'open_time': 'Time',
            'date': 'Date',
            'bill_time': 'Time',
            'bill_date': 'Date',
            'total': 'Price',
            'received': 'Price',
            'taxes': 'VAT Amount',
            'auto_grat': 'Income/Exp. Amount',
            'sale_type': 'Sales Type',
            'discount': 'Discount Amount',
            'taxable': 'VAT Amount',
            'prt_time': 'Time',
            'prt_date': 'Date',
            'rev_center': 'Sales Type',
            'note': 'Information',
            'trans_id': 'Transaction No.',
            'send_time': 'Time',
            'ready_time': 'Time',
            'dlv_time': 'Time'
        }

    @staticmethod
    def _get_sdet_columns() -> list[str]:
        """Returns SDET target columns configuration."""
        return [
            'branch_id', 'brand', 'branch', 'bill_no', 'ref_no',
            'quanty', 'price_paid', 'raw_price', 'cost', 'posted',
            'emp_no', 'del_code', 'prc_adj', 'two4one', 'disc_no',
            'prc_lvl', 'prc_lvl0', 'iscoupon', 'item_adj', 'vat_adj',
            'disc_adj', 'pricemult', 'invmult', 'gd_no', 'send_time',
            'adj_no', 'refundflag', 'cou_rec', 'coupitem', 'hash_stat',
            'ord_date', 'ord_time', 'spec_inst'
        ]

    @staticmethod
    def _get_sdet_column_mapping() -> dict[str, str]:
        """Returns SDET column mapping configuration."""
        return {
            'brand': 'Brand',
            'branch': 'Branch',
            'bill_no': 'Transaction No.',
            'ref_no': 'Item No.',
            'quanty': 'Quantity',
            'price_paid': 'Price',
            'raw_price': 'Net Price',
            'cost': 'Cost Amount',
            'emp_no': 'Staff ID', 'disc_no': 'Discount Module Name',
            'vat_adj': 'VAT Amount',
            'disc_adj': 'Discount Amount',
            'send_time': 'Time',
            'ord_date': 'Date',
            'ord_time': 'Time'
        }

    @staticmethod
    def _get_sls_dtype_map() -> dict[str, str]:
        """Returns SLS data type mapping configuration."""
        return {
            'branch_id': 'int64',
            'brand': 'object',
            'branch': 'object',
            'bill_no': 'object',
            'fact_no': 'float64',
            'session_no': 'float64',
            'table': 'int64',
            'seat_no': 'int64',
            'waiter': 'int64',
            'people_no': 'int64',
            'pay_type': 'int64',
            'disc_type': 'object',
            'open_time': 'object',
            'date': 'object',
            'bill_time': 'object',
            'bill_date': 'object',
            'printed': 'bool',
            'posted': 'bool',
            'total': 'float64',
            'received': 'float64',
            'taxes': 'float64',
            'auto_grat': 'float64',
            'login_ref': 'int64',
            'cash_draw': 'int64',
            'sale_type': 'object',
            'sale_area': 'int64',
            'acc_posted': 'bool',
            'discount': 'float64',
            'dollardisc': 'int64',
            'taxable': 'bool',
            'prt_time': 'object',
            'prt_date': 'object',
            'disc_pct': 'float64',
            'hidden': 'bool',
            'tax_table': 'int64',
            'account_no': 'float64',
            'rev_center': 'object',
            'phone': 'float64',
            'cust_id': 'int64',
            'phone_id': 'int64',
            'gratovrpct': 'float64',
            'gratovramt': 'int64',
            'note': 'object',
            'sls_name': 'float64',
            'cash_back': 'int64',
            'trans_id': 'int64',
            'start_stn': 'int64',
            'settle_stn': 'int64',
            'waiter0': 'int64',
            'send_time': 'object',
            'assg_time': 'float64',
            'tray_ref': 'int64',
            'notaxamt': 'int64',
            'retn_time': 'float64',
            'address_id': 'int64',
            'addr_mode': 'int64',
            'advconvert': 'int64',
            'ready_time': 'object',
            'dlv_time': 'object'
        }

    @staticmethod
    def _get_sdet_dtype_map() -> dict[str, str]:
        """Returns SDET data type mapping configuration."""
        return {
            'branch_id': 'int64',
            'brand': 'object',
            'branch': 'object',
            'bill_no': 'object',
            'ref_no': 'object',
            'quanty': 'int64',
            'price_paid': 'float64',
            'raw_price': 'float64',
            'cost': 'float64',
            'posted': 'bool',
            'emp_no': 'int64',
            'del_code': 'int64',
            'prc_adj': 'bool',
            'two4one': 'bool',
            'disc_no': 'object',
            'prc_lvl': 'int64',
            'prc_lvl0': 'int64',
            'iscoupon': 'bool',
            'item_adj': 'int64',
            'vat_adj': 'float64',
            'disc_adj': 'float64',
            'pricemult': 'int64',
            'invmult': 'int64',
            'gd_no': 'int64',
            'send_time': 'object',
            'adj_no': 'int64',
            'refundflag': 'bool',
            'cou_rec': 'int64',
            'coupitem': 'bool',
            'hash_stat': 'int64',
            'ord_date': 'object',
            'ord_time': 'object',
            'spec_inst': 'float64'
        }
    
if __name__ == "__main__":
    # --- Configuration Parameters ---
    EXCEL_FILE_PATH = "Manam GH August 1-17, 2025.xlsx"
    BRANCH_MAPPING_FILE_PATH = "branch_mapping.xlsx"
    OUTPUT_DIRECTORY = "output_data"

    try:
        # NEW REUSABLE APPROACH - Perfect for orchestrators
        print("=== Using New Reusable Pipeline Interface ===")
        
        # Create pipeline from files (orchestrators can use this pattern)
        pipeline = PipelineRunner.create_from_files(
            excel_file_path=EXCEL_FILE_PATH,
            branch_mapping_file_path=BRANCH_MAPPING_FILE_PATH,
            output_directory=OUTPUT_DIRECTORY
        )
        
        # Example 1: Process only SLS (Sales Header)
        print("\n--- Example: SLS Only ---")
        sls_file = pipeline.process_sls_only(verbose=False)  # Set verbose=False for orchestrators
        print(f"SLS file generated: {sls_file}")
        
        # Example 2: Process only SDET (Sales Detail) 
        print("\n--- Example: SDET Only ---")
        sdet_file = pipeline.process_sdet_only(verbose=False)  # Set verbose=False for orchestrators
        print(f"SDET file generated: {sdet_file}")
        
        # Example 3: Process both (equivalent to run_all_pipelines)
        print("\n--- Example: Both SLS and SDET ---")
        both_files = pipeline.process_both(verbose=True)
        print(f"All files generated: {both_files}")

        # LEGACY APPROACH - Still works for backward compatibility
        print("\n=== Legacy Approach (Still Supported) ===")
        
        # Load branch mapping
        branch_mapping_df = pd.read_excel(BRANCH_MAPPING_FILE_PATH)
        
        # Create components manually (old way)
        data_reader = ExcelDataReader(file_path=EXCEL_FILE_PATH, header_row=2)
        
        sls_transformer = SlsDataFrameTransformer(
            branch_mapping_df=branch_mapping_df,
            sls_columns=PipelineRunner._get_sls_columns(),
            column_mapping=PipelineRunner._get_sls_column_mapping()
        )
        
        sdet_transformer = SdetDataFrameTransformer(
            branch_mapping_df=branch_mapping_df,
            sdet_columns=PipelineRunner._get_sdet_columns(),
            column_mapping=PipelineRunner._get_sdet_column_mapping()
        )
        
        sls_type_converter = DataTypeConverter(sql_to_pandas_dtype_map=PipelineRunner._get_sls_dtype_map())
        sdet_type_converter = DataTypeConverter(sql_to_pandas_dtype_map=PipelineRunner._get_sdet_dtype_map())
        
        data_saver = DataSaver(output_dir=OUTPUT_DIRECTORY)
        
        # Create legacy pipeline runner
        legacy_pipeline = PipelineRunner(
            data_reader=data_reader,
            sls_transformer=sls_transformer,
            sdet_transformer=sdet_transformer,
            sls_type_converter=sls_type_converter,
            sdet_type_converter=sdet_type_converter,
            data_saver=data_saver
        )
        
        # Run legacy way
        legacy_output = legacy_pipeline.run_all_pipelines()
        print(f"Legacy output: {legacy_output}")

    except FileNotFoundError as e:
        print(f"Error: {e}")
        print(f"Please ensure '{EXCEL_FILE_PATH}' and '{BRANCH_MAPPING_FILE_PATH}' exist in the correct directory.")
    except ValueError as e:
        print(f"Data Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")