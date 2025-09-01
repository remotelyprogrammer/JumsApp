# orchestrator_demo.py
"""
Demo script showing how an orchestrator would use the LoyaltyPipeline
This demonstrates the exact interface that your orchestrator team can use.
"""

from LoyaltyPipeline import PipelineRunner

def process_file_for_orchestrator(excel_file_path: str, branch_mapping_path: str, output_dir: str, process_type: str = "both"):
    """
    This is the function that your orchestrator would call.
    
    Args:
        excel_file_path: Path to the Excel transaction file
        branch_mapping_path: Path to the branch mapping file
        output_dir: Output directory for CSV files
        process_type: "sls", "sdet", or "both"
        
    Returns:
        Dictionary with file paths or single file path
    """
    try:
        # Create pipeline instance (one-liner for orchestrators)
        pipeline = PipelineRunner.create_from_files(
            excel_file_path=excel_file_path,
            branch_mapping_file_path=branch_mapping_path,
            output_directory=output_dir
        )
        
        # Process based on type requested
        if process_type.lower() == "sls":
            result = pipeline.process_sls_only(verbose=False)
            return {"sls_file": result}
        elif process_type.lower() == "sdet":
            result = pipeline.process_sdet_only(verbose=False)
            return {"sdet_file": result}
        elif process_type.lower() == "both":
            return pipeline.process_both(verbose=False)
        else:
            raise ValueError(f"Invalid process_type: {process_type}. Use 'sls', 'sdet', or 'both'")
            
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    # Example usage for orchestrator
    
    # File paths
    excel_file = "Manam GH August 1-17, 2025.xlsx"
    branch_mapping = "branch_mapping.xlsx"
    output_directory = "output_data"
    
    print("=== Orchestrator Demo - Processing Only SLS ===")
    sls_result = process_file_for_orchestrator(excel_file, branch_mapping, output_directory, "sls")
    print(f"SLS Result: {sls_result}")
    
    print("\n=== Orchestrator Demo - Processing Only SDET ===")
    sdet_result = process_file_for_orchestrator(excel_file, branch_mapping, output_directory, "sdet")
    print(f"SDET Result: {sdet_result}")
    
    print("\n=== Orchestrator Demo - Processing Both ===")
    both_result = process_file_for_orchestrator(excel_file, branch_mapping, output_directory, "both")
    print(f"Both Result: {both_result}")
