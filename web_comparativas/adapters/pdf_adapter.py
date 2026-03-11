import pandas as pd
from pathlib import Path
from ..tender_processor.orchestrator import TenderProcessor

def process_pdf_tender(file_path: Path, metadata: dict, out_dir: Path):
    """
    Adapter that connects the generic 'services.py' pipeline 
    with the 'TenderProcessor' (Multi-Agent System).
    """
    processor = TenderProcessor()
    
    # Read file bytes
    with open(file_path, "rb") as f:
        content = f.read()
        
    # Prepare input for Orchestrator
    files = {file_path.name: content}
    
    # Run Agents
    final_json = processor.process_files(files)
    
    # Check for error
    if "error" in final_json:
        raise RuntimeError(f"TenderProcessor Error: {final_json['error']}")
        
    # Convert items to DataFrame
    items = final_json.get("items", [])
    if items:
        df = pd.DataFrame(items)
    else:
        # Create empty DF with expected columns if no items found
        df = pd.DataFrame(columns=["descripcion", "cantidad", "unidad"])
        
    # Return expected tuple (DataFrame, SummaryDict)
    return df, final_json
