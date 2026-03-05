from typing import Dict
from .orchestrator import TenderProcessor

async def scan_and_process_pdf(files_map: Dict[str, bytes]) -> dict:
    """
    Wrapper for multi-file processing.
    files_map: { 'filename.pdf': bytes, ... }
    """
    processor = TenderProcessor()
    
    # Run synchronous processing
    result = processor.process_files(files_map)
    
    return result
