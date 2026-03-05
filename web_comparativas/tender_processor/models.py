from dataclasses import dataclass
from typing import Any, Optional, Dict, List

@dataclass
class ExtractedField:
    value: Any
    source: str = "Unknown"
    page: Optional[int] = None 
    section: str = "General"
    evidence_text: str = ""
    confidence: float = 0.8  # Default conservative confidence
    is_inferred: bool = False # Indicates if the value was deduced/calculated rather than extracted exactly
    bbox: Optional[List[float]] = None # [x0, y0, x1, y1] spatial coordinates
    requires_review: bool = False # Indicates if a human MUST review this field before confirmed
    
    def to_dict(self) -> Dict[str, Any]:
        """Returns the structure expected by the UI/Analyst Agent."""
        # Auto trigger review flag if confidence is low
        needs_review = self.requires_review or self.confidence < 0.70
        
        return {
            "value": self.value,
            "is_inferred": self.is_inferred,
            "requires_review": needs_review,
            "provenance": {
                "source": self.source,
                "page": self.page,
                "section": self.section,
                "evidence_text": self.evidence_text[:500], # Truncate if too long
                "confidence": self.confidence,
                "bbox": self.bbox
            }
        }
