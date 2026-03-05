import io
import logging
import fitz  # PyMuPDF
from typing import List, Tuple, Optional
import os
import pytesseract
from pdf2image import convert_from_bytes
from PIL import Image

logger = logging.getLogger("wc.pdf_utils")

# CONFIGURE TESSERACT
# Assuming valid relative path or environment variable. 
# Best practice: use absolute path based on project root if possible, or trust PATH.
# Given user info: C:\Users\ANDRES.TORRES\Desktop\web_comparativas_v2- ok\Tesseract-OCR\tesseract.exe
TESSERACT_PATH = r"C:\Users\ANDRES.TORRES\Desktop\web_comparativas_v2- ok\Tesseract-OCR\tesseract.exe"
if os.path.exists(TESSERACT_PATH):
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_PATH
else:
    logger.warning(f"Tesseract not found at {TESSERACT_PATH}. OCR may fail if not in PATH.")

def extract_text_first_page(pdf_bytes: bytes) -> str:
    """Read only the first page text for classification using PyMuPDF."""
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            if doc.page_count > 0:
                page = doc.load_page(0)
                return page.get_text("text") or ""
    except Exception as e:
        logger.error(f"Error reading first page: {e}")
    return ""

def convert_to_searchable_pdf(file_bytes: bytes, filename: str) -> Optional[bytes]:
    """
    Converts any input (scanned PDF, Image, Native PDF) into a Searchable PDF with text layer.
    Uses Tesseract OCR for scanned content.
    """
    try:
        ext = filename.lower().split('.')[-1]
        
        # 1. IMAGE -> PDF
        if ext in ['jpg', 'jpeg', 'png', 'bmp', 'tiff']:
            image = Image.open(io.BytesIO(file_bytes))
            # Convert to RGB to avoid alpha channel issues
            if image.mode != 'RGB':
                image = image.convert('RGB')
            # Use Tesseract to create searchable PDF
            pdf_bytes = pytesseract.image_to_pdf_or_hocr(image, extension='pdf', lang='spa')
            return pdf_bytes

        # 2. PDF (Check if scanned)
        if ext == 'pdf':
            # Fast check: does it have text?
            text = extract_text_first_page(file_bytes)
            if len(text.strip()) > 50:
                return file_bytes # Already searchable, return as is
            
            # If scanned, convert pages to images then to searchable PDF
            logger.info(f"Converting scanned PDF {filename} to Searchable PDF...")
            images = convert_from_bytes(file_bytes)
            output_pdf = fitz.open() # output document
            
            for img in images:
                # OCR each page
                page_pdf_bytes = pytesseract.image_to_pdf_or_hocr(img, extension='pdf', lang='spa')
                page_doc = fitz.open("pdf", page_pdf_bytes)
                output_pdf.insert_pdf(page_doc)
            
            return output_pdf.tobytes()
            
        return None
    except Exception as e:
        logger.error(f"Error converting {filename} to searchable PDF: {e}")
        return None

def extract_text_robust(pdf_bytes: bytes, force_ocr: bool = False, filename: str = "doc.pdf") -> str:
    """
    Extracts text from PDF/Image, automatically handling scanned documents via OCR.
    Now supports images by converting them first.
    """
    try:
        # Detect if it's an image file pretending to be PDF bytes (relying on filename ext)
        ext = filename.lower().split('.')[-1]
        if ext in ['jpg', 'jpeg', 'png']:
             # It's an image, must OCR
             searchable_pdf = convert_to_searchable_pdf(pdf_bytes, filename)
             if searchable_pdf:
                 # Extract text from the new PDF
                 with fitz.open(stream=searchable_pdf, filetype="pdf") as doc:
                    text_content = [page.get_text("text") for page in doc]
                 return "\n".join(text_content)
             return ""

        # 1. Try Digital Extraction first (FAST)
        if not force_ocr:
            text_content = []
            with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
                for page in doc:
                    text_content.append(page.get_text("text"))
            
            full_text = "\n".join(text_content)
            
            # Heuristic: If text is too short relative to page count, it might be scanned.
            if len(full_text.strip()) > 50 * len(text_content):
                return full_text
            
            logger.info("Low text density detected. Switching to OCR...")
    
        # 2. OCR Fallback (SLOW) - Convert to images then OCR
        images = convert_from_bytes(pdf_bytes)
        ocr_text = []
        for i, img in enumerate(images):
            # Use raw string output for pure text extraction (faster than full PDF reconstruction if just needing text)
            page_text = pytesseract.image_to_string(img, lang='spa')
            ocr_text.append(page_text)
            
        return "\n".join(ocr_text)

    except Exception as e:
        logger.error(f"Error in robust extraction: {e}")
        return ""


def extract_text_pages_robust(pdf_bytes: bytes, force_ocr: bool = False, filename: str = "doc.pdf") -> List[str]:
    """
    Returns a LIST of strings, one per page.
    Used for provenance (tracking which page a match came from).
    """
    try:
        ext = filename.lower().split('.')[-1]
        
        # 1. Handle Images -> Convert, then get pages
        if ext in ['jpg', 'jpeg', 'png']:
             searchable_pdf = convert_to_searchable_pdf(pdf_bytes, filename)
             if searchable_pdf:
                 with fitz.open(stream=searchable_pdf, filetype="pdf") as doc:
                    return [page.get_text("text") for page in doc]
             return []

        # 2. Digital Extraction
        if not force_ocr:
            text_pages = []
            with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
                for page in doc:
                    text_pages.append(page.get_text("text"))
            
            full_text = "".join(text_pages)
            if len(full_text.strip()) > 50 * len(text_pages):
                return text_pages
            
            logger.info("Low text density detected (pages). Switching to OCR...")

        # 3. OCR Fallback
        images = convert_from_bytes(pdf_bytes)
        ocr_pages = []
        for img in images:
            page_text = pytesseract.image_to_string(img, lang='spa')
            ocr_pages.append(page_text)
            
        return ocr_pages

    except Exception as e:
        logger.error(f"Error in robust pages extraction: {e}")
        return []

def extract_text_all_pages(pdf_bytes: bytes, filename: str = "doc.pdf") -> str:
    """Wrapper for robust extraction to replace legacy calls."""
    return extract_text_robust(pdf_bytes, filename=filename)

def get_pages_with_tables(pdf_bytes: bytes) -> List[Tuple[int, str]]:
    """
    Returns a list of (page_index, page_text) for pages that seem to contain Tables or Renglones.
    """
    relevant_pages = []
    keywords = ["renglón", "ítem", "cantidad", "descripción", "detalle", "precio", "unitario"]
    
    try:
        # Check if needs OCR first?
        # If it's a scanned PDF, pdfplumber won't see text.
        # We should use the searchable version if possible, but that requires re-processing.
        # For now, we assume standard PDFs or accept that scanned tables need the searchable conversion first.
        
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for i, page in enumerate(pdf.pages):
                txt = page.extract_text() or ""
                low_txt = txt.lower()
                # Heuristic: if it has at least 2 table keywords
                hits = sum(1 for k in keywords if k in low_txt)
                if hits >= 2:
                    relevant_pages.append((i, txt))
    except Exception as e:
        logger.error(f"Error scanning for tables: {e}")
    
    return relevant_pages

def extract_text_first_page_robust(pdf_bytes: bytes) -> str:
    """
    Extracts text from FIRST PAGE ONLY, handling scanned documents.
    Used for quick classification.
    """
    try:
        # 1. Try Digital
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            if doc.page_count > 0:
                page = doc.load_page(0)
                text = page.get_text("text") or ""
                if len(text.strip()) > 50:
                    return text
        
        # 2. OCR Fallback (First page only)
        images = convert_from_bytes(pdf_bytes, first_page=1, last_page=1)
        if images:
             return pytesseract.image_to_string(images[0], lang='spa')
             
    except Exception as e:
        logger.error(f"Error in robust first page extraction: {e}")
        
    return ""

import pdfplumber

def find_text_bbox(pdf_bytes: bytes, page_number: int, query_text: str) -> Optional[List[float]]:
    """
    Given a PDF page (1-indexed) and a query text, search for the text and return its bounding box
    [x0, y0, x1, y1] using pdfplumber.
    """
    if not query_text or page_number is None or page_number < 1:
        return None
        
    try:
        snippet = query_text.strip()
        if len(snippet) > 50:
            snippet = snippet[:50].strip()
            
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            if page_number <= len(pdf.pages):
                page = pdf.pages[page_number - 1]
                words = page.extract_words()
                
                # Combine words to find snippet
                # Simple matching strategy: find the first word that starts the snippet
                snippet_words = snippet.split()
                if not snippet_words:
                    return None
                    
                first_word = snippet_words[0].lower()
                
                for i, w in enumerate(words):
                    if w['text'].lower() == first_word or first_word in w['text'].lower():
                        # We found a potential start. Let's return the bounding box of this word for simplicity,
                        # or ideally the bounding box of the whole snippet.
                        # For now, returning the bbox of the matching line/word is often enough for highlighting.
                        # Bbox in pdfplumber is (x0, top, x1, bottom) 
                        return [round(w['x0'], 2), round(w['top'], 2), round(w['x1'], 2), round(w['bottom'], 2)]
                        
    except Exception as e:
        logger.error(f"Error finding bbox on page {page_number} with pdfplumber: {e}")
    
    return None

# =========================
# Legacy Helpers
# =========================
import re
from typing import Optional

def clean_whitespace(s: str) -> str:
    s = (s or "").replace("\u00a0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\s*\n\s*", "\n", s)
    return s.strip()

def clean_one_line(s: str) -> str:
    return re.sub(r"\s+", " ", clean_whitespace(s)).strip()

def find_between(text: str, start_regex: str, end_regex: str) -> Optional[str]:
    m = re.search(start_regex + r"(.*?)" + end_regex, text, flags=re.IGNORECASE | re.DOTALL)
    # If not found, try to be less greedy if needed? No, legacy was greedy.
    return clean_whitespace(m.group(1)) if m else None

def normalize_money(num_str: str) -> Optional[float]:
    if not num_str:
        return None
    s = re.sub(r"[^0-9,\.]", "", num_str)

    last_comma = s.rfind(",")
    last_dot = s.rfind(".")
    if last_comma != -1 and last_dot != -1:
        if last_comma > last_dot:
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    else:
        if "," in s and "." not in s:
            if re.search(r",\d{2}$", s): # ,00
                s = s.replace(",", ".")
            else:
                s = s.replace(",", "")
        if "." in s and "," not in s:
            if not re.search(r"\.\d{2}$", s): # .00
                s = s.replace(".", "")

    try:
        return float(s)
    except:
        return None

def parse_pct(s: str) -> Optional[int]:
    if not s:
        return None
    m = re.search(r"(\d{1,3})\s*%", s)
    if m:
        try: return int(m.group(1))
        except: return None
    m = re.search(r"\(\s*(\d{1,3})\s*%\s*\)", s)
    if m:
        try: return int(m.group(1))
        except: return None
    return None

def find_line_after_label(text: str, label_regex: str) -> Optional[str]:
    m = re.search(label_regex + r"\s*:\s*([^\r\n]+)", text, flags=re.IGNORECASE)
    return clean_one_line(m.group(1)) if m else None

def first_sentence_or_line(s: str) -> str:
    one = clean_one_line(s)
    # Cut reasonable length
    if len(one) > 260:
        return one[:260].rstrip() + "…"
    return one
