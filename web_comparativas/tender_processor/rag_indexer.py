import faiss
import numpy as np
import requests
import logging
from typing import List, Dict, Any, Tuple

logger = logging.getLogger("wc.tender_processor.rag")

class RAGIndexer:
    """
    In-memory vector database using FAISS and Ollama embeddings.
    Designed to process PDF text, chunk it, and provide semantic search.
    """
    def __init__(self, ollama_url: str = "http://localhost:11434", model_name: str = "nomic-embed-text"):
        # We use a dedicated lightweight embedding model if available, else fallback to main model
        self.ollama_url = ollama_url
        self.client_session = requests.Session()
        
        # Verify if nomic-embed-text is available, otherwise fallback to qwen2.5:7b (which can also embed)
        self.model_name = self._ensure_model(model_name)
        
        self.index = None
        self.chunks: List[str] = []
        self.chunk_metadata: List[Dict[str, Any]] = []
        
        # Dimension depends on the model. We'll set it dynamically on first embed.
        self.dimension = None 

    def _ensure_model(self, preferred_model: str) -> str:
        """Finds the best model to use for embeddings based on what's installed."""
        try:
            resp = self.client_session.get(f"{self.ollama_url}/api/tags", timeout=5)
            if resp.status_code == 200:
                models = [m["name"] for m in resp.json().get("models", [])]
                if preferred_model in models:
                    logger.info(f"RAG Indexer: Found preferred embedding model '{preferred_model}'.")
                    return preferred_model
                elif f"{preferred_model}:latest" in models:
                    logger.info(f"RAG Indexer: Found preferred embedding model '{preferred_model}:latest'.")
                    return f"{preferred_model}:latest"
                # Fallback to the first available model (usually the main LLM)
                if models:
                    fallback = models[0]
                    logger.warning(f"RAG Indexer: '{preferred_model}' not found. Falling back to '{fallback}' for embeddings. Note: Dedicated embedding models like nomic-embed-text are highly recommended for speed and accuracy.")
                    return fallback
        except Exception as e:
            logger.warning(f"RAG Indexer: Could not check models ({e}). Defaulting to '{preferred_model}'.")
        return preferred_model

    def _get_embedding(self, text: str) -> np.ndarray:
        """Gets a single embedding vector from Ollama."""
        try:
            resp = self.client_session.post(
                f"{self.ollama_url}/api/embeddings",
                json={
                    "model": self.model_name,
                    "prompt": text
                },
                timeout=30 # Embeddings are usually fast
            )
            resp.raise_for_status()
            vector = resp.json().get("embedding", [])
            
            if not vector:
                raise ValueError("Empty embedding returned")
                
            return np.array(vector, dtype='float32')
        except Exception as e:
            logger.error(f"Failed to get embedding for chunk: {e}")
            # Return dummy zero vector if fails so the index doesn't crash
            # Ensure dimension is known or fallback to a common size
            dim = self.dimension if self.dimension else 768 
            return np.zeros(dim, dtype='float32')

    def _chunk_text(self, pages_text: List[str], chunk_size: int = 1500, overlap: int = 300) -> List[Dict[str, Any]]:
        """
        Splits pages into overlapping chunks.
        Returns list of dicts: {"text": chunk_text, "page": page_num}
        """
        raw_chunks = []
        for i, page_text in enumerate(pages_text):
            page_num = i + 1
            text = page_text.strip()
            if not text: continue
            
            start = 0
            while start < len(text):
                end = start + chunk_size
                chunk = text[start:end]
                raw_chunks.append({
                    "text": chunk,
                    "page": page_num
                })
                start += (chunk_size - overlap)
        return raw_chunks

    def build_index(self, pages_text: List[str]):
        """
        Chunks the text, calculates embeddings, and builds the FAISS index.
        """
        logger.info(f"RAG Indexer: Building index from {len(pages_text)} pages...")
        
        # 1. Chunking
        raw_chunks = self._chunk_text(pages_text)
        if not raw_chunks:
            logger.warning("RAG Indexer: No text to index.")
            return

        # 2. Extract texts
        texts_to_embed = [c["text"] for c in raw_chunks]
        
        # 3. Calculate Embeddings
        # We do this sequentially for safety with local Ollama, 
        # but could be parallelized if Ollama supports concurrent embedding requests well.
        embeddings_list = []
        for i, text in enumerate(texts_to_embed):
            vec = self._get_embedding(text)
            
            # Auto-detect dimension on first successful embed
            if self.dimension is None and np.any(vec):
                 self.dimension = len(vec)
                 logger.info(f"RAG Indexer: Detected embedding dimension: {self.dimension}")
                 
            embeddings_list.append(vec)
            
        if not self.dimension:
            logger.error("RAG Indexer: Could not determine embedding dimension. Aborting build.")
            return

        # Stack into a 2D numpy array: shape (num_chunks, dimension)
        embeddings_matrix = np.vstack(embeddings_list)
        
        # Normalize vectors for Cosine Similarity (Inner Product in FAISS on normalized vectors = Cosine)
        faiss.normalize_L2(embeddings_matrix)

        # 4. Initialize FAISS Index (IndexFlatIP for Inner Product / Cosine Similarity)
        self.index = faiss.IndexFlatIP(self.dimension)
        self.index.add(embeddings_matrix)
        
        # Store metadata
        self.chunks = texts_to_embed
        self.chunk_metadata = [{"page": c["page"]} for c in raw_chunks]
        
        logger.info(f"RAG Indexer: Successfully built index with {self.index.ntotal} chunks.")

    def search(self, query: str, top_k: int = 3) -> List[Dict[str, Any]]:
        """
        Searches the index for the most relevant chunks.
        Returns a list of dicts: {"text": text, "page": page_num, "score": similarity}
        """
        if not self.index or self.index.ntotal == 0:
            logger.warning("RAG Indexer: Cannot search, index is empty.")
            return []

        # 1. Embed query
        query_vector = self._get_embedding(query)
        # Reshape to 2D array (1, dimension)
        query_matrix = np.array([query_vector])
        
        # Normalize query vector for Cosine Similarity
        faiss.normalize_L2(query_matrix)

        # 2. Search FAISS
        # distances = cosine similarities (higher is better for IP)
        distances, indices = self.index.search(query_matrix, top_k)
        
        results = []
        for i in range(top_k):
            idx = indices[0][i]
            if idx == -1: continue # Not enough results
            
            score = float(distances[0][i])
            results.append({
                "text": self.chunks[idx],
                "page": self.chunk_metadata[idx]["page"],
                "score": score
            })
            
        return results

