import os
import uuid
import logging
from typing import List, Dict, Any
from datetime import datetime
import asyncio

logger = logging.getLogger(__name__)

class DocumentProcessor:
    def __init__(self):
        self.processing_jobs = {}
        self.processed_documents = []
        self.chunk_size = 512  # Default chunk size

    async def process_documents(self, file_paths: List[str], job_id: str) -> None:
        """Process multiple document types asynchronously"""
        self.processing_jobs[job_id] = {
            'status': 'processing',
            'total_files': len(file_paths),
            'processed_files': 0,
            'start_time': datetime.now(),
            'errors': []
        }
        
        for i, file_path in enumerate(file_paths):
            try:
                # Simulate processing delay
                await asyncio.sleep(1)
                
                # Process individual file
                document_info = await self._process_single_document(file_path)
                self.processed_documents.append(document_info)
                
                # Update job progress
                self.processing_jobs[job_id]['processed_files'] = i + 1
                
                logger.info(f"Processed {i+1}/{len(file_paths)}: {file_path}")
                
            except Exception as e:
                error_msg = f"Failed to process {file_path}: {str(e)}"
                self.processing_jobs[job_id]['errors'].append(error_msg)
                logger.error(error_msg)
        
        self.processing_jobs[job_id]['status'] = 'completed'
        self.processing_jobs[job_id]['end_time'] = datetime.now()

    async def _process_single_document(self, file_path: str) -> Dict[str, Any]:
        """Process a single document file"""
        file_ext = os.path.splitext(file_path)[1].lower()
        file_name = os.path.basename(file_path)
        
        # Determine document type
        doc_type = self._detect_document_type(file_name, file_ext)
        
        # Read file content (simplified)
        content = await self._read_file_content(file_path, doc_type)
        
        # Dynamic chunking
        chunks = self.dynamic_chunking(content, doc_type)
        
        # Generate embeddings (simplified - in real implementation, use sentence-transformers)
        embeddings = await self._generate_embeddings_batch(chunks)
        
        return {
            'id': str(uuid.uuid4()),
            'file_name': file_name,
            'file_path': file_path,
            'type': doc_type,
            'content': content,
            'chunks': chunks,
            'embeddings': embeddings,
            'processed_at': datetime.now(),
            'chunk_count': len(chunks)
        }

    def _detect_document_type(self, file_name: str, file_ext: str) -> str:
        """Detect the type of document based on name and extension"""
        name_lower = file_name.lower()
        
        if 'resume' in name_lower or 'cv' in name_lower:
            return 'resume'
        elif 'review' in name_lower:
            return 'review'
        elif 'contract' in name_lower:
            return 'contract'
        elif 'offer' in name_lower:
            return 'offer_letter'
        else:
            return 'general'

    async def _read_file_content(self, file_path: str, doc_type: str) -> str:
        """Read content from file based on type"""
        # Simplified file reading
        # In real implementation, you would use:
        # - PyPDF2 for PDFs
        # - python-docx for DOCX
        # - built-in methods for TXT/CSV
        
        if doc_type == 'resume':
            return f"Sample resume content from {file_path}. Includes skills, experience, education sections."
        elif doc_type == 'review':
            return f"Performance review content from {file_path}. Includes ratings, comments, goals."
        else:
            return f"Document content from {file_path}"

    def dynamic_chunking(self, content: str, doc_type: str) -> List[str]:
        """Intelligent chunking based on document structure"""
        if doc_type == 'resume':
            return self._chunk_resume(content)
        elif doc_type == 'review':
            return self._chunk_review(content)
        elif doc_type == 'contract':
            return self._chunk_contract(content)
        else:
            return self._chunk_general(content)

    def _chunk_resume(self, content: str) -> List[str]:
        """Chunk resume while keeping skills and experience together"""
        # Simplified resume chunking
        chunks = []
        current_chunk = ""
        
        # Split by sections (simulated)
        sections = content.split('. ')
        for section in sections:
            if len(current_chunk + section) < self.chunk_size:
                current_chunk += section + ". "
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = section + ". "
        
        if current_chunk:
            chunks.append(current_chunk.strip())
        
        return chunks

    def _chunk_review(self, content: str) -> List[str]:
        """Chunk performance reviews maintaining paragraph integrity"""
        # Split by paragraphs or logical sections
        paragraphs = content.split('\n\n')
        chunks = []
        
        for paragraph in paragraphs:
            if len(paragraph) > self.chunk_size:
                # Split long paragraphs
                words = paragraph.split()
                current_chunk = ""
                for word in words:
                    if len(current_chunk + " " + word) <= self.chunk_size:
                        current_chunk += " " + word
                    else:
                        chunks.append(current_chunk.strip())
                        current_chunk = word
                if current_chunk:
                    chunks.append(current_chunk.strip())
            else:
                chunks.append(paragraph)
        
        return chunks

    def _chunk_contract(self, content: str) -> List[str]:
        """Chunk contracts preserving clause boundaries"""
        # Split by clauses (simulated)
        clauses = content.split('; ')
        chunks = []
        current_chunk = ""
        
        for clause in clauses:
            if len(current_chunk + clause) < self.chunk_size:
                current_chunk += clause + "; "
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = clause + "; "
        
        if current_chunk:
            chunks.append(current_chunk.strip())
        
        return chunks

    def _chunk_general(self, content: str) -> List[str]:
        """General purpose chunking"""
        words = content.split()
        chunks = []
        current_chunk = ""
        
        for word in words:
            if len(current_chunk + " " + word) <= self.chunk_size:
                current_chunk += " " + word
            else:
                chunks.append(current_chunk.strip())
                current_chunk = word
        
        if current_chunk:
            chunks.append(current_chunk.strip())
        
        return chunks

    async def _generate_embeddings_batch(self, chunks: List[str]) -> List[List[float]]:
        """Generate embeddings for text chunks in batches"""
        # Simplified embedding generation
        # In real implementation, use sentence-transformers with batching
        embeddings = []
        
        for chunk in chunks:
            # Simulate embedding generation (random vectors for demo)
            embedding = [0.1] * 384  # Standard MiniLM dimension
            embeddings.append(embedding)
        
        return embeddings

    def get_processing_status(self, job_id: str) -> Dict[str, Any]:
        """Get status of a processing job"""
        if job_id not in self.processing_jobs:
            return {'status': 'not_found'}
        
        job = self.processing_jobs[job_id]
        progress = (job['processed_files'] / job['total_files']) * 100 if job['total_files'] > 0 else 0
        
        return {
            'job_id': job_id,
            'status': job['status'],
            'progress': progress,
            'processed_files': job['processed_files'],
            'total_files': job['total_files'],
            'errors': job.get('errors', [])
        }
