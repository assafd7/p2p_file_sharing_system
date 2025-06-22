import os
import uuid
import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Optional, Callable, Set
from datetime import datetime
import hashlib

from .file_metadata import FileMetadata
from ..network.peer import Peer

class FileTransfer:
    """Handles the state and logic for a single P2P file transfer."""
    
    CHUNK_SIZE = 1024 * 1024  # 1MB chunks
    
    def __init__(self,
                 file_path: Path,
                 metadata: FileMetadata,
                 peer: Peer,
                 is_downloader: bool,
                 status_callback: Callable,
                 finished_callback: Callable):
        self.file_path = file_path
        self.metadata = metadata
        self.peer = peer
        self.is_downloader = is_downloader
        self.status_callback = status_callback
        self.finished_callback = finished_callback
        self.logger = logging.getLogger(f"FileTransfer({self.metadata.name})")

        self.is_active = False
        self.is_paused = False
        self._task: Optional[asyncio.Task] = None
        self._bytes_transferred = 0
        
        self._needed_chunks: Set[int] = set(range(len(self.metadata.chunks)))
        self._received_chunks: Set[int] = set()
        self._chunk_data_map = {} # chunk_index -> chunk_data
    
    def start(self):
        """Starts the transfer process."""
        if self.is_active:
            self.logger.warning("Transfer already started.")
            return
        
        self.is_active = True
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        
        if self.is_downloader:
            self._task = asyncio.create_task(self._download_loop())
        else:
            # For uploaders, starting just means it's ready to serve chunks.
            self.logger.info("Uploader is active and waiting for chunk requests.")
    
    def cancel(self):
        """Cancels the transfer."""
        self.logger.info("Cancelling transfer.")
        self.is_active = False
        if self._task:
            self._task.cancel()
        
        if self.is_downloader:
            # Clean up partial file
            try:
                if self.file_path.exists():
                    os.remove(self.file_path)
            except OSError as e:
                self.logger.error(f"Error removing partial file: {e}")
        
        self.finished_callback(self.file_path, "Cancelled")
    
    def pause(self):
        """Pauses the transfer."""
        if self.is_active and not self.is_paused:
            self.logger.info("Pausing transfer.")
            self.is_paused = True
    
    def resume(self):
        """Resumes the transfer."""
        if self.is_active and self.is_paused:
            self.logger.info("Resuming transfer.")
            self.is_paused = False
    
    async def _download_loop(self):
        """The main loop for requesting chunks as a downloader."""
        self.logger.info("Starting download loop.")
        while self.is_active and self._needed_chunks:
            if self.is_paused:
                await asyncio.sleep(1)
                continue

            # Request the next needed chunk
            chunk_index = self._needed_chunks.pop()
            try:
                await self.peer.request_file_chunk(self.metadata.file_id, chunk_index)
            except Exception as e:
                self.logger.error(f"Error requesting chunk {chunk_index}: {e}")
                self._needed_chunks.add(chunk_index) # Add it back to be requested again
                await asyncio.sleep(2) # Wait a bit before retrying

            # Give other tasks a chance to run
            await asyncio.sleep(0.1) 
        
        if not self._needed_chunks:
            self.logger.info("All chunks have been requested.")

    async def handle_chunk(self, chunk_index: int, data: bytes):
        """Handles a received chunk of data."""
        if chunk_index in self._received_chunks:
            return

        chunk_metadata = self.metadata.chunks[chunk_index]
        if hashlib.sha256(data).hexdigest() != chunk_metadata.hash:
            self.logger.warning(f"Chunk {chunk_index} has incorrect hash. Re-requesting.")
            self._needed_chunks.add(chunk_index)
            return

        self._received_chunks.add(chunk_index)
        self._chunk_data_map[chunk_index] = data
        self._bytes_transferred += len(data)
        
        self.status_callback(self.get_status())

        if self.is_complete():
            await self._assemble_file()

    async def _assemble_file(self):
        """Assembles the final file from the received chunks."""
        self.logger.info("All chunks received. Assembling file.")
        try:
            with open(self.file_path, 'wb') as f:
                for i in range(len(self.metadata.chunks)):
                    f.write(self._chunk_data_map[i])
            
            if await self.verify_integrity():
                self.logger.info("File assembled and verified successfully.")
                self.finished_callback(self.file_path, None)
            else:
                self.logger.error("File verification failed after assembly.")
                self.finished_callback(self.file_path, "Integrity check failed")
        except Exception as e:
            self.logger.error(f"Failed to assemble file: {e}")
            self.finished_callback(self.file_path, f"File assembly failed: {e}")
        finally:
            self.is_active = False

    def is_complete(self) -> bool:
        """Checks if the transfer is complete."""
        return len(self._received_chunks) == len(self.metadata.chunks)

    async def verify_integrity(self) -> bool:
        """Verifies the integrity of the downloaded file."""
        if not self.file_path.exists():
            return False
        
        hasher = hashlib.sha256()
        with open(self.file_path, 'rb') as f:
            while chunk := f.read(8192):
                hasher.update(chunk)
        
        return hasher.hexdigest() == self.metadata.hash

    async def get_chunk_data(self, chunk_index: int) -> Optional[bytes]:
        """Reads and returns the data for a specific chunk from the local file."""
        if self.is_downloader:
            return None # Downloaders don't serve chunks
            
        chunk_metadata = self.metadata.chunks[chunk_index]
        offset = sum(c.size for c in self.metadata.chunks if c.index < chunk_index)
        
        try:
            with open(self.file_path, 'rb') as f:
                f.seek(offset)
                return f.read(chunk_metadata.size)
        except Exception as e:
            self.logger.error(f"Could not read chunk {chunk_index} from file: {e}")
            return None

    def get_status(self) -> dict:
        """Returns the current status of the transfer."""
        progress = (self._bytes_transferred / self.metadata.size) * 100 if self.metadata.size > 0 else 0
        status_str = "paused" if self.is_paused else "downloading" if self.is_downloader else "uploading"
        if not self.is_active:
            status_str = "finished" if self.is_complete() else "cancelled"
            
        return {
            "file_id": self.metadata.file_id,
            "progress": progress,
            "bytes_transferred": self._bytes_transferred,
            "total_size": self.metadata.size,
            "status": status_str,
        } 