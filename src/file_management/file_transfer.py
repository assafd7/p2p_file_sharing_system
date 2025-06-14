import os
import uuid
import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import hashlib

from src.network.peer import Peer
from src.network.file_protocol import FileProtocol

class FileTransfer:
    """Handles file transfer operations."""
    
    CHUNK_SIZE = 1024 * 1024  # 1MB chunks
    
    def __init__(self, file_id: str, file_name: str, file_size: int,
                 file_hash: str, target_path: str, owner_id: str,
                 permissions: Dict[str, List[str]], peer: Optional[Peer] = None):
        self.id = str(uuid.uuid4())
        self.file_id = file_id
        self.file_name = file_name
        self.file_size = file_size
        self.file_hash = file_hash
        self.target_path = target_path
        self.owner_id = owner_id
        self.permissions = permissions
        self.progress = 0.0
        self.status = "pending"
        self.logger = logging.getLogger("FileTransfer")
        self._task: Optional[asyncio.Task] = None
        self.peer = peer
        self.is_network_transfer = peer is not None
        self.file_protocol = None
        self._lock = asyncio.Lock()
        self._chunks_received = 0
        self._total_chunks = (file_size + self.CHUNK_SIZE - 1) // self.CHUNK_SIZE
    
    def set_file_protocol(self, protocol: FileProtocol):
        """Set the file protocol for network transfers."""
        self.file_protocol = protocol
    
    def start(self):
        """Start the file transfer."""
        if self._task is not None:
            raise RuntimeError("Transfer already started")
        
        self._task = asyncio.create_task(self._transfer_task())
        self.status = "in_progress"
    
    def cancel(self):
        """Cancel the file transfer."""
        if self._task is None:
            return
        
        self._task.cancel()
        self.status = "cancelled"
        
        # Clean up partial file
        try:
            target = Path(self.target_path)
            if target.exists():
                target.unlink()
        except Exception as e:
            self.logger.error(f"Error cleaning up cancelled transfer: {e}")
    
    async def _transfer_task(self):
        """Main transfer task."""
        try:
            self.logger.debug(f"Starting transfer {self.id} for file {self.file_name}")
            
            # Create target directory
            target = Path(self.target_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            
            if self.is_network_transfer:
                await self._network_transfer(target)
            else:
                await self._local_transfer(target)
            
            # Verify file
            if not self._verify_file(target):
                raise ValueError("File verification failed")
            
            self.status = "completed"
            self.progress = 100.0
            self.logger.info(f"Transfer {self.id} completed successfully")
            
        except asyncio.CancelledError:
            self.logger.info(f"Transfer {self.id} cancelled")
            raise
        except Exception as e:
            self.logger.error(f"Transfer {self.id} failed: {e}")
            self.status = "failed"
            raise
    
    async def _local_transfer(self, target: Path):
        """Handle local file transfer."""
        # Open source file
        source = Path(self.file_id) / self.file_name
        if not source.exists():
            raise FileNotFoundError(f"Source file not found: {source}")
        
        # Copy file
        with open(source, 'rb') as src, open(target, 'wb') as dst:
            total_size = 0
            while True:
                chunk = src.read(self.CHUNK_SIZE)
                if not chunk:
                    break
                dst.write(chunk)
                total_size += len(chunk)
                self.progress = (total_size / self.file_size) * 100
    
    async def _network_transfer(self, target: Path):
        """Handle network file transfer."""
        if not self.peer or not self.file_protocol:
            raise RuntimeError("Network transfer requires peer and file protocol")
        
        # Create empty file
        with open(target, 'wb') as f:
            f.truncate(self.file_size)
        
        # Download chunks
        async with self._lock:
            for chunk_index in range(self._total_chunks):
                try:
                    # Request chunk
                    chunk_data = await self.file_protocol.request_file_chunk(
                        self.peer,
                        self.file_hash,
                        chunk_index
                    )
                    
                    if not chunk_data:
                        raise ValueError(f"Failed to receive chunk {chunk_index}")
                    
                    # Write chunk
                    with open(target, 'r+b') as f:
                        f.seek(chunk_index * self.CHUNK_SIZE)
                        f.write(chunk_data)
                    
                    # Update progress
                    self._chunks_received += 1
                    self.progress = (self._chunks_received / self._total_chunks) * 100
                    
                except Exception as e:
                    self.logger.error(f"Error receiving chunk {chunk_index}: {e}")
                    raise
    
    def _verify_file(self, file_path: Path) -> bool:
        """Verify the transferred file."""
        try:
            # Check file size
            if file_path.stat().st_size != self.file_size:
                self.logger.error("File size mismatch")
                return False
            
            # Check file hash
            sha256_hash = hashlib.sha256()
            with open(file_path, 'rb') as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
            
            if sha256_hash.hexdigest() != self.file_hash:
                self.logger.error("File hash mismatch")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"File verification failed: {e}")
            return False 