import os
import hashlib
import json
from typing import Dict, List, Optional, Tuple, Callable
from dataclasses import dataclass
from datetime import datetime
import logging
import asyncio
from pathlib import Path
import time

@dataclass
class FileChunk:
    index: int
    hash: str
    size: int
    data: Optional[bytes] = None

@dataclass
class FileMetadata:
    """Metadata for a file in the system."""
    name: str
    size: int
    created_at: datetime
    modified_at: datetime
    hash: str
    chunks: List[FileChunk]
    owner_id: str
    permissions: Dict[str, List[str]]  # user_id -> ['read', 'write', 'share']

class FileManager:
    CHUNK_SIZE = 1024 * 1024  # 1MB chunks

    def __init__(self, storage_dir: str):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger("FileManager")
        self.active_transfers: Dict[str, asyncio.Task] = {}

    def _calculate_hash(self, data: bytes) -> str:
        """Calculate SHA-256 hash of data."""
        return hashlib.sha256(data).hexdigest()

    def _get_file_path(self, file_hash: str) -> Path:
        """Get the path where a file should be stored."""
        return self.storage_dir / file_hash

    def _get_chunk_path(self, file_hash: str, chunk_index: int) -> Path:
        """Get the path where a chunk should be stored."""
        return self.storage_dir / f"{file_hash}.chunk{chunk_index}"

    async def create_file_metadata(self, file_path: str, owner_id: str) -> FileMetadata:
        """Create metadata for a new file."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Get file stats
        stats = path.stat()
        created_at = datetime.fromtimestamp(stats.st_ctime)
        modified_at = datetime.fromtimestamp(stats.st_mtime)

        # Calculate file hash and create chunks
        chunks = []
        with open(path, 'rb') as f:
            chunk_index = 0
            while True:
                chunk_data = f.read(self.CHUNK_SIZE)
                if not chunk_data:
                    break

                chunk_hash = self._calculate_hash(chunk_data)
                chunks.append(FileChunk(
                    index=chunk_index,
                    hash=chunk_hash,
                    size=len(chunk_data)
                ))
                chunk_index += 1

        # Calculate overall file hash
        file_hash = self._calculate_hash(b''.join(chunk.hash.encode() for chunk in chunks))

        return FileMetadata(
            name=path.name,
            size=stats.st_size,
            created_at=created_at,
            modified_at=modified_at,
            hash=file_hash,
            chunks=chunks,
            owner_id=owner_id,
            permissions={owner_id: ['read', 'write', 'share']}
        )

    async def save_file_metadata(self, metadata: FileMetadata) -> bool:
        """Save file metadata to disk."""
        try:
            metadata_path = self._get_file_path(metadata.hash) / "metadata.json"
            metadata_path.parent.mkdir(parents=True, exist_ok=True)

            metadata_dict = {
                "name": metadata.name,
                "size": metadata.size,
                "created_at": metadata.created_at.isoformat(),
                "modified_at": metadata.modified_at.isoformat(),
                "hash": metadata.hash,
                "chunks": [
                    {
                        "index": chunk.index,
                        "hash": chunk.hash,
                        "size": chunk.size
                    }
                    for chunk in metadata.chunks
                ],
                "owner_id": metadata.owner_id,
                "permissions": metadata.permissions
            }

            with open(metadata_path, 'w') as f:
                json.dump(metadata_dict, f, indent=2)
            return True
        except Exception as e:
            self.logger.error(f"Error saving file metadata: {e}")
            return False

    async def load_file_metadata(self, file_hash: str) -> Optional[FileMetadata]:
        """Load file metadata from disk."""
        try:
            metadata_path = self._get_file_path(file_hash) / "metadata.json"
            if not metadata_path.exists():
                return None

            with open(metadata_path, 'r') as f:
                data = json.load(f)

            return FileMetadata(
                name=data["name"],
                size=data["size"],
                created_at=datetime.fromisoformat(data["created_at"]),
                modified_at=datetime.fromisoformat(data["modified_at"]),
                hash=data["hash"],
                chunks=[
                    FileChunk(
                        index=chunk["index"],
                        hash=chunk["hash"],
                        size=chunk["size"]
                    )
                    for chunk in data["chunks"]
                ],
                owner_id=data["owner_id"],
                permissions=data["permissions"]
            )
        except Exception as e:
            self.logger.error(f"Error loading file metadata: {e}")
            return None

    async def save_chunk(self, file_hash: str, chunk: FileChunk) -> bool:
        """Save a file chunk to disk."""
        try:
            chunk_path = self._get_chunk_path(file_hash, chunk.index)
            chunk_path.parent.mkdir(parents=True, exist_ok=True)

            with open(chunk_path, 'wb') as f:
                f.write(chunk.data)
            return True
        except Exception as e:
            self.logger.error(f"Error saving chunk: {e}")
            return False

    async def load_chunk(self, file_hash: str, chunk_index: int) -> Optional[FileChunk]:
        """Load a file chunk from disk."""
        try:
            chunk_path = self._get_chunk_path(file_hash, chunk_index)
            if not chunk_path.exists():
                return None

            with open(chunk_path, 'rb') as f:
                data = f.read()

            return FileChunk(
                index=chunk_index,
                hash=self._calculate_hash(data),
                size=len(data),
                data=data
            )
        except Exception as e:
            self.logger.error(f"Error loading chunk: {e}")
            return None

    async def verify_chunk(self, file_hash: str, chunk_index: int) -> bool:
        """Verify the integrity of a chunk."""
        chunk = await self.load_chunk(file_hash, chunk_index)
        if not chunk:
            return False

        metadata = await self.load_file_metadata(file_hash)
        if not metadata:
            return False

        expected_chunk = metadata.chunks[chunk_index]
        return chunk.hash == expected_chunk.hash

    async def start_file_transfer(self, file_hash: str, target_path: str) -> str:
        """Start a file transfer operation."""
        transfer_id = f"{file_hash}_{datetime.now().timestamp()}"
        
        async def transfer_task():
            try:
                metadata = await self.load_file_metadata(file_hash)
                if not metadata:
                    raise FileNotFoundError(f"File metadata not found: {file_hash}")

                target = Path(target_path)
                target.parent.mkdir(parents=True, exist_ok=True)

                with open(target, 'wb') as f:
                    for chunk in metadata.chunks:
                        chunk_data = await self.load_chunk(file_hash, chunk.index)
                        if not chunk_data or not chunk_data.data:
                            raise ValueError(f"Chunk {chunk.index} not found or invalid")
                        f.write(chunk_data.data)

                # Verify the complete file
                with open(target, 'rb') as f:
                    file_hash = self._calculate_hash(f.read())
                    if file_hash != metadata.hash:
                        raise ValueError("File verification failed")

            except Exception as e:
                self.logger.error(f"Error during file transfer: {e}")
                raise

        self.active_transfers[transfer_id] = asyncio.create_task(transfer_task())
        return transfer_id

    def get_transfer_status(self, transfer_id: str) -> Optional[Tuple[float, str]]:
        """Get the status of a file transfer."""
        task = self.active_transfers.get(transfer_id)
        if not task:
            return None

        if task.done():
            try:
                task.result()
                return 1.0, "completed"
            except Exception as e:
                return 0.0, f"failed: {str(e)}"

        # TODO: Implement progress tracking
        return 0.5, "in_progress"

    def cancel_transfer(self, transfer_id: str) -> bool:
        """Cancel an active file transfer."""
        task = self.active_transfers.get(transfer_id)
        if not task:
            return False

        task.cancel()
        del self.active_transfers[transfer_id]
        return True

    def track_progress(self, transfer_id: str, callback: Callable[[float], None]) -> None:
        """Track the progress of a file transfer.
        
        Args:
            transfer_id: ID of the transfer to track
            callback: Function to call with progress updates (0.0 to 1.0)
        """
        if transfer_id not in self.active_transfers:
            raise FileManagerError(f"Transfer {transfer_id} not found")

        transfer = self.active_transfers[transfer_id]
        total_size = transfer.file_size
        last_progress = 0.0

        while transfer.is_active:
            try:
                current_size = transfer.get_bytes_transferred()
                progress = min(current_size / total_size, 1.0)
                
                # Only call callback if progress has changed significantly
                if abs(progress - last_progress) >= 0.01:  # 1% threshold
                    callback(progress)
                    last_progress = progress
                
                time.sleep(0.1)  # Update every 100ms
            except Exception as e:
                self.logger.error(f"Error tracking progress: {e}")
                break

    def get_transfer_progress(self, transfer_id: str) -> float:
        """Get the current progress of a file transfer.
        
        Args:
            transfer_id: ID of the transfer to check
            
        Returns:
            Progress as a float between 0.0 and 1.0
        """
        if transfer_id not in self.active_transfers:
            raise FileManagerError(f"Transfer {transfer_id} not found")

        transfer = self.active_transfers[transfer_id]
        return min(transfer.get_bytes_transferred() / transfer.file_size, 1.0)

    def get_all_transfer_progress(self) -> Dict[str, float]:
        """Get progress for all active transfers.
        
        Returns:
            Dictionary mapping transfer IDs to their progress (0.0 to 1.0)
        """
        return {
            transfer_id: self.get_transfer_progress(transfer_id)
            for transfer_id in self.active_transfers
        }

    async def handle_file_chunk(self, file_id: str, chunk_index: int, chunk_data: bytes) -> None:
        """Handle a received file chunk.
        
        Args:
            file_id: ID of the file
            chunk_index: Index of the chunk
            chunk_data: Binary data of the chunk
        """
        try:
            if file_id not in self.active_transfers:
                raise FileManagerError(f"Transfer {file_id} not found")

            transfer = self.active_transfers[file_id]
            await transfer.handle_chunk(chunk_index, chunk_data)
            self.logger.debug(f"Handled chunk {chunk_index} for file {file_id}")

            # Check if transfer is complete
            if transfer.is_complete():
                await self._finalize_transfer(file_id)

        except Exception as e:
            self.logger.error(f"Error handling file chunk: {e}")
            raise FileManagerError(f"Failed to handle file chunk: {e}")

    async def _finalize_transfer(self, transfer_id: str) -> None:
        """Finalize a completed transfer.
        
        Args:
            transfer_id: ID of the transfer to finalize
        """
        try:
            transfer = self.active_transfers[transfer_id]
            
            # Verify file integrity
            if not transfer.verify_integrity():
                raise FileManagerError("File integrity check failed")

            # Move file to final location
            await transfer.finalize()
            
            # Update database
            await self.db_manager.add_file(transfer.file_id, {
                "name": transfer.file_name,
                "size": transfer.file_size,
                "hash": transfer.file_hash,
                "chunks": transfer.chunks,
                "owner_id": transfer.owner_id,
                "permissions": transfer.permissions
            })

            # Remove from active transfers
            del self.active_transfers[transfer_id]
            
            self.logger.info(f"Transfer {transfer_id} completed successfully")

        except Exception as e:
            self.logger.error(f"Error finalizing transfer: {e}")
            raise FileManagerError(f"Failed to finalize transfer: {e}") 