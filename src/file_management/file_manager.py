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
import shutil

from src.database.db_manager import DatabaseManager
from src.file_management.file_transfer import FileTransfer

@dataclass
class FileChunk:
    """Represents a chunk of a file."""
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

class FileManagerError(Exception):
    """Exception raised for file manager errors."""
    pass

class FileManager:
    """Manages file operations in the P2P file sharing system."""

    # Constants
    CHUNK_SIZE = 1024 * 1024  # 1MB chunks

    def __init__(self, storage_dir: str, temp_dir: str, cache_dir: str,
                 db_manager: DatabaseManager):
        self.storage_dir = Path(storage_dir)
        self.temp_dir = Path(temp_dir)
        self.cache_dir = Path(cache_dir)
        self.db_manager = db_manager
        self.logger = logging.getLogger("FileManager")
        self.active_transfers: Dict[str, FileTransfer] = {}
        
        # Create directories if they don't exist
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def add_file(self, file_path: str, owner_id: str) -> Optional[FileMetadata]:
        """Add a file to the system.
        
        Args:
            file_path: Path to the file to add
            owner_id: ID of the user who owns the file
            
        Returns:
            FileMetadata object if successful, None otherwise
        """
        try:
            self.logger.debug(f"[DEBUG] Starting file addition process for: {file_path}")
            
            # 1. File Verification Stage
            try:
                self.logger.debug("[DEBUG] Verifying file")
                if not os.path.exists(file_path):
                    self.logger.error("[ERROR] File does not exist")
                    raise FileManagerError("File does not exist")
                if not os.access(file_path, os.R_OK):
                    self.logger.error("[ERROR] File is not readable")
                    raise FileManagerError("File is not readable")
                self.logger.debug("[DEBUG] File verification passed")
            except Exception as e:
                self.logger.error(f"[ERROR] File verification failed: {str(e)}")
                raise FileManagerError(f"File verification failed: {str(e)}")
            
            # 2. Metadata Creation Stage
            try:
                self.logger.debug("[DEBUG] Creating file metadata")
                metadata = asyncio.get_event_loop().run_until_complete(
                    self.create_file_metadata(file_path, owner_id)
                )
                if not metadata:
                    self.logger.error("[ERROR] Failed to create metadata")
                    raise FileManagerError("Failed to create metadata")
                self.logger.debug(f"[DEBUG] Metadata created: {metadata}")
            except Exception as e:
                self.logger.error(f"[ERROR] Metadata creation failed: {str(e)}")
                raise FileManagerError(f"Metadata creation failed: {str(e)}")
            
            # 3. Storage Stage
            try:
                self.logger.debug("[DEBUG] Saving metadata")
                if not asyncio.get_event_loop().run_until_complete(
                    self.save_file_metadata(metadata)
                ):
                    self.logger.error("[ERROR] Failed to save metadata")
                    raise FileManagerError("Failed to save metadata")
                
                self.logger.debug("[DEBUG] Copying file to storage")
                target_path = self._get_file_path(metadata.hash) / metadata.name
                shutil.copy2(file_path, target_path)
                self.logger.debug("[DEBUG] File copied successfully")
            except Exception as e:
                self.logger.error(f"[ERROR] Storage stage failed: {str(e)}")
                raise FileManagerError(f"Storage stage failed: {str(e)}")
            
            # 4. Final Verification Stage
            try:
                self.logger.debug("[DEBUG] Performing final verification")
                shared_files = asyncio.get_event_loop().run_until_complete(
                    self.get_shared_files_async()
                )
                if not any(f.hash == metadata.hash for f in shared_files):
                    self.logger.error("[ERROR] File not found in shared files after addition")
                    raise FileManagerError("File not found in shared files after addition")
                self.logger.debug("[DEBUG] Final verification passed")
            except Exception as e:
                self.logger.error(f"[ERROR] Final verification failed: {str(e)}")
                raise FileManagerError(f"Final verification failed: {str(e)}")
            
            self.logger.info(f"File added successfully: {file_path}")
            return metadata
            
        except Exception as e:
            self.logger.error(f"[ERROR] File addition failed: {str(e)}")
            raise FileManagerError(f"Failed to add file: {str(e)}")
    
    def delete_file(self, file_hash: str, user_id: str) -> bool:
        """Delete a file from the system.
        
        Args:
            file_hash: Hash of the file to delete
            user_id: ID of the user requesting deletion
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get file metadata
            metadata = asyncio.get_event_loop().run_until_complete(
                self.load_file_metadata(file_hash)
            )
            if not metadata:
                raise FileManagerError("File not found")
            
            # Check permissions
            if metadata.owner_id != user_id:
                raise FileManagerError("Permission denied: You can only delete your own files")
            
            # Delete file and metadata
            file_path = self._get_file_path(file_hash)
            if file_path.exists():
                shutil.rmtree(file_path)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error deleting file: {e}")
            raise FileManagerError(f"Failed to delete file: {e}")
    
    def start_file_transfer(self, file_hash: str, target_path: str, user_id: str) -> str:
        """Start a file transfer.
        
        Args:
            file_hash: Hash of the file to transfer
            target_path: Path where the file should be saved
            user_id: ID of the user requesting the transfer
            
        Returns:
            Transfer ID
        """
        try:
            # Get file metadata
            metadata = asyncio.get_event_loop().run_until_complete(
                self.load_file_metadata(file_hash)
            )
            if not metadata:
                raise FileManagerError("File not found")
            
            # Check permissions
            if user_id not in metadata.permissions or 'read' not in metadata.permissions[user_id]:
                raise FileManagerError("Permission denied: You don't have read access to this file")
            
            # Create transfer
            transfer = FileTransfer(
                file_id=file_hash,
                file_name=metadata.name,
                file_size=metadata.size,
                file_hash=file_hash,
                target_path=target_path,
                owner_id=metadata.owner_id,
                permissions=metadata.permissions
            )
            
            # Start transfer
            transfer.start()
            self.active_transfers[transfer.id] = transfer
            
            return transfer.id
            
        except Exception as e:
            self.logger.error(f"Error starting transfer: {e}")
            raise FileManagerError(f"Failed to start transfer: {e}")
    
    def cancel_transfer(self, transfer_id: str, user_id: str) -> bool:
        """Cancel an active file transfer.
        
        Args:
            transfer_id: ID of the transfer to cancel
            user_id: ID of the user requesting cancellation
            
        Returns:
            True if successful, False otherwise
        """
        try:
            transfer = self.active_transfers.get(transfer_id)
            if not transfer:
                raise FileManagerError("Transfer not found")
            
            # Check permissions
            if transfer.owner_id != user_id:
                raise FileManagerError("Permission denied: You can only cancel your own transfers")
            
            # Cancel transfer
            transfer.cancel()
            del self.active_transfers[transfer_id]
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error cancelling transfer: {e}")
            raise FileManagerError(f"Failed to cancel transfer: {e}")
    
    def get_shared_files(self) -> List[FileMetadata]:
        """Get list of shared files.
        
        Returns:
            List of FileMetadata objects
        """
        try:
            return asyncio.get_event_loop().run_until_complete(
                self.get_shared_files_async()
            )
        except Exception as e:
            self.logger.error(f"Error getting shared files: {e}")
            return []
    
    def get_active_transfers(self) -> List[FileTransfer]:
        """Get list of active transfers.
        
        Returns:
            List of FileTransfer objects
        """
        return list(self.active_transfers.values())

    def _get_file_path(self, file_hash: str) -> Path:
        """Get the path for a file in storage.
        
        Args:
            file_hash: Hash of the file
            
        Returns:
            Path object for the file
        """
        return self.storage_dir / file_hash

    def _get_chunk_path(self, file_hash: str, chunk_index: int) -> Path:
        """Get the path for a file chunk.
        
        Args:
            file_hash: Hash of the file
            chunk_index: Index of the chunk
            
        Returns:
            Path object for the chunk
        """
        return self._get_file_path(file_hash) / f"chunk_{chunk_index}"

    def _calculate_hash(self, data: bytes) -> str:
        """Calculate SHA-256 hash of data."""
        return hashlib.sha256(data).hexdigest()

    async def create_file_metadata(self, file_path: str, owner_id: str) -> FileMetadata:
        """Create metadata for a file."""
        try:
            self.logger.debug(f"[DEBUG] Starting metadata creation for file: {file_path}")
        path = Path(file_path)

            # 1. File Stats Stage
            try:
                self.logger.debug("[DEBUG] Getting file stats")
        stats = path.stat()
                if stats.st_size == 0:
                    self.logger.error("[ERROR] File is empty")
                    raise FileManagerError("File is empty")
                if stats.st_size > 1024 * 1024 * 1024:  # 1GB limit
                    self.logger.error("[ERROR] File too large")
                    raise FileManagerError("File is too large (max 1GB)")
                self.logger.debug(f"[DEBUG] File stats: size={stats.st_size}, created={stats.st_ctime}, modified={stats.st_mtime}")
            except Exception as e:
                self.logger.error(f"[ERROR] Failed to get file stats: {str(e)}")
                raise FileManagerError(f"Failed to get file stats: {str(e)}")

            # 2. Timestamp Stage
            try:
                self.logger.debug("[DEBUG] Processing timestamps")
        created_at = datetime.fromtimestamp(stats.st_ctime)
        modified_at = datetime.fromtimestamp(stats.st_mtime)
                if created_at > datetime.now() or modified_at > datetime.now():
                    self.logger.error("[ERROR] Invalid timestamps")
                    raise FileManagerError("Invalid file timestamps")
                self.logger.debug(f"[DEBUG] Timestamps processed: created={created_at}, modified={modified_at}")
            except Exception as e:
                self.logger.error(f"[ERROR] Failed to process timestamps: {str(e)}")
                raise FileManagerError(f"Failed to process timestamps: {str(e)}")

            # 3. Chunk Creation Stage
        chunks = []
            chunk_index = 0
            file_hash = hashlib.sha256()
            try:
                self.logger.debug("[DEBUG] Starting chunk creation")
                with open(path, 'rb') as f:
            while True:
                chunk_data = f.read(self.CHUNK_SIZE)
                if not chunk_data:
                    break
                        if len(chunk_data) > self.CHUNK_SIZE:
                            self.logger.error("[ERROR] Chunk size exceeds maximum")
                            raise FileManagerError("Chunk size exceeds maximum")
                        chunk_hash = hashlib.sha256(chunk_data).hexdigest()
                        file_hash.update(chunk_data)
                        chunk = FileChunk(
                    index=chunk_index,
                    hash=chunk_hash,
                    size=len(chunk_data)
                        )
                        chunks.append(chunk)
                        self.logger.debug(f"[DEBUG] Created chunk {chunk_index}: size={len(chunk_data)}, hash={chunk_hash}")
                chunk_index += 1
            except Exception as e:
                self.logger.error(f"[ERROR] Failed to create chunks: {str(e)}")
                raise FileManagerError(f"Failed to create file chunks: {str(e)}")

            if not chunks:
                self.logger.error("[ERROR] No chunks created")
                raise FileManagerError("No chunks were created")

            # 4. Hash Calculation Stage
            try:
                self.logger.debug("[DEBUG] Calculating final hash")
                final_hash = file_hash.hexdigest()
                if not final_hash:
                    self.logger.error("[ERROR] Hash calculation failed")
                    raise FileManagerError("Failed to calculate file hash")
                self.logger.debug(f"[DEBUG] Calculated file hash: {final_hash}")
            except Exception as e:
                self.logger.error(f"[ERROR] Failed to calculate hash: {str(e)}")
                raise FileManagerError(f"Failed to calculate file hash: {str(e)}")

            # 5. Metadata Object Creation Stage
            try:
                self.logger.debug("[DEBUG] Creating metadata object")
                metadata = FileMetadata(
            name=path.name,
            size=stats.st_size,
            created_at=created_at,
            modified_at=modified_at,
                    hash=final_hash,
            chunks=chunks,
            owner_id=owner_id,
            permissions={owner_id: ['read', 'write', 'share']}
        )
                # Verify metadata object
                if not metadata.name or not metadata.hash or not metadata.chunks:
                    self.logger.error("[ERROR] Invalid metadata object created")
                    raise FileManagerError("Invalid metadata object created")
                self.logger.debug(f"[DEBUG] Created metadata object: {metadata}")
                return metadata
            except Exception as e:
                self.logger.error(f"[ERROR] Failed to create metadata object: {str(e)}")
                raise FileManagerError(f"Failed to create metadata object: {str(e)}")
        except Exception as e:
            self.logger.error(f"[ERROR] Failed in create_file_metadata: {str(e)}")
            raise FileManagerError(f"Failed to create file metadata: {str(e)}")

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

    async def add_file_async(self, file_path):
        """Add a file to the shared files list (async version).
        
        Args:
            file_path: Path to the file to be shared
        """
        try:
            self.logger.debug(f"Starting to add file: {file_path}")
            
            # Create file metadata
            self.logger.debug("Creating file metadata...")
            metadata = await self.create_file_metadata(file_path, "local")
            self.logger.debug(f"Created metadata: {metadata}")
            
            # Save file metadata
            self.logger.debug("Saving file metadata...")
            if not await self.save_file_metadata(metadata):
                raise FileManagerError("Failed to save file metadata")
            self.logger.debug("File metadata saved successfully")
            
            # Copy file to storage directory
            source_path = Path(file_path)
            target_path = self._get_file_path(metadata.hash)
            self.logger.debug(f"Copying file to: {target_path}")
            target_path.mkdir(parents=True, exist_ok=True)
            
            # Copy file in chunks
            with open(source_path, 'rb') as src, open(target_path / source_path.name, 'wb') as dst:
                while True:
                    chunk = src.read(self.CHUNK_SIZE)
                    if not chunk:
                        break
                    dst.write(chunk)
            
            self.logger.info(f"File added successfully: {file_path}")
            
        except Exception as e:
            self.logger.error(f"Error adding file: {e}")
            raise FileManagerError(f"Failed to add file: {e}")

    async def get_shared_files_async(self):
        """Return a list of shared files (async version).
        
        Returns:
            List of FileMetadata objects for shared files
        """
        try:
            self.logger.debug("Getting shared files...")
            shared_files = []
            for file_dir in self.storage_dir.iterdir():
                self.logger.debug(f"Checking directory: {file_dir}")
                if file_dir.is_dir():
                    metadata_path = file_dir / "metadata.json"
                    self.logger.debug(f"Checking metadata path: {metadata_path}")
                    if metadata_path.exists():
                        self.logger.debug(f"Loading metadata from: {metadata_path}")
                        metadata = await self.load_file_metadata(file_dir.name)
                        if metadata:
                            self.logger.debug(f"Found metadata: {metadata}")
                            shared_files.append(metadata)
                        else:
                            self.logger.warning(f"No metadata found for: {file_dir}")
            self.logger.debug(f"Found {len(shared_files)} shared files")
            return shared_files
        except Exception as e:
            self.logger.error(f"Error getting shared files: {e}")
            return []

