import hashlib
import os
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, Optional
import logging

@dataclass
class FileMetadata:
    """Represents metadata for a shared file."""
    file_id: str  # Unique identifier for the file
    name: str     # File name
    size: int     # File size in bytes
    hash: str     # SHA-256 hash of the file
    owner_id: str # Peer ID of the owner
    owner_name: str # Username of the owner
    upload_time: datetime # When the file was uploaded
    is_available: bool = True # Whether the file is currently available for download
    ttl: int = 10  # Time-to-live for broadcasting
    origin_peer_id: Optional[str] = None  # ID of the peer that originally uploaded

    def to_dict(self) -> Dict:
        """Convert metadata to dictionary for serialization."""
        data = asdict(self)
        data['upload_time'] = self.upload_time.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: Dict) -> 'FileMetadata':
        """Create metadata from dictionary."""
        if 'upload_time' in data:
            data['upload_time'] = datetime.fromisoformat(data['upload_time'])
        return cls(**data)

    @classmethod
    def create_from_file(cls, file_path: str, owner_id: str, owner_name: str) -> 'FileMetadata':
        """Create metadata from a file."""
        try:
            # Get file stats
            stats = os.stat(file_path)
            
            # Calculate file hash
            file_hash = cls._calculate_file_hash(file_path)
            
            # Generate unique file ID
            file_id = hashlib.sha256(
                f"{file_path}{stats.st_mtime}{stats.st_size}".encode()
            ).hexdigest()
            
            return cls(
                file_id=file_id,
                name=os.path.basename(file_path),
                size=stats.st_size,
                hash=file_hash,
                owner_id=owner_id,
                owner_name=owner_name,
                upload_time=datetime.now(),
                origin_peer_id=owner_id
            )
        except Exception as e:
            logging.error(f"Error creating file metadata: {e}")
            raise

    @staticmethod
    def _calculate_file_hash(file_path: str) -> str:
        """Calculate SHA-256 hash of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            # Read file in chunks to handle large files
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def is_valid(self) -> bool:
        """Check if metadata is valid."""
        return all([
            self.file_id,
            self.name,
            self.size > 0,
            self.hash,
            self.owner_id,
            self.owner_name,
            self.upload_time,
            self.ttl >= 0
        ]) 