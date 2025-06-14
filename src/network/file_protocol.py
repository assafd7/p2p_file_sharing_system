import asyncio
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import hashlib
import os
from pathlib import Path

from .protocol import Message, MessageType
from .peer import Peer
from src.file_management.file_manager import FileManager, FileMetadata, FileChunk

@dataclass
class FileTransferRequest:
    """Represents a file transfer request."""
    file_hash: str
    chunk_index: int
    peer_id: str
    timestamp: datetime = datetime.now()

@dataclass
class FileTransferResponse:
    """Represents a file transfer response."""
    file_hash: str
    chunk_index: int
    chunk_data: bytes
    chunk_hash: str
    peer_id: str
    timestamp: datetime = datetime.now()

class FileProtocol:
    """Handles file-related network operations."""
    
    def __init__(self, file_manager: FileManager):
        self.file_manager = file_manager
        self.logger = logging.getLogger("FileProtocol")
        self.active_transfers: Dict[str, FileTransferRequest] = {}
        self.transfer_callbacks: Dict[str, List[callable]] = {}
        
    async def handle_file_list_request(self, message: Message, peer: Peer) -> None:
        """Handle a file list request from a peer."""
        try:
            # Get shared files
            shared_files = await self.file_manager.get_shared_files_async()
            
            # Create response message
            response = Message(
                type=MessageType.FILE_LIST,
                sender_id=peer.id,
                payload={
                    'files': [
                        {
                            'name': f.name,
                            'size': f.size,
                            'hash': f.hash,
                            'created_at': f.created_at.isoformat(),
                            'modified_at': f.modified_at.isoformat(),
                            'owner_id': f.owner_id,
                            'permissions': f.permissions
                        }
                        for f in shared_files
                    ]
                }
            )
            
            # Send response
            await peer.send_message(response)
            
        except Exception as e:
            self.logger.error(f"Error handling file list request: {e}")
            # Send error response
            error_response = Message(
                type=MessageType.FILE_LIST,
                sender_id=peer.id,
                payload={'error': str(e)}
            )
            await peer.send_message(error_response)
    
    async def handle_file_request(self, message: Message, peer: Peer) -> None:
        """Handle a file request from a peer."""
        try:
            file_hash = message.payload.get('file_hash')
            chunk_index = message.payload.get('chunk_index')
            
            if not file_hash or chunk_index is None:
                raise ValueError("Missing required fields in file request")
            
            # Get file metadata
            metadata = await self.file_manager.load_file_metadata(file_hash)
            if not metadata:
                raise FileNotFoundError(f"File not found: {file_hash}")
            
            # Check permissions
            if peer.id not in metadata.permissions or 'read' not in metadata.permissions[peer.id]:
                raise PermissionError("Permission denied")
            
            # Get chunk
            chunk = await self.file_manager.load_chunk(file_hash, chunk_index)
            if not chunk:
                raise FileNotFoundError(f"Chunk not found: {chunk_index}")
            
            # Create response
            response = Message(
                type=MessageType.FILE_RESPONSE,
                sender_id=peer.id,
                payload={
                    'file_hash': file_hash,
                    'chunk_index': chunk_index,
                    'chunk_data': chunk.data,
                    'chunk_hash': chunk.hash
                }
            )
            
            # Send response
            await peer.send_message(response)
            
        except Exception as e:
            self.logger.error(f"Error handling file request: {e}")
            # Send error response
            error_response = Message(
                type=MessageType.FILE_RESPONSE,
                sender_id=peer.id,
                payload={
                    'error': str(e),
                    'file_hash': message.payload.get('file_hash'),
                    'chunk_index': message.payload.get('chunk_index')
                }
            )
            await peer.send_message(error_response)
    
    async def request_file_list(self, peer: Peer) -> List[Dict]:
        """Request file list from a peer."""
        try:
            # Create request message
            request = Message(
                type=MessageType.FILE_LIST,
                sender_id=peer.id,
                payload={}
            )
            
            # Send request
            await peer.send_message(request)
            
            # Wait for response
            response = await peer.receive_message()
            if not response or response.type != MessageType.FILE_LIST:
                raise ValueError("Invalid response type")
            
            if 'error' in response.payload:
                raise ValueError(response.payload['error'])
            
            return response.payload.get('files', [])
            
        except Exception as e:
            self.logger.error(f"Error requesting file list: {e}")
            raise
    
    async def request_file_chunk(self, peer: Peer, file_hash: str, chunk_index: int) -> Optional[bytes]:
        """Request a file chunk from a peer."""
        try:
            # Create request message
            request = Message(
                type=MessageType.FILE_REQUEST,
                sender_id=peer.id,
                payload={
                    'file_hash': file_hash,
                    'chunk_index': chunk_index
                }
            )
            
            # Send request
            await peer.send_message(request)
            
            # Wait for response
            response = await peer.receive_message()
            if not response or response.type != MessageType.FILE_RESPONSE:
                raise ValueError("Invalid response type")
            
            if 'error' in response.payload:
                raise ValueError(response.payload['error'])
            
            # Verify chunk hash
            chunk_data = response.payload.get('chunk_data')
            chunk_hash = response.payload.get('chunk_hash')
            
            if not chunk_data or not chunk_hash:
                raise ValueError("Missing chunk data or hash")
            
            # Verify hash
            calculated_hash = hashlib.sha256(chunk_data).hexdigest()
            if calculated_hash != chunk_hash:
                raise ValueError("Chunk hash verification failed")
            
            return chunk_data
            
        except Exception as e:
            self.logger.error(f"Error requesting file chunk: {e}")
            raise
    
    def register_transfer_callback(self, transfer_id: str, callback: callable) -> None:
        """Register a callback for transfer progress updates."""
        if transfer_id not in self.transfer_callbacks:
            self.transfer_callbacks[transfer_id] = []
        self.transfer_callbacks[transfer_id].append(callback)
    
    def unregister_transfer_callback(self, transfer_id: str, callback: callable) -> None:
        """Unregister a transfer progress callback."""
        if transfer_id in self.transfer_callbacks:
            self.transfer_callbacks[transfer_id].remove(callback)
            if not self.transfer_callbacks[transfer_id]:
                del self.transfer_callbacks[transfer_id]
    
    def _notify_transfer_progress(self, transfer_id: str, progress: float) -> None:
        """Notify all callbacks of transfer progress."""
        if transfer_id in self.transfer_callbacks:
            for callback in self.transfer_callbacks[transfer_id]:
                try:
                    callback(progress)
                except Exception as e:
                    self.logger.error(f"Error in transfer callback: {e}") 