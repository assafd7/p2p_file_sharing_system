import json
import struct
import time
from enum import Enum
from typing import Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import logging

class MessageType(Enum):
    """Types of messages that can be sent between peers."""
    HELLO = "hello"
    PING = "PING"
    PONG = "PONG"
    FILE_LIST = "FILE_LIST"
    FILE_REQUEST = "file_request"
    FILE_RESPONSE = "file_response"
    PEER_LIST = "peer_list"
    GOODBYE = "goodbye"
    HEARTBEAT = "heartbeat"
    USER_INFO = "user_info"
    FILE_METADATA = "file_metadata"  # New message type for file metadata
    FILE_METADATA_REQUEST = "file_metadata_request"  # Request for file metadata
    FILE_METADATA_RESPONSE = "file_metadata_response"  # Response with file metadata

@dataclass
class Message:
    """Message class for network communication."""
    
    def __init__(self, msg_type: Optional[MessageType] = None, type: Optional[MessageType] = None,
                 sender_id: Optional[str] = None, payload: Optional[Dict] = None):
        """Initialize a message."""
        if msg_type is None and type is None:
            raise ValueError("Either msg_type or type must be provided")
            
        self.type = msg_type or type
        self.sender_id = sender_id
        self.payload = payload or {}
        self.timestamp = time.time()  # Add timestamp for message ordering
        self.logger = logging.getLogger(__name__)

    @classmethod
    def deserialize(cls, data: bytes) -> 'Message':
        """Create a message from serialized data."""
        try:
            json_data = json.loads(data.decode())
            return cls(
                type=MessageType(json_data['type']),
                sender_id=json_data.get('sender_id'),
                payload=json_data.get('payload', {})
            )
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"Error deserializing message: {e}")
            raise

    def serialize(self) -> bytes:
        """Serialize the message to bytes."""
        try:
            data = {
                'type': self.type.value,
                'sender_id': self.sender_id,
                'payload': self.payload,
                'timestamp': self.timestamp
            }
            return json.dumps(data).encode()
        except Exception as e:
            self.logger.error(f"Error serializing message: {e}")
            raise

    @classmethod
    def create(cls, msg_type: MessageType, sender_id: str, payload: Optional[Dict] = None) -> 'Message':
        """Create a new message."""
        return cls(type=msg_type, sender_id=sender_id, payload=payload)

    def is_file_metadata_message(self) -> bool:
        """Check if this is a file metadata related message."""
        return self.type in [
            MessageType.FILE_METADATA,
            MessageType.FILE_METADATA_REQUEST,
            MessageType.FILE_METADATA_RESPONSE
        ]

class ProtocolError(Exception):
    """Base class for protocol-related errors."""
    pass

class MessageSizeError(ProtocolError):
    """Raised when message size exceeds limits."""
    pass

class InvalidMessageError(ProtocolError):
    """Raised when message format is invalid."""
    pass

# Constants
MAX_MESSAGE_SIZE = 10 * 1024 * 1024  # 10MB
CHUNK_SIZE = 4096  # 4KB
CONNECTION_TIMEOUT = 30  # seconds
READ_TIMEOUT = 10  # seconds
MAX_RETRIES = 3  # Maximum number of connection retries
INITIAL_RETRY_DELAY = 1  # Initial delay between retries in seconds
MAX_RETRY_DELAY = 10  # Maximum delay between retries in seconds 