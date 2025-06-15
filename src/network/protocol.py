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
        self.logger.debug(f"Created new message: type={self.type}, sender={self.sender_id}, payload_size={len(str(self.payload))}")

    @classmethod
    def deserialize(cls, data: bytes) -> 'Message':
        """Create a message from serialized data."""
        logger = logging.getLogger(__name__)
        try:
            logger.debug(f"Deserializing message data of length {len(data)}")
            json_data = json.loads(data.decode())
            logger.debug(f"Deserialized JSON data: {json_data}")
            
            # Convert string type to MessageType enum
            msg_type = MessageType(json_data['type'])
            logger.debug(f"Converted message type: {msg_type}")
            
            message = cls(
                type=msg_type,
                sender_id=json_data.get('sender_id'),
                payload=json_data.get('payload', {})
            )
            logger.debug(f"Successfully deserialized message: type={message.type}, sender={message.sender_id}")
            return message
        except Exception as e:
            logger.error(f"Error deserializing message: {e}")
            raise

    def serialize(self) -> bytes:
        """Serialize the message to bytes."""
        try:
            self.logger.debug(f"Serializing message: type={self.type}, sender={self.sender_id}")
            data = {
                'type': self.type.value,  # Use the enum value
                'sender_id': self.sender_id,
                'payload': self.payload,
                'timestamp': self.timestamp
            }
            serialized = json.dumps(data).encode()
            self.logger.debug(f"Serialized message size: {len(serialized)} bytes")
            return serialized
        except Exception as e:
            self.logger.error(f"Error serializing message: {e}")
            raise

    @classmethod
    def create(cls, msg_type: MessageType, sender_id: str, payload: Optional[Dict] = None) -> 'Message':
        """Create a new message."""
        logger = logging.getLogger(__name__)
        logger.debug(f"Creating new message: type={msg_type}, sender={sender_id}")
        return cls(type=msg_type, sender_id=sender_id, payload=payload)

    def is_file_metadata_message(self) -> bool:
        """Check if this is a file metadata related message."""
        is_metadata = self.type in [
            MessageType.FILE_METADATA,
            MessageType.FILE_METADATA_REQUEST,
            MessageType.FILE_METADATA_RESPONSE
        ]
        self.logger.debug(f"Checking if message is file metadata: {is_metadata}")
        return is_metadata

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