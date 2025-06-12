import json
import struct
import time
from enum import Enum
from typing import Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import logging

class MessageType(Enum):
    HELLO = "HELLO"
    PING = "PING"
    PONG = "PONG"
    FILE_LIST = "FILE_LIST"
    FILE_REQUEST = "FILE_REQUEST"
    FILE_RESPONSE = "FILE_RESPONSE"
    PEER_LIST = "PEER_LIST"
    GOODBYE = "GOODBYE"
    HEARTBEAT = "HEARTBEAT"

@dataclass
class Message:
    """Message class for network communication."""
    
    def __init__(self, msg_type: Optional[MessageType] = None, type: Optional[MessageType] = None,
                 sender_id: str = None, payload: Dict[str, Any] = None, timestamp: Optional[float] = None):
        # Handle both type and msg_type parameters for backward compatibility
        self.type = msg_type if msg_type is not None else type
        if self.type is None:
            raise ValueError("Either msg_type or type must be provided")
            
        self.sender_id = sender_id
        self.payload = payload or {}
        self.timestamp = timestamp or time.time()
        self.logger = logging.getLogger(__name__)
        
    def serialize(self) -> bytes:
        """Serialize the message to bytes."""
        try:
            # Create message dictionary
            msg_dict = {
                'type': self.type.value,
                'sender_id': self.sender_id,
                'payload': self.payload,
                'timestamp': self.timestamp
            }
            
            # Convert to JSON
            json_data = json.dumps(msg_dict).encode('utf-8')
            self.logger.debug(f"JSON data size: {len(json_data)} bytes")
            
            # Pack length prefix
            length_prefix = struct.pack('!I', len(json_data))
            self.logger.debug(f"Final message size with prefix: {len(length_prefix) + len(json_data)} bytes")
            
            return length_prefix + json_data
            
        except Exception as e:
            self.logger.error(f"Error serializing message: {e}")
            raise
            
    @classmethod
    def deserialize(cls, data: bytes) -> 'Message':
        """Deserialize bytes to a Message object."""
        try:
            # The length prefix has already been handled by the reader
            # Just decode the JSON data
            json_data = data.decode('utf-8')
            msg_dict = json.loads(json_data)
            
            return cls(
                type=MessageType(msg_dict['type']),  # Use 'type' here since that's what's in the JSON
                sender_id=msg_dict['sender_id'],
                payload=msg_dict['payload'],
                timestamp=msg_dict['timestamp']
            )
            
        except json.JSONDecodeError as e:
            cls.logger.error(f"Error decoding JSON: {e}")
            raise InvalidMessageError(f"Invalid JSON data: {e}")
        except KeyError as e:
            cls.logger.error(f"Missing required field: {e}")
            raise InvalidMessageError(f"Missing required field: {e}")
        except Exception as e:
            cls.logger.error(f"Error deserializing message: {e}")
            raise

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