import json
import struct
import time
from enum import Enum
from typing import Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime

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
    type: MessageType
    sender_id: str
    payload: Dict[str, Any]
    timestamp: float = time.time()

    @classmethod
    def create(cls, msg_type: MessageType, sender_id: str, payload: Dict[str, Any]) -> 'Message':
        return cls(type=msg_type, sender_id=sender_id, payload=payload)

    def serialize(self) -> bytes:
        """Serialize message to bytes with length prefix."""
        try:
            data = {
                'type': self.type.value,
                'sender_id': self.sender_id,
                'payload': self.payload,
                'timestamp': self.timestamp
            }
            json_data = json.dumps(data).encode('utf-8')
            length = len(json_data)
            
            # Validate length
            if length <= 0:
                raise InvalidMessageError("Message length must be positive")
            if length > MAX_MESSAGE_SIZE:
                raise MessageSizeError(f"Message size ({length} bytes) exceeds limit of {MAX_MESSAGE_SIZE} bytes")
                
            # Pack length as 4-byte big-endian integer
            length_prefix = struct.pack('>I', length)
            
            # Verify the packed length
            unpacked_length = struct.unpack('>I', length_prefix)[0]
            if unpacked_length != length:
                raise InvalidMessageError(f"Length verification failed: {length} != {unpacked_length}")
                
            return length_prefix + json_data
            
        except struct.error as e:
            raise InvalidMessageError(f"Error packing message length: {e}")
        except json.JSONDecodeError as e:
            raise InvalidMessageError(f"Error serializing message to JSON: {e}")
        except Exception as e:
            raise InvalidMessageError(f"Error serializing message: {e}")

    @classmethod
    def deserialize(cls, data: bytes) -> 'Message':
        """Deserialize bytes to Message object."""
        try:
            if len(data) < 4:
                raise InvalidMessageError("Message too short (missing length prefix)")
                
            # Unpack length prefix
            try:
                length = struct.unpack('>I', data[:4])[0]
            except struct.error as e:
                raise InvalidMessageError(f"Error unpacking message length: {e}")
                
            # Validate length
            if length <= 0:
                raise InvalidMessageError(f"Invalid message length: {length}")
            if length > MAX_MESSAGE_SIZE:
                raise MessageSizeError(f"Message size ({length} bytes) exceeds limit of {MAX_MESSAGE_SIZE} bytes")
            if length > len(data) - 4:
                raise InvalidMessageError(f"Message length ({length}) exceeds available data ({len(data) - 4} bytes)")
                
            # Extract and decode JSON data
            try:
                json_data = data[4:4+length].decode('utf-8')
                data_dict = json.loads(json_data)
            except UnicodeDecodeError as e:
                raise InvalidMessageError(f"Error decoding message data: {e}")
            except json.JSONDecodeError as e:
                raise InvalidMessageError(f"Error parsing message JSON: {e}")
                
            # Validate required fields
            if not all(key in data_dict for key in ['type', 'sender_id', 'payload', 'timestamp']):
                raise InvalidMessageError("Message missing required fields")
                
            return cls(
                type=MessageType(data_dict['type']),
                sender_id=data_dict['sender_id'],
                payload=data_dict['payload'],
                timestamp=data_dict['timestamp']
            )
            
        except Exception as e:
            if not isinstance(e, (InvalidMessageError, MessageSizeError)):
                raise InvalidMessageError(f"Error deserializing message: {e}")
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