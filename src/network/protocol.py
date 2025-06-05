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
        data = {
            'type': self.type.value,
            'sender_id': self.sender_id,
            'payload': self.payload,
            'timestamp': self.timestamp
        }
        json_data = json.dumps(data).encode('utf-8')
        length = len(json_data)
        
        # Ensure message size doesn't exceed 1MB
        if length > 1024 * 1024:
            raise ValueError("Message size exceeds 1MB limit")
            
        # Pack length as 4-byte big-endian integer
        length_prefix = struct.pack('>I', length)
        return length_prefix + json_data

    @classmethod
    def deserialize(cls, data: bytes) -> 'Message':
        """Deserialize bytes to Message object."""
        if len(data) < 4:
            raise ValueError("Message too short")
            
        # Unpack length prefix
        length = struct.unpack('>I', data[:4])[0]
        if length > 1024 * 1024:
            raise ValueError("Message size exceeds 1MB limit")
            
        json_data = data[4:4+length].decode('utf-8')
        data_dict = json.loads(json_data)
        
        return cls(
            type=MessageType(data_dict['type']),
            sender_id=data_dict['sender_id'],
            payload=data_dict['payload'],
            timestamp=data_dict['timestamp']
        )

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
MAX_MESSAGE_SIZE = 1024 * 1024  # 1MB
CHUNK_SIZE = 4096  # 4KB
CONNECTION_TIMEOUT = 5  # seconds
READ_TIMEOUT = 2  # seconds 