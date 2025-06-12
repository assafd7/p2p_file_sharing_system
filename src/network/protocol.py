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
    type: MessageType
    sender_id: str
    payload: Dict[str, Any]
    timestamp: float = time.time()
    _logger: logging.Logger = None

    def __post_init__(self):
        self._logger = logging.getLogger(__name__)

    @classmethod
    def create(cls, msg_type: MessageType, sender_id: str, payload: Dict[str, Any]) -> 'Message':
        return cls(type=msg_type, sender_id=sender_id, payload=payload)

    def serialize(self) -> bytes:
        """Serialize the message to bytes with length prefix."""
        try:
            # Create message dictionary
            msg_dict = {
                'type': self.type.value,
                'sender_id': self.sender_id,
                'payload': self.payload,
                'timestamp': self.timestamp
            }
            
            # Log message content before serialization
            self._logger.debug(f"Serializing message: {msg_dict}")
            
            # Serialize to JSON
            json_data = json.dumps(msg_dict).encode('utf-8')
            
            # Log actual data size
            self._logger.debug(f"JSON data size: {len(json_data)} bytes")
            
            # Validate size before adding length prefix
            if len(json_data) > MAX_MESSAGE_SIZE:
                self._logger.error(f"Message size {len(json_data)} exceeds limit of {MAX_MESSAGE_SIZE}")
                raise MessageSizeError(f"Message size {len(json_data)} exceeds limit of {MAX_MESSAGE_SIZE}")
            
            # Pack length prefix
            length_prefix = struct.pack('!I', len(json_data))
            
            # Log final message size
            final_size = len(length_prefix) + len(json_data)
            self._logger.debug(f"Final message size with prefix: {final_size} bytes")
            
            # Verify the packed length matches actual data length
            unpacked_length = struct.unpack('!I', length_prefix)[0]
            if unpacked_length != len(json_data):
                self._logger.error(f"Length mismatch: packed={unpacked_length}, actual={len(json_data)}")
                raise InvalidMessageError("Length prefix does not match actual data length")
            
            return length_prefix + json_data
            
        except Exception as e:
            self._logger.error(f"Error serializing message: {e}")
            raise

    @classmethod
    def deserialize(cls, data: bytes) -> 'Message':
        """Deserialize a message from bytes."""
        logger = logging.getLogger(__name__)
        try:
            # Log raw data size
            logger.debug(f"Deserializing data of size: {len(data)} bytes")
            
            if len(data) < 4:
                logger.error("Data too short to contain length prefix")
                raise InvalidMessageError("Data too short to contain length prefix")
            
            # Unpack length prefix
            length = struct.unpack('!I', data[:4])[0]
            logger.debug(f"Unpacked length prefix: {length} bytes")
            
            # Validate length
            if length <= 0:
                logger.error(f"Invalid message length: {length}")
                raise InvalidMessageError(f"Invalid message length: {length}")
            
            if length > MAX_MESSAGE_SIZE:
                logger.error(f"Message size {length} exceeds limit of {MAX_MESSAGE_SIZE}")
                raise MessageSizeError(f"Message size {length} exceeds limit of {MAX_MESSAGE_SIZE}")
            
            if len(data) < 4 + length:
                logger.error(f"Incomplete message: expected {4 + length} bytes, got {len(data)}")
                raise InvalidMessageError("Incomplete message")
            
            # Extract and decode JSON data
            json_data = data[4:4 + length]
            logger.debug(f"Extracted JSON data size: {len(json_data)} bytes")
            
            try:
                msg_dict = json.loads(json_data.decode('utf-8'))
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON data: {e}")
                raise InvalidMessageError(f"Invalid JSON data: {e}")
            
            # Log deserialized message
            logger.debug(f"Deserialized message: {msg_dict}")
            
            # Validate required fields
            if 'type' not in msg_dict or 'sender_id' not in msg_dict:
                logger.error("Missing required fields in message")
                raise InvalidMessageError("Missing required fields in message")
            
            # Create message object
            return cls(
                type=MessageType(msg_dict['type']),
                sender_id=msg_dict['sender_id'],
                payload=msg_dict.get('payload', {})
            )
            
        except struct.error as e:
            logger.error(f"Error unpacking length prefix: {e}")
            raise InvalidMessageError(f"Error unpacking length prefix: {e}")
        except Exception as e:
            logger.error(f"Error deserializing message: {e}")
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