import json
import struct
import asyncio
from enum import Enum
from typing import Any, Dict, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

class MessageType(Enum):
    PEER_LIST = "PEER_LIST"
    HEARTBEAT = "HEARTBEAT"
    FILE_REQUEST = "FILE_REQUEST"
    FILE_RESPONSE = "FILE_RESPONSE"
    CHUNK_REQUEST = "CHUNK_REQUEST"
    CHUNK_RESPONSE = "CHUNK_RESPONSE"
    ERROR = "ERROR"

@dataclass
class Message:
    type: MessageType
    sender_id: str
    payload: Dict[str, Any]
    
    @classmethod
    def create(cls, msg_type: MessageType, sender_id: str, payload: Dict[str, Any]) -> 'Message':
        return cls(type=msg_type, sender_id=sender_id, payload=payload)

class ProtocolError(Exception):
    """Base class for protocol errors"""
    pass

class MessageSizeError(ProtocolError):
    """Raised when a message exceeds the maximum allowed size"""
    pass

class InvalidMessageError(ProtocolError):
    """Raised when a message is invalid or malformed"""
    pass

class MessageFramer:
    """Handles message framing and unframing with proper length prefixing"""
    
    # Constants
    HEADER_SIZE = 4  # 4 bytes for message length
    MAX_MESSAGE_SIZE = 10 * 1024 * 1024  # 10MB
    
    @staticmethod
    def pack_header(length: int) -> bytes:
        """Pack message length into a 4-byte header"""
        if length < 0 or length > MessageFramer.MAX_MESSAGE_SIZE:
            raise MessageSizeError(f"Message length {length} exceeds maximum size of {MessageFramer.MAX_MESSAGE_SIZE}")
        return struct.pack('!I', length)
    
    @staticmethod
    def unpack_header(header: bytes) -> int:
        """Unpack message length from a 4-byte header"""
        if len(header) != MessageFramer.HEADER_SIZE:
            raise InvalidMessageError(f"Invalid header size: {len(header)}")
        length = struct.unpack('!I', header)[0]
        if length < 0 or length > MessageFramer.MAX_MESSAGE_SIZE:
            raise MessageSizeError(f"Message length {length} exceeds maximum size of {MessageFramer.MAX_MESSAGE_SIZE}")
        return length
    
    @staticmethod
    def frame_message(message: Message) -> bytes:
        """Frame a message with length prefix"""
        try:
            # Convert message to JSON
            message_dict = {
                'type': message.type.value,
                'sender_id': message.sender_id,
                'payload': message.payload
            }
            json_data = json.dumps(message_dict).encode('utf-8')
            
            # Check size before framing
            if len(json_data) > MessageFramer.MAX_MESSAGE_SIZE:
                raise MessageSizeError(f"Message size {len(json_data)} exceeds limit of {MessageFramer.MAX_MESSAGE_SIZE}")
            
            # Create header and combine with message
            header = MessageFramer.pack_header(len(json_data))
            return header + json_data
            
        except Exception as e:
            logger.error(f"Error framing message: {e}")
            raise ProtocolError(f"Failed to frame message: {e}")
    
    @staticmethod
    def unframe_message(data: bytes) -> Message:
        """Unframe a message from length-prefixed data"""
        try:
            if len(data) < MessageFramer.HEADER_SIZE:
                raise InvalidMessageError("Data too short to contain header")
            
            # Extract header and message
            header = data[:MessageFramer.HEADER_SIZE]
            message_data = data[MessageFramer.HEADER_SIZE:]
            
            # Verify length
            expected_length = MessageFramer.unpack_header(header)
            if len(message_data) != expected_length:
                raise InvalidMessageError(f"Message length mismatch: expected {expected_length}, got {len(message_data)}")
            
            # Parse JSON
            message_dict = json.loads(message_data.decode('utf-8'))
            
            # Validate required fields
            if not all(k in message_dict for k in ['type', 'sender_id', 'payload']):
                raise InvalidMessageError("Missing required message fields")
            
            # Create message object
            return Message(
                type=MessageType(message_dict['type']),
                sender_id=message_dict['sender_id'],
                payload=message_dict['payload']
            )
            
        except json.JSONDecodeError as e:
            raise InvalidMessageError(f"Invalid JSON in message: {e}")
        except Exception as e:
            logger.error(f"Error unframing message: {e}")
            raise ProtocolError(f"Failed to unframe message: {e}")

class MessageReader:
    """Handles reading framed messages from a stream"""
    
    def __init__(self, reader: asyncio.StreamReader):
        self.reader = reader
        self.buffer = bytearray()
    
    async def read_message(self) -> Optional[Message]:
        """Read a complete message from the stream"""
        try:
            # Read header
            header = await self.reader.readexactly(MessageFramer.HEADER_SIZE)
            message_length = MessageFramer.unpack_header(header)
            
            # Read message data
            message_data = await self.reader.readexactly(message_length)
            
            # Combine header and message data
            complete_message = header + message_data
            
            # Unframe and return message
            return MessageFramer.unframe_message(complete_message)
            
        except asyncio.IncompleteReadError:
            logger.debug("Connection closed while reading message")
            return None
        except Exception as e:
            logger.error(f"Error reading message: {e}")
            return None

class MessageWriter:
    """Handles writing framed messages to a stream"""
    
    def __init__(self, writer: asyncio.StreamWriter):
        self.writer = writer
    
    async def write_message(self, message: Message) -> bool:
        """Write a message to the stream"""
        try:
            # Frame the message
            framed_data = MessageFramer.frame_message(message)
            
            # Write the data
            self.writer.write(framed_data)
            await self.writer.drain()
            return True
            
        except Exception as e:
            logger.error(f"Error writing message: {e}")
            return False 