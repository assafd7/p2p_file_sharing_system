import asyncio
import socket
import hashlib
from typing import Optional, Dict, Set, Callable, Awaitable
from dataclasses import dataclass
from datetime import datetime
import logging
from .protocol import Message, MessageType, ProtocolError, CHUNK_SIZE, CONNECTION_TIMEOUT, READ_TIMEOUT

@dataclass
class PeerInfo:
    """Information about a peer in the network."""
    id: str
    address: str
    port: int
    last_seen: datetime
    is_connected: bool = False

class Peer:
    def __init__(self, host: str, port: int, peer_id: Optional[str] = None):
        self.host = host
        self.port = port
        self.peer_id = peer_id or self._generate_peer_id()
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.is_connected = False
        self.message_handlers: Dict[MessageType, Callable[[Message], Awaitable[None]]] = {}
        self.logger = logging.getLogger(f"Peer-{self.peer_id[:8]}")

    def _generate_peer_id(self) -> str:
        """Generate a unique peer ID based on host and port."""
        data = f"{self.host}:{self.port}".encode()
        return hashlib.sha1(data).hexdigest()

    async def connect(self) -> bool:
        """Establish connection with the peer."""
        try:
            # Validate host and port
            if not self.host or not self.port:
                self.logger.error("Invalid host or port")
                return False

            # Try to resolve hostname
            try:
                socket.gethostbyname(self.host)
            except socket.gaierror:
                self.logger.error(f"Could not resolve hostname: {self.host}")
                return False

            # Attempt connection with timeout
            try:
                self.reader, self.writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=CONNECTION_TIMEOUT
                )
            except asyncio.TimeoutError:
                self.logger.error(f"Connection timeout to {self.host}:{self.port}")
                return False
            except ConnectionRefusedError:
                self.logger.error(f"Connection refused by {self.host}:{self.port}")
                return False
            except Exception as e:
                self.logger.error(f"Failed to connect to {self.host}:{self.port}: {e}")
                return False

            self.is_connected = True
            self.logger.info(f"Connected to peer {self.host}:{self.port}")
            
            # Send HELLO message
            try:
                hello_msg = Message.create(
                    MessageType.HELLO,
                    self.peer_id,
                    {"version": "1.0"}
                )
                await self.send_message(hello_msg)
                return True
            except Exception as e:
                self.logger.error(f"Failed to send HELLO message: {e}")
                await self.disconnect()
                return False
            
        except Exception as e:
            self.logger.error(f"Error in connect: {e}")
            await self.disconnect()
            return False

    async def disconnect(self):
        """Gracefully disconnect from the peer."""
        if self.is_connected:
            try:
                goodbye_msg = Message.create(
                    MessageType.GOODBYE,
                    self.peer_id,
                    {"reason": "graceful_shutdown"}
                )
                await self.send_message(goodbye_msg)
            except Exception as e:
                self.logger.error(f"Error sending goodbye message: {e}")
            
            if self.writer:
                self.writer.close()
                await self.writer.wait_closed()
            
            self.is_connected = False
            self.reader = None
            self.writer = None
            self.logger.info("Disconnected from peer")

    async def send_message(self, message: Message) -> bool:
        """Send a message to the peer."""
        if not self.is_connected or not self.writer:
            raise ConnectionError("Not connected to peer")
            
        try:
            data = message.serialize()
            self.writer.write(data)
            await self.writer.drain()
            return True
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")
            return False

    async def receive_message(self) -> Optional[Message]:
        """Receive a message from the peer."""
        if not self.is_connected or not self.reader:
            raise ConnectionError("Not connected to peer")
            
        try:
            # Read message length (4 bytes)
            length_data = await self.reader.readexactly(4)
            length = int.from_bytes(length_data, byteorder='big')
            
            if length > 1024 * 1024:  # 1MB limit
                raise ProtocolError("Message too large")
                
            # Read message data
            data = await self.reader.readexactly(length)
            return Message.deserialize(data)
            
        except asyncio.IncompleteReadError:
            self.logger.error("Connection closed by peer")
            await self.disconnect()
            return None
        except Exception as e:
            self.logger.error(f"Error receiving message: {e}")
            return None

    def register_handler(self, message_type: MessageType, handler: Callable[[Message], Awaitable[None]]):
        """Register a message handler for a specific message type."""
        self.message_handlers[message_type] = handler

    async def start_listening(self):
        """Start listening for messages from the peer."""
        while self.is_connected:
            try:
                message = await self.receive_message()
                if message is None:
                    break
                    
                handler = self.message_handlers.get(message.type)
                if handler:
                    await handler(message)
                else:
                    self.logger.warning(f"No handler registered for message type: {message.type}")
                    
            except Exception as e:
                self.logger.error(f"Error in message handling: {e}")
                break
                
        await self.disconnect()

    async def ping(self) -> bool:
        """Send a PING message and wait for PONG response."""
        try:
            ping_msg = Message.create(
                MessageType.PING,
                self.peer_id,
                {"timestamp": datetime.now().timestamp()}
            )
            await self.send_message(ping_msg)
            return True
        except Exception as e:
            self.logger.error(f"Error sending ping: {e}")
            return False 