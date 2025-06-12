import asyncio
import socket
import hashlib
from typing import Optional, Dict, Set, Callable, Awaitable, Any
from dataclasses import dataclass
from datetime import datetime
import logging
from .protocol_v2 import Message, MessageType, MessageReader, MessageWriter, ProtocolError
import uuid
import time

# Constants for timeouts and retries
CONNECTION_TIMEOUT = 10.0  # seconds
READ_TIMEOUT = 30.0  # seconds
WRITE_TIMEOUT = 30.0  # seconds
HEARTBEAT_INTERVAL = 30.0  # seconds
HEARTBEAT_TIMEOUT = 10.0  # seconds
INITIAL_RETRY_DELAY = 1.0  # seconds
MAX_RETRIES = 3
CHUNK_SIZE = 8192  # 8KB chunks for reading/writing

logger = logging.getLogger(__name__)

@dataclass
class PeerInfo:
    """Information about a peer in the network."""
    id: str
    address: str
    port: int
    last_seen: datetime
    is_connected: bool = False

class Peer:
    def __init__(self, host: str, port: int, peer_id: Optional[str] = None, is_local: bool = False):
        """Initialize a peer connection."""
        self.host = host
        self.port = port
        self.peer_id = peer_id or str(uuid.uuid4())
        self.is_local = is_local
        self.is_connected = False
        self.reader: Optional[MessageReader] = None
        self.writer: Optional[MessageWriter] = None
        self.known_peers = set()  # Track known peers
        self.logger = logging.getLogger(f"Peer-{self.peer_id}")
        self.last_activity = time.time()
        self.retry_count = 0
        self.retry_delay = INITIAL_RETRY_DELAY
        self.max_retries = MAX_RETRIES
        self.connection_timeout = CONNECTION_TIMEOUT
        self.read_timeout = READ_TIMEOUT
        self.write_timeout = WRITE_TIMEOUT
        self.heartbeat_interval = HEARTBEAT_INTERVAL
        self.heartbeat_timeout = HEARTBEAT_TIMEOUT
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._message_processor_task: Optional[asyncio.Task] = None
        self.message_handlers: Dict[MessageType, Callable[[Message], Awaitable[None]]] = {}

    def _generate_peer_id(self) -> str:
        """Generate a unique peer ID based on host and port."""
        data = f"{self.host}:{self.port}".encode()
        return hashlib.sha1(data).hexdigest()

    async def connect(self) -> bool:
        """Connect to the peer."""
        try:
            # Create connection
            reader, writer = await asyncio.open_connection(self.host, self.port)
            
            # Initialize message reader and writer
            self.reader = MessageReader(reader)
            self.writer = MessageWriter(writer)
            self.is_connected = True
            
            # Start message processing
            self._message_processor_task = asyncio.create_task(self._process_messages())
            
            # Start heartbeat if not local
            if not self.is_local:
                self._heartbeat_task = asyncio.create_task(self._heartbeat())
            
            logger.info(f"Connected to peer {self.host}:{self.port}")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to peer {self.host}:{self.port}: {e}")
            await self.disconnect()
            return False

    async def disconnect(self, send_goodbye: bool = True) -> None:
        """Disconnect from the peer."""
        try:
            # Cancel tasks
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
                self._heartbeat_task = None
                
            if self._message_processor_task:
                self._message_processor_task.cancel()
                self._message_processor_task = None
            
            # Send goodbye message if requested and still connected
            if send_goodbye and self.writer and not self.writer.writer.is_closing():
                try:
                    goodbye_msg = Message(
                        msg_type=MessageType.GOODBYE,
                        sender_id=self.peer_id,
                        data={"reason": "normal_disconnect"}
                    )
                    await self.send_message(goodbye_msg)
                except Exception as e:
                    self.logger.debug(f"Error sending goodbye message: {e}")
                    
            # Close writer
            if self.writer:
                self.writer.writer.close()
                await self.writer.writer.wait_closed()
                self.writer = None
            
            self.reader = None
            self.is_connected = False
            logger.info(f"Disconnected from peer {self.host}:{self.port}")
            
        except Exception as e:
            logger.error(f"Error disconnecting from peer {self.host}:{self.port}: {e}")

    async def send_message(self, message: Message) -> bool:
        """Send a message to the peer."""
        if not self.is_connected or not self.writer:
            logger.error(f"Cannot send message: not connected to peer {self.host}:{self.port}")
            return False
            
        try:
            return await self.writer.write_message(message)
        except Exception as e:
            logger.error(f"Error sending message to peer {self.host}:{self.port}: {e}")
            await self.disconnect()
            return False

    def register_handler(self, message_type: MessageType, handler: Callable[[Message], Awaitable[None]]):
        """Register a message handler for a specific message type."""
        self.message_handlers[message_type] = handler

    async def start_listening(self):
        """Start listening for incoming connections."""
        if not self.is_local:
            self.logger.debug("Not starting server for non-local peer")
            return

        try:
            # Create server socket
            server = await asyncio.start_server(
                self._handle_connection,
                self.host,
                self.port
            )
            
            self.logger.info(f"Started listening on {self.host}:{self.port}")
            
            # Keep the server running
            async with server:
                await server.serve_forever()
                
        except Exception as e:
            self.logger.error(f"Error starting server: {e}")
            raise

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle an incoming connection."""
        try:
            # Get peer address
            peer_addr = writer.get_extra_info('peername')
            self.logger.info(f"New connection from {peer_addr[0]}:{peer_addr[1]}")
            
            # Store reader and writer
            self.reader = MessageReader(reader)
            self.writer = MessageWriter(writer)
            self.is_connected = True
            
            # Start message handling loop
            while self.is_connected:
                try:
                    message = await self.reader.read_message()
                    if message is None:
                        break
                        
                    # Handle message
                    if message.type in self.message_handlers:
                        await self.message_handlers[message.type](message)
                    else:
                        self.logger.warning(f"No handler for message type: {message.type}")
                        
                except Exception as e:
                    self.logger.error(f"Error handling message: {e}")
                    break
                    
        except Exception as e:
            self.logger.error(f"Error handling connection: {e}")
        finally:
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

    def get_known_peers(self) -> Set[PeerInfo]:
        """Get the set of peers known to this peer."""
        return self.known_peers

    def add_known_peer(self, peer_info: PeerInfo):
        """Add a peer to the known peers set."""
        self.known_peers.add(peer_info)

    def remove_known_peer(self, peer_id: str):
        """Remove a peer from the known peers set."""
        self.known_peers = {p for p in self.known_peers if p.id != peer_id} 

    async def _process_messages(self):
        """Process incoming messages from the peer."""
        if not self.reader:
            return
            
        try:
            while self.is_connected:
                message = await self.reader.read_message()
                if message is None:
                    break
                    
                # Handle message
                handler = self.message_handlers.get(message.type)
                if handler:
                    try:
                        await handler(message)
                    except Exception as e:
                        self.logger.error(f"Error handling message from {self.host}:{self.host}: {e}")
                else:
                    self.logger.warning(f"No handler registered for message type {message.type}")
                    
        except Exception as e:
            self.logger.error(f"Error processing messages from {self.host}:{self.port}: {e}")
        finally:
            await self.disconnect()

    async def _heartbeat(self):
        """Send periodic heartbeat messages to keep the connection alive."""
        while self.is_connected:
            try:
                message = Message(
                    type=MessageType.HEARTBEAT,
                    sender_id=self.peer_id,
                    payload={'timestamp': asyncio.get_event_loop().time()}
                )
                await self.send_message(message)
                await asyncio.sleep(self.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error sending heartbeat to {self.host}:{self.port}: {e}")
                await self.disconnect()
                break 