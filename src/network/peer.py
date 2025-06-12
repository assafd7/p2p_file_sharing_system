import asyncio
import socket
import hashlib
from typing import Optional, Dict, Set, Callable, Awaitable
from dataclasses import dataclass
from datetime import datetime
import logging
from .protocol import (
    Message, 
    MessageType, 
    ProtocolError, 
    CHUNK_SIZE, 
    CONNECTION_TIMEOUT, 
    READ_TIMEOUT,
    MAX_RETRIES,
    INITIAL_RETRY_DELAY,
    MAX_RETRY_DELAY,
    MAX_MESSAGE_SIZE,
    MessageSizeError,
    InvalidMessageError
)
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

@dataclass
class PeerInfo:
    """Information about a peer in the network."""
    id: str
    address: str
    port: int
    last_seen: datetime
    is_connected: bool = False

class Peer:
    def __init__(self, host: str, port: int, peer_id: str = None, is_local: bool = False):
        """Initialize a peer connection."""
        self.host = host
        self.port = port
        self.peer_id = peer_id or str(uuid.uuid4())
        self.reader = None
        self.writer = None
        self.is_connected = False
        self.is_disconnecting = False  # Add flag to track disconnection state
        self.is_local = is_local  # Add back is_local flag
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
        self.heartbeat_task = None
        self.message_queue = asyncio.Queue()
        self.processing_task = None
        self._lock = asyncio.Lock()
        self.message_handlers: Dict[MessageType, Callable[[Message], Awaitable[None]]] = {}

    def _generate_peer_id(self) -> str:
        """Generate a unique peer ID based on host and port."""
        data = f"{self.host}:{self.port}".encode()
        return hashlib.sha1(data).hexdigest()

    async def connect(self) -> bool:
        """Establish connection with the peer."""
        retry_count = 0
        retry_delay = INITIAL_RETRY_DELAY

        while retry_count < MAX_RETRIES:
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
                    self.logger.info(f"Attempting to connect to {self.host}:{self.port} (attempt {retry_count + 1}/{MAX_RETRIES})")
                    self.reader, self.writer = await asyncio.wait_for(
                        asyncio.open_connection(self.host, self.port),
                        timeout=CONNECTION_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    self.logger.error(f"Connection timeout to {self.host}:{self.port}")
                    retry_count += 1
                    if retry_count < MAX_RETRIES:
                        self.logger.info(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
                        continue
                    return False
                except ConnectionRefusedError:
                    self.logger.error(f"Connection refused by {self.host}:{self.port}")
                    retry_count += 1
                    if retry_count < MAX_RETRIES:
                        self.logger.info(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
                        continue
                    return False
                except Exception as e:
                    self.logger.error(f"Failed to connect to {self.host}:{self.port}: {e}")
                    retry_count += 1
                    if retry_count < MAX_RETRIES:
                        self.logger.info(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
                        continue
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
                    retry_count += 1
                    if retry_count < MAX_RETRIES:
                        self.logger.info(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
                        continue
                    return False
                
            except Exception as e:
                self.logger.error(f"Error in connect: {e}")
                await self.disconnect()
                retry_count += 1
                if retry_count < MAX_RETRIES:
                    self.logger.info(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
                    continue
                return False

        return False

    async def disconnect(self, send_goodbye: bool = True) -> None:
        """Disconnect from the peer."""
        if self.is_disconnecting:  # Prevent recursive disconnects
            return
            
        self.is_disconnecting = True
        self.is_connected = False
        
        try:
            # Cancel heartbeat task if it exists
            if self.heartbeat_task:
                self.heartbeat_task.cancel()
                try:
                    await self.heartbeat_task
                except asyncio.CancelledError:
                    pass
                self.heartbeat_task = None
                
            # Cancel message processing task if it exists
            if self.processing_task:
                self.processing_task.cancel()
                try:
                    await self.processing_task
                except asyncio.CancelledError:
                    pass
                self.processing_task = None
                
            # Send goodbye message if requested and still connected
            if send_goodbye and self.writer and not self.writer.is_closing():
                try:
                    goodbye_msg = Message(
                        msg_type=MessageType.GOODBYE,
                        sender_id=self.peer_id,
                        data={"reason": "normal_disconnect"}
                    )
                    await self.send_message(goodbye_msg)
                except Exception as e:
                    self.logger.debug(f"Error sending goodbye message: {e}")
                    
            # Close writer if it exists
            if self.writer:
                try:
                    self.writer.close()
                    await self.writer.wait_closed()
                except Exception as e:
                    self.logger.debug(f"Error closing writer: {e}")
                    
            self.reader = None
            self.writer = None
            self.logger.info("Disconnected from peer")
            
        except Exception as e:
            self.logger.error(f"Error during disconnect: {e}")
        finally:
            self.is_disconnecting = False

    async def send_message(self, message: Message) -> bool:
        """Send a message to the peer."""
        if not self.is_connected or not self.writer or self.is_disconnecting:
            return False
            
        try:
            data = message.serialize()
            total_sent = 0
            while total_sent < len(data):
                try:
                    sent = await asyncio.wait_for(
                        self.writer.write(data[total_sent:]),
                        timeout=READ_TIMEOUT
                    )
                    if sent == 0:
                        self.logger.error("Connection closed while sending message")
                        await self.disconnect(send_goodbye=False)
                        return False
                    total_sent += sent
                    self.logger.debug(f"Sent {total_sent}/{len(data)} bytes")
                except asyncio.TimeoutError:
                    self.logger.error("Timeout sending message")
                    await self.disconnect(send_goodbye=False)
                    return False
                    
            await self.writer.drain()
            return True
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")
            await self.disconnect(send_goodbye=False)
            return False

    async def receive_message(self) -> Optional[Message]:
        """Receive a message from the peer."""
        if not self.is_connected or not self.reader:
            raise ConnectionError("Not connected to peer")
            
        try:
            # Read message length (4 bytes)
            try:
                length_data = await asyncio.wait_for(
                    self.reader.readexactly(4),
                    timeout=READ_TIMEOUT
                )
            except asyncio.TimeoutError:
                self.logger.error("Timeout reading message length")
                await self.disconnect()
                return None
            except asyncio.IncompleteReadError:
                self.logger.error("Connection closed while reading message length")
                await self.disconnect()
                return None
                
            # Verify length prefix bytes
            if len(length_data) != 4:
                self.logger.error(f"Invalid length prefix size: {len(length_data)} bytes")
                await self.disconnect()
                return None
                
            # Convert to integer and validate
            try:
                length = int.from_bytes(length_data, byteorder='big')
                self.logger.debug(f"Received message length: {length} bytes")
            except ValueError as e:
                self.logger.error(f"Invalid message length data: {e}")
                await self.disconnect()
                return None
                
            # Validate length
            if length <= 0:
                self.logger.error(f"Invalid message length: {length}")
                await self.disconnect()
                return None
            if length > MAX_MESSAGE_SIZE:
                self.logger.error(f"Message too large: {length} bytes")
                await self.disconnect()
                return None
                
            # Read message data with progress tracking
            try:
                data = bytearray()
                remaining = length
                while remaining > 0:
                    chunk = await asyncio.wait_for(
                        self.reader.read(min(remaining, CHUNK_SIZE)),
                        timeout=READ_TIMEOUT
                    )
                    if not chunk:
                        self.logger.error("Connection closed while reading message data")
                        await self.disconnect()
                        return None
                    data.extend(chunk)
                    remaining -= len(chunk)
                    self.logger.debug(f"Read {len(data)}/{length} bytes")
            except asyncio.TimeoutError:
                self.logger.error("Timeout reading message data")
                await self.disconnect()
                return None
                
            # Verify we got exactly the expected amount of data
            if len(data) != length:
                self.logger.error(f"Message size mismatch: expected {length}, got {len(data)}")
                await self.disconnect()
                return None
                
            try:
                return Message.deserialize(bytes(data))
            except MessageSizeError as e:
                self.logger.error(f"Message size error: {e}")
                await self.disconnect()
                return None
            except InvalidMessageError as e:
                self.logger.error(f"Invalid message: {e}")
                await self.disconnect()
                return None
            except Exception as e:
                self.logger.error(f"Error deserializing message: {e}")
                await self.disconnect()
                return None
                
        except Exception as e:
            self.logger.error(f"Error receiving message: {e}")
            await self.disconnect()
            return None

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
            self.reader = reader
            self.writer = writer
            self.is_connected = True
            
            # Start message handling loop
            while self.is_connected:
                try:
                    message = await self.receive_message()
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