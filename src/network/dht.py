import hashlib
import time
import asyncio
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import logging
from .protocol import Message, MessageType
from .peer import Peer, PeerInfo

@dataclass
class KBucket:
    nodes: List[PeerInfo]
    last_updated: datetime = field(default_factory=datetime.now)
    k: int = 20  # Maximum number of nodes per bucket

    def __post_init__(self):
        if not self.nodes:
            self.nodes = []

    def add_node(self, node: PeerInfo) -> bool:
        """Add a node to the bucket if there's space or if it's closer than existing nodes."""
        if len(self.nodes) < self.k:
            self.nodes.append(node)
            self.last_updated = datetime.now()
            return True
        return False

    def remove_node(self, node_id: str) -> bool:
        """Remove a node from the bucket."""
        for i, node in enumerate(self.nodes):
            if node.id == node_id:
                self.nodes.pop(i)
                return True
        return False

    def update_node(self, node: PeerInfo) -> bool:
        """Update an existing node's information."""
        for i, existing_node in enumerate(self.nodes):
            if existing_node.id == node.id:
                self.nodes[i] = node
                self.last_updated = datetime.now()
                return True
        return False

class DHT:
    def __init__(self, node_id: str, host: str, port: int):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.k_buckets: List[KBucket] = [KBucket([]) for _ in range(160)]  # 160-bit key space
        self.logger = logging.getLogger("DHT")
        self.alpha = 3  # Number of parallel requests
        self.peers: Dict[str, Peer] = {}

    def _get_bucket_index(self, node_id: str) -> int:
        """Get the index of the k-bucket for a given node ID."""
        # XOR the node IDs and find the first differing bit
        xor = int(self.node_id, 16) ^ int(node_id, 16)
        if xor == 0:
            return 0
        return 159 - (xor.bit_length() - 1)

    def _distance(self, id1: str, id2: str) -> int:
        """Calculate XOR distance between two node IDs."""
        return int(id1, 16) ^ int(id2, 16)

    def add_node(self, node: PeerInfo) -> bool:
        """Add a node to the appropriate k-bucket."""
        bucket_index = self._get_bucket_index(node.id)
        bucket = self.k_buckets[bucket_index]
        
        # Check if node already exists
        for existing_node in bucket.nodes:
            if existing_node.id == node.id:
                return bucket.update_node(node)
        
        # Try to add the node
        if bucket.add_node(node):
            return True
            
        # If bucket is full, we need to check if we should split it
        if len(bucket.nodes) == self.k:
            # TODO: Implement bucket splitting logic
            pass
            
        return False

    def remove_node(self, node_id: str) -> bool:
        """Remove a node from the DHT."""
        bucket_index = self._get_bucket_index(node_id)
        return self.k_buckets[bucket_index].remove_node(node_id)

    def find_node(self, target_id: str) -> List[PeerInfo]:
        """Find the k closest nodes to the target ID."""
        bucket_index = self._get_bucket_index(target_id)
        closest_nodes = []
        
        # Start with the target's bucket
        closest_nodes.extend(self.k_buckets[bucket_index].nodes)
        
        # If we don't have enough nodes, look in adjacent buckets
        left = bucket_index - 1
        right = bucket_index + 1
        
        while len(closest_nodes) < self.k and (left >= 0 or right < 160):
            if left >= 0:
                closest_nodes.extend(self.k_buckets[left].nodes)
            if right < 160:
                closest_nodes.extend(self.k_buckets[right].nodes)
            left -= 1
            right += 1
        
        # Sort by distance to target
        closest_nodes.sort(key=lambda x: self._distance(x.id, target_id))
        return closest_nodes[:self.k]

    async def store(self, key: str, value: str) -> bool:
        """Store a key-value pair in the DHT."""
        # Find the k closest nodes to the key
        target_nodes = self.find_node(key)
        
        # Send STORE requests to the closest nodes
        success = False
        for node in target_nodes:
            if node.id in self.peers:
                peer = self.peers[node.id]
                try:
                    store_msg = Message.create(
                        MessageType.STORE,
                        self.node_id,
                        {"key": key, "value": value}
                    )
                    await peer.send_message(store_msg)
                    success = True
                except Exception as e:
                    self.logger.error(f"Error storing value: {e}")
        
        return success

    async def find_value(self, key: str) -> Optional[str]:
        """Find a value in the DHT."""
        # Find the k closest nodes to the key
        target_nodes = self.find_node(key)
        
        # Send FIND_VALUE requests to the closest nodes
        for node in target_nodes:
            if node.id in self.peers:
                peer = self.peers[node.id]
                try:
                    find_msg = Message.create(
                        MessageType.FIND_VALUE,
                        self.node_id,
                        {"key": key}
                    )
                    await peer.send_message(find_msg)
                    # TODO: Handle response
                except Exception as e:
                    self.logger.error(f"Error finding value: {e}")
        
        return None

    async def join_network(self, bootstrap_nodes: List[Tuple[str, int]]):
        """Join the DHT network using bootstrap nodes."""
        for host, port in bootstrap_nodes:
            try:
                peer = Peer(host, port)
                if await peer.connect():
                    self.peers[peer.peer_id] = peer
                    # Send FIND_NODE request to bootstrap node
                    find_msg = Message.create(
                        MessageType.FIND_NODE,
                        self.node_id,
                        {"target": self.node_id}
                    )
                    await peer.send_message(find_msg)
            except Exception as e:
                self.logger.error(f"Error joining network: {e}")

    def cleanup(self):
        """Clean up old nodes and merge buckets if needed."""
        current_time = datetime.now()
        for bucket in self.k_buckets:
            # Remove nodes that haven't been seen in a while
            bucket.nodes = [
                node for node in bucket.nodes
                if (current_time - node.last_seen).total_seconds() < 3600  # 1 hour
            ]
            # TODO: Implement bucket merging logic

    def split_bucket(self, bucket_index: int) -> None:
        """Split a bucket when it becomes too full."""
        bucket = self.k_buckets[bucket_index]
        if len(bucket.nodes) < self.k:
            return

        # Create new bucket
        new_bucket = set()
        old_bucket = set()

        # Split based on the next bit in the ID
        split_bit = bucket_index + 1
        for peer in bucket.nodes:
            if self._get_bit(peer.id, split_bit):
                new_bucket.add(peer)
            else:
                old_bucket.add(peer)

        # Update buckets
        bucket.nodes = list(old_bucket)
        self.k_buckets.insert(bucket_index + 1, KBucket(list(new_bucket)))

        # Update routing table
        self._update_routing_table()
        self.logger.debug(f"Split bucket {bucket_index} into two buckets")

    def merge_buckets(self, bucket_index: int) -> None:
        """Merge a bucket with its neighbor if both are under-utilized."""
        if bucket_index >= len(self.k_buckets) - 1:
            return

        current_bucket = self.k_buckets[bucket_index]
        next_bucket = self.k_buckets[bucket_index + 1]

        # Check if both buckets are under-utilized
        if len(current_bucket.nodes) + len(next_bucket.nodes) <= self.k:
            # Merge buckets
            merged_nodes = current_bucket.nodes + next_bucket.nodes
            self.k_buckets[bucket_index] = KBucket(merged_nodes)
            self.k_buckets.pop(bucket_index + 1)

            # Update routing table
            self._update_routing_table()
            self.logger.debug(f"Merged buckets {bucket_index} and {bucket_index + 1}")

    async def handle_response(self, response: Message) -> None:
        """Handle a response message from a peer."""
        try:
            if response.type == MessageType.PONG:
                # Update peer's last seen time
                peer_id = response.sender
                if peer_id in self.peers:
                    self.peers[peer_id].last_seen = datetime.now()
                    self.logger.debug(f"Updated last seen time for peer {peer_id}")

            elif response.type == MessageType.PEER_LIST:
                # Add new peers from the response
                peers = response.data.get("peers", [])
                for peer_info in peers:
                    await self.add_peer(PeerInfo(**peer_info))
                self.logger.debug(f"Added {len(peers)} peers from response")

            elif response.type == MessageType.FILE_LIST:
                # Update file list from the response
                files = response.data.get("files", [])
                for file_info in files:
                    self.file_manager.add_remote_file(file_info)
                self.logger.debug(f"Updated file list with {len(files)} files")

            elif response.type == MessageType.FILE_RESPONSE:
                # Handle file data response
                file_id = response.data.get("file_id")
                chunk_index = response.data.get("chunk_index")
                chunk_data = response.data.get("chunk_data")
                if all([file_id, chunk_index is not None, chunk_data]):
                    await self.file_manager.handle_file_chunk(file_id, chunk_index, chunk_data)
                    self.logger.debug(f"Received chunk {chunk_index} for file {file_id}")

            else:
                self.logger.warning(f"Unhandled response type: {response.type}")

        except Exception as e:
            self.logger.error(f"Error handling response: {e}")
            raise DHTError(f"Failed to handle response: {e}")

    def _update_routing_table(self) -> None:
        """Update the routing table after bucket changes."""
        self.routing_table.clear()
        for bucket in self.k_buckets:
            for peer in bucket.nodes:
                self.routing_table[peer.id] = peer
        self.logger.debug("Updated routing table")

    def get_connected_peers(self):
        """Return a list of connected peers (stub for UI compatibility)."""
        return [] 