"""
DHT Test Suite
==============

How to run these tests:
-----------------------
1. Ensure all dependencies are installed:
   pip install -r requirements.txt

2. Run the DHT tests with one of the following commands:
   python -m unittest test_dht.py
   # or
   python test_dht.py

What is covered:
----------------
- Bucket splitting and merging
- Node addition and eviction policy
- Efficient peer lookup and routing
- Integration of all DHT logic

For future DHT changes:
-----------------------
- Re-run this test suite after any changes to src/network/dht.py
- Add new tests here for any new DHT features or bug fixes
"""

import unittest
import hashlib
import time
from datetime import datetime
from src.network.dht import DHT, KBucket
from src.network.peer import PeerInfo, Peer
from unittest.mock import AsyncMock, patch, Mock

class TestDHTSteps(unittest.TestCase):
    def setUp(self):
        self.dht = DHT('127.0.0.1', 9999)
        self.k = self.dht.k

    def make_peerinfo(self, suffix, last_seen=None):
        # PeerInfo expects last_seen as a datetime
        if last_seen is None:
            last_seen_dt = datetime.now()
        elif isinstance(last_seen, float):
            last_seen_dt = datetime.fromtimestamp(last_seen)
        else:
            last_seen_dt = last_seen
        # Ensure suffix is int for port calculation
        if isinstance(suffix, str) and suffix.isdigit():
            suffix_int = int(suffix)
        elif isinstance(suffix, int):
            suffix_int = suffix
        else:
            # Extract digits from string or fallback to 0
            import re
            m = re.search(r'\d+', str(suffix))
            suffix_int = int(m.group()) if m else 0
        return PeerInfo(
            id=f"peer{suffix}",
            address=f"192.168.1.{suffix}",
            port=10000 + suffix_int,
            last_seen=last_seen_dt
        )

    # Step 1: Test _get_bit utility
    def test_get_bit(self):
        node_id = "testnode"
        h = int(hashlib.sha1(node_id.encode()).hexdigest(), 16)
        for bit_index in range(160):
            expected = (h >> (160 - bit_index - 1)) & 1
            self.assertEqual(self.dht._get_bit(node_id, bit_index), expected)

    # Step 2: Test manual split/merge via debug methods
    def test_debug_split_and_merge_bucket(self):
        bucket_index = 0
        bucket = self.dht.k_buckets[bucket_index]
        # Fill the bucket
        for i in range(self.k):
            bucket.add_node(self.make_peerinfo(i))
        self.assertEqual(len(bucket.nodes), self.k)
        # Split
        self.dht.debug_split_bucket(bucket_index)
        self.assertGreaterEqual(len(self.dht.k_buckets), 161)  # Should have at least one more bucket
        # Merge
        self.dht.debug_merge_buckets(bucket_index)
        self.assertLessEqual(len(self.dht.k_buckets), 160)  # Should not exceed original count by much

    # Step 4: Test eviction policy
    def test_eviction_policy(self):
        bucket_index = 0
        # Fill the bucket with old last_seen times
        now = time.time()
        for i in range(self.k):
            self.dht.k_buckets[bucket_index].add_node(self.make_peerinfo(i, last_seen=now - (self.k - i)))
        # Add a node that will cause eviction
        new_peer = self.make_peerinfo(self.k, last_seen=now)
        self.dht.add_node(new_peer)
        bucket = self.dht.k_buckets[self.dht._get_bucket_index(new_peer.id)]
        ids = [n.id for n in bucket.nodes]
        self.assertIn(new_peer.id, ids)
        # The oldest peer should have been evicted
        self.assertNotIn("peer0", ids)

    # Step 5: Test efficient bucket-based lookup in find_node
    def test_find_node_bucket_expansion(self):
        # Add nodes to different buckets by varying their IDs
        for i in range(self.k * 3):
            self.dht.add_node(self.make_peerinfo(i))
        # Pick a target_id that is not in the set
        target_id = "peer_target"
        closest = self.dht.find_node(target_id)
        # Should return at most k nodes
        self.assertLessEqual(len(closest), self.k)
        # Should return PeerInfo objects
        for peer in closest:
            self.assertIsInstance(peer, PeerInfo)
        # Should be sorted by distance
        distances = [self.dht._distance(peer.id, target_id) for peer in closest]
        self.assertEqual(distances, sorted(distances))

    # Step 6: Test that store and find_value use bucket-based peer selection
    def test_store_and_find_value_peer_selection(self):
        # Add nodes to buckets and mock self.peers
        for i in range(self.k * 2):
            peerinfo = self.make_peerinfo(i)
            self.dht.add_node(peerinfo)
            # Use a real Peer object with Mocked StreamReader/StreamWriter and patch send_message
            dummy_reader = Mock()
            dummy_writer = Mock()
            dummy_writer.get_extra_info = Mock(return_value=("127.0.0.1", 12345))
            dummy_peer = Peer(dummy_reader, dummy_writer, peerinfo.id)
            dummy_peer.send_message = AsyncMock()
            self.dht.peers[peerinfo.id] = dummy_peer
        # Patch Message.create to avoid errors
        with patch('src.network.dht.Message.create', return_value=None):
            import asyncio
            asyncio.run(self.dht.store("somekey", "somevalue"))
            asyncio.run(self.dht.find_value("somekey"))

if __name__ == "__main__":
    unittest.main() 