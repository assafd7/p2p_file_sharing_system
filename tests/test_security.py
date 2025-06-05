import pytest
from src.network.security import SecurityManager, SecurityError
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64
from typing import Optional

@pytest.fixture
def security_manager():
    """Create a security manager instance."""
    return SecurityManager()

def test_key_generation(security_manager: SecurityManager):
    """Test key generation."""
    # Test symmetric key generation
    key = security_manager.generate_symmetric_key()
    assert isinstance(key, bytes)
    assert len(key) == 32  # 256 bits
    
    # Test asymmetric key pair generation
    private_key, public_key = security_manager.generate_key_pair()
    assert isinstance(private_key, rsa.RSAPrivateKey)
    assert isinstance(public_key, rsa.RSAPublicKey)

def test_encryption_decryption(security_manager: SecurityManager):
    """Test encryption and decryption."""
    # Test symmetric encryption
    key = security_manager.generate_symmetric_key()
    message = b"test message"
    
    encrypted = security_manager.encrypt_symmetric(message, key)
    assert isinstance(encrypted, bytes)
    assert encrypted != message
    
    decrypted = security_manager.decrypt_symmetric(encrypted, key)
    assert decrypted == message
    
    # Test asymmetric encryption
    private_key, public_key = security_manager.generate_key_pair()
    
    encrypted = security_manager.encrypt_asymmetric(message, public_key)
    assert isinstance(encrypted, bytes)
    assert encrypted != message
    
    decrypted = security_manager.decrypt_asymmetric(encrypted, private_key)
    assert decrypted == message

def test_hash_functions(security_manager: SecurityManager):
    """Test hash functions."""
    message = b"test message"
    
    # Test SHA-256
    hash_value = security_manager.hash_sha256(message)
    assert isinstance(hash_value, bytes)
    assert len(hash_value) == 32  # 256 bits
    
    # Test SHA-512
    hash_value = security_manager.hash_sha512(message)
    assert isinstance(hash_value, bytes)
    assert len(hash_value) == 64  # 512 bits

def test_key_exchange(security_manager: SecurityManager):
    """Test key exchange."""
    # Generate key pairs for two parties
    private_key1, public_key1 = security_manager.generate_key_pair()
    private_key2, public_key2 = security_manager.generate_key_pair()
    
    # Generate shared secret
    shared_secret1 = security_manager.generate_shared_secret(private_key1, public_key2)
    shared_secret2 = security_manager.generate_shared_secret(private_key2, public_key1)
    
    # Verify both parties generate the same shared secret
    assert shared_secret1 == shared_secret2

def test_digital_signatures(security_manager: SecurityManager):
    """Test digital signatures."""
    # Generate key pair
    private_key, public_key = security_manager.generate_key_pair()
    message = b"test message"
    
    # Sign message
    signature = security_manager.sign_message(message, private_key)
    assert isinstance(signature, bytes)
    
    # Verify signature
    assert security_manager.verify_signature(message, signature, public_key)
    
    # Test invalid signature
    tampered_message = b"tampered message"
    assert not security_manager.verify_signature(tampered_message, signature, public_key)

def test_password_hashing(security_manager: SecurityManager):
    """Test password hashing."""
    password = "test_password"
    
    # Hash password
    hashed_password = security_manager.hash_password(password)
    assert isinstance(hashed_password, str)
    assert hashed_password != password
    
    # Verify password
    assert security_manager.verify_password(password, hashed_password)
    assert not security_manager.verify_password("wrong_password", hashed_password)

def test_key_derivation(security_manager: SecurityManager):
    """Test key derivation."""
    password = b"test_password"
    salt = b"test_salt"
    
    # Derive key
    key = security_manager.derive_key(password, salt)
    assert isinstance(key, bytes)
    assert len(key) == 32  # 256 bits
    
    # Verify same input produces same key
    key2 = security_manager.derive_key(password, salt)
    assert key == key2
    
    # Verify different salt produces different key
    key3 = security_manager.derive_key(password, b"different_salt")
    assert key != key3

def test_error_handling(security_manager: SecurityManager):
    """Test error handling."""
    # Test invalid key
    with pytest.raises(SecurityError):
        security_manager.encrypt_symmetric(b"test", b"invalid_key")
    
    # Test invalid message
    with pytest.raises(SecurityError):
        # Create a message that will fail type checking
        invalid_message: Optional[bytes] = None
        security_manager.encrypt_symmetric(invalid_message, security_manager.generate_symmetric_key())
    
    # Test invalid signature
    private_key, public_key = security_manager.generate_key_pair()
    with pytest.raises(SecurityError):
        security_manager.verify_signature(b"test", b"invalid_signature", public_key)

def test_key_serialization(security_manager: SecurityManager):
    """Test key serialization."""
    # Test symmetric key
    key = security_manager.generate_symmetric_key()
    serialized = security_manager.serialize_key(key)
    assert isinstance(serialized, str)
    
    deserialized = security_manager.deserialize_key(serialized)
    assert deserialized == key
    
    # Test public key
    _, public_key = security_manager.generate_key_pair()
    serialized = security_manager.serialize_public_key(public_key)
    assert isinstance(serialized, str)
    
    deserialized = security_manager.deserialize_public_key(serialized)
    assert isinstance(deserialized, rsa.RSAPublicKey)

def test_secure_random(security_manager: SecurityManager):
    """Test secure random number generation."""
    # Test random bytes
    random_bytes = security_manager.generate_random_bytes(32)
    assert isinstance(random_bytes, bytes)
    assert len(random_bytes) == 32
    
    # Test random integer
    random_int = security_manager.generate_random_int(0, 100)
    assert isinstance(random_int, int)
    assert 0 <= random_int <= 100

def test_certificate_operations(security_manager: SecurityManager):
    """Test certificate operations."""
    # Generate key pair
    private_key, public_key = security_manager.generate_key_pair()
    
    # Create self-signed certificate
    certificate = security_manager.create_self_signed_certificate(
        private_key,
        public_key,
        "test_certificate"
    )
    assert isinstance(certificate, bytes)
    
    # Verify certificate
    assert security_manager.verify_certificate(certificate, public_key)
    
    # Test invalid certificate
    with pytest.raises(SecurityError):
        security_manager.verify_certificate(b"invalid_certificate", public_key) 