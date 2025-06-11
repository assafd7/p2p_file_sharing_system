import os
import hashlib
import base64
from typing import Tuple, Optional, Dict
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import jwt
from datetime import datetime, timedelta
import logging
import json

class SecurityManager:
    def __init__(self, key_size: int = 2048):
        self.key_size = key_size
        self.logger = logging.getLogger("SecurityManager")
        self._private_key: Optional[rsa.RSAPrivateKey] = None
        self._public_key: Optional[rsa.RSAPublicKey] = None
        self._session_keys: Dict[str, bytes] = {}

    def generate_node_id(self) -> str:
        """Generate a unique node ID."""
        return hashlib.sha1(os.urandom(20)).hexdigest()

    def generate_key_pair(self) -> Tuple[bytes, bytes]:
        """Generate RSA key pair."""
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=self.key_size,
            backend=default_backend()
        )
        public_key = private_key.public_key()

        # Serialize keys
        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        public_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )

        self._private_key = private_key
        self._public_key = public_key

        return private_pem, public_pem

    def load_key_pair(self, private_pem: bytes, public_pem: bytes):
        """Load existing RSA key pair."""
        self._private_key = serialization.load_pem_private_key(
            private_pem,
            password=None,
            backend=default_backend()
        )
        self._public_key = serialization.load_pem_public_key(
            public_pem,
            backend=default_backend()
        )

    def generate_session_key(self) -> bytes:
        """Generate a random AES-256 session key."""
        return os.urandom(32)  # 256 bits

    def encrypt_session_key(self, session_key: bytes, public_key: bytes) -> bytes:
        """Encrypt a session key with RSA public key."""
        public_key = serialization.load_pem_public_key(
            public_key,
            backend=default_backend()
        )
        return public_key.encrypt(
            session_key,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )

    def decrypt_session_key(self, encrypted_key: bytes) -> bytes:
        """Decrypt a session key with RSA private key."""
        if not self._private_key:
            raise ValueError("Private key not loaded")
        return self._private_key.decrypt(
            encrypted_key,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )

    def encrypt_data(self, data: bytes, session_key: bytes) -> Tuple[bytes, bytes, bytes]:
        """Encrypt data with AES-256-GCM."""
        # Generate random IV
        iv = os.urandom(12)
        
        # Create cipher
        cipher = Cipher(
            algorithms.AES(session_key),
            modes.GCM(iv),
            backend=default_backend()
        )
        encryptor = cipher.encryptor()
        
        # Add associated data (optional)
        # encryptor.authenticate_additional_data(associated_data)
        
        # Encrypt data
        ciphertext = encryptor.update(data) + encryptor.finalize()
        
        # Get authentication tag
        tag = encryptor.tag
        
        return iv, ciphertext, tag

    def decrypt_data(self, iv: bytes, ciphertext: bytes, tag: bytes,
                    session_key: bytes) -> bytes:
        """Decrypt data with AES-256-GCM."""
        # Create cipher
        cipher = Cipher(
            algorithms.AES(session_key),
            modes.GCM(iv, tag),
            backend=default_backend()
        )
        decryptor = cipher.decryptor()
        
        # Add associated data (optional)
        # decryptor.authenticate_additional_data(associated_data)
        
        # Decrypt data
        return decryptor.update(ciphertext) + decryptor.finalize()

    def hash_password(self, password: str, salt: Optional[bytes] = None) -> str:
        """Hash a password with PBKDF2.
        
        Returns:
            A string containing the base64-encoded key and salt, separated by a colon.
        """
        if salt is None:
            salt = os.urandom(16)
        
        key = hashlib.pbkdf2_hmac(
            'sha256',
            password.encode(),
            salt,
            100000,  # Number of iterations
            dklen=32  # Length of the derived key
        )
        
        # Encode key and salt in base64 and join with a colon
        key_b64 = base64.b64encode(key).decode('utf-8')
        salt_b64 = base64.b64encode(salt).decode('utf-8')
        return f"{key_b64}:{salt_b64}"

    def verify_password(self, password: str, stored_hash: str) -> bool:
        """Verify a password against stored hash.
        
        Args:
            password: The password to verify
            stored_hash: The stored hash string in format "key:salt"
        """
        try:
            key_b64, salt_b64 = stored_hash.split(':')
            key = base64.b64decode(key_b64)
            salt = base64.b64decode(salt_b64)
            
            # Hash the password with the same salt
            new_key = hashlib.pbkdf2_hmac(
                'sha256',
                password.encode(),
                salt,
                100000,
                dklen=32
            )
            
            return key == new_key
        except Exception as e:
            self.logger.error(f"Password verification failed: {e}")
            return False

    def generate_token(self, user_id: str, secret_key: str,
                      expires_delta: timedelta = timedelta(hours=1)) -> str:
        """Generate a JWT token."""
        expire = datetime.utcnow() + expires_delta
        to_encode = {
            "sub": user_id,
            "exp": expire
        }
        return jwt.encode(to_encode, secret_key, algorithm="HS256")

    def verify_token(self, token: str, secret_key: str) -> Optional[str]:
        """Verify a JWT token and return the user ID."""
        try:
            payload = jwt.decode(token, secret_key, algorithms=["HS256"])
            return payload["sub"]
        except jwt.PyJWTError as e:
            self.logger.error(f"Token verification failed: {e}")
            return None

    def calculate_file_hash(self, file_path: str) -> str:
        """Calculate SHA-256 hash of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def verify_file_integrity(self, file_path: str, expected_hash: str) -> bool:
        """Verify file integrity by comparing hashes."""
        actual_hash = self.calculate_file_hash(file_path)
        return actual_hash == expected_hash

    def store_session_key(self, peer_id: str, session_key: bytes):
        """Store a session key for a peer."""
        self._session_keys[peer_id] = session_key

    def get_session_key(self, peer_id: str) -> Optional[bytes]:
        """Get a stored session key for a peer."""
        return self._session_keys.get(peer_id)

    def remove_session_key(self, peer_id: str):
        """Remove a stored session key for a peer."""
        self._session_keys.pop(peer_id, None)

    def generate_certificate(self, user_id: str, public_key: bytes,
                           issuer: str = "self") -> str:
        """Generate a self-signed certificate."""
        if not self._private_key:
            raise ValueError("Private key not loaded")

        cert_data = {
            "issuer": issuer,
            "subject": user_id,
            "public_key": base64.b64encode(public_key).decode(),
            "issued_at": datetime.utcnow().isoformat(),
            "expires_at": (datetime.utcnow() + timedelta(days=365)).isoformat()
        }

        # Sign the certificate
        signature = self._private_key.sign(
            json.dumps(cert_data).encode(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )

        cert_data["signature"] = base64.b64encode(signature).decode()
        return json.dumps(cert_data)

    def verify_certificate(self, certificate: str) -> bool:
        """Verify a certificate's signature."""
        try:
            cert_data = json.loads(certificate)
            signature = base64.b64decode(cert_data["signature"])
            
            # Remove signature before verification
            cert_data_copy = cert_data.copy()
            cert_data_copy.pop("signature")
            
            # Verify signature
            self._public_key.verify(
                signature,
                json.dumps(cert_data_copy).encode(),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except Exception as e:
            self.logger.error(f"Certificate verification failed: {e}")
            return False 