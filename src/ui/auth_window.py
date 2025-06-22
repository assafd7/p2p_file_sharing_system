from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QMessageBox,
    QStackedWidget, QFormLayout
)
from PyQt6.QtCore import Qt, pyqtSignal, QThread
import uuid
import logging
import asyncio
from src.database.db_manager import DatabaseManager
from src.network.security import SecurityManager
from typing import Tuple

class AuthWorker(QThread):
    """Worker thread for authentication operations."""
    
    finished = pyqtSignal(bool, str, str)  # success, user_id, username
    error = pyqtSignal(str)  # error message
    
    def __init__(self, operation, *args, event_loop=None, **kwargs):
        super().__init__()
        self.operation = operation
        self.args = args
        self.kwargs = kwargs
        self.event_loop = event_loop
    
    def run(self):
        """Run the authentication operation."""
        try:
            if self.event_loop is None:
                raise RuntimeError("No event loop provided to AuthWorker")
            # Schedule the coroutine in the main event loop
            future = asyncio.run_coroutine_threadsafe(self.operation(*self.args, **self.kwargs), self.event_loop)
            result = future.result()  # This will block until the coroutine is done
            self.finished.emit(True, *result)
        except Exception as e:
            self.error.emit(str(e))

class AuthWindow(QMainWindow):
    """Authentication window for login and registration."""
    
    # Signal emitted when authentication is successful
    auth_successful = pyqtSignal(str, str)  # user_id, username
    
    def __init__(self, db_manager: DatabaseManager, security_manager: SecurityManager, event_loop=None):
        super().__init__()
        self.db_manager = db_manager
        self.security_manager = security_manager
        self.logger = logging.getLogger("AuthWindow")
        self.auth_worker = None  # Store reference to worker
        self.event_loop = event_loop
        
        self.setWindowTitle("P2P File Sharing - Authentication")
        self.setMinimumSize(400, 300)
        
        # Create central widget and layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)
        
        # Create stacked widget for login/register pages
        self.stacked_widget = QStackedWidget()
        layout.addWidget(self.stacked_widget)
        
        # Create login page
        self.login_page = QWidget()
        self.setup_login_page()
        self.stacked_widget.addWidget(self.login_page)
        
        # Create register page
        self.register_page = QWidget()
        self.setup_register_page()
        self.stacked_widget.addWidget(self.register_page)
        
        # Show login page by default
        self.stacked_widget.setCurrentWidget(self.login_page)
    
    def setup_login_page(self):
        """Set up the login page UI."""
        layout = QVBoxLayout(self.login_page)
        
        # Title
        title = QLabel("Login")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)
        
        # Form
        form_layout = QFormLayout()
        
        self.login_username = QLineEdit()
        self.login_password = QLineEdit()
        self.login_password.setEchoMode(QLineEdit.EchoMode.Password)
        
        form_layout.addRow("Username:", self.login_username)
        form_layout.addRow("Password:", self.login_password)
        
        layout.addLayout(form_layout)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        login_button = QPushButton("Login")
        login_button.clicked.connect(self.handle_login)
        
        register_button = QPushButton("Register")
        register_button.clicked.connect(lambda: self.stacked_widget.setCurrentWidget(self.register_page))
        
        button_layout.addWidget(login_button)
        button_layout.addWidget(register_button)
        
        layout.addLayout(button_layout)
    
    def setup_register_page(self):
        """Set up the registration page UI."""
        layout = QVBoxLayout(self.register_page)
        
        # Title
        title = QLabel("Register")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)
        
        # Form
        form_layout = QFormLayout()
        
        self.register_username = QLineEdit()
        self.register_password = QLineEdit()
        self.register_password.setEchoMode(QLineEdit.EchoMode.Password)
        self.register_confirm = QLineEdit()
        self.register_confirm.setEchoMode(QLineEdit.EchoMode.Password)
        
        form_layout.addRow("Username:", self.register_username)
        form_layout.addRow("Password:", self.register_password)
        form_layout.addRow("Confirm Password:", self.register_confirm)
        
        layout.addLayout(form_layout)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        register_button = QPushButton("Register")
        register_button.clicked.connect(self.handle_register)
        
        back_button = QPushButton("Back to Login")
        back_button.clicked.connect(lambda: self.stacked_widget.setCurrentWidget(self.login_page))
        
        button_layout.addWidget(register_button)
        button_layout.addWidget(back_button)
        
        layout.addLayout(button_layout)
    
    def handle_login(self):
        """Handle login button click."""
        username = self.login_username.text().strip()
        password = self.login_password.text()
        
        if not username or not password:
            QMessageBox.warning(self, "Error", "Please enter both username and password")
            return
        
        # Clean up previous worker if needed
        if self.auth_worker is not None:
            self.auth_worker.wait()
            self.auth_worker = None
        self.auth_worker = AuthWorker(self._login_async, username, password, event_loop=self.event_loop)
        self.auth_worker.finished.connect(self._on_auth_success)
        self.auth_worker.error.connect(self._on_auth_error)
        self.auth_worker.finished.connect(self._cleanup_worker)
        self.auth_worker.error.connect(self._cleanup_worker)
        self.auth_worker.start()
    
    def handle_register(self):
        """Handle register button click."""
        username = self.register_username.text().strip()
        password = self.register_password.text()
        confirm = self.register_confirm.text()
        
        if not username or not password or not confirm:
            QMessageBox.warning(self, "Error", "Please fill in all fields")
            return
        
        if password != confirm:
            QMessageBox.warning(self, "Error", "Passwords do not match")
            return
        
        # Clean up previous worker if needed
        if self.auth_worker is not None:
            self.auth_worker.wait()
            self.auth_worker = None
        self.auth_worker = AuthWorker(self._register_async, username, password, event_loop=self.event_loop)
        self.auth_worker.finished.connect(self._on_auth_success)
        self.auth_worker.error.connect(self._on_auth_error)
        self.auth_worker.finished.connect(self._cleanup_worker)
        self.auth_worker.error.connect(self._cleanup_worker)
        self.auth_worker.start()
    
    async def _login_async(self, username: str, password: str) -> Tuple[str, str]:
        """Async login operation."""
        # Get user from database
        user = await self.db_manager.get_user(username)
        if not user:
            raise ValueError("Invalid username or password")
        
        # Verify password
        if not self.security_manager.verify_password(password, user["password_hash"]):
            raise ValueError("Invalid username or password")
        
        # Update last login
        await self.db_manager.update_user_login(user["id"])
        
        return user["id"], username
    
    async def _register_async(self, username: str, password: str) -> Tuple[str, str]:
        """Async registration operation."""
        # Check if username exists
        existing_user = await self.db_manager.get_user(username)
        if existing_user:
            raise ValueError("Username already exists")
        
        # Generate user ID and hash password
        user_id = str(uuid.uuid4())
        password_hash = self.security_manager.hash_password(password)
        
        # Log values before calling add_user
        self.logger.debug(f"Registering user: user_id={user_id}, username={username}, password_hash={password_hash}")
        
        # Add user to database
        await self.db_manager.add_user(user_id, username, password_hash)
        
        return user_id, username
    
    def _on_auth_success(self, success: bool, user_id: str, username: str):
        """Handle successful authentication."""
        if success:
            self.auth_successful.emit(user_id, username)
            self.close()
    
    def _on_auth_error(self, error: str):
        """Handle authentication error."""
        QMessageBox.critical(self, "Error", str(error))
    
    def _cleanup_worker(self, *args):
        if self.auth_worker is not None:
            self.auth_worker.wait()
            self.auth_worker = None 