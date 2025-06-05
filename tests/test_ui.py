import pytest
from PyQt6.QtWidgets import QApplication
from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtTest import QTest
from src.ui.main_window import MainWindow
from src.ui.file_list import FileList
from src.ui.transfer_list import TransferList
from src.ui.settings_dialog import SettingsDialog
import sys

@pytest.fixture
def app():
    """Create a QApplication instance."""
    app = QApplication(sys.argv)
    yield app
    app.quit()

@pytest.fixture
def main_window(app):
    """Create a MainWindow instance."""
    window = MainWindow()
    window.show()
    return window

def test_main_window_initialization(main_window: MainWindow):
    """Test main window initialization."""
    assert main_window.windowTitle() == "P2P File Sharing"
    assert main_window.isVisible()
    
    # Check if all widgets are created
    assert main_window.file_list is not None
    assert main_window.transfer_list is not None
    assert main_window.status_bar is not None

def test_file_list_operations(main_window: MainWindow):
    """Test file list operations."""
    file_list = main_window.file_list
    
    # Add file
    file_list.add_file("test_file.txt", 1024, "available")
    assert file_list.rowCount() == 1
    
    # Update file
    file_list.update_file_status("test_file.txt", "transferring")
    item = file_list.findItems("test_file.txt", Qt.MatchFlag.MatchExactly)[0]
    assert item.status == "transferring"
    
    # Remove file
    file_list.remove_file("test_file.txt")
    assert file_list.rowCount() == 0

def test_transfer_list_operations(main_window: MainWindow):
    """Test transfer list operations."""
    transfer_list = main_window.transfer_list
    
    # Add transfer
    transfer_list.add_transfer("test_transfer", "test_file.txt", "download", 0.5)
    assert transfer_list.rowCount() == 1
    
    # Update transfer
    transfer_list.update_transfer_progress("test_transfer", 0.75)
    item = transfer_list.findItems("test_transfer", Qt.MatchFlag.MatchExactly)[0]
    assert item.progress == 0.75
    
    # Complete transfer
    transfer_list.complete_transfer("test_transfer")
    item = transfer_list.findItems("test_transfer", Qt.MatchFlag.MatchExactly)[0]
    assert item.status == "completed"
    
    # Remove transfer
    transfer_list.remove_transfer("test_transfer")
    assert transfer_list.rowCount() == 0

def test_settings_dialog(main_window: MainWindow):
    """Test settings dialog."""
    # Open settings dialog
    main_window.show_settings()
    settings_dialog = main_window.findChild(SettingsDialog)
    assert settings_dialog is not None
    assert settings_dialog.isVisible()
    
    # Test settings changes
    settings_dialog.set_chunk_size(2048)
    assert settings_dialog.get_chunk_size() == 2048
    
    settings_dialog.set_max_peers(10)
    assert settings_dialog.get_max_peers() == 10
    
    # Close dialog
    settings_dialog.close()
    assert not settings_dialog.isVisible()

def test_menu_actions(main_window: MainWindow):
    """Test menu actions."""
    # Test file menu
    main_window.menu_file.triggered.emit(main_window.action_add_file)
    # Note: We can't easily test file dialog in unit tests
    
    # Test settings menu
    main_window.menu_settings.triggered.emit(main_window.action_settings)
    settings_dialog = main_window.findChild(SettingsDialog)
    assert settings_dialog is not None
    
    # Test help menu
    main_window.menu_help.triggered.emit(main_window.action_about)
    # Note: We can't easily test about dialog in unit tests

def test_status_bar(main_window: MainWindow):
    """Test status bar updates."""
    # Test status message
    main_window.status_bar.showMessage("Test message")
    assert main_window.status_bar.currentMessage() == "Test message"
    
    # Test progress bar
    main_window.status_bar.showProgress(50)
    assert main_window.status_bar.progress_bar.value() == 50
    
    # Test clear
    main_window.status_bar.clear()
    assert main_window.status_bar.currentMessage() == ""

def test_drag_and_drop(main_window: MainWindow):
    """Test drag and drop functionality."""
    file_list = main_window.file_list
    
    # Simulate drag and drop
    mime_data = file_list.mimeData([file_list.item(0)])
    drop_event = QDropEvent(
        QPoint(0, 0),
        Qt.DropAction.CopyAction,
        mime_data,
        Qt.MouseButton.LeftButton,
        Qt.KeyboardModifier.NoModifier
    )
    file_list.dropEvent(drop_event)
    
    # Note: We can't easily test actual file drops in unit tests

def test_keyboard_shortcuts(main_window: MainWindow):
    """Test keyboard shortcuts."""
    # Test Ctrl+O (Open file)
    QTest.keySequence(main_window, Qt.Modifier.CTRL | Qt.Key.Key_O)
    # Note: We can't easily test file dialog in unit tests
    
    # Test Ctrl+S (Settings)
    QTest.keySequence(main_window, Qt.Modifier.CTRL | Qt.Key.Key_S)
    settings_dialog = main_window.findChild(SettingsDialog)
    assert settings_dialog is not None

def test_context_menu(main_window: MainWindow):
    """Test context menu."""
    file_list = main_window.file_list
    
    # Add a file
    file_list.add_file("test_file.txt", 1024, "available")
    
    # Show context menu
    item = file_list.item(0)
    file_list.show_context_menu(item, QPoint(0, 0))
    
    # Note: We can't easily test actual menu actions in unit tests

def test_theme_switching(main_window: MainWindow):
    """Test theme switching."""
    # Switch to dark theme
    main_window.set_theme("dark")
    assert main_window.styleSheet().find("dark") != -1
    
    # Switch to light theme
    main_window.set_theme("light")
    assert main_window.styleSheet().find("light") != -1

def test_window_resize(main_window: MainWindow):
    """Test window resize handling."""
    # Resize window
    main_window.resize(800, 600)
    assert main_window.width() == 800
    assert main_window.height() == 600
    
    # Test minimum size
    main_window.resize(100, 100)
    assert main_window.width() >= main_window.minimumWidth()
    assert main_window.height() >= main_window.minimumHeight()

def test_error_handling(main_window: MainWindow):
    """Test error handling in UI."""
    # Test error message display
    main_window.show_error("Test error")
    assert main_window.status_bar.currentMessage() == "Error: Test error"
    
    # Test warning message display
    main_window.show_warning("Test warning")
    assert main_window.status_bar.currentMessage() == "Warning: Test warning"
    
    # Test info message display
    main_window.show_info("Test info")
    assert main_window.status_bar.currentMessage() == "Info: Test info" 