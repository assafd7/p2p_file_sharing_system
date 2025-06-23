# P2P File Sharing System

A secure and efficient peer-to-peer file sharing system with a modern user interface.

## Features

- **P2P Architecture**: Direct peer-to-peer file transfers without central servers
- **Distributed Hash Table (DHT)**: Fully implemented with k-buckets, bucket splitting, merging, and eviction. All peer discovery, routing, and storage use the DHT structure for efficient and scalable operation.
- **Secure Communication**: End-to-end encryption for all transfers
- **Large File Support**: Efficient handling of large files with chunked transfers
- **Resume Capability**: Resume interrupted transfers
- **User Authentication**: Secure user authentication and authorization
- **Modern UI**: Built with PyQt6 for a responsive and intuitive interface

## System Requirements

- Python 3.8 or higher
- Operating System: Windows, macOS, or Linux
- Network connection
- Sufficient storage space for shared files

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/assafd7/p2p_file_sharing_system.git
   cd p2p_file_sharing_system
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Running the System

### Single Computer Setup

To run the system on a single computer for testing:
```bash
python run.py
```

### Network Setup (Multiple Computers)

To run the system on multiple computers:

1. **First Computer (Bootstrap Node)**:
   ```bash
   python run_network.py --bootstrap
   ```
   This will:
   - Start the application as a bootstrap node
   - Display the local IP address
   - Configure the application to run without bootstrap nodes
   
   Note down the displayed IP address to share with other peers.

2. **Other Computers (Peers)**:
   ```bash
   python run_network.py --connect <bootstrap_ip>
   ```
   Replace `<bootstrap_ip>` with the IP address from the bootstrap node.
   This will:
   - Connect to the bootstrap node
   - Configure the application to use the bootstrap node
   - Start the application

### Command Line Options

The `run_network.py` script supports the following options:
- `--bootstrap`: Run as a bootstrap node
- `--connect IP`: Connect to a bootstrap node at the specified IP
- `--port PORT`: Specify a custom port (default: 8000)

## Using the Application

### Main Window

The main window consists of several tabs:

1. **Files Tab**:
   - View shared files
   - Add new files to share
   - Remove files from sharing
   - Search for files
   - Sort files by name, size, or date

2. **Transfers Tab**:
   - Monitor active transfers
   - View transfer progress
   - Pause/resume transfers
   - Cancel transfers
   - View transfer speeds

3. **Peers Tab**:
   - View connected peers
   - Monitor peer status
   - View peer information
   - Disconnect from peers

4. **Settings Tab**:
   - Configure network settings
   - Set storage location
   - Adjust bandwidth limits
   - Configure security options

### Sharing Files

1. **Add Files to Share**:
   - Click "Add File" button or use File > Add File
   - Select the file(s) to share
   - Files will be automatically shared with the network

2. **Remove Files from Sharing**:
   - Select the file(s) in the Files tab
   - Click "Remove" button or press Delete
   - Confirm the removal

### Downloading Files

1. **Browse Available Files**:
   - View the list of shared files in the Files tab
   - Use the search box to find specific files
   - Sort files by different criteria

2. **Download Files**:
   - Select the file(s) to download
   - Click "Download" button
   - Choose the save location
   - Monitor progress in the Transfers tab

### Managing Transfers

1. **Monitor Transfers**:
   - View active transfers in the Transfers tab
   - See progress bars and speed indicators
   - Check transfer status

2. **Control Transfers**:
   - Pause/resume transfers
   - Cancel transfers
   - Set bandwidth limits
   - Prioritize transfers

### Security Features

1. **File Encryption**:
   - All file transfers are encrypted
   - Files are encrypted at rest
   - Secure key exchange

2. **User Authentication**:
   - Secure password protection
   - User account management
   - Access control

## Troubleshooting

### Common Issues

1. **Connection Problems**:
   - Ensure both computers can reach each other
   - Check firewall settings
   - Verify the bootstrap node IP address
   - Try a different port if the default is blocked

2. **File Transfer Issues**:
   - Check available disk space
   - Verify file permissions
   - Ensure stable network connection
   - Check transfer logs

3. **Application Errors**:
   - Check the logs in the `logs` directory
   - Verify Python version and dependencies
   - Ensure all required directories exist

### Getting Help

For additional help:
1. Check the logs in the `logs` directory
2. Review the error messages in the application
3. Check the network status in the Peers tab
4. Verify the configuration in `src/config.py`

## Development

### Project Structure

```
p2p_file_sharing_system/
├── src/
│   ├── network/
│   │   ├── protocol.py      # P2P protocol implementation
│   │   ├── peer.py          # Peer connection management
│   │   ├── dht.py           # Distributed hash table
│   │   └── security.py      # Security features
│   ├── file_management/
│   │   └── file_manager.py  # File handling and transfers
│   ├── database/
│   │   └── db_manager.py    # Database operations
│   ├── ui/
│   │   ├── main_window.py   # Main application window
│   │   ├── file_list.py     # File list widget
│   │   └── transfer_list.py # Transfer list widget
│   └── utils/
│       └── logging_config.py # Logging configuration
├── tests/
│   ├── test_protocol.py     # Protocol tests
│   ├── test_peer.py         # Peer tests
│   ├── test_dht.py          # DHT tests
│   ├── test_file_manager.py # File manager tests
│   ├── test_db_manager.py   # Database tests
│   ├── test_security.py     # Security tests
│   └── test_ui.py           # UI tests
├── data/                    # Application data
├── logs/                    # Log files
├── requirements.txt         # Project dependencies
├── run.py                   # Application entry point
├── run_tests.py            # Test runner
└── clean.py                # Cleanup script
```

### Running Tests

1. Run all tests:
   ```bash
   python run_tests.py
   ```

2. Run specific test file:
   ```bash
   pytest tests/test_protocol.py
   ```

3. Run tests with coverage:
   ```bash
   pytest --cov=src tests/
   ```

### Code Style

- Follow PEP 8 guidelines
- Use type hints
- Write docstrings for all functions and classes
- Keep functions small and focused
- Use meaningful variable and function names

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- PyQt6 for the user interface
- cryptography for security features
- aiohttp for async networking
- aiosqlite for async database operations

## Support

For support, please open an issue in the GitHub repository.

## Roadmap

- [ ] Web interface
- [ ] Mobile app
- [ ] Cloud storage integration
- [ ] Advanced search features
- [ ] File versioning
- [ ] Collaborative editing
- [ ] Real-time chat
- [ ] Bandwidth control
- [ ] NAT traversal
- [ ] IPv6 support

## DHT Implementation

This project features a robust Distributed Hash Table (DHT) implementation:
- **K-Buckets**: Peers are organized into k-buckets for efficient lookup and routing.
- **Bucket Splitting & Merging**: Buckets split and merge dynamically as the network grows or shrinks.
- **Eviction Policy**: Least recently seen nodes are evicted when buckets are full and cannot be split.
- **Efficient Routing**: All peer discovery, file lookup, and storage operations use the DHT structure for scalability.

See the `src/network/dht.py` file for the DHT implementation details.

## DHT Testing

The DHT system is thoroughly tested with a dedicated test suite:
- **Test File**: `test_dht.py`
- **Coverage**: All DHT features, including bucket splitting, merging, eviction, and efficient routing, are tested.

### Running DHT Tests

To run the DHT tests:
```bash
python -m unittest test_dht.py
```
or
```bash
python test_dht.py
```

All future DHT changes should be accompanied by corresponding tests in `test_dht.py`.

### Project Structure

```
p2p_file_sharing_system/
├── src/
│   ├── network/
│   │   ├── protocol.py      # P2P protocol implementation
│   │   ├── peer.py          # Peer connection management
│   │   ├── dht.py           # Distributed hash table
│   │   └── security.py      # Security features
│   ├── file_management/
│   │   └── file_manager.py  # File handling and transfers
│   ├── database/
│   │   └── db_manager.py    # Database operations
│   ├── ui/
│   │   ├── main_window.py   # Main application window
│   │   ├── file_list.py     # File list widget
│   │   └── transfer_list.py # Transfer list widget
│   └── utils/
│       └── logging_config.py # Logging configuration
├── tests/
│   ├── test_protocol.py     # Protocol tests
│   ├── test_peer.py         # Peer tests
│   ├── test_dht.py          # DHT tests
│   ├── test_file_manager.py # File manager tests
│   ├── test_db_manager.py   # Database tests
│   ├── test_security.py     # Security tests
│   └── test_ui.py           # UI tests
├── data/                    # Application data
├── logs/                    # Log files
├── requirements.txt         # Project dependencies
├── run.py                   # Application entry point
├── run_tests.py            # Test runner
└── clean.py                # Cleanup script
```

### Running Tests

1. Run all tests:
   ```bash
   python run_tests.py
   ```

2. Run specific test file:
   ```bash
   pytest tests/test_protocol.py
   ```

3. Run tests with coverage:
   ```bash
   pytest --cov=src tests/
   ```

### Code Style

- Follow PEP 8 guidelines
- Use type hints
- Write docstrings for all functions and classes
- Keep functions small and focused
- Use meaningful variable and function names

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- PyQt6 for the user interface
- cryptography for security features
- aiohttp for async networking
- aiosqlite for async database operations

## Support

For support, please open an issue in the GitHub repository.

## Roadmap

- [ ] Web interface
- [ ] Mobile app
- [ ] Cloud storage integration
- [ ] Advanced search features
- [ ] File versioning
- [ ] Collaborative editing
- [ ] Real-time chat
- [ ] Bandwidth control
- [ ] NAT traversal
- [ ] IPv6 support 