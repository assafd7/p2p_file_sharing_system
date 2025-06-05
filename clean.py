#!/usr/bin/env python3
import os
import shutil
from pathlib import Path

def clean():
    """Clean up the project by removing generated files and directories."""
    # Directories to clean
    directories = [
        "data",
        "logs",
        "__pycache__",
        ".pytest_cache",
        ".coverage",
        "htmlcov"
    ]
    
    # Files to clean
    files = [
        "*.pyc",
        "*.pyo",
        "*.pyd",
        ".coverage",
        "coverage.xml"
    ]
    
    # Remove directories
    for directory in directories:
        if os.path.exists(directory):
            print(f"Removing directory: {directory}")
            shutil.rmtree(directory)
    
    # Remove files
    for pattern in files:
        for file in Path(".").glob(pattern):
            print(f"Removing file: {file}")
            file.unlink()
    
    print("Cleanup completed")

if __name__ == "__main__":
    clean() 