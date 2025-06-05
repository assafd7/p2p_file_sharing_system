#!/usr/bin/env python3
import pytest
import sys
import os

def main():
    """Run all tests in the project."""
    # Add the project root to Python path
    project_root = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, project_root)
    
    # Configure pytest arguments
    args = [
        "--verbose",  # Show verbose output
        "--cov=src",  # Measure code coverage for src directory
        "--cov-report=term-missing",  # Show lines missing coverage
        "--cov-report=html",  # Generate HTML coverage report
        "tests/",  # Run tests in tests directory
    ]
    
    # Run tests
    return pytest.main(args)

if __name__ == "__main__":
    sys.exit(main()) 