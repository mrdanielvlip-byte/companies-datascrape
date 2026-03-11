"""
Pytest configuration and shared fixtures.

For unit tests that don't need a database, these fixtures provide
mock data structures. Integration tests should use a test database.
"""
import sys
import os
import pytest

# Add app to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
