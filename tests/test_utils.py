import os
import tempfile

from src.utils import get_file_checksum


def test_get_file_checksum():
    """Test the get_file_checksum function."""
    # Create a temporary file with known content
    content = b"This is a test file for checksum."
    expected_checksum = "f3791d8512c2d327ccf182dc7ef31c48"

    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_file.write(content)
        tmp_file_path = tmp_file.name

    try:
        # Calculate checksum and assert
        actual_checksum = get_file_checksum(tmp_file_path)
        assert actual_checksum == expected_checksum
    finally:
        # Clean up the temporary file
        os.remove(tmp_file_path)
