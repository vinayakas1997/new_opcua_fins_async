"""
FINS Domain Utilities
=====================
This module contains utility functions for FINS protocol operations.
"""

__version__ = "0.1.0"


def reverse_word_order(data: bytes) -> bytes:
    """
    Reverse word order in byte string.
    
    This function is helpful because FINS reads come in from low to high
    bytes, but since the data is represented as big-endian, the word order 
    of bytes needs to be reversed after reading and before writing data.
    
    Args:
        data: Input bytes to reverse word order
        
    Returns:
        Bytes with reversed word order
        
    Raises:
        ValueError: If data length is not even (incomplete words)
    """
    if len(data) % 2 != 0:
        raise ValueError("Data length must be even (complete words)")
    
    if len(data) == 0:
        return data
    
    reversed_bytes = data[::-1]
    reversed_words = b''
    
    for i in range(0, len(reversed_bytes), 2):
        reversed_words += reversed_bytes[i+1].to_bytes(1, 'big') + reversed_bytes[i].to_bytes(1, 'big')
    
    return reversed_words






