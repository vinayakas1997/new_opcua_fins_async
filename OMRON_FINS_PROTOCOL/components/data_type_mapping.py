"""
Data Type Mapping for FINS Protocol
===================================
This module contains data type mappings and conversion functions for FINS protocol communication.
All data types are configured to return raw bytes without any conversion.
"""

from typing import Callable, Dict, Tuple


def return_raw_bytes(data: bytes) -> bytes:
    """
    Return raw bytes data without any conversion.
    
    Args:
        data: Raw bytes from PLC
        
    Returns:
        Raw bytes data as-is
    """
    return data


# Simplified data type mapping - all return raw bytes
DATA_TYPE_MAPPING: Dict[str, Tuple[int, Callable[[bytes], bytes]]] = {
    'INT16': (1, return_raw_bytes),
    'UINT16': (1, return_raw_bytes),
    'INT32': (2, return_raw_bytes),
    'UINT32': (2, return_raw_bytes),
    'INT64': (4, return_raw_bytes),
    'UINT64': (4, return_raw_bytes),
    'FLOAT': (2, return_raw_bytes),
    'DOUBLE': (4, return_raw_bytes),
    'BCD2DEC': (1, return_raw_bytes),
    'BOOL': (1, return_raw_bytes),
    'CHANNEL': (1, return_raw_bytes),
    'WORD': (1, return_raw_bytes),
    'UDINT': (2, return_raw_bytes),
    'BIN': (1, return_raw_bytes),
    'BITS': (1, return_raw_bytes),
    'RAW': (1, return_raw_bytes),  # Added explicit RAW type
}


__all__ = [
    'return_raw_bytes',
    'DATA_TYPE_MAPPING',
]