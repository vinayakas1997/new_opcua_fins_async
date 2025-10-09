"""
Custom exceptions for OMRON FINS Protocol
"""

from .exception_rules import ( # type: ignore
    FinsBaseException,
    FinsConnectionError,
    FinsTimeoutError,
    FinsAddressError,
    FinsCommandError,
    FinsDataError,
    FinsProtocolError,
    FinsMemoryAreaError,
    FinsPermissionError,
    FinsNetworkError,
    validate_address,
    validate_connection_params,
    validate_read_size
)

__version__ = "0.1.0"

__all__ = [
    'FinsBaseException',
    'FinsConnectionError', 
    'FinsTimeoutError',
    'FinsAddressError',
    'FinsCommandError',
    'FinsDataError',
    'FinsProtocolError',
    'FinsMemoryAreaError',
    'FinsPermissionError',
    'FinsNetworkError',
    'validate_address',
    'validate_connection_params',
    'validate_read_size'
]
