"""
Custom FINS Protocol Exceptions
==============================
This module defines custom exceptions for the OMRON FINS protocol library.
"""

from typing import Optional, Union


class FinsBaseException(Exception):
    """Base exception class for all FINS protocol related errors."""
    
    def __init__(self, message: str, error_code: Optional[str] = None):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)
    
    def __str__(self) -> str:
        if self.error_code:
            return f"[{self.error_code}] {self.message}"
        return self.message


class FinsConnectionError(FinsBaseException):
    """Raised when connection to PLC fails or is lost."""
    pass


class FinsTimeoutError(FinsBaseException):
    """Raised when communication timeout occurs."""
    pass


class FinsAddressError(FinsBaseException):
    """Raised when memory address parsing or validation fails."""
    pass


class FinsCommandError(FinsBaseException):
    """Raised when FINS command execution fails."""
    pass


class FinsDataError(FinsBaseException):
    """Raised when data conversion or validation fails."""
    pass


class FinsProtocolError(FinsBaseException):
    """Raised when protocol-level errors occur."""
    pass


class FinsMemoryAreaError(FinsAddressError):
    """Raised when invalid memory area is accessed."""
    pass


class FinsPermissionError(FinsBaseException):
    """Raised when operation is not permitted (PLC protection, mode restrictions)."""
    pass


class FinsNetworkError(FinsConnectionError):
    """Raised when network-level communication errors occur."""
    pass


def validate_address(address: str) -> None:
    """
    Validate memory address format.
    
    Args:
        address: Memory address string to validate
        
    Raises:
        FinsAddressError: If address format is invalid
    """
    if not address:
        raise FinsAddressError("Address cannot be empty")
    
    if not isinstance(address, str):
        raise FinsAddressError(f"Address must be string, got {type(address).__name__}")


def validate_connection_params(host: str, port: int) -> None:
    """
    Validate connection parameters.
    
    Args:
        host: PLC IP address or hostname
        port: UDP port number
        
    Raises:
        FinsConnectionError: If parameters are invalid
    """
    if not host:
        raise FinsConnectionError("Host cannot be empty")
    
    if not isinstance(port, int) or not (1 <= port <= 65535):
        raise FinsConnectionError(f"Port must be integer between 1-65535, got {port}")


def validate_read_size(size: int) -> None:
    """
    Validate read size parameter.
    
    Args:
        size: Number of items to read
        
    Raises:
        FinsDataError: If size is invalid
    """
    if not isinstance(size, int) or size <= 0:
        raise FinsDataError(f"Read size must be positive integer, got {size}")
    
    if size > 65535:
        raise FinsDataError(f"Read size too large: {size} (max 65535)")
