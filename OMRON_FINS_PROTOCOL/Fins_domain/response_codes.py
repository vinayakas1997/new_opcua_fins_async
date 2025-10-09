"""
FINS Response End Codes
=======================
This module defines FINS protocol response end codes and their meanings.
"""

__version__ = "0.1.0"


class FinsResponseEndCode:
    """
    FINS protocol response end codes.
    
    These codes indicate the result of a FINS command execution.
    """
    
    def __init__(self):
        # Success codes
        self.NORMAL_COMPLETION = b'\x00\x00'
        self.SERVICE_CANCELLED = b'\x00\x01'
        
        # Add more response codes as needed
        self.LOCAL_NODE_ERROR = b'\x01\x01'
        self.DESTINATION_NODE_ERROR = b'\x01\x02'
        self.COMMUNICATIONS_CONTROLLER_ERROR = b'\x01\x03'
        self.SERVICE_UNSUPPORTED = b'\x01\x04'
        self.ROUTING_TABLE_ERROR = b'\x01\x05'
        self.COMMAND_FORMAT_ERROR = b'\x01\x06'
        self.PARAMETER_ERROR = b'\x01\x07'
        self.READ_NOT_POSSIBLE = b'\x01\x08'
        self.WRITE_NOT_POSSIBLE = b'\x01\x09'
        self.NOT_EXECUTABLE_IN_CURRENT_MODE = b'\x01\x0A'
        self.NO_SUCH_DEVICE = b'\x01\x0B'
        self.CANNOT_START_STOP = b'\x01\x0C'
        self.UNIT_ERROR = b'\x01\x0D'
        self.COMMAND_ERROR = b'\x01\x0E'
        self.ACCESS_RIGHT_ERROR = b'\x01\x0F'
    
    def get_error_description(self, end_code: bytes) -> str:
        """
        Get human-readable description of the error code.
        
        Args:
            end_code: The response end code bytes
            
        Returns:
            String description of the error
        """
        error_descriptions = {
            self.NORMAL_COMPLETION: "Normal completion",
            self.SERVICE_CANCELLED: "Service cancelled",
            self.LOCAL_NODE_ERROR: "Local node error",
            self.DESTINATION_NODE_ERROR: "Destination node error",
            self.COMMUNICATIONS_CONTROLLER_ERROR: "Communications controller error",
            self.SERVICE_UNSUPPORTED: "Service unsupported",
            self.ROUTING_TABLE_ERROR: "Routing table error",
            self.COMMAND_FORMAT_ERROR: "Command format error",
            self.PARAMETER_ERROR: "Parameter error",
            self.READ_NOT_POSSIBLE: "Read not possible",
            self.WRITE_NOT_POSSIBLE: "Write not possible",
            self.NOT_EXECUTABLE_IN_CURRENT_MODE: "Not executable in current mode",
            self.NO_SUCH_DEVICE: "No such device",
            self.CANNOT_START_STOP: "Cannot start/stop",
            self.UNIT_ERROR: "Unit error",
            self.COMMAND_ERROR: "Command error",
            self.ACCESS_RIGHT_ERROR: "Access right error"
        }
        
        return error_descriptions.get(end_code, f"Unknown error code: {end_code.hex()}")
    
    def is_success(self, end_code: bytes) -> bool:
        """
        Check if the response indicates success.
        
        Args:
            end_code: The response end code bytes
            
        Returns:
            True if successful, False otherwise
        """
        return end_code == self.NORMAL_COMPLETION
