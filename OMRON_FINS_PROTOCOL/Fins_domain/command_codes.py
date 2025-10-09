"""
FINS Command Codes
==================
This module defines all FINS protocol command codes used for communication with PLCs.
"""

__version__ = "0.1.0"


class FinsCommandCode:
    """
    Hex code for FINS command codes.
    
    Each FINS command has a corresponding hex code. This class provides name-based
    access to them for better code readability and maintainability.
    """
    
    def __init__(self):
        # Memory Area Commands
        self.MEMORY_AREA_READ = b'\x01\x01'
        self.MEMORY_AREA_WRITE = b'\x01\x02'
        self.MEMORY_AREA_FILL = b'\x01\x03'
        self.MULTIPLE_MEMORY_AREA_READ = b'\x01\x04'
        self.MEMORY_AREA_TRANSFER = b'\x01\x05'
        
        # Parameter Area Commands
        self.PARAMETER_AREA_READ = b'\x02\x01'
        self.PARAMETER_AREA_WRITE = b'\x02\x02'
        self.PARAMETER_AREA_FILL = b'\x02\x03'
        
        # Program Area Commands
        self.PROGRAM_AREA_READ = b'\x03\x06'
        self.PROGRAM_AREA_WRITE = b'\x03\x07'
        self.PROGRAM_AREA_CLEAR = b'\x03\x08'
        
        # PLC Control Commands
        self.RUN = b'\x04\x01'
        self.STOP = b'\x04\x02'
        
        # CPU Unit Commands
        self.CPU_UNIT_DATA_READ = b'\x05\x01'
        self.CONNECTION_DATA_READ = b'\x05\x02'
        self.CPU_UNIT_STATUS_READ = b'\x06\x01'
        self.CYCLE_TIME_READ = b'\x06\x20'
        
        # Clock Commands
        self.CLOCK_READ = b'\x07\x01'
        self.CLOCK_WRITE = b'\x07\x02'
        
        # Message Commands
        self.MESSAGE_READ = b'\x09\x20'
        
        # Access Right Commands
        self.ACCESS_RIGHT_ACQUIRE = b'\x0C\x01'
        self.ACCESS_RIGHT_FORCED_ACQUIRE = b'\x0C\x02'
        self.ACCESS_RIGHT_RELEASE = b'\x0C\x03'
        
        # Error Commands
        self.ERROR_CLEAR = b'\x21\x01'
        self.ERROR_LOG_READ = b'\x21\x02'
        self.ERROR_LOG_CLEAR = b'\x21\x03'
        self.FINS_WRITE_ACCESS_LOG_READ = b'\x21\x40'
        self.FINS_WRITE_ACCESS_LOG_CLEAR = b'\x21\x41'
        
        # File Commands
        self.FILE_NAME_READ = b'\x22\x01'
        self.SINGLE_FILE_READ = b'\x22\x02'
        self.SINGLE_FILE_WRITE = b'\x22\x03'
        self.FILE_MEMORY_FORMAT = b'\x22\x04'
        self.FILE_DELETE = b'\x22\x05'
        self.FILE_COPY = b'\x22\x07'
        self.FILE_NAME_CHANGE = b'\x22\x08'
        self.MEMORY_AREA_FILE_TRANSFER = b'\x22\x0A'
        self.PARAMETER_AREA_FILE_TRANSFER = b'\x22\x0B'
        self.PROGRAM_AREA_FILE_TRANSFER = b'\x22\x0C'
        self.DIRECTORY_CREATE_DELETE = b'\x22\x15'
        self.MEMORY_CASSETTE_TRANSFER = b'\x22\x20'
        
        # Forced Set/Reset Commands
        self.FORCED_SET_RESET = b'\x23\x01'
        self.FORCED_SET_RESET_CANCEL = b'\x23\x02'
        
        # Protocol Conversion Commands
        self.CONVERT_TO_COMPOWAY_F_COMMAND = b'\x28\x03'
        self.CONVERT_TO_MODBUS_RTU_COMMAND = b'\x28\x04'
        self.CONVERT_TO_MODBUS_ASCII_COMMAND = b'\x28\x05'
    
    def get_command_name(self, command_code: bytes) -> str:
        """
        Get the human-readable name for a command code.
        
        Args:
            command_code: The command code bytes
            
        Returns:
            String name of the command or 'UNKNOWN' if not found
        """
        for attr_name in dir(self):
            if not attr_name.startswith('_') and attr_name != 'get_command_name':
                if getattr(self, attr_name) == command_code:
                    return attr_name
        return 'UNKNOWN'
