"""
FINS Protocol Frames
====================
This module defines the frame structures used in FINS protocol communication.
"""

__version__ = "0.1.0"


class FinsHeader:
    """
    FINS protocol header structure.
    
    The header contains routing and control information for FINS commands.
    """
    
    def __init__(self):
        self.icf = b'\x00'  # Information Control Field
        self.rsv = b'\x00'  # Reserved
        self.gct = b'\x00'  # Gateway Count
        self.dna = b'\x00'  # Destination Network Address
        self.da1 = b'\x00'  # Destination Node Address
        self.da2 = b'\x00'  # Destination Unit Address
        self.sna = b'\x00'  # Source Network Address
        self.sa1 = b'\x00'  # Source Node Address
        self.sa2 = b'\x00'  # Source Unit Address
        self.sid = b'\x00'  # Service ID
    
    def set(self, icf, rsv, gct, dna, da1, da2, sna, sa1, sa2, sid):
        """
        Set all header fields at once.
        
        Args:
            icf: Information Control Field
            rsv: Reserved field
            gct: Gateway Count
            dna: Destination Network Address
            da1: Destination Node Address
            da2: Destination Unit Address
            sna: Source Network Address
            sa1: Source Node Address
            sa2: Source Unit Address
            sid: Service ID
        """
        self.icf = icf
        self.rsv = rsv
        self.gct = gct
        self.dna = dna
        self.da1 = da1
        self.da2 = da2
        self.sna = sna
        self.sa1 = sa1
        self.sa2 = sa2
        self.sid = sid
        # print(self.dna,self.da1,self.da2,self.sna,self.sa1,self.sa2,self.sid)
    def bytes(self) -> bytes:
        """
        Convert header to bytes for transmission.
        
        Returns:
            Header as bytes
        """
        response = (self.icf + self.rsv + self.gct +
                    self.dna + self.da1 + self.da2 +
                    self.sna + self.sa1 + self.sa2 +
                    self.sid)
        return response
    
    def from_bytes(self, data: bytes) -> None:
        """
        Parse header from received bytes.
        
        Args:
            data: Raw bytes containing header data
        """
        if len(data) < 10:
            raise ValueError("Header data must be at least 10 bytes")
            
        self.icf = data[0:1]
        self.rsv = data[1:2]
        self.gct = data[2:3]
        self.dna = data[3:4]
        self.da1 = data[4:5]
        self.da2 = data[5:6]
        self.sna = data[6:7]
        self.sa1 = data[7:8]
        self.sa2 = data[8:9]
        self.sid = data[9:10]


class FinsCommandFrame:
    """
    FINS command frame structure.
    
    Contains header, command code, and command data.
    """
    
    def __init__(self):
        self.header = FinsHeader()
        self.command_code = b'\x00\x00'
        self.text = b''
    
    def bytes(self) -> bytes:
        """
        Convert command frame to bytes for transmission.
        
        Returns:
            Complete command frame as bytes
        """
        return self.header.bytes() + self.command_code + self.text
    
    def from_bytes(self, data: bytes) -> None:
        """
        Parse command frame from received bytes.
        
        Args:
            data: Raw bytes containing command frame data
        """
        if len(data) < 12:
            raise ValueError("Command frame data must be at least 12 bytes")
            
        self.header.from_bytes(data[0:10])
        self.command_code = data[10:12]
        self.text = data[12:]


class FinsResponseFrame:
    """
    FINS response frame structure.
    
    Contains header, command code, end code, and response data.
    """
    
    def __init__(self):
        self.header = FinsHeader()
        self.command_code = b'\x00\x00'
        self.end_code = b'\x00\x00'
        self.text = b''
    
    def bytes(self) -> bytes:
        """
        Convert response frame to bytes.
        
        Returns:
            Complete response frame as bytes
        """
        return self.header.bytes() + self.command_code + self.end_code + self.text
    
    def from_bytes(self, data: bytes) -> None:
        """
        Parse response frame from received bytes.
        
        Args:
            data: Raw bytes containing response frame data
        """
        if len(data) < 14:
            raise ValueError("Response frame data must be at least 14 bytes")
            
        self.header.from_bytes(data[0:10])
        self.command_code = data[10:12]
        self.end_code = data[12:14]
        self.text = data[14:]
