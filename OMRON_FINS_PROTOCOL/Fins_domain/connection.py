"""
FINS Connection Interface
=========================
This module defines the abstract interface for FINS protocol connections.
"""

from abc import ABCMeta
from typing import List, Union, Any


from OMRON_FINS_PROTOCOL.Fins_domain.frames import FinsCommandFrame


__version__ = "0.1.0"

# (metaclass=ABCMeta)
class FinsConnection(metaclass=ABCMeta):
    """
    Abstract base class for FINS protocol connections.
    
    This class defines the interface that all FINS connection implementations
    must follow, regardless of the underlying transport (TCP, UDP, Serial).
    """
    
    def __init__(self, host: str, port: int = 9600,
                 dest_network: int = 0, dest_node: int = 0, dest_unit: int = 0,
                 src_network: int = 0, src_node: int = 1, src_unit: int = 0,
                 destfinsadr: str = "0.0.0", srcfinsadr: str = "0.1.0"):
    # def __init__(self, host: str, port: int = 9600,
    #              destfinsadr: str = "0.0.0", srcfinsadr: str = "0.1.0"):
        """
        Initialize connection parameters.
        
        Args:
            host: Target host IP address
            port: Target port number
            dest_network: Destination network address (0-127)
            dest_node: Destination node address (0-254) 
            dest_unit: Destination unit address (0-254)
            src_network: Source network address (0-127)
            src_node: Source node address (0-254)
            src_unit: Source unit address (0-254)
            destfinsadr: Alternative: destination address as "network.node.unit"
            srcfinsadr: Alternative: source address as "network.node.unit"
        """
        self.addr = (host, port)
        
        # Handle string format addresses if provided
        if destfinsadr:
            destfins = destfinsadr.split('.')
            if destfinsadr == "0.0.0":
                hostadr = host.split('.')
                destfins[1] = hostadr[3]
            dest_network, dest_node, dest_unit = map(int, destfins)
            # print("My program",dest_network, dest_node, dest_unit)
            # dest_network, dest_node, dest_unit = destfins
        if srcfinsadr:
            srcfins = srcfinsadr.split('.')
            
            src_network, src_node, src_unit = map(int, srcfins)
        
        # Set FINS addressing parameters
        self.dest_net_add = dest_network
        self.dest_node_add = dest_node
        self.dest_unit_add = dest_unit
        
        self.srce_net_add = src_network
        self.srce_node_add = src_node
        self.srce_unit_add = src_unit
    
    # @abstractmethod
    # def execute_fins_command_frame(self, fins_command_frame: bytes) -> bytes:
    #     """
    #     Execute a FINS command frame and return the response.
        
    #     Args:
    #         fins_command_frame: Complete FINS command frame as bytes
            
    #     Returns:
    #         Response frame as bytes
            
    #     Raises:
    #         ConnectionError: If communication fails
    #     """
    #     pass
    
    def fins_command_frame(self, command_code: bytes, text: bytes = b'', 
                      service_id: bytes = b'\x00', icf: bytes = b'\x80', 
                      gct: bytes = b'\x02', rsv: bytes = b'\x00') -> bytes:
        """
        Build a complete FINS command frame using the FinsCommandFrame class.
        
        Args:
            command_code: FINS command code
            text: Command data payload
            service_id: Service identifier
            icf: Information Control Field
            gct: Gateway Count
            rsv: Reserved field
            
        Returns:
            Complete command frame as bytes
        """
        frame = FinsCommandFrame()
        
        # Set header fields
        frame.header.set(
            icf=icf,
            rsv=rsv, 
            gct=gct,
            dna=self.dest_net_add.to_bytes(1, 'big'),
            da1=self.dest_node_add.to_bytes(1, 'big'),
            da2=self.dest_unit_add.to_bytes(1, 'big'),
            sna=self.srce_net_add.to_bytes(1, 'big'),
            sa1=self.srce_node_add.to_bytes(1, 'big'),
            sa2=self.srce_unit_add.to_bytes(1, 'big'),
            sid=service_id
        )
        # print("my program Fins header ", frame.header)
        # Set command data
        frame.command_code = command_code
        frame.text = text
        
        return frame.bytes()
    
# if __name__ == "__main__":
#     finscheck = FinsConnection("192.168.137.2")