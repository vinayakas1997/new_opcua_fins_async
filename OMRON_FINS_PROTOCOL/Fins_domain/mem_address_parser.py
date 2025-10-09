"""
FINS Address Parser
==================
This module handles parsing of PLC addresses and converts them to FINS protocol format.
"""
try:
    from OMRON_FINS_PROTOCOL.Fins_domain.memory_areas import FinsPLCMemoryAreas
except ImportError:
    from memory_areas import FinsPLCMemoryAreas
# from memory_areas import FinsPLCMemoryAreas
__version__ = "0.1.0"


class FinsAddressParser:
    """
    Parser for PLC addresses in string format (e.g., 'D1000', 'W100', 'H200').
    Converts them to FINS protocol memory area codes and offsets.
    """
    
    def __init__(self):
        self.memory_areas = FinsPLCMemoryAreas()
        self._setup_extended_memory_mapping()
    
    def _setup_extended_memory_mapping(self):
        """
        Set up mapping for Extended Memory banks to simplify parsing logic.
        """
        # Single digit/hex banks (EM0-EMF)
        self.em_single_banks = {
            '0': ('EM0', 0), '1': ('EM1', 1), '2': ('EM2', 2), '3': ('EM3', 3),
            '4': ('EM4', 4), '5': ('EM5', 5), '6': ('EM6', 6), '7': ('EM7', 7),
            '8': ('EM8', 8), '9': ('EM9', 9), 'A': ('EMA', 10), 'B': ('EMB', 11),
            'C': ('EMC', 12), 'D': ('EMD', 13), 'E': ('EME', 14), 'F': ('EMF', 15)
        }
        
        # Two digit banks (EM10-EM18)
        self.em_double_banks = {
            '10': 'EM10', '11': 'EM11', '12': 'EM12', '13': 'EM13', '14': 'EM14',
            '15': 'EM15', '16': 'EM16', '17': 'EM17', '18': 'EM18'
        }
    
    def _parse_extended_memory(self, addr_part: str, access_type: str = 'WORD') -> tuple:
        """
        Parse Extended Memory address part and return memory type and address.
        Enhanced logic to distinguish between single-digit and two-digit E memory banks:
        - E1200 -> EM1, address 200 (single-digit bank)
        - E10200 -> EM10, address 200 (two-digit bank)
        - EM1200 -> EM12, address 00 (explicit two-digit bank notation)
        
        Args:
            addr_part: Address part after 'E' prefix (e.g., '0100', '10200', 'A050')
            access_type: 'WORD' or 'BIT' to determine memory area type
            
        Returns:
            Tuple of (memory_type, final_address)
        """
        if len(addr_part) < 3:
            raise ValueError(f"Invalid extended memory address format: E{addr_part}")
        
        # Enhanced logic: Check for two-digit banks ONLY if we have 5+ characters
        # This ensures E1200 goes to EM1 (not EM12) and E10200 goes to EM10
        if len(addr_part) >= 5 and addr_part[:2].isdigit():
            bank_str = addr_part[:2]
            if bank_str in self.em_double_banks:
                # This is a valid two-digit bank with sufficient address digits
                bank_name = self.em_double_banks[bank_str]
                memory_type = getattr(self.memory_areas, f'{bank_name}_{access_type}')
                final_addr = int(addr_part[2:])
                return memory_type, final_addr
        
        # Special case: exactly 4 characters and starts with valid two-digit bank
        # E.g., "1000" -> EM10, address 00
        if len(addr_part) == 4 and addr_part[:2].isdigit():
            bank_str = addr_part[:2]
            if bank_str in self.em_double_banks:
                bank_name = self.em_double_banks[bank_str]
                memory_type = getattr(self.memory_areas, f'{bank_name}_{access_type}')
                final_addr = int(addr_part[2:])
                return memory_type, final_addr
        
        # Single digit/hex bank logic (EM0-EMF)
        # For addresses like E1200, E2300, EA100, etc.
        bank_char = addr_part[0].upper()
        if bank_char in self.em_single_banks:
            bank_name, _ = self.em_single_banks[bank_char]
            memory_type = getattr(self.memory_areas, f'{bank_name}_{access_type}')
            # For single digit banks, the format is E + bank_char + address
            # E1200 = EM1, address 200
            # EA050 = EMA, address 050
            final_addr = int(addr_part[1:])  # Skip only the bank char
            return memory_type, final_addr
        
        raise ValueError(f"Invalid extended memory bank: {bank_char}")
    
    def parse(self, address: str, offset: int = 0) -> dict:
        """
        Main entry point - automatically detects if address is word or bit based on '.' presence.
        
        Args:
            address: Address string (e.g., 'A100' for word, 'A100.01' for bit and for CIO area '100' or '100.03')
            offset: Additional offset to add to the address
            
        Returns:
            Dictionary containing parsed address information
        """
        if not address:
            raise ValueError("Address cannot be empty")
        
        # here make the first char upper case to handle lower case address
        _first_char = address[0].upper()
        
        # handle CIO- Style address with no letter prefix add CIO prefix as Z
        if _first_char.isdigit():
            address = 'Z' + address
        
        # parse with the bit or word method based on presence of '.'
        if '.' in address:
            return self._parse_as_bit_address(address, offset)
        else:
            return self._parse_as_word_address(address, offset)
    
    def _parse_as_bit_address(self, address: str, offset: int = 0) -> dict:
        """
        Parse as bit address (e.g., 'A100.01').
        
        Args:
            address: Bit address string
            offset: Additional offset to add to the word address
            
        Returns:
            Dictionary with bit address information
        """
        memory_type, moffset, bit_num = self.parse_bit_address(address, offset)
        
        # Convert memory type bytes to int if needed
        if isinstance(memory_type, bytes):
            memory_type_int = int.from_bytes(memory_type, 'big')
        else:
            memory_type_int = memory_type
        
        word_address = int.from_bytes(bytes(moffset), 'big')
        
        return {
            'address_type': 'bit',
            'original_address': address,
            'memory_area': self._get_memory_area_name(address),
            'memory_type_code': memory_type_int,
            'memory_type_bytes': memory_type,
            'word_address': word_address,
            'bit_number': bit_num,
            'offset_bytes': moffset,
            'fins_format': {
                'memory_area_code': memory_type,
                'address_high': moffset[0],
                'address_low': moffset[1],
                'bit_position': bit_num
            }
        }
    
    def _parse_as_word_address(self, address: str, offset: int = 0) -> dict:
        """
        Parse as word address (e.g., 'A100').
        
        Args:
            address: Word address string
            offset: Additional offset to add to the address
            
        Returns:
            Dictionary with word address information
        """
        memory_type, moffset = self.parse_address(address, offset)
        
        # Convert memory type bytes to int if needed
        if isinstance(memory_type, bytes):
            memory_type_int = int.from_bytes(memory_type, 'big')
        else:
            memory_type_int = memory_type
        
        word_address = int.from_bytes(bytes(moffset), 'big')
        
        return {
            'address_type': 'word',
            'original_address': address,
            'memory_area': self._get_memory_area_name(address),
            'memory_type_code': memory_type_int,
            'memory_type_bytes': memory_type,
            'word_address': word_address,
            'bit_number': None,
            'offset_bytes': moffset,
            'fins_format': {
                'memory_area_code': memory_type,
                'address_high': moffset[0],
                'address_low': moffset[1]
            }
        }

    def _get_address_prefix_info(self, address: str) -> tuple:
        """
        Determine the memory type prefix and remaining address part.
        Handles both single and multi-character prefixes.
        Enhanced to support EM prefix for two-digit extended memory banks.
        
        Args:
            address: Address string
            
        Returns:
            Tuple of (prefix, remaining_address, is_multi_char)
        """
        address_upper = address.upper()
        
        # Define multi-character prefixes (order matters - check longer ones first)
        multi_char_prefixes = ['EM']  # EM for two-digit extended memory banks
        
        # Check for multi-character prefixes first
        for prefix in multi_char_prefixes:
            if len(address_upper) >= len(prefix) and address_upper.startswith(prefix):
                return prefix, address[len(prefix):], True
        
        # Check for single character prefix
        if len(address) > 0:
            return address_upper[0], address[1:], False
        
        raise ValueError(f"Invalid address format: {address}")

    def parse_address(self, address: str, offset: int = 0) -> tuple:
        """
        Parse a PLC address string and return memory type and offset.
        
        Args:
            address: Address string (e.g., 'D1000', 'W100', 'H200')
            offset: Additional offset to add to the address
            
        Returns:
            Tuple of (memory_type_code, offset_bytes_list)
        """
        if not address:
            raise ValueError("Address cannot be empty")
        
        # Get prefix information
        prefix, addr_part, is_multi_char = self._get_address_prefix_info(address)
        
        # Parse address number
        try:
            addr_num = int(addr_part)
        except ValueError:
            # if prefix == 'E':  # Special handling for Extended Memory
            #     # Use the enhanced _parse_extended_memory method for consistency
            #     # This will be handled in the E prefix section below
            #     addr_num = 0  # Placeholder, will be overridden
            
            # else:
                raise ValueError(f"Invalid address number or format: {addr_part}")
        
        # Determine memory type based on prefix
        if prefix == 'D':  # Data Memory
            memory_type = self.memory_areas.DATA_MEMORY_WORD
            final_addr = addr_num + offset
            
        elif prefix == 'W':  # Work Area
            memory_type = self.memory_areas.WORK_WORD
            final_addr = addr_num + offset
            
        elif prefix == 'H':  # Holding Area
            memory_type = self.memory_areas.HOLDING_WORD
            final_addr = addr_num + offset
            
        elif prefix == 'A':  # Auxiliary Area
            memory_type = self.memory_areas.AUXILIARY_WORD
            final_addr = addr_num + offset
        
        elif prefix == 'Z':  # CIO Area
            memory_type = self.memory_areas.CIO_WORD
            final_addr = addr_num + offset
            
        elif prefix == 'E':  # Extended Memory (single-digit banks)
            # Simple parsing for E prefix: E + single digit/hex + address
            if len(addr_part) < 3:
                raise ValueError(f"Invalid E address format: E{addr_part}")
            
            bank_char = addr_part[0].upper()
            if bank_char in self.em_single_banks:
                bank_name, _ = self.em_single_banks[bank_char]
                memory_type = getattr(self.memory_areas, f'{bank_name}_WORD')
                final_addr = int(addr_part[1:]) + offset  # Skip bank char, parse rest as address
            else:
                raise ValueError(f"Invalid E bank: {bank_char}")
            
        elif prefix == 'EM':  # Extended Memory (two-digit banks)
            # Parse EM prefix for two-digit banks (EM10, EM12, ..., EM18)
            if len(addr_part) < 3:  # Need at least bank number + address
                raise ValueError(f"Invalid EM address format: EM{addr_part}")
            
            # Extract bank number (should be 10-18)
            if len(addr_part) >= 3 and addr_part[:2].isdigit():
                bank_str = addr_part[:2]
                if bank_str in self.em_double_banks:
                    bank_name = self.em_double_banks[bank_str]
                    memory_type = getattr(self.memory_areas, f'{bank_name}_WORD')
                    final_addr = int(addr_part[2:]) + offset
                else:
                    raise ValueError(f"Invalid EM bank number: {bank_str}")
            else:
                raise ValueError(f"Invalid EM address format: EM{addr_part}")
            
        elif prefix == 'T':  # Timer
            memory_type = self.memory_areas.TIMER_WORD
            final_addr = addr_num + offset
            
        elif prefix == 'C':  # Counter
            memory_type = self.memory_areas.COUNTER_WORD
            # Add counter offset as in your original code
            final_addr = addr_num + 0x0800 + offset
            
        else:
            raise ValueError(f"Unsupported memory type: {prefix}")
        
        moffset = list(final_addr.to_bytes(2, 'big'))
        return memory_type, moffset
    
    def parse_bit_address(self, address: str, offset: int = 0, bit: int = 0) -> tuple:
        """
        Parse a PLC bit address string with enhanced bit management.
        
        Args:
            address: Address string (e.g., 'A0.01', 'D1000.05', 'W100.15', '100.03')
            offset: Additional offset to add to the word address
            bit: Bit number (0-15) if not specified in address
            
        Returns:
            Tuple of (memory_type_code, offset_bytes_list, bit_number)
        """
        base_addr, bit_str = address.split('.')
        bit_num = int(bit_str)
        
        if not (0 <= bit_num <= 15):
            raise ValueError(f"Bit number must be between 0-15, got: {bit_num}")
        
        # Use the same prefix detection logic as word addresses
        prefix, addr_part, is_multi_char = self._get_address_prefix_info(base_addr)
        # print(f"from PRG prefix {prefix}")
        # Parse address number
        try:
            addr_num = int(addr_part)
        except ValueError:
            if prefix == 'E':  # Special handling for Extended Memory
                # Use the enhanced _parse_extended_memory method for consistency
                # This will be handled in the E prefix section below
                addr_num = 0  # Placeholder, will be overridden
            else:
                raise ValueError(f"Invalid bit address number format: {addr_part}")
        
        # Determine memory type based on prefix (bit versions)
        if prefix == 'D':  # Data Memory
            memory_type = self.memory_areas.DATA_MEMORY_BIT
            final_addr = addr_num + offset
            
        elif prefix == 'W':  # Work Area
            memory_type = self.memory_areas.WORK_BIT
            final_addr = addr_num + offset
            
        elif prefix == 'H':  # Holding Area
            memory_type = self.memory_areas.HOLDING_BIT
            final_addr = addr_num + offset
            
        elif prefix == 'A':  # Auxiliary Area
            memory_type = self.memory_areas.AUXILIARY_BIT
            final_addr = addr_num + offset
        
        elif prefix == 'Z':  # CIO Area
            memory_type = self.memory_areas.CIO_BIT
            final_addr = addr_num + offset
            
        elif prefix == 'E':  # Extended Memory Bit (single-digit banks)
            # Simple parsing for E prefix: E + single digit/hex + address
            if len(addr_part) < 3:
                raise ValueError(f"Invalid E bit address format: E{addr_part}")
            
            bank_char = addr_part[0].upper()
            if bank_char in self.em_single_banks:
                bank_name, _ = self.em_single_banks[bank_char]
                memory_type = getattr(self.memory_areas, f'{bank_name}_BIT')
                final_addr = int(addr_part[1:]) + offset  # Skip bank char, parse rest as address
            else:
                raise ValueError(f"Invalid E bit bank: {bank_char}")
            
        elif prefix == 'EM':  # Extended Memory Bit (two-digit banks)
            # Parse EM prefix for two-digit banks (EM10, EM12, ..., EM18)
            if len(addr_part) < 3:  # Need at least bank number + address
                raise ValueError(f"Invalid EM bit address format: EM{addr_part}")
            
            # Extract bank number (should be 10-18)
            if len(addr_part) >= 2 and addr_part[:2].isdigit():
                bank_str = addr_part[:2]
                if bank_str in self.em_double_banks:
                    bank_name = self.em_double_banks[bank_str]
                    memory_type = getattr(self.memory_areas, f'{bank_name}_BIT')
                    final_addr = int(addr_part[2:]) + offset
                else:
                    raise ValueError(f"Invalid EM bank number: {bank_str}")
            else:
                raise ValueError(f"Invalid EM bit address format: EM{addr_part}")
            
        elif prefix == 'T':  # Timer Bit
            memory_type = self.memory_areas.TIMER_FLAG
            final_addr = addr_num + offset
            
        elif prefix == 'C':  # Counter Bit
            memory_type = self.memory_areas.COUNTER_FLAG
            final_addr = addr_num + offset
            
        else:
            raise ValueError(f"Unsupported bit memory type: {prefix}")
        
        moffset = list(final_addr.to_bytes(2, 'big'))
        return memory_type, moffset, bit_num

    def _get_memory_area_name(self, address: str) -> str:
        """
        Get human-readable memory area name from address.
        Enhanced to provide specific extended memory bank information.
        Supports both E and EM prefixes for extended memory.
        
        Args:
            address: Address string
            
        Returns:
            Memory area name with specific bank info for extended memory
        """
        address_upper = address.upper()
        
        if address_upper[0].isdigit():
            return 'CIO Area'
        
        # Check for EM prefix first (two-digit banks)
        if address_upper.startswith('EM'):
            try:
                addr_part = address[2:]  # Skip 'EM'
                if len(addr_part) >= 3 and addr_part[:2].isdigit():
                    bank_str = addr_part[:2]
                    if bank_str in self.em_double_banks:
                        return f'Extended Memory ({self.em_double_banks[bank_str]})'
                return 'Extended Memory (Unknown EM Bank)'
            except:
                return 'Extended Memory'
        
        # Enhanced handling for E prefix (single-digit banks)
        elif address_upper.startswith('E'):
            try:
                # Extract the address part after 'E'
                addr_part = address[1:]
                if len(addr_part) >= 3:
                    # Single-digit/hex banks
                    bank_char = addr_part[0].upper()
                    if bank_char in self.em_single_banks:
                        bank_name, _ = self.em_single_banks[bank_char]
                        return f'Extended Memory ({bank_name})'
                
                return 'Extended Memory (Unknown Bank)'
            except:
                return 'Extended Memory'
        
        # Standard area names
        area_names = {
            'D': 'Data Memory',
            'W': 'Work Area',
            'H': 'Holding Area',
            'A': 'Auxiliary Area',
            'T': 'Timer',
            'C': 'Counter',
            'Z': 'CIO Area'
        }
        
        return area_names.get(address_upper[0], f'Unknown ({address_upper[0]})')

   


# Example usage and testing
if __name__ == "__main__":
    # Test the enhanced address parser with automatic detection
    # from memory_areas import FinsPLCMemoryAreas
    parser = FinsAddressParser()
    # helper = FinsAddressHelper()
    
    test_addresses = [
        # 'A100',      # Word address
        # 'A100.01',   # Bit address
        # 'D1000',     # Word address
        # 'D1000.05',  # Bit address
        # 'W200',      # Word address
        # 'W200.15',   # Bit address
        # '100',       # CIO word address
        # '100.03'     # CIO bit address
        # 'D0100'
        '10',
        '0.01'
    ]
    
    print("Testing automatic address detection:")
    print("=" * 50)
    
    for addr in test_addresses:
        try:
            info = parser.parse(addr)
            print(f"Address: {addr}")
            print(f"  Type: {info['address_type']}")
            print(f"  Memory Area: {info['memory_area']}")
            print(f"  Word Address: {info['word_address']}")
            # if info['address_type'] == 'bit':
            #     print(f"  Bit Number: {info['bit_number']}")
            print(f"  Bit Number: {info['bit_number']}")
            # print(f"  Memory Type Code: 0x{info['memory_type_code']:04X}")
            print(f"  Memory Type Code: {info['memory_type_code']}")
            print(f"  Offset Bytes: {info['offset_bytes']}")
            print()
        except Exception as e:
            print(f"Error parsing {addr}: {e}")
            print()
    
    
    
    
    
    # # Test helper class
    # print("\nTesting helper class:")
    # print("=" * 30)
    
    # for addr in ['A100', 'A100.01']:
    #     try:
    #         info = helper.parse_any_address(addr)
    #         print(f"Helper parsed {addr}:")
    #         print(f"  Type: {info['address_type']}")
    #         print(f"  Area: {info['memory_area']}")
    #         if info['bit_number'] is not None:
    #             print(f"  Bit: {info['bit_number']}")
    #         print()
    #     except Exception as e:
    #         print(f"Helper error with {addr}: {e}")
