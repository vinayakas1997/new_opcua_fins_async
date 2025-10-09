"""
FINS PLC Memory Areas
=====================
This module defines PLC memory area codes for FINS protocol communication.
"""

__version__ = "0.1.0"


class FinsPLCMemoryAreas:
    """
    Hex codes for PLC memory areas.
    
    Each memory area has corresponding hex codes for word access, bit access,
    forced word access and forced bit access. This class provides name-based
    access to them for better code readability.
    """
    
    def __init__(self):
        # CIO (Common I/O) Area
        self.CIO_BIT = b'\x30'
        self.CIO_WORD = b'\xB0'
        self.CIO_BIT_FORCED = b'\x70'
        self.CIO_WORD_FORCED = b'\xF0'
        
        # Work Area
        self.WORK_BIT = b'\x31'
        self.WORK_WORD = b'\xB1'
        self.WORK_BIT_FORCED = b'\x71'
        self.WORK_WORD_FORCED = b'\xF1'
        
        # Holding Area
        self.HOLDING_BIT = b'\x32'
        self.HOLDING_WORD = b'\xB2'
        self.HOLDING_BIT_FORCED = b'\x72'
        self.HOLDING_WORD_FORCED = b'\xF2'
        
        # Auxiliary Area
        self.AUXILIARY_BIT = b'\x33'
        self.AUXILIARY_WORD = b'\xB3'
        
        # Timer and Counter Areas
        self.TIMER_FLAG = b'\x09' 
        self.COUNTER_FLAG = b'\x09'
        self.TIMER_FLAG_FORCED = b'\x49'
        self.COUNTER_FLAG_FORCED = b'\x49'
             # the timer can have many alias we ca also use the x89 or x81 
        self.TIMER_WORD = b'\x81'
        self.COUNTER_WORD = b'\x81'
        
        # Data Memory Area
        # self.DATA_MEMORY_BIT = b'\x02'
        self.DATA_MEMORY_BIT = 0x02
        self.DATA_MEMORY_WORD = b'\x82'
        
        # Extended Memory Areas (EM0-EM9, EMA-EMF)
        self.EM0_BIT = b'\x20'
        self.EM1_BIT = b'\x21'
        self.EM2_BIT = b'\x22'
        self.EM3_BIT = b'\x23'
        self.EM4_BIT = b'\x24'
        self.EM5_BIT = b'\x25'
        self.EM6_BIT = b'\x26'
        self.EM7_BIT = b'\x27'
        self.EM8_BIT = b'\x28'
        self.EM9_BIT = b'\x29'
        self.EMA_BIT = b'\x2A'
        self.EMB_BIT = b'\x2B'
        self.EMC_BIT = b'\x2C'
        self.EMD_BIT = b'\x2D'
        self.EME_BIT = b'\x2E'
        self.EMF_BIT = b'\x2F'
        
        # Extended Memory Areas (EM10-EM18)
        self.EM10_BIT = b'\xE0'
        self.EM11_BIT = b'\xE1'
        self.EM12_BIT = b'\xE2'
        self.EM13_BIT = b'\xE3'
        self.EM14_BIT = b'\xE4'
        self.EM15_BIT = b'\xE5'
        self.EM16_BIT = b'\xE6'
        self.EM17_BIT = b'\xE7'
        self.EM18_BIT = b'\xE8'
        
        # Extended Memory Word Areas (EM0-EM9, EMA-EMF)
        self.EM0_WORD = b'\xA0'
        self.EM1_WORD = b'\xA1'
        self.EM2_WORD = b'\xA2'
        self.EM3_WORD = b'\xA3'
        self.EM4_WORD = b'\xA4'
        self.EM5_WORD = b'\xA5'
        self.EM6_WORD = b'\xA6'
        self.EM7_WORD = b'\xA7'
        self.EM8_WORD = b'\xA8'
        self.EM9_WORD = b'\xA9'
        self.EMA_WORD = b'\xAA'
        self.EMB_WORD = b'\xAB'
        self.EMC_WORD = b'\xAC'
        self.EMD_WORD = b'\xAD'
        self.EME_WORD = b'\xAE'
        self.EMF_WORD = b'\xAF'
        
        # Extended Memory Word Areas (EM10-EM18)
        self.EM10_WORD = b'\x60'
        self.EM11_WORD = b'\x61'
        self.EM12_WORD = b'\x62'
        self.EM13_WORD = b'\x63'
        self.EM14_WORD = b'\x64'
        self.EM15_WORD = b'\x65'
        self.EM16_WORD = b'\x66'
        self.EM17_WORD = b'\x67'
        self.EM18_WORD = b'\x68'
        
        # Current Bank Areas
        self.EM_CURR_BANK_BIT = b'\x0A'
        self.EM_CURR_BANK_WORD = b'\x98'
        self.EM_CURR_BANK_NUMBER = b'\xBC'
        
        # Task and System Areas
        self.TASK_FLAG_BIT = b'\x06'
        self.TASK_FLAG_STATUS = b'\x46'
        self.INDEX_REGISTER = b'\xDC'
        self.DATA_REGISTER = b'\xBC'
        self.CLOCK_PULSES = b'\x07'
        self.CONDITION_FLAGS = b'\x07'

        # Precompute all necessary lookup data for efficiency
        self._precompute_area_info()

    def _precompute_area_info(self):
        """
        Precomputes internal maps and sets for efficient lookups of memory area information.
        This avoids repeated iteration over attributes.
        """
        self._memory_area_names_map = {}
        self._bit_area_codes_cache = set()
        self._word_area_codes_cache = set()
        self._all_memory_area_codes_list = []

        # Populate name map and initial bit/word sets based on attribute names
        for attr_name in dir(self):
            if not attr_name.startswith('_') and isinstance(getattr(self, attr_name), bytes):
                code = getattr(self, attr_name)
                self._memory_area_names_map[code] = attr_name
                if code not in self._all_memory_area_codes_list:
                    self._all_memory_area_codes_list.append(code)

        # Explicitly define bit areas based on the original logic
        explicit_bit_codes = [
            self.CIO_BIT, self.WORK_BIT, self.HOLDING_BIT, self.AUXILIARY_BIT,
            self.CIO_BIT_FORCED, self.WORK_BIT_FORCED, self.HOLDING_BIT_FORCED,
            self.TIMER_FLAG, self.COUNTER_FLAG, self.TIMER_FLAG_FORCED,
            self.COUNTER_FLAG_FORCED, self.DATA_MEMORY_BIT, self.EM_CURR_BANK_BIT,
            self.TASK_FLAG_BIT, self.CLOCK_PULSES, self.CONDITION_FLAGS
        ]
        bit_by_suffix = [getattr(self, attr) for attr in dir(self) if attr.endswith('_BIT')]
        self._bit_area_codes_cache.update(explicit_bit_codes + bit_by_suffix)

        # Explicitly define word areas based on the original logic
        explicit_word_codes = [
            self.CIO_WORD, self.WORK_WORD, self.HOLDING_WORD, self.AUXILIARY_WORD,
            self.CIO_WORD_FORCED, self.WORK_WORD_FORCED, self.HOLDING_WORD_FORCED,
            self.TIMER_WORD, self.COUNTER_WORD, self.DATA_MEMORY_WORD,
            self.EM_CURR_BANK_WORD, self.EM_CURR_BANK_NUMBER, self.INDEX_REGISTER, self.DATA_REGISTER
        ]
        word_by_suffix = [getattr(self, attr) for attr in dir(self) if attr.endswith('_WORD')]
        self._word_area_codes_cache.update(explicit_word_codes + word_by_suffix)
    
    
    ## These are like the helper functions first is the 
    # get_memeory_area_name - where if you have the area code get the corresponding name 
    # is it bit area
    # is it a word aea  
    
    ###--> If possible combine this as the internal function and create a common function giving the whole definition 

    def list_all_memory_areas(self):
        """
        Prints a formatted list of all defined FINS memory area variables and their byte codes.
        This is useful for debugging and reference.
        """
        print("Defined FINS Memory Areas:")
        print("-" * 40)

        all_areas = []
        for attr_name in dir(self):
            # Filter for public attributes that are bytes
            if not attr_name.startswith('_') and isinstance(getattr(self, attr_name), bytes):
                all_areas.append((attr_name, getattr(self, attr_name)))

        # Sort alphabetically for consistent, readable output
        all_areas.sort()

        for name, code in all_areas:
            print(f"{name.ljust(25)} = {code}")

    def get_memory_area_name(self, area_code: bytes) -> str:
        """
        Get the human-readable name for a memory area code.
        
        Args:
            area_code: The memory area code bytes
            
        Returns:
            String name of the memory area or 'UNKNOWN' if not found
        """
        name = self._memory_area_names_map.get(area_code)
        if name:
            return name
        return 'UNKNOWN'
    
    def is_bit_area(self, area_code: bytes) -> bool:
        """
        Check if the area code represents a bit-accessible area.
        
        Args:
            area_code: The memory area code bytes
            
        Returns:
            True if bit area, False otherwise
        """
        return area_code in self._bit_area_codes_cache
    
    def is_word_area(self, area_code: bytes) -> bool:
        """
        Check if the area code represents a word-accessible area.
        
        Args:
            area_code: The memory area code bytes
            
        Returns:
            True if word area, False otherwise
        """
        return area_code in self._word_area_codes_cache

    def get_all_memory_area_codes(self) -> list[bytes]:
        """
        Returns a list of all unique defined FINS memory area codes.

        Returns:
            A list of bytes, where each item is a memory area code.
        """
        # Return a copy to prevent external modification of the internal list
        return list(self._all_memory_area_codes_list)

    def get_memory_area_details(self, area_code: bytes) -> dict:
        """
        Returns comprehensive details for a given FINS memory area code.

        Args:
            area_code: The memory area code bytes.

        Returns:
            A dictionary containing:
            - 'name': Human-readable name of the memory area.
            - 'is_bit_area': True if it's a bit-accessible area, False otherwise.
            - 'is_word_area': True if it's a word-accessible area, False otherwise.
            - 'code': The original byte code.
            Returns an empty dictionary if the area_code is not found.
        """
        if area_code not in self._memory_area_names_map:
            print(f"Details requested for unknown area code: {area_code.hex()}")
            return {}

        return {
            'name': self.get_memory_area_name(area_code),
            'is_bit_area': self.is_bit_area(area_code),
            'is_word_area': self.is_word_area(area_code),
            'code': area_code
        }


if __name__ == '__main__':
    # Create an instance of the class
    memory_areas = FinsPLCMemoryAreas()

    # Call the new function to print all defined memory areas
    memory_areas.list_all_memory_areas()

    # Example of using the other helper functions
    print("\n" + "="*40)
    print("Example of getting details for a specific code:")
    details = memory_areas.get_memory_area_details(memory_areas.DATA_MEMORY_WORD)
    print(details)