#!/usr/bin/env python3
"""
Quick test to demonstrate the boolean conversion logic
"""

def _extract_bit_value_from_hex(hex_string: str, bit_number: int) -> int:
    """
    Extract specific bit value from HEX string and return 1 or 0.
    
    Args:
        hex_string: HEX string from PLC (e.g., "8080")
        bit_number: Bit position (0-15 for a 16-bit word)
    
    Returns:
        1 if bit is set, 0 if bit is clear
    """
    try:
        if not hex_string or len(hex_string) < 4:
            return 0
            
        # Convert HEX string to integer (assuming 16-bit word)
        word_value = int(hex_string[:4], 16)  # Take first 4 hex chars (16 bits)
        
        # Extract specific bit (bit 0 = LSB, bit 15 = MSB)
        bit_value = (word_value >> bit_number) & 1
        
        print(f"Boolean extraction: HEX:{hex_string} -> Word:{word_value:016b} -> Bit {bit_number}: {bit_value}")
        return bit_value
        
    except (ValueError, IndexError) as e:
        print(f"Error extracting bit {bit_number} from HEX '{hex_string}': {e}")
        return 0

def _get_bit_number_from_address(plc_address: str) -> int:
    """
    Extract bit number from PLC address like "142.01".
    
    Args:
        plc_address: PLC address string
        
    Returns:
        Bit number (0-15), or 0 if not a bit address
    """
    try:
        if '.' in plc_address:
            bit_part = plc_address.split('.')[1]
            return int(bit_part)
        return 0
    except (ValueError, IndexError):
        return 0

def test_boolean_conversion():
    """Test the boolean conversion logic with various examples."""
    
    print("=== Boolean Conversion Logic Test ===\n")
    
    # Test cases: (hex_value, plc_address, expected_result)
    test_cases = [
        ("8080", "142.00", 0),  # Bit 0 (LSB) should be 0
        ("8080", "142.01", 0),  # Bit 1 should be 0  
        ("8080", "142.07", 1),  # Bit 7 should be 1
        ("8080", "142.15", 1), # Bit 15 (MSB) should be 1
        ("FFFF", "100.05", 1), # All bits set, bit 5 should be 1
        ("0001", "200.00", 1), # Only LSB set, bit 0 should be 1
        ("0001", "200.01", 0), # Only LSB set, bit 1 should be 0
        ("0002", "300.01", 1), # Bit 1 set, should be 1
        ("0004", "400.02", 1), # Bit 2 set, should be 1
    ]
    
    print("Test Cases:")
    print("HEX    | Address | Expected | Actual | Status")
    print("-------|---------|----------|--------|--------")
    
    for hex_val, address, expected in test_cases:
        bit_number = _get_bit_number_from_address(address)
        actual = _extract_bit_value_from_hex(hex_val, bit_number)
        status = "✅ PASS" if actual == expected else "❌ FAIL"
        print(f"{hex_val:<6} | {address:<7} | {expected:<8} | {actual:<6} | {status}")
    
    print("\n=== Binary Representation Examples ===")
    
    examples = ["8080", "FFFF", "0001", "0002", "0004", "1000"]
    for hex_val in examples:
        word_value = int(hex_val, 16)
        print(f"HEX: {hex_val} -> Binary: {word_value:016b} -> Decimal: {word_value}")

if __name__ == "__main__":
    test_boolean_conversion()