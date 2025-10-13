#!/usr/bin/env python3
"""
Test script to verify the refactored data type mapping imports work correctly.
"""

def test_direct_import():
    """Test importing directly from data_type_mapping module."""
    try:
        from OMRON_FINS_PROTOCOL.components.data_type_mapping import DATA_TYPE_MAPPING, return_raw_bytes
        print("✅ Direct import from data_type_mapping module successful")
        print(f"   - DATA_TYPE_MAPPING has {len(DATA_TYPE_MAPPING)} entries")
        print(f"   - return_raw_bytes function: {return_raw_bytes}")
        return True
    except ImportError as e:
        print(f"❌ Direct import failed: {e}")
        return False

def test_components_import():
    """Test importing through components package."""
    try:
        from OMRON_FINS_PROTOCOL.components import DATA_TYPE_MAPPING, return_raw_bytes
        print("✅ Import through components package successful")
        print(f"   - DATA_TYPE_MAPPING has {len(DATA_TYPE_MAPPING)} entries")
        print(f"   - return_raw_bytes function: {return_raw_bytes}")
        return True
    except ImportError as e:
        print(f"❌ Components package import failed: {e}")
        return False

def test_functionality():
    """Test that the function actually works."""
    try:
        from OMRON_FINS_PROTOCOL.components.data_type_mapping import return_raw_bytes, DATA_TYPE_MAPPING
        
        # Test the function
        test_data = b'\x12\x34'
        result = return_raw_bytes(test_data)
        assert result == test_data, f"Expected {test_data}, got {result}"
        print("✅ return_raw_bytes function works correctly")
        
        # Test mapping structure
        assert 'INT16' in DATA_TYPE_MAPPING, "INT16 missing from mapping"
        assert 'RAW' in DATA_TYPE_MAPPING, "RAW missing from mapping"
        words_per_item, conversion_func = DATA_TYPE_MAPPING['INT16']
        assert words_per_item == 1, f"Expected 1 word for INT16, got {words_per_item}"
        assert conversion_func == return_raw_bytes, "Conversion function mismatch"
        print("✅ DATA_TYPE_MAPPING structure is correct")
        
        return True
    except Exception as e:
        print(f"❌ Functionality test failed: {e}")
        return False

def test_udp_connection_import():
    """Test that UDP connection can import the mapping."""
    try:
        from OMRON_FINS_PROTOCOL.Infrastructure.udp_connection import FinsUdpConnection
        print("✅ UDP connection imports successfully with new mapping")
        return True
    except ImportError as e:
        print(f"❌ UDP connection import failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing refactored data type mapping imports...")
    print("=" * 50)
    
    tests = [
        test_direct_import,
        test_components_import,
        test_functionality,
        test_udp_connection_import,
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()
    
    print("=" * 50)
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Refactoring successful!")
    else:
        print("⚠️  Some tests failed. Check the import structure.")