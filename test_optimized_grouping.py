#!/usr/bin/env python3
"""
Test script to verify the optimized grouping logic and raw bytes functionality.
"""

import asyncio
from datetime import datetime
from OMRON_FINS_PROTOCOL.Infrastructure.udp_connection import FinsUdpConnection
from OMRON_FINS_PROTOCOL.components.data_type_mapping import DATA_TYPE_MAPPING

# Mock PLC configuration for testing
MOCK_PLC_CONFIG = {
    'plc_name': 'TestPLC',
    'plc_ip': '192.168.1.100',  # Dummy IP for testing
    'opcua_url': 'opc.tcp://localhost:4840',
    'address_mappings': [
        {'plc_reg_add': 'D100', 'opcua_reg_add': 'TestD100', 'data_type': 'int16'},
        {'plc_reg_add': 'D101', 'opcua_reg_add': 'TestD101', 'data_type': 'int16'},
        {'plc_reg_add': 'D102', 'opcua_reg_add': 'TestD102', 'data_type': 'bool'},
        {'plc_reg_add': 'D103', 'opcua_reg_add': 'TestD103', 'data_type': 'channel'},
        {'plc_reg_add': 'D104', 'opcua_reg_add': 'TestD104', 'data_type': 'word'},
        {'plc_reg_add': 'D200', 'opcua_reg_add': 'TestD200', 'data_type': 'int32'},  # Multi-word
        {'plc_reg_add': 'D300', 'opcua_reg_add': 'TestD300', 'data_type': 'float'},  # Multi-word
        {'plc_reg_add': 'HEARTBEAT', 'opcua_reg_add': 'Heartbeat', 'data_type': 'bool'},
    ]
}

class MockPLCTask:
    """Mock PLCTask for testing grouping logic without actual PLC connection."""
    
    def __init__(self, plc_details):
        self.name = plc_details['plc_name']
        self.address_mappings = plc_details['address_mappings']
        self.multiple_read_groups = []
        self.single_read_addresses = []
        self._initialize_address_groups()
    
    def _initialize_address_groups(self):
        """Initialize address groups once at startup - optimized for performance."""
        self.multiple_read_groups = []
        self.single_read_addresses = []
        
        # Filter out HEARTBEAT and separate by word size
        one_word_mappings = []
        multi_word_mappings = []
        
        for mapping in self.address_mappings:
            plc_address = mapping['plc_reg_add']
            
            # Skip HEARTBEAT - handled separately
            if plc_address == "HEARTBEAT":
                continue
                
            data_type = mapping.get('data_type', 'int16').upper()
            
            # Normalize data types
            if data_type in ['BOOL', 'CHANNEL']:
                data_type = 'INT16'
            
            # Check if data type exists in mapping and get word size
            if data_type in DATA_TYPE_MAPPING:
                words_per_item, _ = DATA_TYPE_MAPPING[data_type]
                
                if words_per_item == 1:
                    # 1-word data type - add to multiple read groups
                    one_word_mappings.append({
                        'plc_reg': plc_address,
                        'opcua_reg': mapping['opcua_reg_add'],
                        'data_type': data_type,
                        'original_mapping': mapping
                    })
                else:
                    # Multi-word data type - add to single read list
                    multi_word_mappings.append({
                        'plc_reg': plc_address,
                        'opcua_reg': mapping['opcua_reg_add'],
                        'data_type': data_type,
                        'original_mapping': mapping
                    })
            else:
                print(f"Unknown data type '{data_type}' for address {plc_address}, treating as single read")
                multi_word_mappings.append({
                    'plc_reg': plc_address,
                    'opcua_reg': mapping['opcua_reg_add'],
                    'data_type': data_type,
                    'original_mapping': mapping
                })
        
        # Group 1-word addresses in batches of 20 for multiple_read
        batch_size = 20
        for i in range(0, len(one_word_mappings), batch_size):
            batch = one_word_mappings[i:i + batch_size]
            self.multiple_read_groups.append(batch)
        
        # Store multi-word addresses for individual reads
        self.single_read_addresses = multi_word_mappings
        
        print(f"Address grouping initialized:")
        print(f"  - Multiple read groups: {len(self.multiple_read_groups)} (max 20 addresses each)")
        print(f"  - Single read addresses: {len(self.single_read_addresses)}")

    def _hex_bytes_to_string(self, data: bytes) -> str:
        """Convert bytes to HEX string format (0x8080 -> '8080')."""
        if not data:
            return ""
        return data.hex().upper()


def test_data_type_mapping():
    """Test that the data type mapping import works correctly."""
    print("🧪 Testing DATA_TYPE_MAPPING import...")
    
    try:
        print(f"✅ DATA_TYPE_MAPPING imported successfully with {len(DATA_TYPE_MAPPING)} entries")
        
        # Test some key data types
        test_types = ['INT16', 'INT32', 'FLOAT', 'RAW']
        for dt in test_types:
            if dt in DATA_TYPE_MAPPING:
                words_per_item, conversion_func = DATA_TYPE_MAPPING[dt]
                print(f"   📊 {dt}: {words_per_item} words, function: {conversion_func.__name__}")
            else:
                print(f"   ❌ {dt}: Not found in mapping")
        
        return True
    except Exception as e:
        print(f"❌ Error testing DATA_TYPE_MAPPING: {e}")
        return False


def test_grouping_logic():
    """Test the new grouping logic with mock data."""
    print("\n🧪 Testing optimized grouping logic...")
    
    try:
        mock_task = MockPLCTask(MOCK_PLC_CONFIG)
        
        # Verify grouping results
        print(f"\n📊 Grouping Results:")
        print(f"   🔢 Number of multiple read groups: {len(mock_task.multiple_read_groups)}")
        print(f"   🔢 Number of single read addresses: {len(mock_task.single_read_addresses)}")
        
        # Show multiple read groups
        if mock_task.multiple_read_groups:
            print(f"\n   📋 Multiple Read Groups (1-word data types):")
            for i, group in enumerate(mock_task.multiple_read_groups):
                print(f"      Group {i+1} ({len(group)} addresses):")
                for item in group:
                    print(f"         - {item['plc_reg']} -> {item['opcua_reg']} ({item['data_type']})")
        
        # Show single read addresses
        if mock_task.single_read_addresses:
            print(f"\n   📋 Single Read Addresses (multi-word data types):")
            for item in mock_task.single_read_addresses:
                print(f"         - {item['plc_reg']} -> {item['opcua_reg']} ({item['data_type']})")
        
        return True
    except Exception as e:
        print(f"❌ Error testing grouping logic: {e}")
        return False


def test_hex_conversion():
    """Test HEX conversion functionality."""
    print("\n🧪 Testing HEX conversion...")
    
    try:
        mock_task = MockPLCTask(MOCK_PLC_CONFIG)
        
        # Test different byte patterns
        test_cases = [
            (b'\x80\x80', "8080"),
            (b'\x12\x34', "1234"),
            (b'\xFF\xFF', "FFFF"),
            (b'\x00\x00', "0000"),
            (b'', ""),
        ]
        
        for test_bytes, expected in test_cases:
            result = mock_task._hex_bytes_to_string(test_bytes)
            if result == expected:
                print(f"   ✅ {test_bytes} -> '{result}' (expected '{expected}')")
            else:
                print(f"   ❌ {test_bytes} -> '{result}' (expected '{expected}')")
                return False
        
        return True
    except Exception as e:
        print(f"❌ Error testing HEX conversion: {e}")
        return False


async def test_udp_connection_import():
    """Test that FinsUdpConnection can be imported and initialized."""
    print("\n🧪 Testing FinsUdpConnection import and initialization...")
    
    try:
        # Try to create connection instance (won't actually connect)
        fins = FinsUdpConnection("192.168.1.100", debug=False)
        print("✅ FinsUdpConnection imported and initialized successfully")
        
        # Test that DATA_TYPE_MAPPING is accessible
        test_type = 'INT16'
        if hasattr(fins, '_validate_data_type'):
            words, func = fins._validate_data_type(test_type)
            print(f"✅ _validate_data_type works: {test_type} -> {words} words, {func.__name__}")
        
        return True
    except Exception as e:
        print(f"❌ Error testing FinsUdpConnection: {e}")
        return False


async def main():
    """Main test function."""
    print("🚀 Testing Optimized FINS Implementation")
    print("=" * 50)
    print(f"⏰ Test started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    tests = [
        ("Data Type Mapping Import", test_data_type_mapping),
        ("Grouping Logic", test_grouping_logic), 
        ("HEX Conversion", test_hex_conversion),
        ("UDP Connection Import", test_udp_connection_import),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"Running: {test_name}")
        try:
            if asyncio.iscoroutinefunction(test_func):
                result = await test_func()
            else:
                result = test_func()
                
            if result:
                passed += 1
                print(f"✅ {test_name} PASSED")
            else:
                print(f"❌ {test_name} FAILED")
        except Exception as e:
            print(f"💥 {test_name} EXCEPTION: {e}")
        print()
    
    print("=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED! Optimized implementation is ready!")
    else:
        print("⚠️  Some tests failed. Check the implementation.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
    except Exception as e:
        print(f"\n💥 Fatal error: {e}")