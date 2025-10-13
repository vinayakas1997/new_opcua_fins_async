#!/usr/bin/env python3
"""
FINS Protocol Direct Command Frame Testing
==========================================
This script tests data fetching by sending FINS command frames directly to the PLC.
You can modify the PLC_IP variable below to match your PLC's IP address.
"""

import asyncio
import sys
from datetime import datetime

# Import the FINS protocol components
from OMRON_FINS_PROTOCOL.Infrastructure.udp_connection import FinsUdpConnection
from OMRON_FINS_PROTOCOL.Fins_domain.mem_address_parser import FinsAddressParser
from OMRON_FINS_PROTOCOL.components.conversion import (
    toInt16, toUInt16, toInt32, toUInt32, toFloat, 
    WordToHex, WordToBin, bcd_to_decimal
)
from OMRON_FINS_PROTOCOL.Fins_domain.frames import FinsResponseFrame
from OMRON_FINS_PROTOCOL.exception.exception_rules import (
    FinsConnectionError, FinsTimeoutError, FinsAddressError
)

# =============================================================================
# CONFIGURATION - MODIFY THIS SECTION
# =============================================================================

# Change this to your PLC's IP address
PLC_IP = "192.168.2.2"

# Test addresses - modify these to match your PLC's memory layout
TEST_ADDRESSES = {
    "D100": "INT16",     # Data memory word
    "D150": "INT16",    # Data memory word (unsigned)
    "D200": "INT16",     # Float value (requires 2 words)
    "D250": "INT16",      # Work area
    "D300": "INT16",     # Holding area
    "D400": "INT16",      # CIO area (no prefix)
    "D450": "INT16",     # Timers
    "D500": "INT16"      # Counters
}

# Multiple memory read test - specific D addresses as requested
MULTIPLE_READ_ADDRESSES = {
    "D100": "INT16",
    "D150": "INT16",
    "D200": "INT16",
    "D250": "INT16",
    "D300": "INT16",
    "D350": "INT16",
    "D400": "INT16",
    "D450": "INT16",
    "D500": "INT16",
    "D101": "INT16",
    "D102": "INT16",
    "D103": "INT16",
    "D104": "INT16",
    "D105": "INT16",
    "D106": "INT16",
    "D107": "INT16",
    "D108": "INT16",
    "D109": "INT16",
    "D201": "INT16",
    "D202": "INT16",
    "D203": "INT16",
    "D204": "INT16",
    "D205": "INT16",
    "D206": "INT16",
    "D207": "INT16",
    "D208": "INT16",
    # "D209": "INT16"
}


# Batch read test
BATCH_START_ADDRESS = "D200"
BATCH_READ_COUNT = 5

# =============================================================================
# TEST FUNCTIONS
# =============================================================================

async def test_cpu_details(fins):
    """Test CPU unit details read - simplest command to verify connection"""
    print("🔍 Testing CPU Unit Details Read")
    print("=" * 50)
    
    try:
        command_codes = fins.command_codes
        
        # Build command frame for CPU details
        command_frame = fins.fins_command_frame(
            command_code=command_codes.CPU_UNIT_DATA_READ,
            service_id=b'\x00'
        )
        
        print(f"📤 Sending command frame: {command_frame.hex()}")
        
        # Execute the command frame directly
        response = await fins.execute_fins_command_frame(command_frame)
        print(f"📥 Raw response ({len(response)} bytes): {response.hex()}")
        
        # Parse response manually
        response_frame = FinsResponseFrame()
        response_frame.from_bytes(response)
        
        print(f"🔗 Header: {response[:10].hex()}")
        print(f"📋 Command code: {response_frame.command_code.hex()}")
        print(f"⚡ End code: {response_frame.end_code.hex()}")
        
        if response_frame.end_code == b'\x00\x00':
            data = response_frame.text
            unit_name = data[0:20].decode().strip()
            boot_version = data[20:25].decode().strip()
            model_number = data[28:32].decode().strip()
            os_version = data[32:37].decode().strip()
            
            print("✅ SUCCESS - CPU Details:")
            print(f"   📟 Unit Name: {unit_name}")
            print(f"   🚀 Boot Version: {boot_version}")
            print(f"   🏷️  Model Number: {model_number}")
            print(f"   💻 OS Version: {os_version}")
            return True
        else:
            print(f"❌ ERROR - End code: {response_frame.end_code.hex()}")
            return False
            
    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return False
    finally:
        print()

async def test_cpu_status(fins):
    """Test CPU unit status read"""
    print("📊 Testing CPU Unit Status Read")
    print("=" * 50)
    
    try:
        command_codes = fins.command_codes
        
        command_frame = fins.fins_command_frame(
            command_code=command_codes.CPU_UNIT_STATUS_READ,
            service_id=b'\x00'
        )
        
        print(f"📤 Sending command frame: {command_frame.hex()}")
        
        response = await fins.execute_fins_command_frame(command_frame)
        print(f"📥 Raw response: {response.hex()}")
        
        response_frame = FinsResponseFrame()
        response_frame.from_bytes(response)
        
        if response_frame.end_code == b'\x00\x00':
            # Status and mode mapping
            status_map = {b'\x00': 'Stop', b'\x01': 'Run', b'\x80': 'CPU on standby'}
            mode_map = {b'\x00': 'PROGRAM', b'\x02': 'MONITOR', b'\x04': 'RUN'}
            
            status_byte = response[14:15]
            mode_byte = response[15:16]
            
            print("✅ SUCCESS - CPU Status:")
            print(f"   🚦 Status: {status_map.get(status_byte, 'Unknown')}")
            print(f"   🎮 Mode: {mode_map.get(mode_byte, 'Unknown')}")
            return True
        else:
            print(f"❌ ERROR - End code: {response_frame.end_code.hex()}")
            return False
            
    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return False
    finally:
        print()

async def test_single_memory_read(fins, address, data_type):
    """Test single memory area read with address parsing"""
    print(f"📖 Testing Single Memory Read: {address} ({data_type})")
    print("=" * 50)
    
    try:
        address_parser = FinsAddressParser()
        command_codes = fins.command_codes
        
        # Parse address to FINS format
        addr_info = address_parser.parse(address)
        print(f"🎯 Parsed address info:")
        print(f"   📍 Memory Area: {addr_info['memory_area']}")
        print(f"   🔢 Memory Type Code: 0x{addr_info['memory_type_code']:02X}")
        print(f"   📊 Word Address: {addr_info['word_address']}")
        print(f"   🔗 Offset Bytes: {addr_info['offset_bytes']}")
        
        # Determine read size based on data type
        read_size = 2 if data_type == "FLOAT" else 1  # FLOAT needs 2 words
        
        # Build command data
        command_data = bytearray(8)
        command_data[0:2] = command_codes.MEMORY_AREA_READ
        command_data[2] = addr_info['memory_type_code']
        command_data[3:5] = bytes(addr_info['offset_bytes'])
        command_data[5] = addr_info['bit_number'] if addr_info['bit_number'] else 0
        command_data[6:8] = read_size.to_bytes(2, 'big')
        
        # Build complete frame
        command_frame = fins.fins_command_frame(
            command_code=command_data,
            service_id=b'\x00'
        )
        
        print(f"📤 Command frame: {command_frame.hex()}")
        
        response = await fins.execute_fins_command_frame(command_frame)
        print(f"📥 Raw response: {response.hex()}")
        
        # Parse response
        response_frame = FinsResponseFrame()
        response_frame.from_bytes(response)
        
        if response_frame.end_code == b'\x00\x00':
            raw_data = response_frame.text
            print(f"📦 Raw data: {raw_data.hex()}")
            
            # Convert data based on type
            print("🔄 Converted values:")
            if data_type == "INT16":
                value = toInt16(raw_data)[0]
                print(f"   📈 INT16: {value}")
            elif data_type == "UINT16":
                value = toUInt16(raw_data)[0]
                print(f"   📈 UINT16: {value}")
            elif data_type == "FLOAT":
                value = toFloat(raw_data)[0]
                print(f"   📈 FLOAT: {value}")
            
            # Show additional formats
            hex_value = WordToHex(raw_data[:2])[0]
            print(f"   🔢 HEX: {hex_value}")
            
            print("✅ SUCCESS")
            return True
        else:
            print(f"❌ ERROR - End code: {response_frame.end_code.hex()}")
            return False
            
    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return False
    finally:
        print()

async def test_batch_memory_read(fins, start_address, count):
    """Test batch memory read"""
    print(f"📚 Testing Batch Memory Read: {start_address} (x{count})")
    print("=" * 50)
    
    try:
        address_parser = FinsAddressParser()
        command_codes = fins.command_codes
        
        # Parse starting address
        addr_info = address_parser.parse(start_address)
        print(f"🎯 Starting at: {addr_info['memory_area']} word {addr_info['word_address']}")
        
        # Build command for batch read
        command_data = bytearray(8)
        command_data[0:2] = command_codes.MEMORY_AREA_READ
        command_data[2] = addr_info['memory_type_code']
        command_data[3:5] = bytes(addr_info['offset_bytes'])
        command_data[5] = 0  # No bit for word reads
        command_data[6:8] = count.to_bytes(2, 'big')
        
        command_frame = fins.fins_command_frame(
            command_code=command_data,
            service_id=b'\x00'
        )
        
        print(f"📤 Command frame: {command_frame.hex()}")
        
        response = await fins.execute_fins_command_frame(command_frame)
        response_frame = FinsResponseFrame()
        response_frame.from_bytes(response)
        
        if response_frame.end_code == b'\x00\x00':
            raw_data = response_frame.text
            values = toInt16(raw_data)
            
            print(f"📦 Raw data ({len(raw_data)} bytes): {raw_data.hex()}")
            print(f"✅ SUCCESS - Read {len(values)} values:")
            
            for i, value in enumerate(values):
                addr_prefix = start_address[0] if start_address[0].isalpha() else ""
                addr_num = int(start_address[1:] if start_address[0].isalpha() else start_address)
                current_addr = f"{addr_prefix}{addr_num + i}"
                print(f"   📊 {current_addr}: {value} (0x{value & 0xFFFF:04X})")
            
            return True
        else:
            print(f"❌ ERROR - End code: {response_frame.end_code.hex()}")
            return False
            
    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return False
    finally:
        print()

async def test_multiple_memory_read(fins, addresses_dict):
    """
    Test multiple memory areas read in one command using FINS command 0x0104
    Format: CommandCode(0x0104) + NumberOfAreas(2bytes) + [AreaCode+Address+BitPos](4bytes each)
    """
    print(f"📋 Testing Multiple Memory Areas Read ({len(addresses_dict)} addresses)")
    print("=" * 50)
    
    try:
        address_parser = FinsAddressParser()
        command_codes = fins.command_codes
        
        # Build command data for multiple read
        addresses = list(addresses_dict.keys())
        
        print("🔧 Building MULTIPLE_MEMORY_AREA_READ command (0x0104):")
        print(f"   📊 Number of addresses to read: {len(addresses)}")
        
        data_part = bytearray()
        hex_parts = []  # To show the hex breakdown clearly
        
        print("🎯 Reading addresses:")
        for i, address in enumerate(addresses):
            addr_info = address_parser.parse(address)
            print(f"   📍 [{i+1}] {address}: {addr_info['memory_area']}")
            print(f"       🔢 Memory Type Code: 0x{addr_info['memory_type_code']:02X}")
            print(f"       📊 Word Address: {addr_info['word_address']}")
            print(f"       🔗 Offset Bytes: {addr_info['offset_bytes']}")
            
            # Build 4-byte structure for each address:
            # Byte 1: Memory area code
            # Bytes 2-3: Address (2 bytes)
            # Byte 4: Bit position (0x00 for word access)
            area_data = bytearray(4)
            area_data[0] = addr_info['memory_type_code']      # Area code (1 byte)
            area_data[1:3] = bytes(addr_info['offset_bytes']) # Address (2 bytes)
            area_data[3] = 0x00                               # Bit position (1 byte)
            
            area_hex = area_data.hex().upper()
            print(f"       📦 {address} -> 4 bytes: {area_hex}")
            hex_parts.append(f"{address}({area_hex})")
            data_part += area_data
        
        print()
        print("🔥 HEX BREAKDOWN:")
        print(f"   🎯 Command Format: 0104 + {' + '.join([part.split('(')[0] for part in hex_parts])}")
        print(f"   🔧 Command Code (0x0104): 0104")
        for part in hex_parts:
            addr, hex_val = part.split('(')
            hex_val = hex_val.rstrip(')')
            print(f"   📦 {addr}: {hex_val}")
        print(f"   ✨ Final Hex Pattern: 0104{data_part.hex().upper()}")
        print(f"   🔧 Complete data part ({len(data_part)} bytes): {data_part.hex().upper()}")
        
        # Build FINS command frame with 0x0104 command code
        command_frame = fins.fins_command_frame(
            command_code=command_codes.MULTIPLE_MEMORY_AREA_READ,  # 0x0104
            text=data_part,
            service_id=b'\x00'
        )
        
        print(f"📤 Complete command frame: {command_frame.hex()}")
        print(f"   🔧 Header (10 bytes): {command_frame[:10].hex()}")
        print(f"   📋 Command code (2 bytes): {command_frame[10:12].hex()}")
        print(f"   📦 Data part ({len(data_part)} bytes): {command_frame[12:].hex()}")
        
        response = await fins.execute_fins_command_frame(command_frame)
        response_frame = FinsResponseFrame()
        response_frame.from_bytes(response)
        
        print(f"📥 Raw response: {response.hex()}")
        print(f"   🔧 Response header: {response[:10].hex()}")
        print(f"   📋 Response command: {response[10:12].hex()}")
        print(f"   ⚡ End code: {response[12:14].hex()}")
        print(f"   📦 Response data: {response[14:].hex()}")
        
        if response_frame.end_code == b'\x00\x00':
            raw_data = response_frame.text
            print("✅ SUCCESS - Values read:")
            print(f"📦 Response format analysis:")
            print(f"   🔍 Total response data: {raw_data.hex().upper()} ({len(raw_data)} bytes)")
            
            # Parse each address: 1 status byte + 2 data bytes (3 bytes total per address)
            for i, (address, data_type) in enumerate(addresses_dict.items()):
                start_idx = i * 3  # Each address takes 3 bytes: 1 status + 2 data
                
                # Extract status byte and data bytes
                status_byte = raw_data[start_idx:start_idx + 1]
                value_bytes = raw_data[start_idx + 1:start_idx + 3]  # Skip status byte, get 2 data bytes
                
                print(f"   🎯 {address} parsing:")
                print(f"       📍 Byte position: {start_idx}-{start_idx + 2}")
                print(f"       🚦 Status byte: {status_byte.hex().upper()} (area code: 0x{status_byte[0]:02X})")
                print(f"       📦 Data bytes: {value_bytes.hex().upper()}")
                
                if data_type == "INT16":
                    value = toInt16(value_bytes)[0]
                elif data_type == "UINT16":
                    value = toUInt16(value_bytes)[0]
                else:
                    value = toInt16(value_bytes)[0]  # Default to INT16
                
                hex_val = WordToHex(value_bytes)[0]
                print(f"       📊 Final value: {value} (0x{hex_val})")
                print(f"   📊 {address}: {value} (0x{hex_val}) [status: {status_byte.hex()}, data: {value_bytes.hex()}]")
            
            return True
        else:
            print(f"❌ ERROR - End code: {response_frame.end_code.hex()}")
            return False
            
    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return False
    finally:
        print()

async def main():
    """Main test function"""
    print("🚀 FINS Protocol Direct Command Frame Testing")
    print("=" * 60)
    print(f"🌐 PLC IP Address: {PLC_IP}")
    print(f"⏰ Test started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print()
    
    fins = None
    try:
        # Initialize connection
        print("🔌 Initializing FINS UDP Connection...")
        fins = FinsUdpConnection(PLC_IP, debug=True)
        await fins.connect()
        print("✅ Connection initialized successfully")
        print()
        
        # Test results tracking
        tests_passed = 0
        total_tests = 0
        
        # # Test 1: CPU Details (Connection verification)
        # total_tests += 1
        # if await test_cpu_details(fins):
        #     tests_passed += 1
        
        # # Test 2: CPU Status
        # total_tests += 1
        # if await test_cpu_status(fins):
        #     tests_passed += 1
        
        # # Test 3: Single memory reads
        # for address, data_type in TEST_ADDRESSES.items():
        #     total_tests += 1
        #     if await test_single_memory_read(fins, address, data_type):
        #         tests_passed += 1
        
        # # # Test 4: Batch read
        # # total_tests += 1
        # # if await test_batch_memory_read(fins, BATCH_START_ADDRESS, BATCH_READ_COUNT):
        # #     tests_passed += 1
        
        # # Test 5: Multiple memory read - D100,D150,D200,D250,D300,D350,D400,D450,D500
        # total_tests += 1
        starttime = datetime.now()
        #round1
        if await test_multiple_memory_read(fins, MULTIPLE_READ_ADDRESSES):
            tests_passed += 1
        #round2 
        if await test_multiple_memory_read(fins, MULTIPLE_READ_ADDRESSES):
            tests_passed += 1
        #round3 
        if await test_multiple_memory_read(fins, MULTIPLE_READ_ADDRESSES):
            tests_passed += 1
        #round4 
        if await test_multiple_memory_read(fins, MULTIPLE_READ_ADDRESSES):
            tests_passed += 1
        # #round5 
        # if await test_multiple_memory_read(fins, MULTIPLE_READ_ADDRESSES):
        #     tests_passed += 1 
        endtime = datetime.now()
        print(f"=====Total time to execute{(endtime - starttime).total_seconds()} =======") 
        # # Summary
        # print("📊 TEST SUMMARY")
        # print("=" * 60)
        # print(f"✅ Tests Passed: {tests_passed}/{total_tests}")
        # print(f"❌ Tests Failed: {total_tests - tests_passed}/{total_tests}")
        # success_rate = (tests_passed / total_tests) * 100 if total_tests > 0 else 0
        # print(f"📈 Success Rate: {success_rate:.1f}%")
        
        # if tests_passed == total_tests:
        #     print("🎉 ALL TESTS PASSED! Your FINS communication is working perfectly.")
        # elif tests_passed > 0:
        #     print("⚠️  Some tests passed. Check failed tests for potential issues.")
        # else:
        #     print("🚨 ALL TESTS FAILED. Check your PLC connection and addresses.")
            
    except FinsConnectionError as e:
        print(f"🚨 CONNECTION ERROR: {e}")
        print("💡 Check if:")
        print("   - PLC IP address is correct")
        print("   - PLC is powered on and connected to network")
        print("   - Firewall is not blocking UDP port 9600")
        
    except FinsTimeoutError as e:
        print(f"⏰ TIMEOUT ERROR: {e}")
        print("💡 PLC might be busy or network is slow")
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {e}")
        
    finally:
        if fins:
            await fins.disconnect()
            print("🔌 Connection closed")
        
        print(f"⏰ Test completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
if __name__ == "__main__":
    print("Starting FINS Protocol Test...")
    print("Press Ctrl+C to interrupt")
    print()
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
    except Exception as e:
        print(f"\n💥 Fatal error: {e}")
        sys.exit(1)