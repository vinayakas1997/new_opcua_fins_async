#!/usr/bin/env python3
"""
FINS Protocol Timing Test - D0001 to D500
==========================================
This script performs timing analysis comparing single read vs multiple read operations
for PLC registers D0001 to D500.
"""

import asyncio
import sys
from datetime import datetime
import time
import csv

# Import the FINS protocol components
from OMRON_FINS_PROTOCOL.Infrastructure.udp_connection import FinsUdpConnection
from OMRON_FINS_PROTOCOL.Fins_domain.mem_address_parser import FinsAddressParser
from OMRON_FINS_PROTOCOL.components.conversion import (
    toInt16, toUInt16, WordToHex
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

# Batch configuration
BATCH_SIZE = 20  # Number of addresses per batch for multiple read

# Generate addresses from D0001 to D500
def generate_test_addresses():
    """Generate D0001 to D500 addresses"""
    addresses = {}
    for i in range(1, 501):  # D0001 to D500
        addr = f"D{i:04d}"  # Format as D0001, D0002, etc.
        addresses[addr] = "INT16"
    return addresses

# Test addresses D0001 to D500
TEST_ADDRESSES = generate_test_addresses()

print(f"📊 D0001からD500まで{len(TEST_ADDRESSES)}個のテストアドレスを生成しました")

# =============================================================================
# TEST FUNCTIONS
# =============================================================================

async def single_read_address(fins, address, data_type):
    """Read a single address and return success status and timing"""
    try:
        address_parser = FinsAddressParser()
        command_codes = fins.command_codes
        
        # Parse address to FINS format
        addr_info = address_parser.parse(address)
        
        # Determine read size based on data type
        read_size = 1  # Single word for INT16
        
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
        
        # Execute with timing
        start_time = time.perf_counter()
        response = await fins.execute_fins_command_frame(command_frame)
        end_time = time.perf_counter()
        
        # Parse response
        response_frame = FinsResponseFrame()
        response_frame.from_bytes(response)
        
        if response_frame.end_code == b'\x00\x00':
            raw_data = response_frame.text
            value = toInt16(raw_data)[0]
            return True, end_time - start_time, value
        else:
            return False, end_time - start_time, None
            
    except Exception as e:
        return False, 0, None

async def multiple_read_address(fins, addresses_dict):
    """Read multiple addresses in a single command and return timing info"""
    try:
        address_parser = FinsAddressParser()
        command_codes = fins.command_codes
        
        # Build command data for multiple read
        addresses = list(addresses_dict.keys())
        
        data_part = bytearray()
        
        for address in addresses:
            addr_info = address_parser.parse(address)
            
            # Build 4-byte structure for each address:
            # Byte 1: Memory area code
            # Bytes 2-3: Address (2 bytes)
            # Byte 4: Bit position (0x00 for word access)
            area_data = bytearray(4)
            area_data[0] = addr_info['memory_type_code']      # Area code (1 byte)
            area_data[1:3] = bytes(addr_info['offset_bytes']) # Address (2 bytes)
            area_data[3] = 0x00                               # Bit position (1 byte)
            
            data_part += area_data
        
        # Build FINS command frame with 0x0104 command code
        command_frame = fins.fins_command_frame(
            command_code=command_codes.MULTIPLE_MEMORY_AREA_READ,  # 0x0104
            text=data_part,
            service_id=b'\x00'
        )
        
        # Execute with timing
        start_time = time.perf_counter()
        response = await fins.execute_fins_command_frame(command_frame)
        end_time = time.perf_counter()
        
        response_frame = FinsResponseFrame()
        response_frame.from_bytes(response)
        
        if response_frame.end_code == b'\x00\x00':
            raw_data = response_frame.text
            values = {}
            
            # Parse each address: 1 status byte + 2 data bytes (3 bytes total per address)
            for i, (address, data_type) in enumerate(addresses_dict.items()):
                start_idx = i * 3  # Each address takes 3 bytes: 1 status + 2 data
                
                # Extract status byte and data bytes
                status_byte = raw_data[start_idx:start_idx + 1]
                value_bytes = raw_data[start_idx + 1:start_idx + 3]  # Skip status byte, get 2 data bytes
                
                if data_type == "INT16":
                    value = toInt16(value_bytes)[0]
                elif data_type == "UINT16":
                    value = toUInt16(value_bytes)[0]
                else:
                    value = toInt16(value_bytes)[0]  # Default to INT16
                
                values[address] = value
            
            return True, end_time - start_time, values
        else:
            return False, end_time - start_time, None
            
    except Exception as e:
        return False, 0, None

async def batch_read_addresses(fins, addresses_dict, batch_size=20):
    """Read addresses in batches using multiple read commands"""
    try:
        total_start_time = time.perf_counter()
        all_values = {}
        total_command_time = 0
        successful_batches = 0
        failed_batches = 0
        
        # Convert addresses_dict to list for batching
        addresses_list = list(addresses_dict.items())
        total_batches = (len(addresses_list) + batch_size - 1) // batch_size  # Ceiling division
        
        print(f"📦 {len(addresses_list)}個のアドレスを{batch_size}個ずつ{total_batches}バッチで処理します")
        
        # Process addresses in batches
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(addresses_list))
            batch_addresses = dict(addresses_list[start_idx:end_idx])
            
            print(f"   📊 バッチ {batch_num + 1}/{total_batches} を処理中 ({len(batch_addresses)}個のアドレス)")
            
            # Perform multiple read for this batch
            success, command_time, batch_values = await multiple_read_address(fins, batch_addresses)
            
            if success and batch_values:
                successful_batches += 1
                total_command_time += command_time
                all_values.update(batch_values)
                print(f"      ✅ バッチ {batch_num + 1} 成功 ({len(batch_values)}個の値)")
            else:
                failed_batches += 1
                print(f"      ❌ バッチ {batch_num + 1} 失敗")
        
        total_end_time = time.perf_counter()
        total_execution_time = total_end_time - total_start_time
        
        return {
            'success': successful_batches > 0,
            'total_addresses': len(addresses_list),
            'addresses_read': len(all_values),
            'successful_batches': successful_batches,
            'failed_batches': failed_batches,
            'total_batches': total_batches,
            'batch_size': batch_size,
            'total_execution_time': total_execution_time,
            'total_command_time': total_command_time,
            'avg_batch_time': total_command_time / successful_batches if successful_batches > 0 else 0,
            'addresses_per_second': len(all_values) / total_execution_time if total_execution_time > 0 else 0,
            'values': all_values
        }
        
    except Exception as e:
        print(f"❌ バッチ読み取り例外: {e}")
        return {
            'success': False,
            'total_addresses': len(addresses_dict),
            'addresses_read': 0,
            'successful_batches': 0,
            'failed_batches': 0,
            'total_batches': 0,
            'batch_size': batch_size,
            'total_execution_time': 0,
            'total_command_time': 0,
            'avg_batch_time': 0,
            'addresses_per_second': 0,
            'values': {}
        }

async def test_batch_read_timing(fins, addresses_dict, batch_size=20):
    """Test reading addresses in batches and measure timing"""
    print(f"📦 バッチ読み取りパフォーマンステスト (バッチサイズ: {batch_size})")
    print("=" * 60)
    print(f"📊 {len(addresses_dict)}個のアドレスを{batch_size}個ずつバッチで読み取ります...")
    
    batch_results = await batch_read_addresses(fins, addresses_dict, batch_size)
    
    print("📊 バッチ読み取り結果:")
    print("=" * 60)
    
    if batch_results['success']:
        print(f"✅ {batch_results['addresses_read']}/{batch_results['total_addresses']}個のアドレスの読み取りに成功しました")
        print(f"📦 成功したバッチ: {batch_results['successful_batches']}/{batch_results['total_batches']}")
        print(f"❌ 失敗したバッチ: {batch_results['failed_batches']}")
        print(f"📏 バッチサイズ: バッチあたり{batch_results['batch_size']}個のアドレス")
        print(f"⏱️  総実行時間: {batch_results['total_execution_time']:.4f}秒")
        print(f"⏱️  総コマンド時間: {batch_results['total_command_time']:.4f}秒")
        print(f"⏱️  平均バッチ時間: {batch_results['avg_batch_time']:.6f}秒")
        print(f"📈 1秒あたりの読み取りアドレス数: {batch_results['addresses_per_second']:.2f}")
        
        # Show sample values
        if batch_results['values']:
            addr_list = list(batch_results['values'].keys())
            print(f"📋 サンプル値 (最初の10個):")
            for addr in addr_list[:10]:
                print(f"   📊 {addr}: {batch_results['values'][addr]}")
            print(f"📋 サンプル値 (最後の10個):")
            for addr in addr_list[-10:]:
                print(f"   📊 {addr}: {batch_results['values'][addr]}")
    else:
        print("❌ バッチ読み取りに失敗しました")
    
    print()
    return batch_results

async def test_single_reads_timing(fins, addresses_dict):
    """Test reading all addresses one by one and measure timing"""
    print("🔍 単一読み取りパフォーマンステスト")
    print("=" * 60)
    print(f"📊 {len(addresses_dict)}個のアドレスを個別に読み取ります...")
    
    total_start_time = time.perf_counter()
    successful_reads = 0
    failed_reads = 0
    total_read_time = 0
    min_time = float('inf')
    max_time = 0
    single_values = {}  # Store values for CSV comparison
    
    # Read each address individually
    for i, (address, data_type) in enumerate(addresses_dict.items(), 1):
        success, read_time, value = await single_read_address(fins, address, data_type)
        
        if success:
            successful_reads += 1
            total_read_time += read_time
            min_time = min(min_time, read_time)
            max_time = max(max_time, read_time)
            single_values[address] = value  # Store value for CSV
            
            # Print progress every 50 reads
            if i % 50 == 0:
                print(f"   📈 進捗: {i}/{len(addresses_dict)} 読み取り完了")
        else:
            failed_reads += 1
            single_values[address] = None  # Mark failed reads
    
    total_end_time = time.perf_counter()
    total_execution_time = total_end_time - total_start_time
    
    # Calculate statistics
    avg_read_time = total_read_time / successful_reads if successful_reads > 0 else 0
    
    print("📊 単一読み取り結果:")
    print("=" * 60)
    print(f"✅ 成功した読み取り: {successful_reads}")
    print(f"❌ 失敗した読み取り: {failed_reads}")
    print(f"⏱️  総実行時間: {total_execution_time:.4f}秒")
    print(f"⏱️  総読み取り時間(合計): {total_read_time:.4f}秒")
    print(f"⏱️  平均読み取り時間: {avg_read_time:.6f}秒")
    print(f"⏱️  最小読み取り時間: {min_time:.6f}秒")
    print(f"⏱️  最大読み取り時間: {max_time:.6f}秒")
    print(f"📈 1秒あたりの読み取り数: {successful_reads / total_execution_time:.2f}")
    print()
    
    return {
        'successful_reads': successful_reads,
        'failed_reads': failed_reads,
        'total_execution_time': total_execution_time,
        'total_read_time': total_read_time,
        'avg_read_time': avg_read_time,
        'min_read_time': min_time if min_time != float('inf') else 0,
        'max_read_time': max_time,
        'reads_per_second': successful_reads / total_execution_time if total_execution_time > 0 else 0,
        'values': single_values  # Include values for CSV export
    }

async def test_multiple_read_timing(fins, addresses_dict):
    """Test reading all addresses in a single command and measure timing"""
    print("📚 複数読み取りパフォーマンステスト")
    print("=" * 60)
    print(f"📊 {len(addresses_dict)}個のアドレスを1つのコマンドで読み取ります...")
    
    total_start_time = time.perf_counter()
    
    # Perform multiple read
    success, read_time, values = await multiple_read_address(fins, addresses_dict)
    
    total_end_time = time.perf_counter()
    total_execution_time = total_end_time - total_start_time
    
    print("📊 複数読み取り結果:")
    print("=" * 60)
    
    if success and values is not None:
        print(f"✅ {len(values)}個のアドレスの読み取りに成功しました")
        print(f"⏱️  総実行時間: {total_execution_time:.4f}秒")
        print(f"⏱️  コマンド実行時間: {read_time:.6f}秒")
        print(f"📈 1秒あたりの読み取りアドレス数: {len(values) / total_execution_time:.2f}")
        print(f"⚡ 速度優位性: アドレスあたり{len(values) / read_time:.0f}倍高速")
        
        # Show first 10 and last 10 values as sample
        addr_list = list(values.keys())
        print(f"📋 サンプル値 (最初の10個):")
        for addr in addr_list[:10]:
            print(f"   📊 {addr}: {values[addr]}")
        print(f"📋 サンプル値 (最後の10個):")
        for addr in addr_list[-10:]:
            print(f"   📊 {addr}: {values[addr]}")
            
        result = {
            'success': True,
            'addresses_read': len(values),
            'total_execution_time': total_execution_time,
            'command_execution_time': read_time,
            'addresses_per_second': len(values) / total_execution_time
        }
    else:
        print("❌ 複数読み取りに失敗しました")
        result = {
            'success': False,
            'addresses_read': 0,
            'total_execution_time': total_execution_time,
            'command_execution_time': 0,
            'addresses_per_second': 0
        }
    
    print()
    return result

def compare_performance(single_results, multiple_results, batch_results=None):
    """Compare the performance of single vs batch reads"""
    print("⚡ パフォーマンス比較")
    print("=" * 60)
    
    if single_results['successful_reads'] > 0:
        single_rate = single_results['reads_per_second']
        
        print(f"📊 単一読み取り方式:")
        print(f"   ⏱️  総時間: {single_results['total_execution_time']:.4f}秒")
        print(f"   📈 レート: {single_rate:.2f}アドレス/秒")
        print(f"   📊 成功率: {single_results['successful_reads'] / (single_results['successful_reads'] + single_results['failed_reads']) * 100:.1f}%")
        print()
        
        # Compare batch results if provided
        if batch_results and batch_results['success']:
            batch_rate = batch_results['addresses_per_second']
            
            print(f"📦 バッチ読み取り方式:")
            print(f"   ⏱️  総時間: {batch_results['total_execution_time']:.4f}秒")
            print(f"   📈 レート: {batch_rate:.2f}アドレス/秒")
            print(f"   📊 バッチサイズ: {batch_results['batch_size']}アドレス")
            print(f"   📈 成功率: {batch_results['successful_batches']}/{batch_results['total_batches']} ({batch_results['successful_batches']/batch_results['total_batches']*100:.1f}%)")
            print()
            
            if single_rate > 0:
                speed_improvement = batch_rate / single_rate
                time_reduction = ((single_results['total_execution_time'] - batch_results['total_execution_time']) / single_results['total_execution_time']) * 100
                
                print(f"🚀 パフォーマンス向上:")
                print(f"   ⚡ 速度改善: {speed_improvement:.1f}倍高速")
                print(f"   ⏱️  時間短縮: {time_reduction:.1f}%")
                print(f"   💰 効率向上: {((speed_improvement - 1) * 100):.1f}%効率的")
                
                if speed_improvement > 10:
                    print(f"   🏆 優秀: バッチ読み取りは大幅に高速です！")
                elif speed_improvement > 5:
                    print(f"   👍 良好: バッチ読み取りは良好なパフォーマンス改善を示しています")
                elif speed_improvement > 2:
                    print(f"   ✅ 中程度: バッチ読み取りは中程度に高速です")
                else:
                    print(f"   ⚠️  最小限: パフォーマンス改善は限定的です")
        else:
            print("❌ バッチ読み取りテストが失敗したか、利用できません")
    else:
        print("❌ 比較を実行できません - 単一読み取りテストが失敗しました")
    
    print()

def create_data_verification_csv(single_results, batch_results, default_filename="data_500.csv"):
    """Create a CSV file to compare values from both reading methods"""
    print("📝 データ検証CSVファイルを作成中...")
    print("=" * 60)
    current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{current_timestamp}_{default_filename}"
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            writer.writerow(['plc_reg', 'method_1', 'method_2'])
            
            # Get all addresses (should be the same for both methods)
            all_addresses = set()
            if 'values' in single_results:
                all_addresses.update(single_results['values'].keys())
            if batch_results.get('values'):
                all_addresses.update(batch_results['values'].keys())
            
            # Sort addresses for consistent ordering
            sorted_addresses = sorted(all_addresses, key=lambda x: int(x[1:]))  # Sort by numeric part
            
            # Write data rows
            matches = 0
            mismatches = 0
            single_missing = 0
            batch_missing = 0
            
            for address in sorted_addresses:
                single_value = single_results.get('values', {}).get(address, 'N/A')
                batch_value = batch_results.get('values', {}).get(address, 'N/A')
                
                # Convert None values to 'FAILED' for better readability
                if single_value is None:
                    single_value = 'FAILED'
                    single_missing += 1
                if batch_value is None:
                    batch_value = 'FAILED'
                    batch_missing += 1
                
                # Count matches/mismatches
                if single_value != 'N/A' and batch_value != 'N/A' and single_value != 'FAILED' and batch_value != 'FAILED':
                    if single_value == batch_value:
                        matches += 1
                    else:
                        mismatches += 1
                
                writer.writerow([address, single_value, batch_value])
        
        print(f"✅ CSVファイル '{filename}' が正常に作成されました！")
        print(f"📊 データ検証サマリー:")
        print(f"   📋 総アドレス数: {len(sorted_addresses)}")
        print(f"   ✅ 一致する値: {matches}")
        print(f"   ❌ 不一致の値: {mismatches}")
        print(f"   🚫 単一読み取り失敗: {single_missing}")
        print(f"   🚫 バッチ読み取り失敗: {batch_missing}")
        
        if mismatches == 0:
            print(f"   🎉 完璧: 読み取りに成功したすべての値が両方の方式で一致しています！")
        elif mismatches > 0:
            print(f"   ⚠️  警告: {mismatches}個の値が方式間で異なります - PLCデータの安定性を確認してください")
        
        print(f"   💾 ファイル保存先: {filename}")
        print()
        
        return True
        
    except Exception as e:
        print(f"❌ CSVファイルの作成エラー: {e}")
        return False


async def main():
    """Main test function"""
    print("🚀 FINSプロトコルパフォーマンステスト - D0001からD500")
    print("=" * 60)
    print(f"🌐 PLC IPアドレス: {PLC_IP}")
    print(f"📊 テストアドレス: {len(TEST_ADDRESSES)}個 (D0001からD500)")
    print(f"⏰ テスト開始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print()
    
    fins = None
    try:
        # Initialize connection
        print("🔌 FINS UDP接続を初期化中...")
        fins = FinsUdpConnection(PLC_IP, debug=False)  # Disable debug for timing tests
        await fins.connect()
        print("✅ 接続の初期化に成功しました")
        print()
        
        # Test 1: Single reads timing
        print("🔍 フェーズ1: 単一読み取りパフォーマンステスト")
        print("=" * 60)
        single_results = await test_single_reads_timing(fins, TEST_ADDRESSES)
        
        # Test 2: Batch read timing
        print("📦 フェーズ2: バッチ読み取りパフォーマンステスト")
        print("=" * 60)
        batch_results = await test_batch_read_timing(fins, TEST_ADDRESSES, BATCH_SIZE)
        
        # Compare performance
        compare_performance(single_results, None, batch_results)
        
        # Final summary
        print("📊 最終サマリー")
        print("=" * 60)
        print(f"📋 テストした総アドレス数: {len(TEST_ADDRESSES)}")
        print(f"⏰ テスト完了: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if single_results['successful_reads'] > 0:
            print("🎉 パフォーマンステストが正常に完了しました！")
            
        
        # Create CSV file for data verification
        print("📝 データ検証")
        print("=" * 60)
        csv_success = create_data_verification_csv(single_results, batch_results, "data_500.csv")
        
        if csv_success:
            print("✅ データ検証CSVが正常に作成されました")
        else:
            print("❌ データ検証CSVの作成に失敗しました")
            
    except FinsConnectionError as e:
        print(f"🚨 接続エラー: {e}")
        print("💡 以下を確認してください:")
        print("   - PLC IPアドレスが正しいか")
        print("   - PLCが電源オンでネットワークに接続されているか")
        print("   - ファイアウォールがUDPポート9600をブロックしていないか")
        
    except FinsTimeoutError as e:
        print(f"⏰ タイムアウトエラー: {e}")
        print("💡 PLCがビジー状態か、ネットワークが遅い可能性があります")
        
    except Exception as e:
        print(f"❌ 予期しないエラー: {e}")
        
    finally:
        if fins:
            await fins.disconnect()
            print("🔌 接続を閉じました")
        
        print(f"⏰ テスト完了: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    print("FINSパフォーマンステストを開始します...")
    print("Ctrl+Cで中断できます")
    print()
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 テストがユーザーによって中断されました")
    except Exception as e:
        print(f"\n💥 致命的なエラー: {e}")
        sys.exit(1)