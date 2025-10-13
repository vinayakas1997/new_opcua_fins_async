import re

class AddressGroupChecker:
    """
    A simple class to demonstrate address grouping functionality
    without PLC connections, OPC UA, or logging.
    """
    
    def __init__(self, address_mappings):
        """
        Initialize with address mappings
        
        Args:
            address_mappings: List of dictionaries containing address mapping info
        """
        self.address_mappings = address_mappings
    
    def group_consecutive_addresses(self, mappings):
        """Group consecutive addresses of the same data_type for batch reading."""
        groups = []
        current_group = []

        for mapping in mappings:
            plc_address = mapping['plc_reg_add']
            data_type = mapping.get('data_type', 'int16').upper()

            # Skip HEARTBEAT
            if plc_address == "HEARTBEAT":
                continue

            # Normalize data types special cases 
            if data_type in ['BOOL', 'CHANNEL']:
                data_type = 'INT16'

            # Try to extract base address and number
            match = re.match(r'^([A-Z]+)(\d+)$', plc_address)
            if not match:
                # Non-consecutive address, start new group
                if current_group:
                    groups.append(current_group)
                current_group = [mapping]
                continue

            base, num_str = match.groups()
            try:
                num = int(num_str)
            except ValueError:
                # Invalid number, treat as non-consecutive
                if current_group:
                    groups.append(current_group)
                current_group = [mapping]
                continue

            # Check if can extend current group
            if current_group:
                last_mapping = current_group[-1]
                last_addr = last_mapping['plc_reg_add']
                last_match = re.match(r'^([A-Z]+)(\d+)$', last_addr)
                if last_match:
                    last_base, last_num_str = last_match.groups()
                    try:
                        last_num = int(last_num_str)
                        last_data_type = last_mapping.get('data_type', 'int16').upper()
                        if last_data_type in ['BOOL', 'CHANNEL']:
                            last_data_type = 'INT16'

                        # Same base, consecutive numbers, same data type
                        if (base == last_base and
                            num == last_num + 1 and
                            data_type == last_data_type):
                            current_group.append(mapping)
                            continue
                    except ValueError:
                        pass

            # Start new group
            if current_group:
                groups.append(current_group)
            current_group = [mapping]

        if current_group:
            groups.append(current_group)

        return groups

    def batch_individual_addresses(self, mappings, batch_size=10):
        """Batch individual addresses into groups for multiple_read."""
        batches = []
        for i in range(0, len(mappings), batch_size):
            batch = mappings[i:i + batch_size]
            batches.append(batch)
        return batches

    def analyze_and_print_grouping(self):
        """Analyze address mappings and print grouping results."""
        print("=" * 60)
        print("ADDRESS GROUPING ANALYSIS")
        print("=" * 60)
        
        # Print original mappings
        print("\n1. ORIGINAL ADDRESS MAPPINGS:")
        print("-" * 40)
        for i, mapping in enumerate(self.address_mappings, 1):
            plc_addr = mapping['plc_reg_add']
            opcua_addr = mapping['opcua_reg_add']
            data_type = mapping.get('data_type', 'int16')
            print(f"  {i:2}. PLC: {plc_addr:10} | OPC UA: {opcua_addr:15} | Type: {data_type}")
        
        # Separate regular mappings from HEARTBEAT
        regular_mappings = [m for m in self.address_mappings if m['plc_reg_add'] != "HEARTBEAT"]
        heartbeat_mappings = [m for m in self.address_mappings if m['plc_reg_add'] == "HEARTBEAT"]
        
        print(f"\n2. MAPPING STATISTICS:")
        print("-" * 40)
        print(f"  Total mappings: {len(self.address_mappings)}")
        print(f"  Regular mappings: {len(regular_mappings)}")
        print(f"  Heartbeat mappings: {len(heartbeat_mappings)}")
        
        # Group consecutive addresses
        consecutive_groups = self.group_consecutive_addresses(regular_mappings)
        
        print(f"\n3. CONSECUTIVE ADDRESS GROUPS:")
        print("-" * 40)
        print(f"  Number of groups formed: {len(consecutive_groups)}")
        
        batch_operations = 0
        individual_operations = 0
        
        for i, group in enumerate(consecutive_groups, 1):
            print(f"\n  Group {i}: ({len(group)} addresses)")
            
            if len(group) == 1:
                individual_operations += 1
                print(f"    → INDIVIDUAL READ operation")
            else:
                batch_operations += 1
                print(f"    → BATCH READ operation")
            
            for j, mapping in enumerate(group):
                plc_addr = mapping['plc_reg_add']
                opcua_addr = mapping['opcua_reg_add']
                data_type = mapping.get('data_type', 'int16')
                print(f"      {j+1}. {plc_addr:10} | {opcua_addr:15} | {data_type}")
        
        print(f"\n4. OPERATION SUMMARY:")
        print("-" * 40)
        print(f"  Batch operations: {batch_operations}")
        print(f"  Individual operations: {individual_operations}")
        print(f"  Total operations: {batch_operations + individual_operations}")
        
        # Calculate efficiency
        original_operations = len(regular_mappings)
        optimized_operations = batch_operations + individual_operations
        efficiency = ((original_operations - optimized_operations) / original_operations * 100) if original_operations > 0 else 0
        
        print(f"  Efficiency gain: {efficiency:.1f}%")
        print(f"  (Reduced from {original_operations} to {optimized_operations} operations)")
        
        # Show individual address batching example
        print(f"\n5. INDIVIDUAL ADDRESS BATCHING EXAMPLE:")
        print("-" * 40)
        individual_mappings = [group[0] for group in consecutive_groups if len(group) == 1]
        if individual_mappings:
            batches = self.batch_individual_addresses(individual_mappings, batch_size=5)
            print(f"  Individual addresses can be batched into {len(batches)} groups:")
            for i, batch in enumerate(batches, 1):
                print(f"    Batch {i}: {len(batch)} addresses")
                for mapping in batch:
                    print(f"      - {mapping['plc_reg_add']}")
        else:
            print("  No individual addresses found for batching example.")
        
        print("\n" + "=" * 60)

def create_sample_mappings():
    """Create sample address mappings for demonstration."""
    return [
        {'plc_reg_add': 'D100', 'opcua_reg_add': 'Temperature1', 'data_type': 'int16'},
        {'plc_reg_add': 'D101', 'opcua_reg_add': 'Temperature2', 'data_type': 'int16'},
        {'plc_reg_add': 'D102', 'opcua_reg_add': 'Temperature3', 'data_type': 'int16'},
        {'plc_reg_add': 'D105', 'opcua_reg_add': 'Pressure1', 'data_type': 'int16'},
        {'plc_reg_add': 'D106', 'opcua_reg_add': 'Pressure2', 'data_type': 'int16'},
        {'plc_reg_add': 'W200', 'opcua_reg_add': 'Status1', 'data_type': 'bool'},
        {'plc_reg_add': 'W201', 'opcua_reg_add': 'Status2', 'data_type': 'bool'},
        {'plc_reg_add': 'W202', 'opcua_reg_add': 'Status3', 'data_type': 'bool'},
        {'plc_reg_add': 'D300', 'opcua_reg_add': 'Flow_Rate', 'data_type': 'float32'},
        {'plc_reg_add': 'D302', 'opcua_reg_add': 'Volume', 'data_type': 'float32'},
        {'plc_reg_add': 'D400', 'opcua_reg_add': 'Counter1', 'data_type': 'int32'},
        {'plc_reg_add': 'D402', 'opcua_reg_add': 'Counter2', 'data_type': 'int32'},
        {'plc_reg_add': 'D500', 'opcua_reg_add': 'Setpoint1', 'data_type': 'int16'},
        {'plc_reg_add': 'H100', 'opcua_reg_add': 'HoldingReg1', 'data_type': 'int16'},
        {'plc_reg_add': 'H101', 'opcua_reg_add': 'HoldingReg2', 'data_type': 'int16'},
        {'plc_reg_add': 'HEARTBEAT', 'opcua_reg_add': 'PLC_Heartbeat', 'data_type': 'bool'},
    ]

def main():
    """Main function to demonstrate address grouping."""
    print("ADDRESS GROUP CHECKER")
    print("This script demonstrates address grouping without PLC or OPC UA connections.")
    print()
    
    # Create sample mappings
    sample_mappings = create_sample_mappings()
    
    # Create address group checker
    checker = AddressGroupChecker(sample_mappings)
    
    # Analyze and print grouping results
    checker.analyze_and_print_grouping()
    
    print("\nScript completed successfully!")

if __name__ == "__main__":
    main()