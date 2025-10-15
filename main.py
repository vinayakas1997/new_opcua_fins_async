from OMRON_FINS_PROTOCOL.Infrastructure.udp_connection import FinsUdpConnection
from OMRON_FINS_PROTOCOL.exception import *
from OMRON_FINS_PROTOCOL.components.data_type_mapping import DATA_TYPE_MAPPING
from opcua import Client
import logging
import logging.config
import json
from opcua_json import OpcuaAutoNodeMapper
import asyncio
from asyncio import Event as AsyncEvent
import re
import csv
from datetime import datetime
import os
import signal


class PLCTask:
    def __init__(self, plc_details, queue, reload=False, csv_enabled=False, sleep_interval=0.01):
        #initialize the async task
        self.name = plc_details['plc_name']
        self.plc_ip = plc_details['plc_ip']
        self.opcua_url = plc_details['opcua_url']
        self.address_mappings = plc_details['address_mappings']
        # special opcua running tag
        self.plc_running_tag=None

        self.queue = queue
        self.reload = reload
        self.csv_enabled = csv_enabled
        self.sleep_interval = plc_details.get('sleep_interval', sleep_interval)  # Use config value or default
        #derieved variables
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.INFO)
        

        # adding the file handler to the logger
        file_handler = logging.FileHandler(f"logs/{self.name}.log")
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # Use shared JSON file from the OPC UA node manager container
        self.opcua_json_file_name = "opcua_json_files/nodes.json"

        # missing counts
        self.threshold = 3
        self.failed_to_read= 0
        self.failed_to_push= 0
        
        # OPC UA connection status tracking
        self.opcua_connected = False
        self.opcua_reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 10  # seconds
        
        # CSV file setup - always create for fallback storage when OPC UA is down
        # or when explicitly enabled by user
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        database_folder = "PLC_Data"
        if not os.path.exists(database_folder):
            os.makedirs(database_folder)
            self.logger.info(f"Created data base folder: {database_folder}")
            
        # Create folder with PLC name if it doesn't exist
        folder_name = os.path.join(database_folder, self.name)
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)
            self.logger.info(f"Created folder: {folder_name}")
            
        # Store CSV file in the PLC-specific folder
        self.csv_filename = os.path.join(folder_name, f"{self.name}_{timestamp}.csv")
        self.csv_file = None
        self.csv_writer = None
        self.csv_header = ['Timestamp'] + [mapping['opcua_reg_add'] for mapping in self.address_mappings]
        
        # Initialize CSV file if csv_enabled or as fallback
        if self.csv_enabled:
            self._initialize_csv_file()
            self.logger.info(f"CSV mode enabled - data will be stored in: {self.csv_filename}")
        else:
            self.logger.info(f"CSV mode disabled - will only use CSV as fallback when OPC UA is down")
        
        self._stop_event = AsyncEvent()
        
        # Background task tracking for parallel writing operations
        self.background_tasks = set()
        
        # Initialize optimized address grouping (done once, not every cycle)
        self.multiple_read_groups = []  # Groups of 20 for 1-word data types
        self.single_read_addresses = []  # Individual addresses for multi-word data types
        self._initialize_address_groups()
        
        # storing the task status
        self.logger.info(f"Initializing PLCTask(Program initialised) for {self.name} with IP {self.plc_ip} and OPC UA URL {self.opcua_url}")
        print("\n\n -----------------:::::::::::::::::::::::::-----------------")
        print("   **** PLC INFO ****")
        print(f"    PLC_Name - {self.name}")
        print(f"    PLC_IP - {self.plc_ip}")
        print(f"    OPC UA Server {self.opcua_url}")
        #print date and time
        print(f"    Date and Time - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        # print("-----------------:::::::::::::::::::::::::-----------------")
        
    def _initialize_csv_file(self):
        """Initialize CSV file and writer"""
        if self.csv_file is None:
            self.csv_file = open(self.csv_filename, 'w', newline='')
            self.csv_writer = csv.writer(self.csv_file)
            self.csv_writer.writerow(self.csv_header)
            self.logger.info(f"CSV file initialized: {self.csv_filename}")
    
    def _ensure_csv_file(self):
        """Ensure CSV file is available for fallback storage"""
        if self.csv_file is None:
            self._initialize_csv_file()
    
    async def _ensure_csv_file_async(self):
        """Ensure CSV file is available for fallback storage (async version)"""
        if self.csv_file is None:
            await self._initialize_csv_file_async()
    
    async def _initialize_csv_file_async(self):
        """Initialize CSV file and writer asynchronously"""
        if self.csv_file is None:
            # For now, we'll use synchronous file operations as they're typically fast
            # In a future enhancement, we could use aiofiles for true async file I/O
            self.csv_file = open(self.csv_filename, 'w', newline='')
            self.csv_writer = csv.writer(self.csv_file)
            self.csv_writer.writerow(self.csv_header)
            self.logger.info(f"CSV file initialized: {self.csv_filename}")
    
    # add stop and stopped methods to control the async task
    def stop(self):
        self._stop_event.set()
    def stopped(self):
        return self._stop_event.is_set()
    
    # to update the plc_value and with current timestamp
    def _update_plc_value(self,plc_value):
        #time format HH:MM:SS:MS
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S") + ":" + now.strftime("%f")[:-3]
        return plc_value + "&&" + current_time
    
    
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
            
            # Normalize data type bool to fetch the entire word
            if data_type == 'BOOL':
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
                self.logger.warning(f"Unknown data type '{data_type}' for address {plc_address}, treating as single read")
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
        
        self.logger.info(f"Address grouping initialized:")
        self.logger.info(f"  - Multiple read groups: {len(self.multiple_read_groups)} (max 20 addresses each)")
        self.logger.info(f"  - Single read addresses: {len(self.single_read_addresses)}")
        
    def _hex_bytes_to_string(self, data: bytes) -> str:
        """Convert bytes to HEX string format (0x8080 -> '8080')."""
        if not data:
            return ""
        return data.hex().upper()

    def _extract_bit_value_from_hex(self, hex_string: str, bit_number: int) -> int:
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
            
            # self.logger.debug(f"Boolean extraction: HEX:{hex_string} -> Word:{word_value:016b} -> Bit {bit_number}: {bit_value}")
            return bit_value
            
        except (ValueError, IndexError) as e:
            # self.logger.error(f"Error extracting bit {bit_number} from HEX '{hex_string}': {e}")
            return 0

    def _get_bit_number_from_address(self, plc_address: str) -> int:
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

    async def _write_to_csv_async(self, plc_values: dict, plc_cycle_ok: bool):
        """
        Write data to CSV file asynchronously.
        
        Args:
            plc_values: Dictionary of OPC UA tag -> PLC value mappings
            plc_cycle_ok: Whether any PLC read was successful (for HEARTBEAT)
        """
        try:
            # Initialize row data with timestamp
            row_data = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
            
            # Process all address mappings in original order for consistent CSV columns
            for mapping in self.address_mappings:
                plc_address = mapping['plc_reg_add']
                opcua_tag = mapping['opcua_reg_add']

                # Handle HEARTBEAT with correct plc_cycle_ok value
                if plc_address == "HEARTBEAT":
                    plc_value = plc_cycle_ok
                else:
                    plc_value = plc_values.get(opcua_tag, None)

                # Append the value to the row data (maintains column order!)
                row_data.append(str(plc_value) if plc_value is not None else 'NaN')

            # Write to CSV file if enabled or if OPC UA is down
            if self.csv_enabled or not self.opcua_connected:
                await self._ensure_csv_file_async()
                if self.csv_writer and self.csv_file:
                    self.csv_writer.writerow(row_data)
                    self.csv_file.flush()
                    self.logger.debug(f"CSV data written: {len(row_data)} columns")
                    
        except Exception as e:
            self.logger.error(f"Error writing to CSV: {e}")

    async def _write_to_opcua_async(self, opcua_manager, plc_values: dict, plc_cycle_ok: bool):
        """
        Write data to OPC UA server asynchronously.
        
        Args:
            opcua_manager: OPC UA node mapper instance
            plc_values: Dictionary of OPC UA tag -> PLC value mappings
            plc_cycle_ok: Whether any PLC read was successful (for HEARTBEAT)
        """
        if not self.opcua_connected or opcua_manager is None:
            return
            
        try:
            # Process all address mappings
            for mapping in self.address_mappings:
                plc_address = mapping['plc_reg_add']
                opcua_tag = mapping['opcua_reg_add']

                # Handle HEARTBEAT with correct plc_cycle_ok value
                if plc_address == "HEARTBEAT":
                    plc_value = plc_cycle_ok
                else:
                    plc_value = plc_values.get(opcua_tag, None)

                if plc_value is None:
                    continue  # Skip None values for OPC UA

                # Determine the value to send to OPC UA based on data type
                opcua_value = plc_value
                original_data_type = mapping.get('data_type', '').lower()

                if original_data_type == 'bool' and '.' in plc_address:
                    # Boolean type: Extract bit number and convert to 1/0
                    bit_number = self._get_bit_number_from_address(plc_address)
                    opcua_value = self._extract_bit_value_from_hex(str(plc_value), bit_number)
                    self.logger.debug(f"Boolean conversion: {plc_address} -> HEX:{plc_value} -> Bit {bit_number} -> {opcua_value}")
                elif original_data_type == 'string':
                    # String type: Add timestamp using existing function
                    opcua_value = self._update_plc_value(str(plc_value))
                    self.logger.debug(f"String with timestamp: {plc_address} -> {opcua_value}")

                # Push value to OPCUA server
                opcua_manager.write(opcua_tag, opcua_value)

        except Exception as e:
            # Log any missed data pushes and mark connection as disconnected
            self.failed_to_push += 1
            self.opcua_connected = False
            self.logger.error(f"Missed Pushing {self.failed_to_push}, Error in OPC UA batch write: {e}")
            self.logger.warning("OPC UA connection lost - switching to CSV fallback mode")

    async def _perform_plc_update_cycle(self, fins, opcua_manager):
        """Perform one cycle of reading from PLC and writing to OPC UA with optimized grouping."""
        plc_cycle_ok = False  # Track if at least one PLC read was successful
        plc_values = {}  # Store values by opcua_tag to maintain order

        # Process multiple read groups (1-word data types in batches of 20)
        for group in self.multiple_read_groups:
            # Build dictionary for multiple_read
            memory_codes_dict = {}
            for item in group:
                memory_codes_dict[item['plc_reg']] = item['data_type']
            
            try:
                multiple_result = await fins.multiple_read(memory_codes_dict)
                
                if multiple_result['status'] == 'success':
                    plc_cycle_ok = True
                    for item in group:
                        plc_address = item['plc_reg']
                        opcua_tag = item['opcua_reg']
                        data_type_orig = item['original_mapping'].get('data_type', 'int16')
                        
                        """
                        as the result in the dictinary {key = plc_reg: {data_type,value }}
                        """
                        # first checking if the plc_address is present
                        if plc_address in multiple_result['data']:
                            item_data = multiple_result['data'][plc_address]
                            if 'value' in item_data:
                                raw_bytes = item_data['value']
                                # Convert bytes to HEX string (0x8080 -> "8080")
                                if isinstance(raw_bytes, bytes):
                                    hex_string = self._hex_bytes_to_string(raw_bytes)
                                    plc_value = hex_string
                                else:
                                    plc_value = str(raw_bytes) if raw_bytes is not None else None
                            else:
                                plc_value = None
                        else:
                            plc_value = None
                        
                        plc_values[opcua_tag] = plc_value
                else:
                    # Multiple read failed, fall back to individual reads for this group
                    self.logger.warning(f"Multiple read failed for group: {multiple_result['message']}, falling back to individual reads")
                    for item in group:
                        read_success = await self._single_read_fallback(fins, item, plc_values)
                        if read_success:
                            plc_cycle_ok = True
                        
            except Exception as e:
                self.logger.error(f"Multiple read exception: {e}, falling back to individual reads")
                for item in group:
                    read_success = await self._single_read_fallback(fins, item, plc_values)
                    if read_success:
                        plc_cycle_ok = True

        # Process single read addresses (multi-word data types)
        for item in self.single_read_addresses:
            read_success = await self._single_read_fallback(fins, item, plc_values)
            if read_success:
                plc_cycle_ok = True
            
        # HYBRID WRITING PHASE: Parallel CSV and OPC UA writing with background task tracking
        
        # Wait for previous cycle's writing tasks to complete (if any)
        if self.background_tasks:
            try:
                await asyncio.gather(*self.background_tasks, return_exceptions=True)
                self.logger.debug(f"Completed {len(self.background_tasks)} background writing tasks from previous cycle")
            except Exception as e:
                self.logger.warning(f"Error waiting for background tasks: {e}")
            finally:
                self.background_tasks.clear()
        
        # Create parallel writing tasks for current cycle
        csv_task = asyncio.create_task(self._write_to_csv_async(plc_values, plc_cycle_ok))
        csv_task.set_name(f"{self.name}_csv_write")
        
        opcua_task = asyncio.create_task(self._write_to_opcua_async(opcua_manager, plc_values, plc_cycle_ok))
        opcua_task.set_name(f"{self.name}_opcua_write")
        
        # Add tasks to background set for next cycle to wait on
        self.background_tasks.add(csv_task)
        self.background_tasks.add(opcua_task)
        
        self.logger.debug(f"Started parallel writing tasks: CSV and OPC UA for {len(plc_values)} values")
        
        # Note: Tasks run in background - next read cycle can start immediately
            
    async def _single_read_fallback(self, fins, item, plc_values):
        """Helper method for single address reads with error handling."""
        plc_address = item['plc_reg']
        opcua_tag = item['opcua_reg']
        data_type = item['data_type']
        original_mapping = item['original_mapping']
        data_type_orig = original_mapping.get('data_type', 'int16')
        bool_temp = data_type_orig.lower() == 'bool'
        read_success = False
        
        try:
            pack_value = await fins.read(plc_address, data_type=data_type)
            
            if pack_value['status'] == 'success' and pack_value['data']:
                read_success = True
                raw_bytes = pack_value['data']
                # Convert bytes to HEX string (0x8080 -> "8080")
                if isinstance(raw_bytes, bytes):
                    hex_string = self._hex_bytes_to_string(raw_bytes)
                    plc_value = hex_string
                elif isinstance(raw_bytes, list) and len(raw_bytes) > 0:
                    if isinstance(raw_bytes[0], bytes):
                        hex_string = self._hex_bytes_to_string(raw_bytes[0])
                        plc_value = hex_string
                    else:
                        plc_value = str(raw_bytes[0]) if raw_bytes[0] is not None else None
                else:
                    plc_value = str(raw_bytes) if raw_bytes is not None else None
            else:
                plc_value = None

        except Exception as e:
            plc_value = None
            self.failed_to_read += 1
            self.logger.error(f"Missed reading {self.failed_to_read},\n Error reading PLC {self.name} address {plc_address}: \n{e}")
            if self.failed_to_read > self.threshold:
                self.logger.error(f"Too many errors encountered: {self.failed_to_read} reads the threshold met {self.threshold}. Closing task.")
                await self.queue.put(f"{self.name}-too many errors")
                self.stop()
                return False

        # Store the read value in plc_values dictionary
        plc_values[opcua_tag] = plc_value
        return read_success
    
    async def run(self):
        # Header for the connection status check
        print("\n")
        print("   ****Connection Status Check****")
        print("\n")

        # Step 1: establish the fins connection to plc
        print("1. Fins Connection Check")
        print("          Fetching CPU Details ..........")
        try:
            # Initialize FINS connection
            fins = FinsUdpConnection(self.plc_ip)
            await fins.connect()
            # As UDP is just broad casting doesn't confirm the actual connection test so lets get cpu details to confirm
            cpu_details = await fins.cpu_unit_details_read()

            if cpu_details["status"] == "success":
                self.logger.info(f"Succesfully Connected to PLC {self.name} at {self.plc_ip}")
                print(f"          PLC Unit Name : {cpu_details['data']['unit_name']}")
                print(f"          ✅ Successfully Connected to PLC ")
            else:
                raise Exception(f"Could not fetch CPU detials")
        except Exception as e:
            self.logger.error(f"Failed to connect to PLC {self.name} at {self.plc_ip}: \n{e}")
            print(f"          ❌ Unsuccessful Connection to PLC ")
            await self.queue.put(f"{self.name}-fins connection error")
            # Also deleting the csv file
            # a) close the file first
            if hasattr(self, 'csv_file') and self.csv_file:
                self.csv_file.close()
            # b) delete the file
            if os.path.exists(self.csv_filename):
                os.remove(self.csv_filename)
            return
        

        # Step 2: setup the connection to the opcua server with resilience
        client = Client(self.opcua_url)
        opcua_manager = None
        print("\n")
        print("2. OPCUA Connection Check")
        print("          Fetching Node Details from OPC UA Server ..........")
        
        # Try to establish OPC UA connection, but continue with CSV fallback if it fails
        try:
            client.connect()
            self.opcua_connected = True
            self.logger.info(f"Connected to OPC UA server at {self.opcua_url}")
            
            # Step 3: create the opcua manager using shared JSON file
            try:
                # Wait for the shared JSON file to be available (created by node manager container)
                max_wait_time = 60  # Wait up to 60 seconds
                wait_time = 0
                while not os.path.exists(self.opcua_json_file_name) and wait_time < max_wait_time:
                    self.logger.info(f"Waiting for shared JSON file: {self.opcua_json_file_name}")
                    await asyncio.sleep(2)
                    wait_time += 2
                
                if not os.path.exists(self.opcua_json_file_name):
                    raise Exception(f"Shared JSON file not found after {max_wait_time} seconds: {self.opcua_json_file_name}")
                
                # Create OpcuaAutoNodeMapper using existing JSON file (no reload to avoid conflicts)
                opcua_manager = OpcuaAutoNodeMapper(client, 
                                                    json_path=self.opcua_json_file_name, 
                                                    reload=self.reload,  
                                                    console_print=False)
                self.logger.info(f"Successfully loaded OpcuaAutoNodeMapper from shared JSON file: {self.opcua_json_file_name}")
                print("          ✅ Successfully connected to OPC UA Server ")
            except Exception as e:
                self.logger.error(f"Failed to load OpcuaAutoNodeMapper from shared JSON file: {self.opcua_json_file_name}: \n {e}")
                print("          ❌ Failed to create OPC UA Manager - will use CSV fallback")
                self.opcua_connected = False
                opcua_manager = None
                
        except Exception as e:
            self.logger.error(f"Failed to connect to OPC UA server- {self.opcua_url}: \n {e}")
            print("          ❌ Failed to connect to OPC UA Server - will use CSV fallback")
            self.opcua_connected = False
            opcua_manager = None
            
        # If OPC UA connection failed, ensure CSV fallback is available
        if not self.opcua_connected:
            self._ensure_csv_file()
            self.logger.warning(f"OPC UA connection failed - data will be stored in CSV: {self.csv_filename}")
            print(f"          📁 CSV fallback enabled: {self.csv_filename}")
        
        
        # Just intemediate step toprint the csv file details 
        if self.csv_enabled:
            print("\n")
            print("   ****CSV FILE DETAILS****")
            print(f"    ✅ CSV File Name - {self.csv_filename}")
                
    
        # Step4: two tasks {get the variable values from the PLC , push value to OPC UA}
        self.logger.info(f"PLC {self.name} running in continuous mode")
        print(f"    ✅ PLC {self.name} running in continuous mode")
        
        while not self.stopped():
            await self._perform_plc_update_cycle(fins, opcua_manager)
            
            ## This is actually the stopping the program as per the threshold limit of the errors but it i sin the wrong place
            # if self.failed_to_read > self.threshold or self.failed_to_push > self.threshold:
            #     self.logger.error(f"Too many errors encountered: {self.failed_to_read} reads and {self.failed_to_push} writes failed. Closing task.")
            #     await self.queue.put(f"{self.name}-too many errors")
            #     self.stop()
            #     break
            
            # Add a small sleep to reduce CPU usage
            await asyncio.sleep(self.sleep_interval)
        
        # Step 5: close the connections
        try:
            await fins.disconnect()
            
            # Close CSV file if it was opened
            if self.csv_file:
                self.csv_file.close()

            self.logger.info(f"Disconnected from PLC {self.name} at {self.plc_ip}")
        except Exception as e:
            await self.queue.put(f"{self.name}-fins disconnection error")
            self.logger.error(f"Failed to disconnect from PLC {self.name} at {self.plc_ip}: \n{e}")
        
        try:
            client.disconnect()
            self.logger.info(f"Disconnected from OPC UA server at {self.opcua_url}")
        except Exception as e:
            await self.queue.put(f"{self.name}-opcua disconnection error")
            self.logger.error(f"Failed to disconnect from OPC UA server at {self.opcua_url}: \n{e}")
   
            
    

def load_config(config_file):
    """
    Load configuration from a JSON file.
    """
    try:
        encodings_to_try = ['utf-8', 'shift_jis', 'cp932', 'iso-8859-1', 'latin-1']
        for encoding in encodings_to_try:
                try:
                        with open(config_file, 'r', encoding=encoding) as f:
                                return json.load(f)
                        break
                except UnicodeDecodeError:
                        print(f"Failed to read with encoding: {encoding}")
                        continue
        # with open(config_file, 'r') as f:
        #     return json.load(f)
        
    except Exception as e:
        logging.error(f"Error loading configuration file: {e}")

async def process_queue_async(queue, tasks):
    while True:
        try:
            message = await asyncio.wait_for(queue.get(), timeout=1.0)  # Wait for a message
            print(f"[Error] message received: {message}")
            print(f"Stopping the program.........")
            logging.info(f"Queue message received: {message}")
            
            # Find the task associated with the error and cancel it
            for task in tasks[:]:  # Use slice copy to safely modify during iteration
                if hasattr(task, 'get_name') and task.get_name() in message:
                    logging.info(f"Cancelling task {task.get_name()}")
                    task.cancel()
                    tasks.remove(task)
                    break
            queue.task_done()
        except asyncio.TimeoutError:
            # If the queue is empty, continue to check for new messages
            continue
        except asyncio.CancelledError:
            # Task was cancelled, break the loop
            break

async def create_tasks(plc_details, reload:bool = False, csv_enabled:bool = False):
    tasks = []
    queue = asyncio.Queue()
    
    for plc in plc_details['plcs']:
        plc_task = PLCTask(plc, queue, reload=reload, csv_enabled=csv_enabled)
        task = asyncio.create_task(plc_task.run())
        task.set_name(plc_task.name)  # Set task name for identification
        tasks.append(task)

    # Create async queue processor task
    queue_task = asyncio.create_task(process_queue_async(queue, tasks))
    queue_task.set_name("queue_processor")
    tasks.append(queue_task)
    
    return tasks
    
async def main(reload:bool = False, config_file:str = 'plc_data.json', csv_enabled:bool = False):
    # Setting up the initials
    
    # 1. Custom logging configuration
    logging.config.fileConfig('logging.conf')
    
    # 2. Load the PLC json file
    try:
        plc_details = load_config(config_file)
        logging.info(f"Loaded PLC details from {config_file}")
    except Exception as e:
        logging.error(f"Error in opening {config_file}: {e}")
        return
    
    if not plc_details:
        logging.info(f"Error data or no data in the {config_file}")
        return
    
    # 3. Create the async tasks
    tasks = await create_tasks(plc_details, reload=reload, csv_enabled=csv_enabled)
    
    # 4. Setup signal handlers for graceful shutdown
    def signal_handler():
        logging.info("Received shutdown signal, cancelling all tasks...")
        for task in tasks:
            task.cancel()
    
    # Register signal handlers (Windows compatibility)
    loop = asyncio.get_running_loop()
    try:
        # Unix-style signal handling
        for sig in [signal.SIGTERM, signal.SIGINT]:
            loop.add_signal_handler(sig, signal_handler)
    except NotImplementedError:
        # Windows doesn't support add_signal_handler
        # We'll rely on KeyboardInterrupt handling
        logging.info("Signal handlers not supported on this platform, using KeyboardInterrupt handling")
    
    try:
        # 5. Run all tasks concurrently
        await asyncio.gather(*tasks, return_exceptions=True)
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received, shutting down...")
        signal_handler()
        # Wait for tasks to finish
        await asyncio.gather(*tasks, return_exceptions=True)
    
    logging.info("All tasks completed, exiting main function")
    

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="OPC UA FINS Bridge - Connect OMRON PLCs to OPC UA servers")
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable reload mode for OpcuaAutoNodeMapper"
        )
    parser.add_argument(
        "--config",
        "-c",
        type=str,
        default="plc_data.json",
        help="Path to the PLC configuration JSON file (default: plc_data.json)"
        )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Enable CSV data storage alongside OPC UA (default: OPC UA only)"
        )
    args = parser.parse_args()
    
    try:
        # Run the main async function
        asyncio.run(main(reload=args.reload, config_file=args.config, csv_enabled=args.csv))
    except KeyboardInterrupt:
        logging.info("Program terminated by user")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
