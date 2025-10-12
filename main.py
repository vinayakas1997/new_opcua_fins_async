from OMRON_FINS_PROTOCOL.Infrastructure.udp_connection import FinsUdpConnection
from OMRON_FINS_PROTOCOL.exception import *
from opcua import Client
import logging
import logging.config
import json
# from opcua_json import OpcuaAutoNodeMapper
from threading import Thread, Event 
from queue import Queue, Empty
import re
import csv
import time
from datetime import datetime 
import os
# from signal_manager import register_update_callback, start_signal_monitoring, stop_signal_monitoring


class PLCThread(Thread):
    def __init__(self, plc_details, queue, reload=False, signal_based=False, csv_enabled=False, sleep_interval=0.01):
        #initialize the thread
        Thread.__init__(self)
        self.name = plc_details['plc_name']
        self.plc_ip = plc_details['plc_ip']
        self.opcua_url = plc_details['opcua_url']
        self.address_mappings = plc_details['address_mappings']
        # special opcua running tag
        self.plc_running_tag=None

        self.queue = queue
        self.reload = reload
        self.signal_based = signal_based
        self.csv_enabled = csv_enabled
        self.sleep_interval = plc_details.get('sleep_interval', sleep_interval)  # Use config value or default
        self.update_requested = Event()
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
        
        self._stop_event = Event()
        
        
        # storing the thraed status
        self.logger.info(f"Initializing PLCThread(Program initialised) for {self.name} with IP {self.plc_ip} and OPC UA URL {self.opcua_url}")
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
    
    # add stop and stopped methods to control the thread
    def stop(self):
        self._stop_event.set()
    def stopped(self):
        return self._stop_event.is_set()
    
    def _perform_plc_update_cycle(self, fins, opcua_manager):
        """Perform one cycle of reading from PLC and writing to OPC UA"""
        row_data = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]  # Timestamp
        plc_cycle_ok = False  # Track if at least one PLC read was successful
        plc_values = {}  # Store values by opcua_tag to maintain order
        
        # PASS 1: Read all PLC registers (skip HEARTBEAT for now)
        for mapping in self.address_mappings:
            plc_address = mapping['plc_reg_add']
            opcua_tag = mapping['opcua_reg_add']
            data_type = mapping.get('data_type', 'int16') # Default to int16 if not specified
            bool_temp = False
            
            # Skip HEARTBEAT during read phase - we'll handle it in Pass 2
            if plc_address == "HEARTBEAT":
                plc_values[opcua_tag] = None  # Placeholder, will be calculated later
                continue
            
            if data_type == 'bool':
                bool_temp = True

            if data_type in ['bool','channel']:
                data_type = 'int16'
            
            try:
                # Read from PLC
                pack_value = fins.read(plc_address, data_type=data_type)
                unpack_value = pack_value['data'][0]
                
                # sometimes the value can be none    
                if unpack_value is not None:
                    plc_cycle_ok = True  # ✅ at least one PLC read succeeded
                    if bool_temp:
                        plc_value = bool(unpack_value)
                    else:
                        plc_value = unpack_value
                else:
                    plc_value = None
                    
            except Exception as e:
                # in the continuous reading any missed data reason is logged
                plc_value = None
                self.failed_to_read += 1 
                self.logger.error(f"Missed reading {self.failed_to_read},\n Error reading PLC {self.name} address {plc_address}: \n{e}")
                # for the immidiate stop it is written here after theshold after the cycle you put it in th csv 
                if self.failed_to_read > self.threshold:
                    self.logger.error(f"Too many errors encountered: {self.failed_to_read} reads the threshold met {self.threshold}. Closing thread.")
                    self.queue.put(f"{self.name}-too many errors")
                    self.stop()
                    return  # Exit the method early
                
            # Store the value for Pass 2
            plc_values[opcua_tag] = plc_value
        
        # PASS 2: Write to CSV and OPC UA in original mapping order
        for mapping in self.address_mappings:
            plc_address = mapping['plc_reg_add']
            opcua_tag = mapping['opcua_reg_add']
            
            # Handle HEARTBEAT with correct plc_cycle_ok value
            if plc_address == "HEARTBEAT":
                plc_value = plc_cycle_ok  # Now correctly reflects if any PLC read succeeded
            else:
                plc_value = plc_values[opcua_tag]
            
            # Append the value to the row data (maintains column order!)
            row_data.append(str(plc_value) if plc_value is not None else 'NaN')
            
            # Writing to the OPCUA server (only if connected and manager is available)
            if self.opcua_connected and opcua_manager is not None:
                try:
                    # Push value to OPCUA server
                    opcua_manager.write(opcua_tag, plc_value)
                except Exception as e:
                    # in the continuous pushing any missed data reason is logged
                    self.failed_to_push += 1
                    self.opcua_connected = False  # Mark as disconnected on error
                    self.logger.error(f"Missed Pushing {self.failed_to_push},\n Error pushing value to OPCUA server {opcua_tag}: {e}")
                    self.logger.warning("OPC UA connection lost - switching to CSV fallback mode")
                    continue
                
        # Write to CSV file if enabled or if OPC UA is down
        if self.csv_enabled or not self.opcua_connected:
            self._ensure_csv_file()
            if self.csv_writer and self.csv_file:
                self.csv_writer.writerow(row_data)
                self.csv_file.flush()
    
    def request_update(self):
        """Request an update cycle (for signal-based mode)"""
        self.update_requested.set()
    
    
    def run(self):
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
            fins.connect()
            # As UDP is just broad casting doesn't confirm the actual connection test so lets get cpu details to confirm
            cpu_details = fins.cpu_unit_details_read()

            if cpu_details["status"] == "success":
                self.logger.info(f"Succesfully Connected to PLC {self.name} at {self.plc_ip}")
                print(f"          PLC Unit Name : {cpu_details['data']['unit_name']}")
                print(f"          ✅ Successfully Connected to PLC ")
            else:
                raise Exception(f"Could not fetch CPU detials")
        except Exception as e:
            self.logger.error(f"Failed to connect to PLC {self.name} at {self.plc_ip}: \n{e}")
            print(f"          ❌ Unsuccessful Connection to PLC ")
            self.queue.put(f"{self.name}-fins connection error")
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
                    time.sleep(2)
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
        print("\n")
        print("   ****CSV FILE DETAILS****")
        print(f"    ✅ CSV File Name - {self.csv_filename}")
            
    
        # Step4: two tasks {get the variable values from the PLC , push value to OPC UA}
        if self.signal_based:
            self.logger.info(f"PLC {self.name} running in signal-based mode - waiting for update signals")
            print(f"    ✅ PLC {self.name} running in signal-based mode")
            
            while not self.stopped():
                try:
                    # Wait for update signal or check every second for shutdown
                    if self.update_requested.wait(timeout=1.0):
                        # Update signal received - perform one read/write cycle
                        self._perform_plc_update_cycle(fins, opcua_manager)
                        self.update_requested.clear()
                except Exception as e:
                    self.logger.error(f"Error in signal-based loop: {e}")
                    time.sleep(5)  # Brief pause before retrying
        else:
            self.logger.info(f"PLC {self.name} running in continuous mode")
            print(f"    ✅ PLC {self.name} running in continuous mode")
            
            while not self.stopped():
                self._perform_plc_update_cycle(fins, opcua_manager)
                
                ## This is actually the stopping the program as per the threshold limit of the errors but it i sin the wrong place 
                # if self.failed_to_read > self.threshold or self.failed_to_push > self.threshold:
                #     self.logger.error(f"Too many errors encountered: {self.failed_to_read} reads and {self.failed_to_push} writes failed. Closing thread.")
                #     self.queue.put(f"{self.name}-too many errors")
                #     self.stop()
                #     break
                
                # Add a small sleep to reduce CPU usage
                time.sleep(self.sleep_interval)
        
        # Step 5: close the connections
        try:
            fins.disconnect()
            
            # Close CSV file if it was opened
            if self.csv_file:
                self.csv_file.close()

            self.logger.info(f"Disconnected from PLC {self.name} at {self.plc_ip}")
        except Exception as e:
            self.queue.put(f"{self.name}-fins disconnection error")
            self.logger.error(f"Failed to disconnect from PLC {self.name} at {self.plc_ip}: \n{e}")
        
        try:
            client.disconnect()
            self.logger.info(f"Disconnected from OPC UA server at {self.opcua_url}")
        except Exception as e:
            self.queue.put(f"{self.name}-opcua disconnection error")
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

def process_queue(queue, threads):
    while True:
        try:
            message = queue.get(timeout=1)  # Wait for a message
            print(f"[Error] message received: {message}")
            print(f"Stopping the program.........")
        except Empty:
            # If the queue is empty, continue to check for new messages
            continue
        logging.info(f"Queue message received: {message}")
        # Find the thread associated with the error and stop it
        for thread in threads:
            if thread.name in message:
                logging.info(f"Stopping thread {thread.name}")
                thread.stop()
                thread.join(timeout=10)  # Wait for the thread to finish
                threads.remove(thread)
                break
        queue.task_done()

def create_threads(plc_details, reload:bool = False, signal_based:bool = False, csv_enabled:bool = False):
    threads = []
    queue = Queue()
    for plc in plc_details['plcs']:
        thread = PLCThread(plc, queue, reload=reload, signal_based=signal_based, csv_enabled=csv_enabled)
        threads.append(thread) 
        thread.start()

    # one more thread to process the queue
    queue_thread = Thread(target=process_queue, args=(queue, threads))
    queue_thread.daemon = True  # Make it a daemon thread
    queue_thread.start()
    
    return threads  # Return threads for signal-based control
    
def main(reload:bool = False, signal_based:bool = False, config_file:str = 'plc_data.json', csv_enabled:bool = False):
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
    
    # 3. Create the threads 
    threads = create_threads(plc_details, reload=reload, signal_based=signal_based, csv_enabled=csv_enabled)
    
    if signal_based:
        logging.info("System running in signal-based mode. Use request_update() method to trigger updates.")
        print("\n🔔 SIGNAL-BASED MODE ACTIVE")
        print("   To trigger updates, you can:")
        print("   1. Send SIGUSR1 signal to the node manager process")
        print("   2. Call request_update() method on PLC threads")
        print("   3. Use external trigger mechanisms")
    
    return threads  # Return threads for external control
    

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="OPC UA FINS Bridge - Connect OMRON PLCs to OPC UA servers")
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable reload mode for OpcuaAutoNodeMapper"
        )
    parser.add_argument(
        "--signal-based",
        action="store_true",
        help="Enable signal-based mode (updates only on signal)"
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
        main(reload=args.reload, signal_based=args.signal_based, config_file=args.config, csv_enabled=args.csv)
    except KeyboardInterrupt:
        logging.info("Program terminated by user")
