"""
FINS UDP Connection Implementation
==================================
This module provides UDP implementation of the FINS protocol connection.
Enhanced with comprehensive error handling, logging, type hints, and optimized performance.
"""

import asyncio
import logging
import socket
from datetime import datetime
from typing import Optional, Tuple, Union, Dict, List, Any, Callable

# Fix the import path - adjust based on your actual project structure
from OMRON_FINS_PROTOCOL.Fins_domain.connection import FinsConnection
from OMRON_FINS_PROTOCOL.Fins_domain.command_codes import FinsCommandCode
from OMRON_FINS_PROTOCOL.Fins_domain.frames import FinsResponseFrame
from OMRON_FINS_PROTOCOL.Fins_domain.fins_error import FinsResponseError
from OMRON_FINS_PROTOCOL.Fins_domain.mem_address_parser import FinsAddressParser
from OMRON_FINS_PROTOCOL.components import *
from OMRON_FINS_PROTOCOL.exception import *
from OMRON_FINS_PROTOCOL.exception.exception_rules import (
    FinsConnectionError,
    FinsTimeoutError,
    FinsAddressError,
    FinsCommandError,
    FinsDataError,
    validate_address,
    validate_connection_params,
    validate_read_size
)

__version__ = "0.2.0"

# Constants
DEFAULT_FINS_PORT = 9600
DEFAULT_TIMEOUT = 5
MAX_CHUNK_SIZE = 990  # Maximum words per FINS command chunk
MAX_READ_SIZE = 65535  # Maximum total read size
MAX_RETRIES = 3
CONNECTION_CHECK_INTERVAL = 30  # seconds

# Data type mapping with validation
DATA_TYPE_MAPPING = {
    'INT16': (1, toInt16),
    'UINT16': (1, toUInt16),
    'INT32': (2, toInt32),
    'UINT32': (2, toUInt32),
    'INT64': (4, toInt64),
    'UINT64': (4, toUInt64),
    'FLOAT': (2, toFloat),
    'DOUBLE': (4, toDouble),
    'BCD2DEC': (1, bcd_to_decimal),
    'BOOL': (1, toInt16),
    'CHANNEL': (1, WordToHex),
    'WORD': (1, WordToHex),
    'UDINT': (2, WordToHex32),
    'BIN': (1, toBin),
    'BITS': (1, WordToBin),
}


class FinsUdpConnection(FinsConnection):
    """
    UDP implementation of FINS protocol connection.

    This class handles FINS communication over UDP networks with enhanced
    error handling, logging, connection management, and performance optimizations.

    Attributes:
        host (str): PLC IP address or hostname
        port (int): UDP port number (default 9600)
        timeout (int): Response timeout in seconds
        debug (bool): Enable debug logging and information
        logger (logging.Logger): Instance logger
        connected (bool): Connection status
        last_activity (datetime): Timestamp of last successful communication
    """

    def __init__(
        self,
        host: str,
        port: int = DEFAULT_FINS_PORT,
        timeout: int = DEFAULT_TIMEOUT,
        dest_network: int = 0,
        dest_node: int = 0,
        dest_unit: int = 0,
        src_network: int = 0,
        src_node: int = 1,
        src_unit: int = 0,
        destfinsadr: str = "0.0.0",
        srcfinsadr: str = "0.1.0",
        debug: bool = False,
        logger: Optional[logging.Logger] = None
    ) -> None:
        """
        Initialize UDP connection with enhanced validation and logging.

        Args:
            host: PLC IP address or hostname
            port: UDP port number (default 9600 for FINS)
            timeout: Response timeout in seconds
            dest_network: Destination network address
            dest_node: Destination node address
            dest_unit: Destination unit address
            src_network: Source network address
            src_node: Source node address
            src_unit: Source unit address
            destfinsadr: Alternative destination address format
            srcfinsadr: Alternative source address format
            debug: Enable debug mode
            logger: Custom logger instance

        Raises:
            FinsConnectionError: If connection parameters are invalid
        """
        # Validate connection parameters
        validate_connection_params(host, port)

        # Call parent constructor with proper parameters
        super().__init__(
            host=host,
            port=port,
            dest_network=dest_network,
            dest_node=dest_node,
            dest_unit=dest_unit,
            src_network=src_network,
            src_node=src_node,
            src_unit=src_unit,
            destfinsadr=destfinsadr,
            srcfinsadr=srcfinsadr
        )

        self.timeout = timeout
        self.debug = debug
        self.socket: Optional[socket.socket] = None
        self.connected = False
        self.last_activity = datetime.now()

        # Initialize logger
        self.logger = logger or logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        if not self.logger.handlers and self.debug:
            self._setup_logger()

        # Get Fins Command Code
        self.command_codes = FinsCommandCode()
        # Initialize address parser
        self.address_parser = FinsAddressParser()

        self.logger.info(f"Initialized UDP connection to {host}:{port}")

    def _setup_logger(self) -> None:
        """Setup default logger configuration for debug mode."""
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)

    async def connect(self) -> None:
        """
        Initialize UDP socket asynchronously with enhanced error handling.

        Raises:
            FinsConnectionError: If socket creation fails
        """
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.socket.settimeout(self.timeout)
            self.connected = True
            self.last_activity = datetime.now()
            self.logger.info("UDP socket initialized successfully")

        except socket.error as e:
            self.connected = False
            if self.socket:
                self.socket.close()
                self.socket = None
            error_msg = f"Failed to create UDP socket: {e}"
            self.logger.error(error_msg)
            raise FinsConnectionError(error_msg) from e

    async def disconnect(self) -> None:
        """Close the UDP socket asynchronously with proper cleanup."""
        if self.socket:
            try:
                self.socket.close()
                self.logger.debug("UDP socket closed")
            except socket.error as e:
                self.logger.warning(f"Error closing socket: {e}")
            finally:
                self.socket = None
                self.connected = False
                self.last_activity = datetime.now()

    def _check_connection_health(self) -> bool:
        """
        Check if connection is healthy based on activity timestamp.

        Returns:
            True if connection is healthy, False otherwise
        """
        if not self.connected:
            return False

        time_since_activity = (datetime.now() - self.last_activity).total_seconds()
        return time_since_activity < CONNECTION_CHECK_INTERVAL

    async def execute_fins_command_frame(self, fins_command_frame: bytes) -> bytes:
        """
        Execute a FINS command frame over UDP asynchronously with retry logic.

        Args:
            fins_command_frame: Complete FINS command frame

        Returns:
            Response frame bytes

        Raises:
            FinsConnectionError: If communication fails after retries
            FinsTimeoutError: If response timeout occurs
        """
        if not self.connected or not self.socket:
            raise FinsConnectionError("UDP socket not initialized")

        last_exception = None
        for attempt in range(MAX_RETRIES):
            try:
                self.socket.sendto(fins_command_frame, self.addr)
                self.logger.debug(f"Sent command frame to {self.addr}")

                # Receive response with timeout
                response_data = self.socket.recv(4096)
                self.last_activity = datetime.now()
                self.logger.debug(f"Received response ({len(response_data)} bytes)")
                return response_data

            except socket.timeout as e:
                last_exception = e
                self.logger.warning(f"Timeout on attempt {attempt + 1}/{MAX_RETRIES}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(0.1 * (attempt + 1))  # Exponential backoff
                    continue
                raise FinsTimeoutError(f"UDP communication timeout after {MAX_RETRIES} attempts") from e

            except socket.error as e:
                last_exception = e
                self.logger.warning(f"Socket error on attempt {attempt + 1}/{MAX_RETRIES}: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(0.1 * (attempt + 1))
                    continue
                raise FinsConnectionError(f"UDP communication error after {MAX_RETRIES} attempts: {e}") from e

        # This should never be reached, but just in case
        raise FinsConnectionError(f"Failed to execute command after {MAX_RETRIES} attempts") from last_exception

    def _parse_response(self, response_data: bytes) -> FinsResponseFrame:
        """
        Parse response data using FinsResponseFrame.

        Args:
            response_data: Raw response bytes from PLC

        Returns:
            Parsed response frame

        Raises:
            FinsProtocolError: If response parsing fails
        """
        try:
            response_frame = FinsResponseFrame()
            response_frame.from_bytes(response_data)
            return response_frame
        except Exception as e:
            raise FinsProtocolError(f"Failed to parse response frame: {e}") from e

    def _check_response(self, response_data_end_code: bytes) -> Tuple[bool, str]:
        """
        Check response end code and return status.

        Args:
            response_data_end_code: End code bytes from response

        Returns:
            Tuple of (success: bool, message: str)
        """
        if response_data_end_code == b'\x00\x00':
            return True, "Service success"
        elif response_data_end_code == b'\x00\x01':
            return False, "Service cancelled"
        else:
            # Use custom FinsResponseError class
            try:
                error = FinsResponseError(response_data_end_code)
                return False, str(error)
            except Exception:
                # Fallback if error code not recognized
                return False, f"Unknown error code: {response_data_end_code.hex()}"

    def _validate_data_type(self, data_type: str) -> Tuple[int, Callable]:
        """
        Validate and get data type information.

        Args:
            data_type: Data type string

        Returns:
            Tuple of (words_per_item, conversion_function)

        Raises:
            FinsDataError: If data type is invalid
        """
        if data_type is None:
            data_type = 'INT16'

        data_type = data_type.upper()
        if data_type not in DATA_TYPE_MAPPING:
            raise FinsDataError(
                f"Invalid data type: '{data_type}'. Allowed types are: {', '.join(DATA_TYPE_MAPPING.keys())}",
                error_code="INVALID_TYPE"
            )

        return DATA_TYPE_MAPPING[data_type]

    def _calculate_read_chunks(self, total_words: int) -> List[Tuple[int, int]]:
        """
        Calculate read chunks for large data transfers.

        Args:
            total_words: Total number of words to read

        Returns:
            List of (offset, chunk_size) tuples
        """
        chunks = []
        remaining = total_words
        offset = 0

        while remaining > 0:
            chunk_size = min(remaining, MAX_CHUNK_SIZE)
            chunks.append((offset, chunk_size))
            offset += chunk_size
            remaining -= chunk_size

        return chunks

    async def read(
        self,
        memory_area_code: str,
        data_type: str = 'INT16',
        service_id: int = 0
    ) -> Dict[str, Any]:
        """
        Read data from PLC memory area using FINS command codes.

        Args:
            memory_area_code: Memory area identifier (e.g., 'D1000', 'W100', 'A50.1')
            data_type: Data type to read (default 'INT16')
            service_id: Service ID for the command

        Returns:
            Dictionary with status, message, and data

        Raises:
            FinsAddressError: If memory address is invalid
            FinsDataError: If data type is invalid
            FinsConnectionError: If communication fails
        """
        # Validate inputs
        validate_address(memory_area_code)

        # Validate data type and get conversion info
        words_per_item, conversion_function = self._validate_data_type(data_type)

        # Parse memory address
        try:
            info = self.address_parser.parse(memory_area_code)
        except Exception as e:
            raise FinsAddressError(f"Failed to parse memory address '{memory_area_code}': {e}") from e

        # Determine read size based on address type
        if '.' in memory_area_code:
            readsize = 1  # Bit address
        else:
            readsize = words_per_item

        # Initialize result structure
        final_result = self._create_result_structure(data_type)

        # Log debug information
        if self.debug:
            self._log_address_info(memory_area_code, info)

        # Calculate chunks for large reads
        chunks = self._calculate_read_chunks(readsize)
        data = bytearray()

        # Process each chunk
        for chunk_offset, chunk_size in chunks:
            chunk_data = await self._read_chunk(
                memory_area_code, info, chunk_offset, chunk_size, service_id, final_result
            )
            if chunk_data is None:
                # Error occurred, result already updated
                return final_result
            data.extend(chunk_data)

        # Process final data
        return self._process_final_data(data, conversion_function, info, memory_area_code, final_result)

    async def _read_chunk(
        self,
        memory_area_code: str,
        info: Dict,
        chunk_offset: int,
        chunk_size: int,
        service_id: int,
        final_result: Dict
    ) -> Optional[bytes]:
        """Read a single chunk of data."""
        # Calculate offset for this chunk
        chunk_address_info = self.address_parser.parse(memory_area_code, chunk_offset)

        # Prepare command frame
        command_frame = self._build_read_command_frame(
            chunk_address_info, chunk_size, service_id
        )
        final_result["debug"]["command_frame"] = str(command_frame)

        if self.debug:
            self.logger.debug(f"Sent FinsCommand frame: {command_frame}")
            self.logger.debug(f"Destination: {self.addr}")

        # Execute command
        try:
            response_data = await self.execute_fins_command_frame(command_frame)
            final_result["debug"]["raw_response_bytes"] = str(response_data)

            if self.debug:
                self.logger.debug(f"Received response: {response_data}")

            # Parse and check response
            response_frame = self._parse_response(response_data)
            final_result["debug"]["response_frame_header"] = str(response_data[0:10])
            final_result["debug"]["response_frame_command_code"] = str(response_frame.command_code)
            final_result["debug"]["response_frame"] = str(response_frame.end_code)

            is_success, msg = self._check_response(response_frame.end_code)

            if self.debug:
                self.logger.debug(f"Response status: {msg}")

            if is_success:
                return response_frame.text
            else:
                # Error occurred
                converted_data = []  # Empty data on error
                self._update_result_on_error(
                    final_result, msg, converted_data, chunk_address_info,
                    memory_area_code, chunk_offset + 1
                )
                return None

        except Exception as e:
            error_msg = f"Communication error during chunk read: {e}"
            self.logger.error(error_msg)
            final_result["status"] = "error"
            final_result["message"] = error_msg
            return None

    def _build_read_command_frame(
        self,
        info: Dict,
        read_size: int,
        service_id: int
    ) -> bytes:
        """Build FINS command frame for memory read."""
        sid = service_id.to_bytes(1, 'big')

        # Create command frame
        finsary = bytearray(8)
        finsary[0:2] = self.command_codes.MEMORY_AREA_READ
        finsary[2] = info['memory_type_code']
        finsary[3:5] = info['offset_bytes']
        if info['address_type'] == 'bit':
            finsary[5] = info['bit_number']
        else:
            finsary[5] = 0x00
        finsary[6] = (read_size >> 8) & 0xFF  # High byte
        finsary[7] = read_size & 0xFF        # Low byte

        # Build FINS command frame
        return self.fins_command_frame(command_code=finsary, service_id=sid)

    def _process_final_data(
        self,
        data: bytearray,
        conversion_function: Callable,
        info: Dict,
        memory_area_code: str,
        final_result: Dict
    ) -> Dict:
        """Process and convert final accumulated data."""
        # Ensure even number of bytes
        if len(data) % 2 != 0:
            data.append(0)

        try:
            converted_data = conversion_function(data)
            final_result["status"] = "success"
            final_result["message"] = "Read successful"
            final_result["data"] = converted_data
            final_result["meta"]["address_type"] = info["address_type"]
            final_result["meta"]["original_address"] = memory_area_code[1:] if 'Z' in memory_area_code else memory_area_code
            final_result["meta"]["memory_area"] = info["memory_area"]
            final_result["meta"]["word_address"] = info["word_address"]
            final_result["meta"]["bit_number"] = info["bit_number"]
            final_result["meta"]["read_chunks"] = len(self._calculate_read_chunks(len(data) // 2))
            final_result["meta"]["offset_bytes"] = info["offset_bytes"]

        except Exception as e:
            final_result["status"] = "error"
            final_result["message"] = f"Data conversion error: {e}"
            final_result["data"] = []

        return final_result

    def _update_result_on_error(
        self,
        final_result: Dict,
        msg: str,
        converted_data: List,
        info: Dict,
        memory_area_code: str,
        chunks_read: int
    ) -> None:
        """Update result structure when an error occurs."""
        final_result["status"] = "error"
        final_result["message"] = msg
        final_result["data"] = converted_data
        final_result["meta"]["address_type"] = info["address_type"]
        final_result["meta"]["original_address"] = memory_area_code
        final_result["meta"]["memory_area"] = info["memory_area"]
        final_result["meta"]["word_address"] = info["word_address"]
        final_result["meta"]["bit_number"] = info["bit_number"]
        final_result["meta"]["read_chunks"] = chunks_read
        final_result["meta"]["offset_bytes"] = info["offset_bytes"]

    def _create_result_structure(self, data_type: str) -> Dict[str, Any]:
        """Create standardized result structure."""
        return {
            "status": "",
            "message": "",
            "data": None,
            "data_format": data_type,
            "meta": {},
            "debug": {}
        }

    def _log_address_info(self, memory_area_code: str, info: Dict) -> None:
        """Log detailed address information in debug mode."""
        self.logger.debug("=" * 50)
        self.logger.debug(f"Address Given: {memory_area_code}")
        self.logger.debug(f"Type: {info['address_type']}")
        self.logger.debug(f"Memory Area: {info['memory_area']}")
        self.logger.debug(f"Word Address: {info['word_address']}")
        self.logger.debug(f"Bit Number: {info['bit_number']}")
        self.logger.debug(f"Memory Type Code: {info['memory_type_code']}")
        self.logger.debug(f"Offset Bytes: {info['offset_bytes']}")
        self.logger.debug(f"Fins_Format: {info['fins_format']}")

    async def batch_read(
        self,
        memory_area_code: str,
        data_type: str = 'INT16',
        no_items_to_read: int = 1,
        service_id: int = 0
    ) -> Dict[str, Any]:
        """
        Read multiple consecutive data items from PLC memory area.

        Args:
            memory_area_code: Memory area identifier (starting address)
            data_type: Data type to read (default 'INT16')
            no_items_to_read: Number of items to read
            service_id: Service ID for the command

        Returns:
            Dictionary with status, message, and data (list of converted values)

        Raises:
            FinsAddressError: If memory address is invalid
            FinsDataError: If data type or read size is invalid
            FinsConnectionError: If communication fails
        """
        # Validate inputs
        validate_address(memory_area_code)
        validate_read_size(no_items_to_read)

        # Validate data type and get conversion info
        words_per_item, conversion_function = self._validate_data_type(data_type)

        # Calculate total words to read
        readsize = no_items_to_read * words_per_item

        # Initialize result
        final_result = self._create_result_structure(data_type)
        final_result["meta"]["no_items_to_read"] = no_items_to_read

        # Parse initial address
        try:
            info = self.address_parser.parse(memory_area_code)
        except Exception as e:
            raise FinsAddressError(f"Failed to parse memory address '{memory_area_code}': {e}") from e

        if self.debug:
            self._log_address_info(memory_area_code, info)

        # Calculate chunks
        chunks = self._calculate_read_chunks(readsize)
        data = bytearray()

        # Process chunks
        for chunk_offset, chunk_size in chunks:
            chunk_data = await self._read_chunk(
                memory_area_code, info, chunk_offset, chunk_size, service_id, final_result
            )
            if chunk_data is None:
                # Error occurred, update result with partial data if available
                self._handle_batch_read_error(final_result, data, conversion_function, words_per_item, info, memory_area_code)
                return final_result
            data.extend(chunk_data)

        # Process successful batch read
        return self._process_batch_read_success(data, conversion_function, words_per_item, info, memory_area_code, final_result)

    def _handle_batch_read_error(
        self,
        final_result: Dict,
        data: bytearray,
        conversion_function: Callable,
        words_per_item: int,
        info: Dict,
        memory_area_code: str
    ) -> None:
        """Handle errors in batch read with partial data processing."""
        bytes_per_item = words_per_item * 2
        num_complete_items = len(data) // bytes_per_item

        converted_data = []
        for i in range(num_complete_items):
            item_bytes = data[i * bytes_per_item : (i + 1) * bytes_per_item]
            try:
                converted_data.append(conversion_function(item_bytes))
            except Exception as e:
                self.logger.warning(f"Failed to convert item {i}: {e}")
                converted_data.append(None)

        final_result["data"] = converted_data
        final_result["meta"]["address_type"] = info["address_type"]
        final_result["meta"]["original_address"] = memory_area_code
        final_result["meta"]["memory_area"] = info["memory_area"]
        final_result["meta"]["word_address"] = info["word_address"]
        final_result["meta"]["bit_number"] = info["bit_number"]
        final_result["meta"]["read_chunks"] = 1  # Error occurred in first chunk
        final_result["meta"]["offset_bytes"] = info["offset_bytes"]
        final_result["meta"]["items_read"] = num_complete_items

    def _process_batch_read_success(
        self,
        data: bytearray,
        conversion_function: Callable,
        words_per_item: int,
        info: Dict,
        memory_area_code: str,
        final_result: Dict
    ) -> Dict:
        """Process successful batch read data."""
        # Ensure even number of bytes
        if len(data) % 2 != 0:
            data.append(0)

        bytes_per_item = words_per_item * 2
        num_items = len(data) // bytes_per_item

        converted_data = []
        for i in range(num_items):
            item_bytes = data[i * bytes_per_item : (i + 1) * bytes_per_item]
            try:
                converted_data.append(conversion_function(item_bytes))
            except Exception as e:
                self.logger.error(f"Failed to convert item {i}: {e}")
                converted_data.append(None)

        final_result["status"] = "success"
        final_result["message"] = "Batch read successful"
        final_result["data"] = converted_data
        final_result["meta"]["address_type"] = info["address_type"]
        final_result["meta"]["original_address"] = memory_area_code[1:] if 'Z' in memory_area_code else memory_area_code
        final_result["meta"]["memory_area"] = info["memory_area"]
        final_result["meta"]["word_address"] = info["word_address"]
        final_result["meta"]["bit_number"] = info["bit_number"]
        final_result["meta"]["read_chunks"] = len(self._calculate_read_chunks(len(data) // 2))
        final_result["meta"]["offset_bytes"] = info["offset_bytes"]
        final_result["meta"]["items_read"] = num_items

        return final_result

    async def multiple_read(
        self,
        dict_memory_codes: Dict[str, str],
        service_id: int = 0
    ) -> Dict[str, Any]:
        """
        Read from multiple non-consecutive memory areas using FINS MULTIPLE_MEMORY_AREA_READ command.

        Args:
            dict_memory_codes: Dictionary with memory codes as keys and data types as values
                              e.g., {"D0": "INT16", "W100": "INT32", "A50.1": "BOOL"}
            service_id: Service ID for the command

        Returns:
            Dictionary with status, message, and data (updated dict with 'value' key for each entry)

        Raises:
            FinsDataError: If memory codes dictionary is empty or invalid
            FinsConnectionError: If communication fails
        """
        if not dict_memory_codes:
            raise FinsDataError("No memory codes provided", error_code="EMPTY_MEMORY_CODES")

        final_result = {
            "status": "",
            "message": "",
            "data": {},
            "data_format": "MULTIPLE",
            "meta": {},
            "debug": {}
        }

        # Build command frame for multiple memory area read
        command_code = self.command_codes.MULTIPLE_MEMORY_AREA_READ
        sid = service_id.to_bytes(1, 'big')

        # Construct the data part: number of items + item specifications
        num_items = len(dict_memory_codes)
        data_part = num_items.to_bytes(2, 'big')  # Number of memory areas to read

        item_list = list(dict_memory_codes.items())
        for memory_code, data_type in item_list:
            # Validate address
            validate_address(memory_code)

            # Parse each memory address
            try:
                info = self.address_parser.parse(memory_code)
            except Exception as e:
                raise FinsAddressError(f"Failed to parse memory address '{memory_code}': {e}") from e

            data_part += info['memory_type_code']  # Memory area code
            data_part += bytes(info['offset_bytes'])  # Beginning address (2 bytes)
            if info['address_type'] == 'bit':
                data_part += info['bit_number'].to_bytes(1, 'big')  # Bit number
            else:
                data_part += b'\x00'  # No bit number for word addresses

        # Build FINS command frame
        command_frame = self.fins_command_frame(command_code=command_code, service_id=sid, data=data_part)
        final_result["debug"]["command_frame"] = str(command_frame)

        if self.debug:
            self.logger.debug(f"Sent Multiple Memory Area Read command: {command_frame}")
            self.logger.debug(f"Destination: {self.addr}")

        try:
            # Execute the FINS command frame
            response_data = await self.execute_fins_command_frame(command_frame)
            final_result["debug"]["raw_response_bytes"] = str(response_data)

            if self.debug:
                self.logger.debug(f"Received response: {response_data}")

            # Parse the response data
            response_frame = self._parse_response(response_data)
            final_result["debug"]["response_frame_header"] = str(response_data[0:10])
            final_result["debug"]["response_frame_command_code"] = str(response_frame.command_code)
            final_result["debug"]["response_frame"] = str(response_frame.end_code)

            # Check the response status
            is_success, msg = self._check_response(response_frame.end_code)

            if self.debug:
                self.logger.debug(f"Response status: {msg}")

            if is_success:
                # Parse the response data: data from each memory area in sequence
                data_index = 0
                updated_dict = {}
                for memory_code, data_type in item_list:
                    # Get conversion function and word_size per item from mapping
                    words_per_item, conversion_function = self._validate_data_type(data_type)
                    bytes_per_item = words_per_item * 2

                    # Extract bytes for this item
                    item_bytes = response_frame.text[data_index:data_index + bytes_per_item]
                    if len(item_bytes) < bytes_per_item:
                        updated_dict[memory_code] = {"error": "Insufficient data in response"}
                    else:
                        try:
                            converted_value = conversion_function(item_bytes)
                            updated_dict[memory_code] = {
                                "type": data_type,
                                "value": converted_value
                            }
                        except Exception as e:
                            updated_dict[memory_code] = {"error": f"Conversion failed: {e}"}

                    data_index += bytes_per_item

                final_result["status"] = "success"
                final_result["message"] = "Multiple Memory Area Read Successful"
                final_result["data"] = updated_dict
                final_result["meta"]["num_items"] = num_items
            else:
                final_result["status"] = "error"
                final_result["message"] = msg
                final_result["data"] = {}

        except FinsConnectionError:
            raise  # Re-raise connection errors
        except Exception as e:
            final_result["status"] = "error"
            final_result["message"] = f"An unexpected error occurred: {str(e)}"
            self.logger.error(f"Multiple read error: {e}", exc_info=True)

        return final_result

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()

    async def cpu_unit_details_read(self) -> Dict[str, Any]:
        """
        Read CPU unit details.

        Returns:
            Dictionary with status, message, and data

        Raises:
            FinsConnectionError: If communication fails
        """
        final_result = self._create_result_structure("N/A")
        final_result["data_format"] = "CPU_DETAILS"

        # Command code for CPU unit data status read
        command_code = self.command_codes.CPU_UNIT_DATA_READ

        # Build FINS command frame
        command_frame = self.fins_command_frame(command_code=command_code)
        final_result["debug"]["command_frame"] = str(command_frame)

        try:
            rcv = await self.execute_fins_command_frame(command_frame)
            final_result["debug"]["raw_response_bytes"] = str(rcv)
            response_data = rcv[10:]
            data = response_data[4:]  # Skip 4 bytes (command + end code)

            unit_name = data[0:20].decode().strip()
            boot_version = data[20:25].decode().strip()
            model_number = data[28:32].decode().strip()
            os_version = data[32:37].decode().strip()

            if response_data[2:4] == b'\x00\x00':
                final_result["status"] = "success"
                final_result["message"] = "CPU Unit Details Read Successfully"
                final_result["data"] = {
                    "unit_name": unit_name,
                    "boot_version": boot_version,
                    "model_number": model_number,
                    "os_version": os_version
                }
            else:
                error_code = response_data[2:4]
                is_success, msg = self._check_response(error_code)
                final_result["status"] = "error"
                final_result["message"] = f"Error reading CPU Unit Details: {msg}"
                final_result["data"] = {"error_code": str(error_code)}

            if self.debug:
                self.logger.debug("CPU Unit Data Read Response:")
                self.logger.debug(f"  Raw response: {response_data.hex()}")
                self.logger.debug(f"  Data after header: {data}")
                self.logger.debug(f"  Unit Name: {unit_name}")
                self.logger.debug(f"  Boot Version: {boot_version}")
                self.logger.debug(f"  Model Number: {model_number}")
                self.logger.debug(f"  OS Version: {os_version}")

            return final_result

        except FinsConnectionError:
            raise
        except Exception as e:
            error_msg = f"An unexpected error occurred: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            final_result["status"] = "error"
            final_result["message"] = error_msg
            return final_result

    async def cpu_unit_status_read(self) -> Dict[str, Any]:
        """
        Read CPU unit status.

        Returns:
            Dictionary with status, message, and data

        Raises:
            FinsConnectionError: If communication fails
        """
        # Mode and status dictionaries
        mode_code_dict = {b'\x00': 'PROGRAM', b'\x02': 'MONITOR', b'\x04': 'RUN'}
        status_code_dict = {b'\x00': 'Stop', b'\x01': 'Run', b'\x80': 'CPU on standby', b'\x05': 'No data available'}

        final_result = self._create_result_structure("N/A")
        final_result["data_format"] = "CPU_STATUS"

        # Command code for CPU unit status read
        command_code = self.command_codes.CPU_UNIT_STATUS_READ

        # Build FINS command frame
        command_frame = self.fins_command_frame(command_code=command_code)
        final_result["debug"]["command_frame"] = str(command_frame)

        try:
            response_data = await self.execute_fins_command_frame(command_frame)
            final_result["debug"]["raw_response_bytes"] = str(response_data)
            final_result["debug"]["response_frame_header"] = str(response_data[0:10])
            final_result["debug"]["response_frame_command_code"] = str(response_data[10:12])
            final_result["debug"]["response_frame_code"] = str(response_data[12:14])

            data = response_data[12:]  # Skip 12 bytes (header + command + end code)

            if self.debug:
                self.logger.debug("CPU Unit Status Read Response:")
                self.logger.debug(f"  Raw response: {response_data.hex()}")
                self.logger.debug(f"  Data after header: {response_data[10:]}")
                self.logger.debug(f"  Status byte: {response_data[14:15]}")
                self.logger.debug(f"  Mode byte: {response_data[15:16]}")

            if response_data[12:14] == b'\x00\x00':
                final_result["status"] = "success"
                final_result["message"] = "CPU Unit Status Read Successfully"
                final_result["data"] = {
                    "Status": status_code_dict.get(response_data[14:15], 'Unknown Status'),
                    "Mode": mode_code_dict.get(response_data[15:16], 'Unknown Mode')
                }
            else:
                error_code = response_data[12:14].hex()
                final_result["status"] = "error"
                final_result["message"] = f"Error reading CPU Unit Status. Error code: {error_code}"
                final_result["data"] = {"error_code": error_code}

            return final_result

        except FinsConnectionError:
            raise
        except Exception as e:
            error_msg = f"An unexpected error occurred: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            final_result["status"] = "error"
            final_result["message"] = error_msg
            return final_result

    async def clock_read(self) -> Dict[str, Any]:
        """
        Read PLC clock.

        Returns:
            Dictionary with status, message, and data

        Raises:
            FinsConnectionError: If communication fails
        """
        final_result = self._create_result_structure("DATETIME")

        # Command code for clock read
        command_code = self.command_codes.CLOCK_READ

        # Build FINS command frame
        command_frame = self.fins_command_frame(command_code=command_code)
        final_result["debug"]["command_frame"] = str(command_frame)

        try:
            rcv = await self.execute_fins_command_frame(command_frame)
            final_result["debug"]["raw_response_bytes"] = str(rcv)
            finsres = rcv[10:]

            if finsres[2:4] == b'\x00\x00':
                dt_array = finsres[4:10]
                dt_str = dt_array.hex()
                plc_date_time = datetime.strptime(dt_str, '%y%m%d%H%M%S')
                final_result["status"] = "success"
                final_result["message"] = "Clock Read Successfully"
                final_result["data"] = plc_date_time.isoformat()
            else:
                error_message = str(FinsResponseError(finsres[2:4]))
                final_result["status"] = "error"
                final_result["message"] = f"Error reading clock: {error_message}"
                final_result["data"] = None

            return final_result

        except FinsConnectionError:
            raise
        except Exception as e:
            error_msg = f"Exception Error: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            final_result["status"] = "error"
            final_result["message"] = error_msg
            return final_result