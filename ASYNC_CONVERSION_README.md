# Async Conversion README

## Overview
This document summarizes the complete conversion of the OMRON FINS OPCUA Bridge from a multi-threaded synchronous implementation to an asynchronous implementation using Python's `asyncio` library.

## Major Changes Made

### 1. Class Conversion
- **PLCThread** → **PLCTask**: Removed `Thread` inheritance and converted to async task
- Replaced `threading.Event` with `asyncio.Event` for stop signaling
- Updated all method signatures to use `async`/`await` patterns

### 2. FINS Protocol Operations
All FINS operations now use `await`:
- `await fins.connect()`
- `await fins.cpu_unit_details_read()`
- `await fins.read()`
- `await fins.batch_read()`
- `await fins.disconnect()`

### 3. Queue System
- Replaced `queue.Queue` with `asyncio.Queue`
- Converted `process_queue()` to `process_queue_async()`
- Updated error handling with async patterns

### 4. Main Orchestration
- `create_threads()` → `create_tasks()`: Creates async tasks instead of threads
- `main()` function converted to async
- Added proper signal handling for graceful shutdown (Unix/Windows compatible)

### 5. Sleep Operations
- `time.sleep()` → `await asyncio.sleep()`
- Non-blocking sleep operations throughout the application

### 6. Error Handling
- Queue operations now use `await queue.put()`
- Task cancellation instead of thread termination
- Improved exception handling for async contexts

## Benefits of Async Implementation

1. **Better Resource Utilization**: Single-threaded concurrency eliminates thread overhead
2. **Improved Scalability**: Can handle more PLCs without thread limits
3. **Non-blocking I/O**: All network and file operations are non-blocking
4. **Better Error Handling**: Structured concurrency with proper task cancellation
5. **Memory Efficiency**: Lower memory footprint compared to threads

## Usage

The application can be run exactly the same way as before:

```bash
python main.py --config plc_data.json --csv
```

### Command Line Arguments
- `--reload`: Enable reload mode for OpcuaAutoNodeMapper
- `--config`, `-c`: Path to PLC configuration JSON file (default: plc_data.json)
- `--csv`: Enable CSV data storage alongside OPC UA

## Architecture Changes

### Before (Threading Model)
```
Main Thread
├── PLCThread 1
├── PLCThread 2
├── PLCThread N
└── Queue Processing Thread
```

### After (Async Model)
```
Main Event Loop
├── PLCTask 1 (async)
├── PLCTask 2 (async)
├── PLCTask N (async)
└── Queue Processing Task (async)
```

## Key Implementation Details

### Signal Handling
- Unix systems: Uses `loop.add_signal_handler()` for SIGTERM/SIGINT
- Windows: Relies on KeyboardInterrupt handling (add_signal_handler not supported)

### CSV Operations
- CSV file operations remain synchronous for optimal performance
- Async wrappers added for initialization functions
- File I/O is typically fast enough that async benefits are minimal

### Task Management
- Tasks are properly named for identification during error handling
- Graceful shutdown cancels all tasks and waits for completion
- Error propagation through async queue system

## Compatibility Notes

1. **Python Version**: Requires Python 3.7+ for asyncio features used
2. **OPC UA Library**: Existing `opcua` library compatibility maintained
3. **FINS Protocol**: All FINS operations converted to async properly
4. **Configuration**: No changes to JSON configuration format required

## Testing Recommendations

1. **Connection Testing**: Verify FINS UDP connections work properly
2. **Multiple PLCs**: Test concurrent connections to multiple PLCs
3. **Error Handling**: Test network disconnections and recovery
4. **Signal Handling**: Test graceful shutdown with Ctrl+C
5. **Performance**: Compare memory usage and CPU efficiency vs threading

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure all required modules are available
2. **Event Loop**: If running in Jupyter/IPython, may need event loop handling
3. **Signal Handlers**: Windows users may see warning about signal handlers

### Debug Mode
Enable debug logging in the FINS connection for detailed async operation logs.

## Future Enhancements

1. **True Async File I/O**: Could implement `aiofiles` for CSV operations
2. **Connection Pooling**: Async connection pooling for better resource management
3. **Metrics Collection**: Async metrics gathering for performance monitoring
4. **Health Checks**: Periodic async health checks for all connections