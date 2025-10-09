import struct

def toBin(  data):
        outdata = format(int.from_bytes(data, 'big'), 'b')

        return [outdata]

def WordToHex(data: bytes):
    # Interpret the bytes as a big-endian integer
    val = int.from_bytes(data, byteorder='big')
    # Format as 4-digit uppercase hex (like CX-Programmer shows)
    return [f"{val:04X}"]


def WordToHex32(data: bytes, little_endian=True) -> str:
    """
    Convert 4 bytes from PLC into an 8-digit hex string.

    Parameters:
        data (bytes): 4 bytes from PLC (2 words)
        little_endian (bool): if True, PLC sends low word first (typical in Omron)

    Returns:
        str: 8-digit hex string, e.g., '1234ABCD'
    """
    if len(data) != 4:
        raise ValueError("Data must be exactly 4 bytes for a UDINT")
    
    # Reverse bytes if PLC is little-endian (low word first)
    if little_endian:
        data = data[2:4] + data[0:2]  # swap words (2 bytes each)
    
    # Convert to integer
    val = int.from_bytes(data, byteorder='big')
    
    # Format as 8-digit hex
    return [f"{val:08X}"]

def WordToBin(  data):
    size = len(data) * 8
    strBin = format(int.from_bytes(data, 'big'), 'b')
    outdata = (('0' * (size)) + strBin) [-size:]
    print(f"*********from the conversion program ----> {outdata}")
    return [outdata]

def toInt16(  data):
    outdata = []
    arydata = bytearray(data)
    for idx in range(0, len(arydata), 2):
        tmpdata = arydata[idx:idx+2]
        outdata += (struct.unpack('>h',tmpdata))
    # print(f"*********from the conversion program toInt16 ----> {outdata}")
    return outdata

def toUInt16(  data):
    outdata = []
    arydata = bytearray(data)
    for idx in range(0, len(arydata), 2):
        tmpdata = arydata[idx:idx+2]
        outdata += (struct.unpack('>H',tmpdata))
    
    return outdata

def toInt32_old(  data):
    outdata = []
    arydata = bytearray(data)
    for idx in range(0, len(arydata), 4):
        tmpdata = arydata[idx:idx+4]
        outdata += (struct.unpack('>i',tmpdata))
    
    return outdata

def toInt32(  data):
    outdata = []
    arydata = bytearray(data)
    for idx in range(0, len(arydata), 4):
        tmpdata = arydata[idx:idx+4]
        tmpdata[0:2], tmpdata[2:4] = tmpdata[2:4], tmpdata[0:2]
        outdata += (struct.unpack('>i',tmpdata))

    return outdata

def toUInt32(  data):
    outdata = []
    arydata = bytearray(data)
    for idx in range(0, len(arydata), 4):
        tmpdata = arydata[idx:idx+4]
        tmpdata[0:2], tmpdata[2:4] = tmpdata[2:4], tmpdata[0:2]
        outdata += (struct.unpack('>I',tmpdata))
    
    return outdata

def toInt64(  data):
    outdata = []
    arydata = bytearray(data)
    for idx in range(0, len(arydata), 8):
        tmpdata = arydata[idx:idx+8]
        tmpdata[0:2],tmpdata[2:4],tmpdata[4:6],tmpdata[6:8] = tmpdata[6:8],tmpdata[4:6],tmpdata[2:4],tmpdata[0:2]
        outdata += (struct.unpack('>q',tmpdata))

    return outdata
    
def toUInt64(  data):
    outdata = []
    arydata = bytearray(data)
    for idx in range(0, len(arydata), 8):
        tmpdata = arydata[idx:idx+8]
        tmpdata[0:2],tmpdata[2:4],tmpdata[4:6],tmpdata[6:8] = tmpdata[6:8],tmpdata[4:6],tmpdata[2:4],tmpdata[0:2]
        outdata += (struct.unpack('>Q',tmpdata))
    
    return outdata

def toFloat(  data):
    outdata = []
    arydata = bytearray(data)
    for idx in range(0, len(arydata), 4):
        tmpdata = arydata[idx:idx+4]
        tmpdata[0:2], tmpdata[2:4] = tmpdata[2:4], tmpdata[0:2]
        outdata += (struct.unpack('>f', tmpdata))

    return outdata

def toDouble(  data):
    outdata = []
    arydata = bytearray(data)
    for idx in range(0, len(arydata), 8):
        tmpdata = arydata[idx:idx+8]
        tmpdata[0:2],tmpdata[2:4],tmpdata[4:6],tmpdata[6:8] = tmpdata[6:8],tmpdata[4:6],tmpdata[2:4],tmpdata[0:2]
        outdata += (struct.unpack('>d', tmpdata))

    return outdata

def toString(  data):
    outdata = data.decode("ascii")
    return outdata


def bcd_to_decimal(bcd):
    if isinstance(bcd, bytes):
        bcd = int.from_bytes(bcd, 'big')
        
    return [((bcd >> 4) * 10) + (bcd & 0x0F)]
    
def bcd_to_decimal2(bcd_bytes):
    return [((bcd_bytes[0] >> 4) * 1000) + ((bcd_bytes[0] & 0x0F) * 100) + \
        ((bcd_bytes[1] >> 4) * 10) + (bcd_bytes[1] & 0x0F)]