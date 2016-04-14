
from datetime import datetime
import struct


def to_hex(s):
    hex_str = ' '.join("{0:02x}".format(ord(c)) for c in s)
    return '\n'.join([hex_str[i:i+48] for i in range(0, len(hex_str), 48)])


def to_date(epoch_micros):
    epoch_secs = epoch_micros / 1000000.0
    return datetime.fromtimestamp(epoch_secs).strftime('%Y-%m-%d %H:%M:%S.%f')


def pack_ts(ts):
    return struct.pack(">Q", ts)


def unpack_ts(packed_ts):
    return struct.unpack_from(">Q", bytes(packed_ts), 0)[0]
