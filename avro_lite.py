#!/usr/bin/env python3
"""Minimal Avro binary encoder/decoder."""
import struct

def _encode_varint(n):
    n = (n << 1) ^ (n >> 63)  # zigzag
    result = bytearray()
    while n > 0x7f:
        result.append((n & 0x7f) | 0x80)
        n >>= 7
    result.append(n & 0x7f)
    return bytes(result)

def _decode_varint(data, pos):
    result = 0; shift = 0
    while True:
        b = data[pos]; pos += 1
        result |= (b & 0x7f) << shift
        if not (b & 0x80): break
        shift += 7
    return (result >> 1) ^ -(result & 1), pos  # zigzag decode

def encode_value(schema, value) -> bytes:
    if schema == "null": return b""
    if schema == "boolean": return b"\x01" if value else b"\x00"
    if schema == "int" or schema == "long": return _encode_varint(value)
    if schema == "float": return struct.pack("<f", value)
    if schema == "double": return struct.pack("<d", value)
    if schema == "string":
        b = value.encode()
        return _encode_varint(len(b)) + b
    if schema == "bytes":
        return _encode_varint(len(value)) + value
    if isinstance(schema, dict) and schema.get("type") == "record":
        result = b""
        for field in schema["fields"]:
            result += encode_value(field["type"], value[field["name"]])
        return result
    if isinstance(schema, dict) and schema.get("type") == "array":
        items = schema["items"]
        if len(value) == 0: return _encode_varint(0)
        result = _encode_varint(len(value))
        for item in value:
            result += encode_value(items, item)
        result += _encode_varint(0)
        return result
    raise ValueError(f"Unsupported schema: {schema}")

def decode_value(schema, data, pos=0):
    if schema == "null": return None, pos
    if schema == "boolean": return data[pos] != 0, pos + 1
    if schema == "int" or schema == "long": return _decode_varint(data, pos)
    if schema == "float": return struct.unpack("<f", data[pos:pos+4])[0], pos + 4
    if schema == "double": return struct.unpack("<d", data[pos:pos+8])[0], pos + 8
    if schema == "string":
        length, pos = _decode_varint(data, pos)
        return data[pos:pos+length].decode(), pos + length
    if schema == "bytes":
        length, pos = _decode_varint(data, pos)
        return data[pos:pos+length], pos + length
    if isinstance(schema, dict) and schema.get("type") == "record":
        result = {}
        for field in schema["fields"]:
            result[field["name"]], pos = decode_value(field["type"], data, pos)
        return result, pos
    if isinstance(schema, dict) and schema.get("type") == "array":
        items_schema = schema["items"]
        result = []
        while True:
            count, pos = _decode_varint(data, pos)
            if count == 0: break
            for _ in range(abs(count)):
                val, pos = decode_value(items_schema, data, pos)
                result.append(val)
        return result, pos
    raise ValueError(f"Unsupported schema: {schema}")

if __name__ == "__main__":
    schema = {"type": "record", "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"}
    ]}
    data = encode_value(schema, {"name": "Alice", "age": 30})
    print(f"Encoded: {data.hex()}")
    print(f"Decoded: {decode_value(schema, data)}")

def test():
    # Primitives
    for val in [0, 1, -1, 127, -128, 10000]:
        enc = _encode_varint(val)
        dec, _ = _decode_varint(enc, 0)
        assert dec == val, f"Varint failed for {val}"
    # String
    enc = encode_value("string", "hello")
    dec, _ = decode_value("string", enc)
    assert dec == "hello"
    # Record
    schema = {"type": "record", "fields": [
        {"name": "x", "type": "int"}, {"name": "y", "type": "string"}
    ]}
    enc = encode_value(schema, {"x": 42, "y": "test"})
    dec, _ = decode_value(schema, enc)
    assert dec == {"x": 42, "y": "test"}
    # Array
    arr_schema = {"type": "array", "items": "int"}
    enc = encode_value(arr_schema, [1, 2, 3])
    dec, _ = decode_value(arr_schema, enc)
    assert dec == [1, 2, 3]
    print("  avro_lite: ALL TESTS PASSED")
