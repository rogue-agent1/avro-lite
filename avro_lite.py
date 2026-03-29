#!/usr/bin/env python3
"""Apache Avro-style schema-based binary encoder/decoder."""
import sys, struct, json

def encode_varint(n):
    n = (n << 1) ^ (n >> 63)  # zigzag
    result = bytearray()
    while n > 0x7F: result.append((n & 0x7F) | 0x80); n >>= 7
    result.append(n & 0x7F); return bytes(result)

def decode_varint(data, offset):
    result = 0; shift = 0
    while True:
        b = data[offset]; offset += 1
        result |= (b & 0x7F) << shift; shift += 7
        if not (b & 0x80): break
    return (result >> 1) ^ -(result & 1), offset  # zigzag decode

def encode_value(schema, value):
    t = schema if isinstance(schema, str) else schema.get("type", "null")
    if t == "null": return b""
    if t == "boolean": return b"\x01" if value else b"\x00"
    if t == "int" or t == "long": return encode_varint(value)
    if t == "float": return struct.pack("<f", value)
    if t == "double": return struct.pack("<d", value)
    if t == "string":
        b = value.encode(); return encode_varint(len(b)) + b
    if t == "bytes": return encode_varint(len(value)) + value
    if t == "array":
        items_schema = schema["items"]; result = bytearray()
        if value: result.extend(encode_varint(len(value)))
        for item in value: result.extend(encode_value(items_schema, item))
        result.extend(b"\x00"); return bytes(result)
    if t == "record":
        result = bytearray()
        for field in schema["fields"]:
            result.extend(encode_value(field["type"], value.get(field["name"])))
        return bytes(result)
    if t == "map":
        vals_schema = schema["values"]; result = bytearray()
        if value: result.extend(encode_varint(len(value)))
        for k, v in value.items():
            result.extend(encode_value("string", k))
            result.extend(encode_value(vals_schema, v))
        result.extend(b"\x00"); return bytes(result)
    return b""

def decode_value(schema, data, offset):
    t = schema if isinstance(schema, str) else schema.get("type", "null")
    if t == "null": return None, offset
    if t == "boolean": return bool(data[offset]), offset + 1
    if t == "int" or t == "long": return decode_varint(data, offset)
    if t == "float": return struct.unpack_from("<f", data, offset)[0], offset + 4
    if t == "double": return struct.unpack_from("<d", data, offset)[0], offset + 8
    if t == "string":
        length, offset = decode_varint(data, offset)
        return data[offset:offset+length].decode(), offset + length
    if t == "bytes":
        length, offset = decode_varint(data, offset)
        return data[offset:offset+length], offset + length
    if t == "array":
        items = []; items_schema = schema["items"]
        while True:
            count, offset = decode_varint(data, offset)
            if count == 0: break
            for _ in range(count):
                val, offset = decode_value(items_schema, data, offset)
                items.append(val)
        return items, offset
    if t == "record":
        result = {}
        for field in schema["fields"]:
            val, offset = decode_value(field["type"], data, offset)
            result[field["name"]] = val
        return result, offset
    return None, offset

def main():
    print("=== Avro Lite ===\n")
    schema = {"type": "record", "name": "User", "fields": [
        {"name": "name", "type": "string"}, {"name": "age", "type": "int"},
        {"name": "score", "type": "double"}, {"name": "active", "type": "boolean"},
        {"name": "tags", "type": {"type": "array", "items": "string"}}
    ]}
    record = {"name": "Alice", "age": 30, "score": 95.5, "active": True, "tags": ["dev", "python"]}
    encoded = encode_value(schema, record)
    json_size = len(json.dumps(record).encode())
    print(f"Schema: {schema['name']}")
    print(f"Record: {record}")
    print(f"Avro: {len(encoded)} bytes, JSON: {json_size} bytes ({len(encoded)/json_size*100:.0f}%)")
    decoded, _ = decode_value(schema, encoded, 0)
    print(f"Decoded: {decoded}")
    print(f"Roundtrip: {'✅' if decoded['name'] == record['name'] and decoded['age'] == record['age'] else '❌'}")

if __name__ == "__main__": main()
