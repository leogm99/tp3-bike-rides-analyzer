def receive_json_message(recv):
    payload_size_buffer = recv(PACKET_LENGTH_FIELD_SIZE)
    payload_size = struct.unpack('!I', payload_size_buffer)[0]
    return json.loads(
        struct.unpack(f'{payload_size}s',
                      recv(payload_size))[0]
        .decode('utf8')
    )
