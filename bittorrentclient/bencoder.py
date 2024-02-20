class BenCoderEncodeError(Exception):
    pass

class BenCoder:
    @classmethod
    def decode(cls, data: bytes, info_loc=False):
        """Decodint torrent files. info_loc parameter is using to get location info part for cumputing hash"""

        result = None
        stack = list()
        stack_keys = list()
        info_start = None;
        info_end = None

        key = None
        current_index = 0
        end_index = len(data)
        while current_index < end_index:
            o = None  # Object
            if data[current_index] == 0x64:  # d
                stack.append(dict())
                current_index += 1
                stack_keys.append(key)
                if key == b"info":  # start for info_hash
                    info_start = current_index
                key = None
            elif data[current_index] == 0x6C:  # "l"
                stack.append(list())
                stack_keys.append(key)
                key = None
                current_index += 1
            elif data[current_index] == 0x69:  # "i"
                index_end = current_index + data[current_index:].find(b"\x65")
                num = data[current_index + 1:index_end]
                num = int(num)
                o = num
                current_index = index_end + 1
            elif data[current_index] == 0x65:  # "e"
                o = stack.pop()
                key = stack_keys.pop()
                if key == b"info":  # start for info_end
                    info_end = current_index
                current_index += 1
            else:
                index_end = current_index + data[current_index:].find(b"\x3a")  # ":"
                length_word = data[current_index:index_end]
                length_word = int(length_word)
                word = data[index_end + 1:index_end + length_word + 1]
                o = word
                current_index = index_end + length_word + 1

            if o and len(stack):
                if isinstance(stack[-1], dict):
                    if isinstance(o, dict) or isinstance(o, list):
                        key = cls.__encode_ascii(key)
                        stack[-1][key] = o
                        key = None
                    else:
                        if key is None:
                            key = o
                        else:
                            key = cls.__encode_ascii(key)
                            if not key == "pieces": o = cls.__encode_ascii(o)
                            stack[-1][key] = o
                            key = None
                elif isinstance(stack[-1], list):
                    stack[-1].append(cls.__encode_ascii(o))

            if result is None:
                result = stack[0]

        if info_loc:
            return (info_start, info_end,), result
        return result

    @classmethod
    def encode(cls, data):
        '''Кодування обєктів пітону в бенкод формат'''
        if isinstance(data, dict):
            encoded_part = [cls.encode(key) + cls.encode(data[key]) for key in data]
            encoded_part = b''.join(encoded_part)
            return b'd' + encoded_part + b'e'
        elif isinstance(data, str) or isinstance(data, bytes) or isinstance(data, bytearray):
            if isinstance(data, str): data = data.encode()
            if isinstance(data, bytearray): data = bytes(data)
            return str(len(data)).encode() + b':' + data
        elif isinstance(data, int):
            return b'i' + str(data).encode() + b'e'
        elif isinstance(data, list):
            encoded_part = [cls.encode(e) for e in data]
            return b"l" + b''.join(encoded_part) + b'e'
        else:
            raise BenCoderEncodeError("Accepted only: dict, list, str, int, bytes, bytearray")

    @classmethod
    def __encode_ascii(cls, input: bytes):
        if not isinstance(input, bytes): return input
        try:
            return input.decode()
        except:
            return input

