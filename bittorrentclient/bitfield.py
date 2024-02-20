import typing
from math import ceil

class UnsupportedDifference(Exception):
    pass


class BitField:
    """ Об'єкти класу представляють бітове поле. Використовується для відображення
    кількості завантажених кусків торент-файлів піром."""
    def __init__(self, bits_count: int, bits: bytes | None= None):
        self._bits_count = bits_count
        if bits: self._bits = bytearray(bits)
        else: self._bits = bytearray(ceil(bits_count/8))

    def count_available_blocks(self):
        return sum([int(self.has(i)) for i in range(self._bits_count)])

    def count_missing_blocks(self, other: typing.Self) -> int:
        if not len(self) == len(other): raise UnsupportedDifference
        original = bytes(self)
        other = bytes(other)
        dif = 0
        for x1, x2 in zip(original, other):
            while x1 or x2:
                dif += ~(x1 & 1) & (x2 & 1)
                x1 >>= 1
                x2 >>= 1
        return dif

    def full(self) -> bool:
        last_value = 255 & (0xff00 >> (self._bits_count % 8))
        last_value = last_value if last_value else 255
        for b in self._bits[:-1]:
            if not b == 0xff:return False
        if self._bits[-1] != last_value:
            return False
        return True

    def empty(self) -> bool:
        return not any(self._bits)

    def has(self, index) -> bool:
        if index >= self._bits_count or index < 0:
            raise IndexError("BitField index out of range")
        i = index // 8
        offset = index % 8
        return bool((self._bits[i] >> (7 - offset)) & 1)

    def set(self, index) -> None:
        if 0>= index >= self._bits_count:
            raise IndexError("BitField index out of range")
        i = index//8
        offset = index % 8
        self._bits[i] |= (1 << (7-offset))

    def copy(self, other: typing.Self):
        if not len(other) == ceil(len(self)/8):
            raise Exception("Other bitfield is not same size")
        self._bits = bytearray(other)

    @staticmethod
    def serialize(bitfield):
        return (bitfield.count_bits, bytes(bitfield).hex())

    @classmethod
    def deserialize(cls, data: tuple[int, str]):
        return cls(data[0], bytes.fromhex(data[1]))

    @property
    def count_bits(self):
        return self._bits_count

    @property
    def bits(self):
        return self._bits

    def __bytes__(self):
        return bytes(self._bits)

    def __len__(self):
        return self._bits_count

    def __getitem__(self, item):
        return self.has(item)

    def __eq__(self, other):
        return self.bits == other.bits and self.count_bits == other.count_bits
