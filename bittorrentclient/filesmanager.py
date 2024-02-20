import hashlib
import os.path
import typing
from math import ceil
from functools import lru_cache

from .bitfield import BitField

class FilesManager:
    """Об'єкти цього класу надають можливість записувати певну кількість байтів за номером куску торент-файлів
    та отримувати дані торент-файлів не зважаючи на структуру завантажувальних файлів"""
    def __init__(self, full_length: int, bitfield: BitField, block_size: int, destination: str,
                 files: typing.Sequence[tuple[int, str]]):
        self._full_length = full_length
        self._bitfield = bitfield
        self._data_count_per_piece = block_size
        self._files = files
        self._destination = destination

    def write_block(self, data: bytes, block_index: int) -> None:
        block_start_index = block_index * self._data_count_per_piece
        block_end_index = block_start_index + self._data_count_per_piece

        total = 0
        for file_size, file in self._files:
            file_start_index = total
            file_end_index = total + file_size - 1

            if file_start_index <= block_start_index <= block_end_index <= file_end_index:  # блок всередині файлу
                self._write_data_file(file, data, block_start_index)
            elif block_start_index <= file_start_index <= file_end_index <= block_end_index:  # файл всередині блоку
                self._write_data_file(file,
                                      data[file_start_index - block_start_index:file_end_index - block_start_index + 1],
                                      file_start_index)
            elif file_start_index <= block_start_index <= file_end_index <= block_end_index:  # файл починається до бло
                self._write_data_file(file, data[:file_end_index - block_start_index + 1], block_start_index)
            elif block_start_index <= file_start_index <= block_end_index <= file_end_index:  # файл починається в серд
                self._write_data_file(file, data[file_start_index - block_start_index:], file_start_index)
            total += file_size
        self._bitfield.set(block_index)

    @lru_cache(1000)
    def read_piece(self, piece_index: int) -> bytes:
        res = b''
        block_start_index = piece_index * self._data_count_per_piece
        block_end_index = block_start_index + self._data_count_per_piece - 1

        total = 0
        for file_size, file in self._files:
            file_start_index = total
            file_end_index = total + file_size - 1

            if file_start_index <= block_start_index <= block_end_index <= file_end_index:  # блок всередині файлу
                res += self._read_data_file(file, block_start_index - file_start_index,
                                            block_end_index - block_start_index + 1)
            elif block_start_index <= file_start_index <= file_end_index <= block_end_index:  # файл всередині блоку
                res += self._read_data_file(file, 0, file_size)
            elif file_start_index <= block_start_index <= file_end_index <= block_end_index:  # файл починається до бло
                res += self._read_data_file(file, block_start_index - file_start_index,
                                            file_end_index - block_start_index + 1)
            elif block_start_index <= file_start_index <= block_end_index <= file_end_index:  # файл починається в серд
                res += self._read_data_file(file, 0, block_end_index - file_start_index)
            total += file_size
        return res

    def _write_data_file(self, filepath: str, data: bytes, start_index: int) -> int:
        paths = os.path.join(os.path.dirname(filepath))
        if paths: os.makedirs(paths)
        open_mode = "w+b" if not os.path.exists(os.path.join(self._destination, filepath)) else "r+b"
        with open(os.path.join(self._destination, filepath), open_mode) as f:
            f.seek(start_index)
            return f.write(data)

    def _read_data_file(self, filepath: str, start_index: int, length_block: int) -> bytes:
        with open(os.path.join(self._destination, filepath), "rb") as f:
            f.seek(start_index)
            return f.read(length_block)

    @property
    def bitfield(self):
        return self._bitfield

    @classmethod
    def open(cls, destination: str, files: typing.Sequence[tuple[int, str]],
             length_piece: int, pieces_hashes: bytes) -> typing.Self:
        full_length = sum([file[0] for file in files])
        bitfield = BitField(ceil(full_length / length_piece))

        obj = cls(full_length=full_length, bitfield=bitfield, block_size=length_piece,
                  destination=destination, files=files)

        for i in range(len(bitfield)):
            try:
                data = obj.read_piece(i)
                if hashlib.sha1(data[:]).digest() == pieces_hashes[i * 20: i * 20 + 20]:
                    obj.bitfield.set(i)
            except FileNotFoundError:
                pass
        return obj
