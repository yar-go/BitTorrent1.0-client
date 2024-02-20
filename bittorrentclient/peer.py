import asyncio
import time
import struct
import typing

from .bitfield import BitField


class PeerNotConnected(Exception):
    pass


class Peer:
    def __init__(self, ip: str, port: int, peer_id=None):
        self._ip = ip.strip()
        self._port = port
        self._peer_id = peer_id

        self._stream_reader: asyncio.StreamReader | None = None
        self._stream_writer: asyncio.StreamWriter | None = None
        self._connected = False

        self._am_choking = True
        self._am_interested = False
        self._peer_choking = True
        self._peer_interested = False

        self._keep_aliver_task: asyncio.Task | None = None
        self._last_message_time = 0

        self._bitfield: BitField | None = None

        self._requested_blocks: dict[tuple[int, int], tuple[int, asyncio.Future]] = dict()

        self._data_taker_clb: typing.Callable | None = None

    @property
    def ip(self):
        return self._ip

    @property
    def port(self):
        return self._port

    @property
    def id(self):
        return self._peer_id

    @property
    def connected(self):
        return self._connected

    @property
    def bitfield(self):
        return self._bitfield

    @property
    def am_choking(self):
        return self._am_choking

    @property
    def am_choked(self):
        return self._peer_choking

    @property
    def am_interesting(self):
        return self._am_interested

    @property
    def am_interested(self):
        return self._peer_interested

    async def connect(self, info_hash: bytes, pieces_count: int, peer_id: bytes, timeout: int = 3) -> bool:
        async def _keep_aliver():
            await asyncio.sleep(5)
            while True:
                if time.time() - self._last_message_time >= 10 and not self.am_choked:
                    await self.keep_alive()
                await asyncio.sleep(1)

        del self._stream_writer
        del self._stream_reader
        self._stream_writer = self._stream_reader = None
        try:
            self._stream_reader, self._stream_writer = \
                await asyncio.wait_for(asyncio.open_connection(self.ip, self.port), timeout)
        except:
            return False

        pstr = bytearray(b"BitTorrent protocol")
        reversed = bytearray(8)
        handshake = struct.pack(f"!B{len(pstr)}s8s20s20s",
                                len(pstr), pstr, reversed,
                                info_hash, peer_id)


        try:
            await self._safe_write(handshake)
            r = await self._stream_reader.read(len(handshake))
        except:
            return False

        if not r:
            return False

        _, r_pstr, r_reversed, r_info_hash, r_peer_id = struct.unpack(f"!B{len(r) - 49}s8s20s20s", r)

        if info_hash == r_info_hash and (self._peer_id is None or self._peer_id == r_peer_id):
            self._connected = True
            self._peer_id = r_peer_id
            self._bitfield = BitField(pieces_count)
            self._am_choking = self._peer_choking = True
            self._am_interested = self._peer_interested = False
            self._keep_aliver_task = asyncio.create_task(_keep_aliver())
            self._last_message_time = time.time()

            return True

        try:
            self._stream_writer.close()
            await self._stream_writer.wait_closed()
        except:
            pass
        return False

    async def disconnect(self) -> None:
        if not self._connected:
            return None
        self._connected = False
        self._keep_aliver_task.cancel()
        try: await self._keep_aliver_task
        except asyncio.CancelledError: pass
        [i[1].cancel() for i in self._requested_blocks.values()]

        try:
            print("DISCON 131", self)
            if not self._stream_writer.is_closing():
                self._stream_writer.close()
                await self._stream_writer.wait_closed()
                print("DISCON SUCCESS 135")
        except: pass

        self._peer_choking = self._am_choking = True
        self._am_interested = self._peer_interested = True
        self._last_message_time = 0

    async def listen(self) -> None:
        if not self._connected:
            raise PeerNotConnected()

        buffer = bytes()
        while self._connected:
            try:
                data = await self._stream_reader.read(2**14)
            except:
                await self.disconnect()
                break
            if not data:
                await self.disconnect()
                break
            buffer += data

            while len(buffer) >= 4:
                message_len = int.from_bytes(buffer[:4])
                if not len(buffer) >= message_len + 4:
                    break

                if message_len == 0:
                    pass  # TODO keep-alive
                    buffer = buffer[4:]
                    continue

                message_id = buffer[4]
                message = buffer[5:4 + message_len]
                if message_id == 0:  # Choke
                    self._peer_choking = True
                elif message_id == 1:  # Unchoke
                    self._peer_choking = False
                elif message_id == 2:  # interested
                    self._peer_interested = True
                elif message_id == 3:  # uninterested
                    self._peer_interested = False
                elif message_id == 4:  # have
                    index = struct.unpack('!i', message)[0]
                    self._bitfield.set(index)
                elif message_id == 5:  # bitfield
                    try:
                        self._bitfield.copy(message)
                    except:
                        self._connected = False
                        break
                elif message_id == 6:  # requests
                    index, begin, length = struct.unpack(f'!iii', message)
                    self._me_requested(index, begin, length)
                elif message_id == 7:  # piece
                    index, begin, block = struct.unpack(f'!ii{message_len-9}s', message)
                    self._get_piece(index, begin, block)
                elif message_id == 8:  # cancel
                    pass
                elif message_id == 9:  # port
                    pass  # DHT. I can nothing to do now
                buffer = buffer[4 + message_len:]

    def _me_requested(self, index:int, begin:int, lenght:int) -> None:
        if not self._data_taker_clb:
            return
        self._data_taker_clb(self, index, begin, lenght)

    async def send_piece(self, data: bytes, index: int, begin: int) -> None:
        query = struct.pack(f'!ibii{len(data)}s', 9+len(data), 7, index, begin, data)
        await self._safe_write(query)
        self._last_message_time = time.time()

    async def keep_alive(self) -> None:
        query = bytearray(b"\x00\x00\x00\x00")
        await self._safe_write(query)
        self._last_message_time = time.time()

    async def have(self, index: int) -> None:
        query = struct.pack(f'!ibi', 5, 4, index)
        await self._safe_write(query)
        self._last_message_time = time.time()

    async def interested(self) -> None:
        query = bytearray(b"\x00\x00\x00\x01\x02")
        await self._safe_write(query)
        self._am_interested = True
        self._last_message_time = time.time()

    async def uninterested(self) -> None:
        query = bytearray(b"\x00\x00\x00\x01\x03")
        await self._safe_write(query)
        self._am_interested = False
        self._last_message_time = time.time()

    async def choke(self) -> None:
        query = bytearray(b"\x00\x00\x00\x01\x00")
        await self._safe_write(query)
        self._am_choking = True
        self._last_message_time = time.time()

    async def unchoke(self) -> None:
        query = bytearray(b"\x00\x00\x00\x01\x01")
        await self._safe_write(query)
        self._am_choking = False
        self._last_message_time = time.time()

    async def send_bitfield(self, bitfield: BitField) -> None:
        byter = bitfield.bits
        query = struct.pack(f'!ib{len(byter)}s', 1 + len(byter), 5, bytes(byter))
        await self._safe_write(query)
        self._last_message_time = time.time()

    async def request(self, index: int, begin: int, length: int) -> bytes:
        future = asyncio.get_running_loop().create_future()
        self._requested_blocks[(index, begin,)] = (length, future,)

        query = struct.pack('!iB3i', 13, 6, index, begin, length)
        await self._safe_write(query)
        self._last_message_time = time.time()
        return await future

    async def cancel_piece(self, index:int, begin: int, length: int) -> None:
        if not (t := self._requested_blocks.get((index, begin,))): return
        length_d, future = t
        if not length_d == length:
            raise Exception("Bad length")
        future.cancel()
        self._requested_blocks.pop((index, begin,))

        query = struct.pack('!iB3i', 13, 8, index, begin, length)
        await self._safe_write(query)
        self._last_message_time = time.time()

    def reg_data_taker(self, clb) -> None:
        self._data_taker_clb = clb

    def _get_piece(self, index: int, begin: int, block: bytes) -> None:
        if not (t := self._requested_blocks.get((index, begin,))): return
        _, future = t

        future.set_result(block)
        self._requested_blocks.pop((index, begin,))

    async def _safe_write(self, data: bytes) -> None:
        # if self._stream_writer.is_closing():
        #     await self.disconnect()
        #     return
        try:
            self._stream_writer.write(data)
            await self._stream_writer.drain()
        except:
            await self.disconnect()
            return

    @classmethod
    def de_peer(cls, data) -> typing.Self:
        if isinstance(data, dict):
            if all([i in data.keys() for i in ("ip", 'port')]):
                return cls(ip=data['ip'], port=data['port'], peer_id=data.get('peer id'))
        if isinstance(data, list) or isinstance(data, tuple):
            if len(data) == 2:
                return cls(ip=data[0], port=data[1])

    def __repr__(self):
        return f"Peer {self.id if self.id else ''} {self.ip}:{self.port}"

    def __eq__(self, other):
        if self.ip == other.ip and self.port == other.port: return True
        return False

    def __hash__(self):
        return hash((self.ip, self.port))

