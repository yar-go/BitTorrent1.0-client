import asyncio
import hashlib
import itertools
import queue
import typing

from .filesmanager import FilesManager
from .torrentfile import TorrentFile
from .statistic import Statistic

if typing.TYPE_CHECKING:
    import peer


class LoadManager:
    """Відповідає за завантаження та відвантаження контенту торенту.
    Стежить за підключенням до пірів, їх обслоговуванням."""
    def __init__(self, torrent: TorrentFile, destination: str, peer_id: bytes, max_connections: int = 10,
                 log_func: typing.Callable[[str], None] = None, peer_connect_timeout=10, piece_receive_timeout=5):
        self._info_hash = torrent.infoHash
        self._piece_len = torrent.piece_length
        self._pieces = torrent.pieces
        self._length = torrent.length
        self._peer_id = peer_id

        self._max_connections = max_connections
        self._peer_connect_timeout = peer_connect_timeout
        self._piece_receive_timeout = piece_receive_timeout
        self._connection_supporter: asyncio.Task | None = None
        self._interesting_supporter: asyncio.Task | None = None

        self.filesmanager = FilesManager.open(destination, torrent.files, self._piece_len, torrent.pieces)
        self._peers: ['peer.Peer'] = list()

        self._connected_peers: list['peer.Peer'] = list()
        self._interesting_peers: list['peer.Peer'] = list()
        self._listhening_tasks: dict['peer.Peer', asyncio.Task] = dict()

        self._queue_pieces = queue.PriorityQueue(len(self.filesmanager.bitfield))
        self._requested_task_per_peer: dict['peer.Peer', asyncio.Task] = dict()
        self._requested_peers_per_num: dict[int, list['peer.Peer']] = dict()

        self.upload_queue = asyncio.Queue(1000)

        self._download_work = True
        self._upload_work = True

        self._uploaded_bytes = 0
        self._downloaded_bytes = self.filesmanager.bitfield.count_available_blocks() * self._piece_len

        self._log_func = log_func if log_func else lambda a: a

    def _run(self) -> None:
        """Запуск задачі, яка підтримує підключення до пірів"""
        self._connection_supporter = asyncio.Task(
            self._support_connected_peers(timeout_to_connect=self._peer_connect_timeout,
                                          once_to_connect=self._max_connections))

    async def start_download(self) -> None:
        """Початок завантаження торенту. Якщо торент завантажено, то завершення."""
        if not self._connection_supporter: self._run()
        self._interesting_supporter = asyncio.Task(self._support_interesting())

        peer_iter = self._interesting_iter()
        next_block_index = None
        self._download_work = True
        self._log_func("Start downloading")
        while self._download_work:
            if self.filesmanager.bitfield.full():
                self._download_work = False
                break

            if not next_block_index:
                if self._queue_pieces.empty():
                    self._prioritize()
                    await asyncio.sleep(0)
                    continue
                _, next_block_index = self._queue_pieces.get()

            for peer in peer_iter:
                await asyncio.sleep(0)
                if peer is None: break
                task = self._requested_task_per_peer.get(peer)
                if task is not None and (task.done() or task.cancelled()):
                    try:
                        await self._requested_task_per_peer[peer]
                    except asyncio.CancelledError:
                        pass
                    self._requested_task_per_peer.pop(peer)
                elif task is not None:
                    break

                if (peer in self._requested_peers_per_num.get(next_block_index, [])
                        or not peer.bitfield.has(next_block_index)):
                    continue
                self._requested_task_per_peer[peer] = asyncio.Task(self._request_piece(peer, next_block_index))
                self._requested_peers_per_num[next_block_index] = (
                    self._requested_peers_per_num.get(next_block_index, list()))
                self._requested_peers_per_num[next_block_index].append(peer)
                next_block_index = None
                break
        self._download_work = False
        self._log_func("Download is stopped")

    async def start_upload(self) -> None:
        """Розпочати відвантаження контенту торенту іншим підключеним пірам"""
        if not self._connection_supporter: self._run()
        self._log_func("Start uploading")
        while self._upload_work:
            if self.upload_queue.empty():
                await asyncio.sleep(0)
                continue
            peer, index, begin, length = await self.upload_queue.get()
            if not self.filesmanager.bitfield.has(index): continue
            data = self.filesmanager.read_piece(index)
            data = data[begin:begin + length]

            self._uploaded_bytes += len(data)
            await peer.send_piece(data, index, begin)
        self._log_func("Upload is stopped")

    async def _request_piece(self, peer: 'peer.Peer', piece_index: int):
        """Запит у піра куска з певним індексом.
        Якщо один кусок запитувався в різних пірів, то перша відповідь надсилає відміну іншим.
        Після отримання шматка, йде надсилання have-повідомлень іншим пірам."""

        block_size = 2 ** 14
        requested_blocks = list()
        if piece_index == len(self.filesmanager.bitfield) - 1:
            piece_length = self._length % self._piece_len
        else:
            piece_length = self._piece_len
        jobs = list()
        for begin in range(int(piece_length / block_size)):
            jobs.append(asyncio.create_task(peer.request(piece_index, begin * block_size, block_size)))
            requested_blocks.append((piece_index, begin * block_size, block_size,))
            # await asyncio.sleep(0)
        if not (piece_length > 0 and (piece_length & (piece_length - 1)) == 0):
            jobs.append(asyncio.create_task(
                peer.request(piece_index, int(piece_length / block_size) * block_size, piece_length % block_size)))

        piece = b""

        done, pending = await asyncio.wait(jobs, timeout=self._piece_receive_timeout, return_when=asyncio.ALL_COMPLETED)
        if pending:
            for bl in requested_blocks:
                await peer.cancel_piece(*bl)

            for t in itertools.chain(pending, done):
                try: await t
                except asyncio.CancelledError: pass
            tmp = self._requested_peers_per_num.get(piece_index)
            if tmp: tmp.remove(peer)
            return False

        bad = False
        for t in jobs:
            if t.exception() is None:
                piece += t.result()
            else:
                bad = True
        if bad:
            return None
        self._downloaded_bytes += len(piece)

        del jobs
        if not hashlib.sha1(piece).digest() == self._pieces[piece_index * 20: piece_index * 20 + 20]:
            return False

        if self.filesmanager.bitfield.has(piece_index):
            self._requested_peers_per_num.pop(piece_index)
            return False

        self.filesmanager.bitfield.set(piece_index)
        self.filesmanager.write_block(piece, piece_index)
        self._log_func(f"Piece {piece_index} got from {peer.ip}:{peer.port}")
        for peer2 in self._requested_peers_per_num.pop(piece_index):
            if peer2 == peer: continue
            [asyncio.Task(peer2.cancel_piece(*block)) for block in requested_blocks]

        self._send_haves(piece_index)
        return True

    async def _support_interesting(self) -> None:
        """Аналізування всіх підключених пірів в пошуку пірів, які мають цікаву інформація."""
        tasks = list()
        while True:
            for peer in self._connected_peers:
                if self.filesmanager.bitfield.count_missing_blocks(peer._bitfield):
                    if not peer.am_interesting:
                        tasks.append(asyncio.Task(peer.interested()))
                    elif peer.am_interesting and not peer.am_choked and peer not in self._interesting_peers:
                        self._interesting_peers.append(peer)
                        self._prioritize()
                else:
                    if peer.am_interesting:
                        tasks.append(asyncio.Task(peer.uninterested()))
                        if peer in self._interesting_peers:  # TODO немає запитань до нього
                            self._interesting_peers.remove(peer)
            for peer in self._interesting_peers:
                if not peer in self._connected_peers: self._interesting_peers.remove(peer)

            for task in tasks:
                if task.done():
                    await task
                    tasks.remove(task)
            try:
                await asyncio.sleep(0)
                # await asyncio.sleep(1)
                # print("INTERESTED:", self._interesting_peers)
            except asyncio.CancelledError as e:
                for task in tasks:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                raise e

    def _interesting_iter(self) -> typing.Iterable[typing.Union['peer.Peer', None]]:
        """Циклічний ітератор по пірам, в яких зацікавлені у завантажені"""
        while True:
            if not self._interesting_peers: yield None
            for p in self._interesting_peers:
                yield p

    def _prioritize(self) -> None:
        """Створення пріоритезованої черги кусків. Найбільш рідкісний кусок перший в черзі"""
        self._queue_pieces = queue.PriorityQueue(len(self.filesmanager.bitfield))
        counter = [[0, i] for i in range(len(self.filesmanager.bitfield))]
        for ip in self._interesting_peers:
            for _, i in counter:
                counter[i][0] += 1 if ip.bitfield.has(i) else 0
        [self._queue_pieces.put(tuple(b)) for b in counter
         if b[0] != 0 and not self.filesmanager.bitfield.has(b[1])]

    async def _connect_peer(self, peer: 'peer.Peer', timeout: int = 10):
        k = await peer.connect(self._info_hash, int(len(self._pieces) / 20), self._peer_id, timeout=timeout)
        if k:
            self._connected_peers.append(peer)
            self._listhening_tasks[peer] = asyncio.Task(peer.listen())
            self._log_func(f"Peer {peer.ip}:{peer.port} connected")
            if not self.filesmanager.bitfield.empty():
                if self.filesmanager.bitfield.count_missing_blocks(peer.bitfield) == 0:
                    await peer.send_bitfield(self.filesmanager.bitfield)
            await peer.unchoke()
            peer.reg_data_taker(self._upload_request)

        return peer

    async def _disconnect_peer(self, peer: 'peer.Peer') -> None:
        if not peer.connected: await peer.disconnect()
        task = self._listhening_tasks.get(peer)
        if not (task is None):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            self._listhening_tasks.pop(peer)
        for l in self._requested_peers_per_num.values():
            if peer in l:
                l.remove(peer)

        # print("PEER DISCONECTED", self._requested_peers_per_num)
        self._log_func(f"Peer {peer.ip}:{peer.port} disconnected")

    async def _support_connected_peers(self, timeout_to_connect: int = 10, once_to_connect=2) -> None:
        while True:
            disconected_peers = list(filter(lambda p: not p.connected, self._connected_peers))
            self._connected_peers = list(
                filter(lambda p: p not in disconected_peers, self._connected_peers))  # remove disconnected

            for dp in disconected_peers:
                await self._disconnect_peer(dp)

            if len(self._connected_peers) < len(self._peers) and len(self._connected_peers) < self._max_connections:
                unconnected = list(filter(lambda a: not a.connected, self._peers))
                # try_to_connect = unconnected[0:self._max_connections-len(self._connected_peers)]
                try_to_connect = unconnected[0:once_to_connect]
                try_to_connect = [asyncio.Task(self._connect_peer(p, timeout_to_connect)) for p in try_to_connect]

                done, _ = await asyncio.wait(try_to_connect, return_when=asyncio.ALL_COMPLETED)
                tried_connected = [await p for p in done]
                # self._peers.append(p)
                [(self._peers.remove(p)) for p in tried_connected if not p.connected]
            # print(len(self._peers), len(self._connected_peers))
            await asyncio.sleep(0)

    def update_peers(self, peers: typing.Sequence['peer.Peer']):
        new_peers = list(filter(lambda a: a not in self._peers, peers))
        self._peers.extend(new_peers)

    def _upload_request(self, peer: 'peer.Peer', index: int, begin: int, lenght: int):
        if self.upload_queue.full(): return
        self.upload_queue.put_nowait((peer, index, begin, lenght,))

    def _send_haves(self, index: int) -> None:
        for peer in self._connected_peers:
            if peer.bitfield.has(index): continue
            asyncio.Task(peer.have(index))

    async def shutdown(self) -> None:
        self._download_work = False
        self._upload_work = False
        if self._interesting_supporter:
            self._interesting_supporter.cancel()
            try:
                await self._interesting_supporter
            except asyncio.CancelledError:
                pass
            self._interesting_supporter = None

        if self._connection_supporter:
            self._connection_supporter.cancel()
            try:
                await self._connection_supporter
            except asyncio.CancelledError:
                pass
            self._connection_supporter = None

            [cp.cancel() for cp in self._listhening_tasks.values()]
            for task in self._listhening_tasks.values():
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            await asyncio.gather(*[cp.disconnect() for cp in self._connected_peers])

    def get_stat(self) -> Statistic:
        uploaded = self._uploaded_bytes
        downloaded = self._downloaded_bytes
        left = self._length - self._piece_len * self.filesmanager.bitfield.count_available_blocks() \
            if not self.filesmanager.bitfield.full() else 0
        connected = len(self._connected_peers)
        interesting = len(self._interesting_peers)
        peers_count = len(self._peers)
        length = self._length
        return Statistic(uploaded=uploaded, downloaded=downloaded, left=left, connected=connected,
                         interesting=interesting, length=length, peers_count=peers_count)

