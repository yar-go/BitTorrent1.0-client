import asyncio
import dataclasses
import enum
import random
import typing
from time import time

import aiohttp

from .bencoder import BenCoder
from .peer import Peer


if typing.TYPE_CHECKING:
    import statistic
    import torrentfile


def bin_to_rfc1738(bin: bytes) -> str:
    result = ""
    for char in bin:
        if 48 <= char <= 57 or 65 <= char <= 90 or 97 <= char<= 122 or char == 0x7F or char in b"~$-_.+!*'(),":
            result += chr(char)
        else:
            r = hex(char).removeprefix("0x")
            if len(r) == 1: r = "%0" + r
            else: r = "%"+r
            result += r.upper()
    return result


@dataclasses.dataclass(frozen=True)
class TrackResponse:
    failure: str | None
    warning: str | None
    interval: int
    min_interval: int | None
    tracker_id: str | None
    complete: int
    incomplete: int
    peers: list[Peer] | None

    @classmethod
    def de_dict(cls, data):
        if isinstance(data.get("peers"), bytes):
            b = bytearray(data.get("peers"))
            peers = list()
            for index in range(0, len(b), 6):
                ip = '.'.join([str(i) for i in b[index:index + 4]])
                port = int.from_bytes(bytes(b[index + 4:index + 6]), "big",
                                      signed=False)  # (b[index+4] << 8) + b[index+5]
                peers.append((ip, port,))
        elif isinstance(data.get("peers"), dict):
            peers = data.get("peers")
        else:
            peers = None
        peers = [Peer.de_peer(i) for i in peers]
        return cls(failure=data.get('failure reason'),
                   warning=data.get('warning message'),
                   interval=data.get('interval'),
                   min_interval=data.get('min interval'),
                   tracker_id=data.get('tracker id'),
                   complete=data.get('complete'),
                   incomplete=data.get('incomplete'),
                   peers=peers)


class TrackerRequestEvent(enum.Enum):
    STARTED = "started"
    STOPPED = "stopped"
    COMPLETED = "completed"
    REGULAR = ""


@dataclasses.dataclass()
class Tracker:
    urls: list[str]
    interval: int = dataclasses.field(default=0, compare=False)
    min_interval: int = dataclasses.field(default=0, compare=False)
    next_time_query: int = dataclasses.field(default=0, compare=False)

    def __post_init__(self):
        random.shuffle(self.urls)

    def get_url(self):
        return self.urls[0]

    def move(self):
        self.urls.append(self.urls.pop(0))

    def __len__(self):
        return len(self.urls)

class CallbackSetterPeersNotSet(Exception):
    pass


class TrackerManager:
    def __init__(self, torrent: "torrentfile.TorrentFile", peer_id: bytes, compact=True, timeout=5, log_func=None):
        self.torrent = torrent

        if self.torrent.announce_list:
            self._trackers = [Tracker(urls=track_group) for track_group in self.torrent.announce_list]
        else:
            self._trackers = [Tracker(urls=[self.torrent.announce,])]

        self._peer_id = peer_id
        self._port = 10101
        self._compact = compact

        self._uploaded = 0
        self._downloaded = 0
        self._left = self.torrent.length - self._downloaded

        self._set_peers_clb: typing.Callable[[typing.Sequence[Peer]], None] | None = None
        self._get_info_clb: typing.Callable[[], "statistic.Statistic"] | None = None

        self._working = False
        self._timeout = timeout
        self._session: aiohttp.ClientSession | None = None

        self._log_func = log_func if log_func else lambda a: a

    async def run(self) -> None:
        if not self._set_peers_clb or not self._get_info_clb:
            raise CallbackSetterPeersNotSet
        timeout = aiohttp.ClientTimeout(total=self._timeout)
        self._session = aiohttp.ClientSession(timeout=timeout)

        self._working = True
        while self._working:
            for tracker in self._trackers:
                if tracker.next_time_query < time():
                    info = self._get_info_clb()
                    self._uploaded = info.uploaded
                    self._downloaded = info.downloaded
                    self._left = self.torrent.length - self._downloaded

                    await self._regular_service_tracker(tracker)
            await asyncio.sleep(1)
        self._log_func("Tracker manager is stopped")

    async def _regular_service_tracker(self, tracker: Tracker) -> None:
        event = TrackerRequestEvent.REGULAR
        if tracker.next_time_query == 0:
            event = TrackerRequestEvent.STARTED

        for i in range(len(tracker)):
            response = await self._send_request(tracker, event)
            if response:
                self._trackers.remove(tracker)
                self._trackers.insert(0, tracker)
                self._set_peers_clb(response.peers)
                self._log_func(f"Tracker {tracker.get_url()} has given {len(response.peers)} peers")

                tracker.interval = response.interval
                tracker.min_interval = response.min_interval if response.min_interval else response.interval
                tracker.next_time_query = time() + min(tracker.min_interval, tracker.interval)
                break
            else:
                tracker.move()

    async def _send_request(self, tracker: Tracker, event: TrackerRequestEvent) -> TrackResponse | None:
        params = {"info_hash": bin_to_rfc1738(self.torrent.infoHash),
                          "peer_id": bin_to_rfc1738(self._peer_id),
                          "uploaded": self._uploaded,
                          "downloaded": self._downloaded,
                          "compact": int(self._compact),
                          "left": self._left,
                          "port": self._port}
        if not event == TrackerRequestEvent.REGULAR:
            params["event"] = event.value

        url = tracker.get_url() + "?" + '&'.join([f"{name}={value}" for name, value in params.items()])
        try:
            async with self._session.get(url) as resp:
                if not resp.status == 200:
                    return None
                r = await resp.content.read()

                resp_dicted = BenCoder.decode(r)
                tr = TrackResponse.de_dict(resp_dicted)
        except:
            return None
        return tr

    def reg_clb_peers(self, clb: typing.Callable[[typing.Sequence[Peer]], None]) -> None:
        self._set_peers_clb = clb

    def reg_clb_info(self, clb: typing.Callable[[], "statistic.Statistic"]) -> None:
        self._get_info_clb = clb

    async def complete(self) -> None:
        self._left = 0
        tracks = self._trackers[:]
        await asyncio.gather(*[self._send_request(track, TrackerRequestEvent.COMPLETED) for track in tracks])

    async def stop(self) -> None:
        self._working = False
        self._log_func("Tracker manager is stopping")
        tracks = self._trackers[:]
        await asyncio.gather(*[self._send_request(track, TrackerRequestEvent.STOPPED) for track in tracks])
        if self._session:
            await self._session.close()
            self._session = None

