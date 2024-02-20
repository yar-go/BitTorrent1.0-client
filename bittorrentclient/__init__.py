import argparse
import asyncio
import os.path
import random
import signal

from .torrentfile import TorrentFile
from .bencoder import BenCoderEncodeError
from .loadmanager import LoadManager
from .tracker import TrackerManager
from .ui import UI


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("torrent", help="the torrent file")
    parser.add_argument("destination", help="folder, where torrent files will be downloaded")
    parser.add_argument("--no-upload", action="store_true", default=False, help="don't upload")
    parser.add_argument("--no-download", action="store_true", default=False, help="don't download")
    parser.add_argument("-m", "--max-connections", type=int, default=10, help="max count of peers")
    parser.add_argument("--peer-connection-timeout", type=int, default=10, help="peer connect timeout")
    parser.add_argument("--piece-receive-timeout", type=int, default=5, help="receive piece timeout")
    parser.add_argument("--tracker-connection-timeout", type=int, default=5, help="tracker connect timeout")

    return parser.parse_args()


async def run():
    global loadmanager, track_manager, ui, tasks
    args = get_args()

    torrent_path = args.torrent
    destination_path = args.destination
    to_upload = not args.no_upload
    to_download = not args.no_download
    max_count_peers = args.max_connections
    peer_con_timeout = args.peer_connection_timeout
    piece_receive_timeout = args.piece_receive_timeout
    track_con_timeout = args.tracker_connection_timeout

    try:
        torrent = TorrentFile.open(torrent_path)
    except FileNotFoundError:
        print("Torrent file not found")
        exit(1)
    except BenCoderEncodeError:
        print("Torrent file are spoiled")
        exit(1)

    if not os.path.exists(destination_path):
        print("Desination folder not exist")
        exit(1)

    ui = UI()
    ui.set_static_info(torrent_path, destination_path, max_count_peers)
    ui.set_speed_ava(to_download, to_upload)

    peer_id = b"-PY0001-" + bytes([random.randint(48, 57) for _ in range(12)])
    loadmanager = LoadManager(torrent, destination_path, peer_id, log_func=ui.print, max_connections=max_count_peers,
                              peer_connect_timeout= peer_con_timeout, piece_receive_timeout=piece_receive_timeout)
    track_manager = TrackerManager(torrent, peer_id, timeout=track_con_timeout, log_func=ui.print)
    track_manager.reg_clb_peers(loadmanager.update_peers)
    track_manager.reg_clb_info(loadmanager.get_stat)

    tasks = list()

    asyncio.Task(ui.render(loadmanager.get_stat))
    tasks.append(asyncio.Task(track_manager.run()))
    if to_download:
        tasks.append(asyncio.Task(loadmanager.start_download()))
    if to_upload:
        tasks.append(asyncio.Task(loadmanager.start_upload()))

class ShutdownException(SystemExit):
    pass


def sigint_clb():
    raise ShutdownException()


def main():
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, sigint_clb)

    async def shutdown():
        await loadmanager.shutdown()
        await track_manager.stop()
        await asyncio.gather(*tasks)
        ui.shutdown()

    try:
        loop.run_until_complete(run())
        loop.run_forever()
    except ShutdownException:
        loop.run_until_complete(shutdown())
    ui.shutdown()
