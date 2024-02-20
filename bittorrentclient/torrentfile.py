import datetime
import hashlib
import os

from .bencoder import BenCoder


class BadTorrentFile(Exception):
    pass


class TorrentFile:
    def __init__(self, data, infohash):
        self._infohash = infohash
        self._data = data
        if not self._data['info'].get('files'):
            self._full_file_size = self._data['info']['length']
        else:
            self._full_file_size = sum([i['length'] for i in self._data['info']["files"]])

    @property
    def announce(self):
        return self._data.get("announce")

    @property
    def announce_list(self):
        return self._data.get("announce-list")

    @property
    def infoHash(self):
        return self._infohash

    @property
    def comment(self):
        return self._data.get("comment")

    @property
    def name(self):
        return self._data['info']['name']

    @property
    def creationDate(self):
        creation_date = self._data.get('creation date')
        if creation_date: return datetime.datetime.fromtimestamp(creation_date)

    @property
    def createdBy(self):
        return self._data.get("created by")

    @property
    def countFiles(self):
        return len(self._data['info'].get("files", [""]))

    @property
    def files(self):
        if not self._data['info'].get('files'):
            files = [(self._data['info']['length'], self._data['info']['name'],), ]
        else:
            files = [(i['length'], os.path.sep.join(i['path']),) for i in self._data['info']["files"]]
        return files

    @property
    def length(self):
        return self._full_file_size

    @property
    def piece_length(self):
        return self._data['info']["piece length"]

    @property
    def pieces(self):
        return self._data['info']["pieces"]

    @classmethod
    def open(cls, file: str):
        with open(file, "rb") as f:
            data_encoded = f.read()
            info_loc, data = BenCoder.decode(data_encoded, True)

        if not all((data.get("info"), data.get("announce"), data.get("info", dict()).get("piece length"),
                    data.get("info", dict()).get("pieces"),)):
            raise BadTorrentFile

        infohash = hashlib.sha1(data_encoded[info_loc[0] - 1:info_loc[1] + 1]).digest()
        return cls(data, infohash)
