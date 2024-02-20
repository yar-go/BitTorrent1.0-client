# BitTorrent 1.0 client  

The torrent client is written in asynchronous python. The program is written using only built-in modules except for the
aiohttp module for creating requests to web servers.

The program was implemented in accordance with the BitTorrent specification 1.0 protocol at this link https://wiki.theory.org/index.php/BitTorrentSpecification

## Features

- Authentication of torrent content
- Resume a download
- Support multi-file torrents
- Seeding pieces
- pseudo graphical interface

It is necessary to implement

- support uTP protocol
- support DHT protocol

## Usage

    $ git clone https://github.com/yar-go/BitTorrent1.0-client.git
    $ cd BitTorrent1.0-client
    $ pip3 install -r requirements.txt
    $ python3 start.py torrent_file destination_folder

