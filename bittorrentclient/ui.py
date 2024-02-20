import asyncio
import curses
import time

from .loadmanager import Statistic

def sizeof_fmt(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

def progress_bar(screen: curses.window, y:int, x:int, size:int, progress:float):
    """Створює progres-bar. Координати x, y задаються для верхнього лівого кута. size відповідає за ширину. """
    screen.hline(y, x+1, curses.ACS_HLINE, size-2)
    screen.hline(y+2, x+1, curses.ACS_HLINE, size-2)
    screen.addch(y, x, curses.ACS_ULCORNER)
    screen.addch(y, x+size-1, curses.ACS_URCORNER)
    screen.addch(y+2, x, curses.ACS_LLCORNER)
    screen.addch(y+2, x + size-1, curses.ACS_LRCORNER)
    screen.addch(y+1, x, curses.ACS_VLINE)
    screen.addch(y + 1, x+ size-1, curses.ACS_VLINE)

    progress = progress if 0 <= progress <= 1 else 1
    bars = int(progress * (size-2))
    color = curses.color_pair(1) if progress == 1 else curses.color_pair(0)
    screen.hline(y+1, x+1, curses.ACS_BOARD, bars, color)
    screen.addstr(y, x+2, str(int(progress*100))+"%")


class UI:
    """Відвовідає за відображення стану програми"""
    def __init__(self):
        self.screen = curses.initscr()
        curses.noecho()
        curses.cbreak()
        curses.start_color()
        curses.curs_set(0)
        curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_GREEN)

        self._stat: Statistic = Statistic(0, 0, 0, 0, 0, 0, 0)
        self._another_info = dict()
        self._down_indicators = (False, False)
        self._speedometr = self._speed()
        self._speedometr.send(None)

        self.to_print = list()
        self._work = False

    def set_static_info(self, torrent, destination_path, max_connections):
        """Встановлення статичної інформації"""
        self._another_info["torrent"] = torrent
        self._another_info["destination"] = destination_path
        self._another_info["max_connections"] = max_connections

    def set_speed_ava(self, download: bool, upload: bool):
        """Увімкнення показу швидкості для завантаження та відвантаження"""
        self._down_indicators = (download, upload,)

    def _speed(self) -> (str, str):
        """Розрахунок швидкості завантаження та відвантаження"""
        old_time = time.time()
        old_down = self._stat.downloaded
        old_up = self._stat.uploaded
        up_speed = down_speed = "..."
        first = True
        while True:
            if time.time() - old_time < 1:
                yield down_speed, up_speed
                continue
            new_time = time.time()
            new_down = self._stat.downloaded
            new_up = self._stat.uploaded
            diff_time = new_time-old_time
            diff_down = new_down - old_down
            diff_up = new_up - old_up
            old_time = new_time
            old_down = new_down
            old_up = new_up
            down_speed = sizeof_fmt(diff_down // diff_time) + "/s" if self._down_indicators[0] else "off"
            up_speed = sizeof_fmt(diff_up // diff_time) + "/s" if self._down_indicators[1] else "off"
            if first:
                down_speed = up_speed = "..."
                first = False
            yield down_speed, up_speed

    async def render(self, update_getter, period=0.1):
        self._work = True
        while self._work:
            self.update(update_getter())
            self.screen.clear()
            curses.update_lines_cols()

            if curses.COLS < 60 or curses.LINES<16:
                self.screen.refresh()
                print("Minimum screen size is 60x16")
                return

            down_up_str = f"Downloaded: {sizeof_fmt(self._stat.downloaded)}    Uploaded: {sizeof_fmt(self._stat.uploaded)}"
            self.screen.addstr(3, 0, down_up_str)
            full_str = f"Full: {sizeof_fmt(self._stat.length)}"
            self.screen.addstr(3, curses.COLS - len(full_str), full_str)
            p = self._stat.downloaded / self._stat.length if self._stat.length != 0 else 0
            progress_bar(self.screen, 0, 0, curses.COLS, p)
            self.screen.hline(4, 0, curses.ACS_S1, curses.COLS)

            torrent_loc = f"Torrent: {self._another_info.get('torrent', '...')}"
            destination_loc = f"Destination: {self._another_info.get('destination', '...')}"
            self.screen.addstr(5, 0, torrent_loc)
            self.screen.addstr(6, 0, destination_loc)

            self.screen.hline(7, 0, curses.ACS_S1, curses.COLS)
            self.screen.addstr(8, 0, f"All peers: {self._stat.peers_count}")
            self.screen.addstr(8, 20, f"Max count of peers: {self._another_info.get('max_connections', '...')}")
            self.screen.addstr(9, 0, f"Connected: {self._stat.connected}")
            self.screen.addstr(9, 20, f"Interesting: {self._stat.interesting}")
            self.screen.hline(10, 0, curses.ACS_S1, curses.COLS)

            for i, line in enumerate(self.to_print[-(curses.LINES - 12):]):
                self.screen.addstr(i + 11, 0, line[:curses.COLS])

            self.screen.hline(curses.LINES - 2, 0, curses.ACS_S9, curses.COLS)
            down, up = self._speedometr.send(None)
            speed_str = f"D: {down} U: {up}"
            self.screen.addstr(curses.LINES-1, 0, speed_str)
            self.screen.addch(curses.LINES-1, speed_str.find("D"), curses.ACS_DARROW)
            self.screen.addch(curses.LINES - 1, speed_str.find("U"), curses.ACS_UARROW)

            shutdown_str = "for shutdown press Ctrl+C"
            self.screen.addstr(curses.LINES - 1, curses.COLS - 1 - len(shutdown_str), shutdown_str)

            self.screen.refresh()
            await asyncio.sleep(period)

    def shutdown(self):
        self._work = False
        curses.endwin()

    def print(self, data):
        """Друкування певних даних у спеціальній області інтерфейсу"""
        curses.update_lines_cols()
        if len(self.to_print) > curses.LINES:
            [self.to_print.pop(0) for _ in range(len(self.to_print) - curses.LINES)]
        self.to_print.append(str(data))

    def update(self, info: Statistic):
        self._stat = info
