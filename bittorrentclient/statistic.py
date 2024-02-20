import dataclasses


@dataclasses.dataclass()
class Statistic:
    """Використовується для передачі інформації про стан завантаження та відвантаження для інших компонентів програми"""
    uploaded: int
    downloaded: int
    left: int
    peers_count: int
    connected: int
    interesting: int
    length: int
