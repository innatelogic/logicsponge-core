# core_rs.pyi

from logicsponge.core import DataItem

class Sender:
    def send(self, obj: DataItem) -> None: ...

class Receiver:
    def recv(self) -> DataItem: ...

def make_channel() -> tuple[Sender, Receiver]: ...
