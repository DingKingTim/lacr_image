import abc
from abc import ABC


class Status(ABC):

    @abc.abstractmethod
    def open(self, **kwargs): ...

    @abc.abstractmethod
    def close(self): ...

    @abc.abstractmethod
    def update(self, topic: str, partition: int, offset: int) -> bool: ...

    @abc.abstractmethod
    def read(self, topic: str, partition: int) -> int: ...
