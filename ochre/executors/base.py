from abc import ABC, abstractmethod


class Executor(ABC):

    @abstractmethod
    def schedule(self, component):
        pass

    @abstractmethod
    def execute(self, jobs):
        pass
