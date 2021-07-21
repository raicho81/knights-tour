import collections
import sys


class FIFOSet:

    def __init__(self, maxsize=None, evict_count=10000):
        self.__maxsize = maxsize
        self.__currsize = 0
        self.__set = set()
        self.__set_fifo = collections.deque()
        self.__evict_count = evict_count

    def __repr__(self):
        return "%s(%r, maxsize=%r, currsize=%r)" % (
            self.__class__.__name__,
            self.__set.__repr__(),
            self.__maxsize,
            self.__currsize,
        )

    def __contains__(self, key):
        return key in self.__set

    def __iter__(self):
        return iter(self.__set)

    def __len__(self):
        return len(self.__set)

    def __sizeof__(self):
        size = sys.getsizeof(self.__set) + sys.getsizeof(self.__set_fifo)
        return size

    def add(self, key):
        while self.__maxsize and (self.__currsize + self.getsizeof(key)) >= self.__maxsize:
            for _ in range(self.__evict_count):     # Evict some old elements
                self.__set.remove(self.__set_fifo.popleft())
                self.__currsize -= 1

        self.__currsize += self.getsizeof(key)
        self.__set_fifo.append(key)
        self.__set.add(key)

    @property
    def maxsize(self):
        """The maximum size of the cache."""
        return self.__maxsize

    @property
    def currsize(self):
        """The current size of the cache."""
        return self.__currsize

    @staticmethod
    def getsizeof(value):
        """Return the size of a cache element's value."""
        return 1
