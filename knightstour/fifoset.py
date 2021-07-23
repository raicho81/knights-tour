import collections
import sys


class FIFOSet:
    """
        Class representing a set, which can be bound in size. When the [[maximum]] size is reached the first
        elements added equal to [[evict_count]] are removed to make place for new ones or if the elements are less than
        [[evict_count]] then only the available are evicted resulting in an empty set.
        If [[maxsize]] is None the set behaves like a       ordinary set.
    """
    def __init__(self, maxsize=None, evict_count=1000):
        self.__maxsize = maxsize
        self.__currsize = 0
        self.__evict_count = evict_count
        self.__hits = 0
        self.__misses = 0
        self.__set = set()
        self.__set_first_added = collections.deque()

    def __repr__(self):
        return "%s(%r, maxsize=%r, currsize=%r)" % (
            self.__class__.__name__,
            self.__set.__repr__(),
            self.__maxsize,
            self.__currsize,
        )

    def __contains__(self, key):
        ret = key in self.__set
        if ret:
            self.__hits += 1
        else:
            self.__misses += 1

        return ret

    def __iter__(self):
        return iter(self.__set)

    def __len__(self):
        return len(self.__set)

    def __sizeof__(self):
        size = sys.getsizeof(self.__set) + sys.getsizeof(self.__set_first_added)
        return size

    def add(self, key):
        if self.__maxsize and (self.__currsize + self.getsizeof(key)) > self.__maxsize:
            for _ in range(self.__evict_count):
                k = self.__set_first_added.popleft()
                if k == 26792947407:
                    pass
                self.__set.remove(k)
                self.__currsize -= self.getsizeof(key)

        if key not in self.__set:
            self.__set_first_added.append(key)
            self.__set.add(key)
        self.__currsize += self.getsizeof(key)

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

    @property
    def hits(self):
        """Return the # of hits."""
        return self.__hits

    @property
    def misses(self):
        """Return the # of misses"""

        return self.__misses

    @property
    def cache_info(self):
        return f"FIFOSet Cache Info : [Hits: {self.__hits}, Misses: {self.__misses}, Size: {len(self.__set)}]"
