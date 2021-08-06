import collections
import sys
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class FIFOSet:
    """
        Class representing a set, which can be bound in size. When the maximum size is reached the first
        elements added equal to evict_count are removed to make place for new ones or if the elements are less than
        evict_count then only the available are evicted resulting in an empty set.
        If maxsize is None the set behaves like an ordinary set() and is not bound.
    """
    def __init__(self, maxsize=None, evict_count=1000):
        self.__maxsize = maxsize
        self.__currsize = 0
        self.__evict_count = evict_count
        self.__hits = 0
        self.__misses = 0
        self.__set = set()
        self.__set_first_added = collections.deque() if maxsize else None

    def __repr__(self):
        return "{} (maxsize={}, currsize={}, hit_rate={}, hits={}, misses={}, evict_count={})".format(
            self.__class__.__name__,
            self.__maxsize,
            self.__currsize,
            self.hits * 100 / self.misses or 1,
            self.__hits,
            self.__misses,
            self.__evict_count,
        )

    def __contains__(self, key):
        if not isinstance(key, int):
            key = int(key)
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
        size = sys.getsizeof(self.__set) + sys.getsizeof(self.__set_first_added) if self.maxsize else 0
        return size

    def __delitem__(self, key):
        try:
            index = self.__set_first_added.index(key) if self.maxsize else None
            if self.maxsize:
                del self.__set_first_added[index]
            del self.__set[key]
            self.__currsize -= self.getsizeof(key)
            logger.debug("self.__currsize: {}".format(self.__currsize))
        except IndexError as e:
            logger.error(e)    

    def __evict(self, key):
        if self.__maxsize and (self.__currsize + self.getsizeof(key)) < self.__maxsize:
            return
        for _ in range(min(self.__evict_count, self.__currsize)):
            k = self.__set_first_added.popleft()
            self.__set.remove(k)
            self.__currsize -= self.getsizeof(key)
            logger.debug("self.__currsize: {}".format(self.__currsize))

    def add(self, key):
        self.__evict(key)
        if key not in self.__set:
            if self.maxsize:
                self.__set_first_added.append(key)
            self.__set.add(key)
            self.__currsize += self.getsizeof(key)
            logger.debug("self.__currsize: {}".format(self.__currsize))

    def pop(self, key):
        try:
            if self.maxsize:
                index = self.__set_first_added.index(key)
                self.__set_first_added.remove(index)
            self.__set.remove(key)
            self.__currsize -= self.getsizeof(key)
            logger.debug("self.__currsize: {}".format(self.__currsize))
        except IndexError as e:
            logger.error(e)
        return key

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

    def cache_info(self):
        return f"{self.__class__.__name__} Cache Info : [Hit Rate %: {100 * self.__hits / (self.__misses or 1)}, "\
        f"Hits: {self.__hits}, Misses: {self.__misses}, Size: {len(self.__set)}, Max Size: {self.maxsize}]"

    def cache_clear(self):
        """
            Clear FIFOSet data
        """
        self.maxsize and self.__set_first_added.clear()
        self.__set.clear()
        self.__currsize = 0
        self.__hits = 0
        self.__misses = 0
        logger.info("FIFOSet cache cleared")
