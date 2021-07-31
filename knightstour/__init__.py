import crc16

from .fifoset import FIFOSet
from .redisfifoset import RedisFIFOSet
from .knightstour import KnightsTourAlgo
from .redispathspmshset import RedisPathsPmsHSet


__all__ = (
    "KnightsTourAlgo",
    "FIFOSet",
    "RedisFIFOSet",
    "RedisPathsPmsHSet",
    "_crc_16"
)


def _crc_16(self, key):
    crc_16 = crc16.crc16xmodem(bytes(key), 0xFFFF)
    sn = crc_16 % self.cache_slots_count
    return sn