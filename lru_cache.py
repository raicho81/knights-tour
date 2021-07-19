class LinkedNode(object):

    def __init__(self, val, key):
        self.val = val  # Used to store the real cache value
        self.key = key  # Reversely points to a key in the dictionary for reverse deletion
        self.next = None
        self.prev = None

class LinkedNode(object):

    def __init__(self, val, key):
        self.val = val  # Used to store the real cache value
        self.key = key  # Reversely points to a key in the dictionary for reverse deletion
        self.next = None
        self.prev = None


class LRUCache:

    def __init__(self, capacity: int):
        self.capacity = capacity
        self.lookup = {}
        self.dummy = LinkedNode(0, 0)
        self.head = self.dummy.next
        self.tail = self.dummy.next
        self.cache_hits = 0
        self.cache_misses = 0

    def size(self):
        return len(self.lookup)

    def remove_head_node(self):
        if not self.head:
            return
        prev = self.head
        self.head = self.head.next
        if self.head:
            self.head.prev = None
        del prev

    def append_new_node(self, new_node):
        """  add the new node to the tail end
        """
        if not self.tail:
            self.head = self.tail = new_node
        else:
            self.tail.next = new_node
            new_node.prev = self.tail
            self.tail = self.tail.next

    def unlink_cur_node(self, node):
        """ unlink current linked node
        """
        if self.head is node:
            self.head = node.next
            if node.next:
                node.next.prev = None
            return

        # removing the node from somewhere in the middle; update pointers
        prev, nex = node.prev, node.next
        prev.next = nex
        nex.prev = prev

    def get(self, key: int) -> int:
        if key not in self.lookup:
            self.cache_misses += 1
            return -1

        self.cache_hits += 1
        node = self.lookup[key]

        if node is not self.tail:
            self.unlink_cur_node(node)
            self.append_new_node(node)

        return node.val

    def put(self, key: int, value: int) -> None:
        if key in self.lookup:
            self.lookup[key].val = value
            self.get(key)
            return

        if self.size() == self.capacity:
            # remove head node and correspond key
            self.lookup.pop(self.head.key)
            self.remove_head_node()

        # add new node and hash key
        new_node = LinkedNode(val=value, key=key)
        self.lookup[key] = new_node
        self.append_new_node(new_node)
