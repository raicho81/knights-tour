import redis

r = redis.Redis(host="192.168.1.3",
                port="6379",
                password="secret",
                decode_responses=True)

# r.execute_command("CLIENT TRACKING ON")  # Turn on client tracking in Redis

total_size = 0
for sn in range(0, 4):
    slot = "{}_slot_{}".format("knights_tour_negative_outcomes_set_cache", sn)
    print("slot: {}".format(slot))
    # slot_elements = set(r.sscan_iter(slot))
    slot_size = r.scard(slot)  # r.scard(slot)
    total_size += slot_size
    # print("{}, size: {}, content: {}".format(slot, slot_size, slot_elements))

print("total slots elements: {}".format(total_size))
# llen = r.llen("knights_tour_negative_outcomes_evict_list")
# print("ev list len: {}, ev list {}".format(llen, r.lrange("knights_tour_negative_outcomes_evict_list", 0, llen)))
