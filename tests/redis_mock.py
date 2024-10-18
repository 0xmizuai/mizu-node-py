class RedisMock:
    def __init__(self):
        self.data = {}
        self.expiry = {}

    def set(self, key, value, ex=int | None):
        self.data[key] = value
        if ex is not None:
            self.expiry[key] = ex

    def setex(self, key, time: int, value):
        # Not implementing expiry
        self.set(key, value, time)

    def get(self, key):
        return self.data.get(key)

    def lpush(self, key, *values):
        if key not in self.data:
            self.data[key] = []
        for v in values:
            self.data[key].insert(0, v)

    def rpop(self, key):
        if key not in self.data:
            return None
        return self.data[key].pop()

    def incr(self, key):
        if key not in self.data:
            self.data[key] = 0
        self.data[key] += 1

    def decr(self, key):
        if key not in self.data:
            self.data[key] = 0
        self.data[key] -= 1
        if self.data[key] < 0:
            self.data[key] = 0

    def incrby(self, key, value):
        if key not in self.data:
            self.data[key] = 0
        self.data[key] += value

    def decrby(self, key, value):
        if key not in self.data:
            self.data[key] = 0
        self.data[key] -= value
        if self.data[key] < 0:
            self.data[key] = 0

    def llen(self, key):
        if key not in self.data:
            return 0
        return len(self.data[key])

    def delete(self, key):
        if key in self.data:
            del self.data[key]
        if key in self.expiry:
            del self.expiry[key]

    def exists(self, key):
        return key in self.data
