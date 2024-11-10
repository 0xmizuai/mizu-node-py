class RedisMock:
    def __init__(self):
        self.data = {}
        self.expiry = {}
        self.in_pipeline = False
        self.pipeline_output = []

    def _check_pipeline(self, value=None):
        if self.in_pipeline:
            if value:
                self.pipeline_output.append(value)
            return self
        else:
            return value

    def set(self, key, value, ex=int | None):
        self.data[key] = value
        if ex is not None:
            self.expiry[key] = ex
        return self._check_pipeline()

    def setex(self, key, time: int, value):
        # Not implementing expiry
        self.set(key, value, time)
        return self._check_pipeline()

    def get(self, key):
        value = self.data.get(key)
        return self._check_pipeline(value)

    def lpush(self, key, *values):
        if key not in self.data:
            self.data[key] = []
        for v in values:
            self.data[key].insert(0, v)
        return self._check_pipeline()

    def rpop(self, key):
        value = None
        if key in self.data and len(self.data[key]) > 0:
            value = self.data[key].pop()
        return self._check_pipeline(value)

    def lmove(self, key1, key2):
        l = self.rpop(key1)
        if l is not None:
            self.lpush(key2, l)
        return self._check_pipeline(l)

    def lrem(self, key, count, value):
        if count != 0:
            raise NotImplementedError("count != 0 is not implemented")
        data = self.get(key)
        if not data:
            return
        filterd = [v for v in data if v != value]
        self.set(key, filterd)
        return self._check_pipeline()

    def incr(self, key):
        if key not in self.data:
            self.data[key] = 0
        self.data[key] += 1
        return self._check_pipeline()

    def decr(self, key):
        if key not in self.data:
            self.data[key] = 0
        self.data[key] -= 1
        if self.data[key] < 0:
            self.data[key] = 0
        return self._check_pipeline()

    def incrby(self, key, value):
        if key not in self.data:
            self.data[key] = 0
        self.data[key] += value
        return self._check_pipeline()

    def decrby(self, key, value):
        if key not in self.data:
            self.data[key] = 0
        self.data[key] -= value
        if self.data[key] < 0:
            self.data[key] = 0
        return self._check_pipeline()

    def llen(self, key):
        value = 0
        if key in self.data:
            value = len(self.data[key])
        return self._check_pipeline(value)

    def delete(self, key):
        if key in self.data:
            del self.data[key]
        if key in self.expiry:
            del self.expiry[key]
        return self._check_pipeline(1)

    def exists(self, key):
        value = key in self.data
        return self._check_pipeline(value)

    def pipeline(self):
        self.in_pipeline = True
        self.pipeline_output = []
        return self

    def execute(self):
        self.in_pipeline = False
        return tuple(self.pipeline_output)
