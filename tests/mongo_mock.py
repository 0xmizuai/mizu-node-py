class MongoMock:
    def __init__(self):
        self.jobs = self
        self.data = {}

    def insert_one(self, value):
        key = value["_id"]
        self.data[key] = value

    def find_one(self, payload):
        key = payload["_id"]
        return self.data.get(key)
