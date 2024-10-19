class KeyPrefix:
    def __init__(self, prefix: str):
        self.prefix = prefix

    def of(self, name: str) -> str:
        return self.prefix + name

    @classmethod
    def concat(cls, prefix, name: str):
        return cls(prefix.of(name))
