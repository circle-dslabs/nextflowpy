import os

class StagedPath:
    def __init__(self, value: str):
        self.value = value

    def __repr__(self):
        return f"path({self.value!r})"

    def __str__(self):
        return self.value

    def is_remote(self) -> bool:
        return self.value.startswith(("s3://", "http://", "https://"))

    def exists(self) -> bool:
        return os.path.exists(self.value)

def path(value: str) -> StagedPath:
    return StagedPath(value)
