from typing import Callable, Any

class BaseDataService:
    def __init__(self, normalizer: Callable[[Any], Any]):
        self.normalizer = normalizer

    async def fetch_data(self):
        raise NotImplementedError("Implementar fetch_data() en la subclase")