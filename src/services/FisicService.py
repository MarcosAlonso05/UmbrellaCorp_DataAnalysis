import asyncio
import random
from typing import Optional, Dict, Any
from .base_service import BaseDataService
from processing.normalizer import normalize_physical_data

class FisicoService(BaseDataService):
    
    def __init__(self):
        super().__init__(normalize_physical_data)

    async def fetch_data(self) -> Optional[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.1, 0.4))
        
        raw_data = random.choice([
            f"P{random.randint(100, 999)},temp,{round(random.uniform(35.0, 40.0), 1)}",
            f"P{random.randint(100, 999)},pres,{round(random.uniform(1.0, 1.5), 2)}",
            "P800,voltaje",
            "PX,temp,37.A"
        ])
        
        print(f"[FisicoService] Dato crudo recibido: {raw_data}")
        normalized_data = self.normalizer(raw_data)
        
        return normalized_data