import asyncio
import random
from typing import Optional, Dict, Any
from .base_service import BaseDataService
from processing.normalizer import normalize_physical_data

class FisicService(BaseDataService):
    
    def __init__(self):
        super().__init__(normalize_physical_data)

    async def fetch_data(self) -> Optional[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.1, 0.4))
        
        temp_string_1 = f"P{random.randint(100, 999)},temp,{round(random.uniform(35.0, 38.0), 1)}"
        temp_string_2 = f"P{random.randint(100, 999)},temp,{round(random.uniform(39.0, 41.0), 1)}"
        pres_string = f"P{random.randint(100, 999)},pres,{round(random.uniform(1.0, 1.5), 2)}"
        
        raw_data = random.choice([
            temp_string_1,
            temp_string_2,
            pres_string,
            "P800,voltaje",
            "PX,temp,37.A"
        ])
        
        #print(f"FisicService --> Dato crudo recibido: {raw_data}")
        normalized_data = self.normalizer(raw_data)
        
        return normalized_data