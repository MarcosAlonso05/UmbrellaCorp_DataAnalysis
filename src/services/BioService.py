import asyncio
import random
from typing import Optional, Dict, Any
from .base_service import BaseDataService
from processing.normalizer import normalize_biochemical_data

class BioquimicService(BaseDataService):
    
    def __init__(self):
        super().__init__(normalize_biochemical_data)

    async def fetch_data(self) -> Optional[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.2, 0.8))
        
        raw_data = {
            'sample_id': random.randint(1000, 9999),
            'comp': random.choice(['Glucosa', 'H2O', 'CO2']),
            'val': round(random.uniform(0.1, 15.0), 3)
        }

        if random.random() < 0.05:
            raw_data = {'otro_formato': 'dato_inutil'}

        print(f"BioquimicService --> Dato crudo recibido: {raw_data}")
        normalized_data = self.normalizer(raw_data)
        
        return normalized_data