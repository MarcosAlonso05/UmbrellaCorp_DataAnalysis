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
        
        if random.random() < 0.3:
            generated_value = round(random.uniform(14.1, 15.0), 3)
            id_generated = f"ALERT-{random.randint(1000, 9999)}"
        else:
            generated_value = round(random.uniform(0.1, 14.0), 3)
            id_generated = random.randint(1000, 9999)
        
        raw_data = {
            'sample_id': id_generated,
            'comp': random.choice(['Glucosa', 'H2O', 'CO2']),
            'val': generated_value
        }

        if random.random() < 0.05:
            raw_data = {'otro_formato': 'dato_inutil'}

        #print(f"BioquimicService --> Dato crudo recibido: {raw_data}")
        normalized_data = self.normalizer(raw_data)
        
        return normalized_data