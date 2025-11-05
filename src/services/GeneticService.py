import asyncio
import random
from typing import Optional, Dict, Any
from .base_service import BaseDataService
from processing.normalizer import normalize_genetic_data

class GeneticoService(BaseDataService):
    
    def __init__(self):
        super().__init__(normalize_genetic_data)

    async def fetch_data(self) -> Optional[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.5, 1.5))
        
        raw_data = {
            'id_muestra': f"G-{random.randint(100, 999)}",
            'seq': random.choice(["ATCGGCTA", "CGTAATGC", "INVALID_DATA", "ATGC"])
        }
        
        if random.random() < 0.1:
            raw_data = {'id': 'error', 'datos': 'faltan campos'}

        print(f"[GeneticoService] Dato crudo recibido: {raw_data}")
        
        normalized_data = self.normalizer(raw_data)
        
        return normalized_data