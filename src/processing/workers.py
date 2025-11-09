import asyncio
import random
import time
from typing import Dict, Any

def analyze_genetic_data(data: Dict[str, Any], shared_results: Dict) -> Dict[str, Any]:
    
    sample_id = data['sample_id']
    sequence = data['payload']['sequence']
    
    print(f"Analisis complejo de: {sample_id}")
    
    time.sleep(1.0) 
    
    result = {
        'sample_id': sample_id,
        'genetic_analysis': {
            'length': len(sequence),
            'complexity': random.uniform(0.1, 1.0)
        }
    }
    print(f"Analisis de {sample_id} finalizado")
    
    shared_results[sample_id] = result
    
    return result

async def save_io_data(data: Dict[str, Any], shared_results: Dict) -> Dict[str, Any]:

    sample_id = data['sample_id']
    data_type = data['type']
    
    print(f"Guardando en BBDD: {sample_id} ({data_type})")
    
    await asyncio.sleep(0.2)
    
    result = {
        'sample_id': sample_id,
        'status_save': 'OK'
    }
    
    shared_results[sample_id] = result
    
    print(f"Guardado de {sample_id} completado")
    return result