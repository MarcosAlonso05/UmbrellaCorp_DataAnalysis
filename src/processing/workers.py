import asyncio
import random
import time
from typing import Dict, Any

def analyze_genetic_data(data: Dict[str, Any]) -> Dict[str, Any]:
    sample_id = data['sample_id']
    sequence = data['payload']['sequence']
    
    #print(f"[CPU-Worker] Analisis complejo de: {sample_id}")
    time.sleep(1.0) 
    
    resultado = {
        'sample_id': sample_id,
        'genetic_analysis': {
            'length': len(sequence),
            'complexity': random.uniform(0.1, 1.0)
        },
        'status_analysis': 'OK',
        'raw_payload': data.get('payload')
    }
    
    #print(f"[CPU-Worker] Analisis de {sample_id} finalizado")
    return resultado

async def analyze_biochem_data(data: Dict[str, Any]) -> Dict[str, Any]:
    sample_id = data['sample_id']
    print(f"[Fast-Worker] Analizando: {sample_id}")
    await asyncio.sleep(0.01)
    
    return {
        'sample_id': sample_id,
        'biochem_analysis': {
            'is_toxic': data['payload'].get('valor', 0) > 10.0
        },
        'status_analysis': 'OK',
        'raw_payload': data.get('payload')
    }

async def analyze_physical_data(data: Dict[str, Any]) -> Dict[str, Any]:
    sample_id = data['sample_id']
    #print(f"[Fast-Worker] Analizando: {sample_id}")
    await asyncio.sleep(0.01)
    
    return {
        'sample_id': sample_id,
        'physical_analysis': {
            'range': 'normal' if data['payload'].get('valor', 0) < 38.5 else 'high'
        },
        'status_analysis': 'OK',
        'raw_payload': data.get('payload')
    }

async def save_io_data(data: Dict[str, Any]) -> Dict[str, Any]:
    sample_id = data['sample_id']
    data_type = data['type']
    
    #print(f"[I/O-Worker] Guardando en BBDD: {sample_id} ({data_type})")
    await asyncio.sleep(0.2)
    
    resultado = {
        'sample_id': sample_id,
        'status_save': 'OK'
    }

    #print(f"[I/O-Worker] Guardado de {sample_id} completado")
    return resultado