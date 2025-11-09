import time
from typing import Dict, Any, Optional

def get_timestamp() -> float:
    return time.time()

def validate_data(data: Dict[str, Any], required_fields: list) -> bool:
    if not isinstance(data, dict):
        return False
    return all(field in data for field in required_fields)

def normalize_genetic_data(raw_data: Any) -> Optional[Dict[str, Any]]:
    required = ['sample_id', 'seq']
    if not validate_data(raw_data, required):
        print(f"[WARN] Dato genético inválido descartado: {raw_data}")
        return None
        
    return {
        'sample_id': str(raw_data['sample_id']),
        'type': 'GENETIC',
        'timestamp': get_timestamp(),
        'payload': {
            'sequence': str(raw_data['seq']).upper()
        },
        'metadata': {'origin': 'GeneticService'}
    }

def normalize_biochemical_data(raw_data: Any) -> Optional[Dict[str, Any]]:
    required = ['sample_id', 'comp', 'val']
    if not validate_data(raw_data, required):
        print(f"[WARN] Dato bioquímico inválido descartado: {raw_data}")
        return None

    return {
        'sample_id': f"B-{raw_data['sample_id']}",
        'type': 'BIOQUIMIC',
        'timestamp': get_timestamp(),
        'payload': {
            'compound': str(raw_data['comp']),
            'value': float(raw_data['val'])
        },
        'metadata': {'origin': 'BioquimicService'}
    }

def normalize_physical_data(raw_data: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw_data, str):
        print(f"[WARN] Dato físico inválido (no es str): {raw_data}")
        return None
        
    parts = raw_data.split(',')
    if len(parts) != 3:
        print(f"[WARN] Dato físico inválido (formato): {raw_data}")
        return None

    try:
        return {
            'sample_id': str(parts[0]),
            'type': 'FISIC',
            'timestamp': get_timestamp(),
            'payload': {
                'metrics': str(parts[1]),
                'value': float(parts[2])
            },
            'metadata': {'origin': 'FisicService'}
        }
    except ValueError:
        print(f"[WARN] Dato físico inválido (valor no numérico): {raw_data}")
        return None