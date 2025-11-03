import time
from typing import Dict, Any, Optional

def get_timestamp() -> float:
    return time.time()

def validate_data(data: Dict[str, Any], required_fields: list) -> bool:
    if not isinstance(data, dict):
        return False
    return all(field in data for field in required_fields)

def normalize_genetic_data(raw_data: Any) -> Optional[Dict[str, Any]]:
    required = ['id_muestra', 'seq']
    if not validate_data(raw_data, required):
        print(f"[WARN] Dato genético inválido descartado: {raw_data}")
        return None
        
    return {
        'id_muestra': str(raw_data['id_muestra']),
        'tipo': 'GENETICO',
        'timestamp': get_timestamp(),
        'payload': {
            'secuencia': str(raw_data['seq']).upper()
        },
        'metadata': {'origen': 'GeneticoService'}
    }

def normalize_biochemical_data(raw_data: Any) -> Optional[Dict[str, Any]]:
    required = ['sample_id', 'comp', 'val']
    if not validate_data(raw_data, required):
        print(f"[WARN] Dato bioquímico inválido descartado: {raw_data}")
        return None

    return {
        'id_muestra': f"B-{raw_data['sample_id']}",
        'tipo': 'BIOQUIMICO',
        'timestamp': get_timestamp(),
        'payload': {
            'compuesto': str(raw_data['comp']),
            'valor': float(raw_data['val'])
        },
        'metadata': {'origen': 'BioquimicoService'}
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
            'id_muestra': str(parts[0]),
            'tipo': 'FISICO',
            'timestamp': get_timestamp(),
            'payload': {
                'metrica': str(parts[1]),
                'valor': float(parts[2])
            },
            'metadata': {'origen': 'FisicoService'}
        }
    except ValueError:
        print(f"[WARN] Dato físico inválido (valor no numérico): {raw_data}")
        return None