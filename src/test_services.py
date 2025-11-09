# test_services.py (colócalo en la raíz del proyecto)
import asyncio

# Añade src al path para que Python encuentre los módulos
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from services.GeneticService import GeneticoService
from services.BioService import BioquimicoService
from services.FisicService import FisicoService

async def main():
    # Instancia los servicios
    s_gen = GeneticoService()
    s_bio = BioquimicoService()
    s_fis = FisicoService()

    # Simula la obtención de un dato de cada uno
    print("--- Probando Servicios ---")
    
    data_gen = await s_gen.fetch_data()
    print(f"Normalizado Genético: {data_gen}\n")
    
    data_bio = await s_bio.fetch_data()
    print(f"Normalizado Bioquímico: {data_bio}\n")

    data_fis = await s_fis.fetch_data()
    print(f"Normalizado Físico: {data_fis}\n")

    # Prueba con un dato físico inválido
    print("--- Probando dato inválido ---")
    data_fis_inv = await s_fis.fetch_data()
    print(f"Normalizado Físico Inválido: {data_fis_inv}\n")


if __name__ == "__main__":
    asyncio.run(main())