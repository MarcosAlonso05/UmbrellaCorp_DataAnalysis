import asyncio

from services.GeneticService import GeneticService
from services.BioService import BioquimicService
from services.FisicService import FisicService

async def main():
    s_gen = GeneticService()
    s_bio = BioquimicService()
    s_fis = FisicService()

    print("--- Probando Servicios ---")
    
    data_gen = await s_gen.fetch_data()
    print(f"Normalizado Genético: {data_gen}\n")
    
    data_bio = await s_bio.fetch_data()
    print(f"Normalizado Bioquímico: {data_bio}\n")

    data_fis = await s_fis.fetch_data()
    print(f"Normalizado Físico: {data_fis}\n")

    print("--- Probando dato inválido ---")
    data_fis_inv = await s_fis.fetch_data()
    print(f"Normalizado Físico Inválido: {data_fis_inv}\n")


if __name__ == "__main__":
    asyncio.run(main())