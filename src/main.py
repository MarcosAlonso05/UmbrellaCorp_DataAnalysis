import asyncio
import sys, os
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, Any

from services.GeneticService import GeneticService
from services.BioService import BioquimicService
from services.FisicService import FisicService
from services.base_service import BaseDataService

from processing.workers import analyze_genetic_data, save_io_data

MAX_QUEUE_SIZE = 100

async def producer(queue: asyncio.Queue, service: BaseDataService):
    while True:
        try:
            data = await service.fetch_data()
            if data:
                print(f"Productor --> Dato {data['sample_id']} puesto en la cola.")
                await queue.put(data)
            if queue.full():
                await asyncio.sleep(0.5)
        except Exception as e:
            print(f"Error en productor: {e}")

async def consumer(queue: asyncio.Queue, cpu_pool: ProcessPoolExecutor):
    loop = asyncio.get_running_loop()
    
    while True:
        data = await queue.get()
        print(f"Consumidor --> Dato {data['sample_id']} recibido.")

        if data['type'] == 'GENETIC':
            print(f"-> Delegando {data['sample_id']} al Pool de CPU")
            asyncio.create_task(
                loop.run_in_executor(
                    cpu_pool,
                    analyze_genetic_data,
                    data
                )
            )
            
        elif data['type'] in ['BIOQUIMIC', 'FISIC']:
            print(f"-> Creando tarea I/O para {data['sample_id']}")
            asyncio.create_task(save_io_data(data))
            
        else:
            print(f"[WARN] type de dato desconocido: {data['type']}")

        queue.task_done()

async def main():
    
    queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
    
    cpu_worker_count = os.cpu_count() or 4
    print(f"--- Iniciando Sistema de Analisis Concurrente ---")
    print(f"--- Usando {cpu_worker_count} workers para CPU ---")
    
    with ProcessPoolExecutor(max_workers=cpu_worker_count) as cpu_pool:
        
        services = [
            GeneticService(),
            BioquimicService(),
            FisicService()
        ]
        
        producer_tasks = [
            asyncio.create_task(producer(queue, srv)) for srv in services
        ]
        
        consumer_tasks = [
            asyncio.create_task(consumer(queue, cpu_pool)) for _ in range(3)
        ]
        
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            print("Cerrando simulacion...")
        finally:
            print("--- Deteniendo Productores...")
            for task in producer_tasks:
                task.cancel()
            
            print("--- Esperando a que la cola se procese...")
            await queue.join()
            
            print("--- Deteniendo Consumidores...")
            for task in consumer_tasks:
                task.cancel()
                
            print("--- Simulacion finalizada. ---")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Simulacion interrumpida por el usuario.")