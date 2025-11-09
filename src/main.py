import asyncio
import sys, os
import multiprocessing
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
async def cpu_worker_wrapper(loop, cpu_pool, data, shared_results, queue: asyncio.Queue):
    try:
        await loop.run_in_executor(
            cpu_pool,
            analyze_genetic_data,
            data,
            shared_results
        )
    except Exception as e:
        print(f"[ERROR-CPU-WORKER] Fallo en {data.get('sample_id', 'DATO DESCONOCIDO')}: {e}")
    finally:
        queue.task_done()

async def io_worker_wrapper(data, shared_results, queue: asyncio.Queue):
    try:
        await save_io_data(data, shared_results)
    except Exception as e:
        print(f"[ERROR-I/O-WORKER] Fallo en {data.get('sample_id', 'DATO DESCONOCIDO')}: {e}")
    finally:
        queue.task_done()

async def consumer(queue: asyncio.Queue, cpu_pool: ProcessPoolExecutor, shared_results: Dict):
    loop = asyncio.get_running_loop()
    
    while True:
        try:
            data = await queue.get()
            
            print(f"Consumidor --> Dato {data['sample_id']} recibido.")

            if data['type'] == 'GENETIC':
                print(f"-> Delegando {data['sample_id']} al Pool de CPU")
                asyncio.create_task(
                    cpu_worker_wrapper(loop, cpu_pool, data, shared_results, queue)
                )
                
            elif data['type'] in ['BIOQUIMIC', 'FISIC']:
                print(f"-> Creando tarea I/O para {data['sample_id']}")
                asyncio.create_task(
                    io_worker_wrapper(data, shared_results, queue)
                )
                
            else:
                print(f"[WARN] type de dato desconocido: {data.get('type')}")
                queue.task_done()

        except (AttributeError, KeyError) as e:
            print(f"[ERROR-CONSUMIDOR] Dato malformado, no se puede procesar: {data}. Error: {e}")
            queue.task_done()
        except Exception as e:
            print(f"[ERROR-CONSUMIDOR] Error inesperado: {e}")
            queue.task_done()

async def main():
    
    queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
    
    cpu_worker_count = os.cpu_count() or 4
    print(f"--- Iniciando Sistema de Analisis Concurrente ---")
    print(f"--- Usando {cpu_worker_count} workers para CPU ---")
    
    with multiprocessing.Manager() as manager:
        shared_results = manager.dict()
        
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
                asyncio.create_task(consumer(queue, cpu_pool, shared_results)) for _ in range(3)
            ]
            
            try:
                await asyncio.sleep(20)
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
                
                print("\n--- RESULTADOS AGREGADOS ---")
                print(dict(shared_results))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Simulacion interrumpida por el usuario.")