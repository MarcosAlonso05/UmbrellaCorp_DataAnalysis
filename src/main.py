import asyncio
import sys, os
import multiprocessing
import time
import pandas as pd
import matplotlib.pyplot as plt
import random
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, Any, List

from services.GeneticService import GeneticService
from services.BioService import BioquimicService
from services.FisicService import FisicService
from services.base_service import BaseDataService

from processing.workers import analyze_genetic_data, save_io_data

MAX_QUEUE_SIZE = 100
SIMULATION_TIME_SECONDS = 30

alert_queue = asyncio.Queue()
metrics_log: List[Dict[str, Any]] = []
alert_latencies: List[float] = []

async def producer(queue: asyncio.Queue, service: BaseDataService):
    while True:
        try:
            data = await service.fetch_data()
            if data:
                print(f"Productor --> Dato {data['sample_id']} puesto en la cola.")
                await queue.put(data)
                
                is_critical = False
                payload = data.get('payload', {})
                if data['type'] == 'BIOQUIMIC' and payload.get('valor', 0) > 14.0:
                    is_critical = True
                elif data['type'] == 'FISIC' and payload.get('metrica') == 'temp' and payload.get('valor', 0) > 39.5:
                    is_critical = True
                
                if is_critical:
                    print(f"[ALERTA] Evento critico: {data['sample_id']}")
                    await alert_queue.put(data)
                
            if queue.full():
                await asyncio.sleep(0.5)
        except Exception as e:
            print(f"Error en productor: {e}")

async def alert_processor():
    while True:
        try:
            alert_data = await alert_queue.get()
            
            detection_latency = time.time() - alert_data['timestamp']
            alert_latencies.append(detection_latency)
            
            print(f"[ALERTA PROCESADA] {alert_data['sample_id']}. Latencia: {detection_latency*1000:.2f} ms")
            
            await asyncio.sleep(0.05)
            
            alert_queue.task_done()
        except Exception as e:
            print(f"Error en procesador de alertas: {e}")

async def cpu_worker_wrapper(loop, cpu_pool, data, shared_results, queue: asyncio.Queue):
    start_time = time.time()
    status = 'FAILURE'
    try:
        await loop.run_in_executor(
            cpu_pool,
            analyze_genetic_data,
            data,
            shared_results
        )
        status = 'SUCCESS'
    except Exception as e:
        print(f"[ERROR-CPU-WORKER] Fallo en {data.get('sample_id', 'DATO DESCONOCIDO')}: {e}")
    finally:
        end_time = time.time()
        metrics_log.append({
            'sample_id': data.get('sample_id', 'N/A'),
            'type': data.get('type', 'N/A'),
            'start_time': start_time,
            'end_time': end_time,
            'status': status
        })
        queue.task_done()

async def io_worker_wrapper(data, shared_results, queue: asyncio.Queue):
    start_time = time.time()
    status = 'FAILURE'
    try:
        await save_io_data(data, shared_results)
        status = 'SUCCESS'
    except Exception as e:
        print(f"[ERROR-I/O-WORKER] Fallo en {data.get('sample_id', 'DATO DESCONOCIDO')}: {e}")
    finally:
        end_time = time.time()
        metrics_log.append({
            'sample_id': data.get('sample_id', 'N/A'),
            'type': data.get('type', 'N/A'),
            'start_time': start_time,
            'end_time': end_time,
            'status': status
        })
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
    print(f"=================================================")
    print(f"=== Iniciando Sistema de Analisis Concurrente ===")
    print(f"=================================================")
    print(f"{cpu_worker_count} workers para CPU")
    
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

            alert_task = asyncio.create_task(alert_processor())
            
            try:
                await asyncio.sleep(SIMULATION_TIME_SECONDS)
            except asyncio.CancelledError:
                print("Cerrando simulacion...")
            finally:
                print("\n--- [FASE DE APAGADO] ---")
                print("--- Deteniendo Productores...")
                for task in producer_tasks:
                    task.cancel()
                
                print("(Esperando a que la cola de trabajo se procese)")
                await queue.join()
                
                print("(Esperando a que la cola de alertas se procese)")
                await alert_queue.join()
                
                print("(Deteniendo Consumidores y Procesador de Alertas)")
                for task in consumer_tasks:
                    task.cancel()
                alert_task.cancel()
                
                print("--- Simulacion finalizada. ---")
                
                print("\n=== RESULTADOS ===")
                print(dict(list(shared_results.items())[:5]))

                generate_report(metrics_log, alert_latencies)

def generate_report(metrics_log: List[Dict[str, Any]], alert_latencies: List[float]):
    
    print("\n\n=== INFORME FINAL DE MÉTRICAS ===")
    
    if not metrics_log:
        print("No se procesaron métricas. Terminando informe.")
        return

    df = pd.DataFrame(metrics_log)
    df['latency_ms'] = (df['end_time'] - df['start_time']) * 1000
    df = df[df['status'] == 'SUCCESS']

    if df.empty:
        print("No se procesaron operaciones exitosas.")
        return

    print("\n1. Tiempo de respuesta medio (ms) por tipo:")
    avg_times = df.groupby('type')['latency_ms'].mean()
    print(avg_times.to_string())

    print("\n2. Latencia a la detección y envío de alertas (ms):")
    if alert_latencies:
        avg_alert_latency = (sum(alert_latencies) / len(alert_latencies)) * 1000
        print(f"Latencia media de alertas: {avg_alert_latency:.2f} ms")
    else:
        print("No se generaron alertas.")

    print("\n3. Tasa de errores / fallos:")
    total_ops = len(metrics_log)
    total_errors = total_ops - len(df)
    error_rate_per_million = (total_errors / total_ops) * 1_000_000 if total_ops > 0 else 0
    print(f"Operaciones totales: {total_ops}")
    print(f"Fallos totales: {total_errors}")
    print(f"Tasa de fallos por millón de operaciones: {error_rate_per_million:.2f}")

    print("\n4. [Visual] Tabla de eventos procesados por tipo:")
    summary_table = df.groupby('type').agg(
        total_procesados=('sample_id', 'count'),
        latencia_media_ms=('latency_ms', 'mean')
    )
    print(summary_table.to_string())

    try:
        print("\n5. [Visual] Generando gráfico 'rendimiento_latencia.png'...")
        df_sorted = df.sort_values('end_time')
        df_sorted['tiempo_relativo'] = df_sorted['end_time'] - df_sorted['end_time'].min()
        
        plt.figure(figsize=(12, 6))
        
        for data_type in df_sorted['type'].unique():
            df_type = df_sorted[df_sorted['type'] == data_type]
            plt.plot(
                df_type['tiempo_relativo'], 
                df_type['latency_ms'], 
                'o', 
                label=f'Tipo {data_type}',
                alpha=0.6
            )
            
        plt.title('Rendimiento: Latencia vs. Tiempo')
        plt.xlabel('Tiempo (segundos desde el inicio)')
        plt.ylabel('Latencia (ms)')
        plt.legend()
        plt.grid(True)
        plt.savefig('docs/rendimiento_latencia.png')
        print("-> Gráfico guardado exitosamente.")
        
    except Exception as e:
        print(f"[ERROR] No se pudo generar el gráfico: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Simulacion interrumpida por el usuario.")