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

from processing.workers import (
    analyze_genetic_data, 
    analyze_biochem_data,
    analyze_physical_data,
    save_io_data
)

MAX_QUEUE_SIZE = 100
SIMULATION_TIME_SECONDS = 30

alert_queue = asyncio.Queue()
metrics_log: List[Dict[str, Any]] = []
alert_latencies: List[float] = []
critical_alerts_log: List[Dict[str, Any]] = []

async def producer(queue: asyncio.Queue, service: BaseDataService):
    while True:
        try:
            data = await service.fetch_data()
            if data:
                print(f"Productor --> Dato {data['sample_id']} puesto en la cola.")
                await queue.put(data)
                
                is_critical = False
                payload = data.get('payload', {})
                if data['type'] == 'BIOQUIMIC' and payload.get('value', 0) > 14.0:
                    is_critical = True
                elif data['type'] == 'FISIC' and payload.get('metrics') == 'temp' and payload.get('value', 0) > 38.5:
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
            
            critical_alerts_log.append(alert_data)
            await asyncio.sleep(0.05)
            
            alert_queue.task_done()
        except Exception as e:
            print(f"Error en procesador de alertas: {e}")

def safe_write_to_results(shared_results: Dict, lock: multiprocessing.Lock, key: str, new_data: Dict):

    with lock:
        current_data = shared_results.get(key, {})
        current_data.update(new_data)
        shared_results[key] = current_data

async def cpu_worker_wrapper(loop, cpu_pool, data, shared_results, lock, queue):
    start_time = time.time()
    status = 'FAILURE'
    sample_id = data['sample_id']
    try:
        analysis_result = await loop.run_in_executor(
            cpu_pool,
            analyze_genetic_data,
            data
        )
        
        safe_write_to_results(shared_results, lock, sample_id, analysis_result)
        status = 'SUCCESS'
        
    except Exception as e:
        print(f"[ERROR-CPU-WORKER] Fallo en {sample_id}: {e}")
    finally:
        end_time = time.time()
        metrics_log.append({
            'sample_id': sample_id, 'type': 'GENETIC',
            'start_time': start_time, 'end_time': end_time, 'status': status
        })
        queue.task_done()

async def io_worker_wrapper(data, shared_results, lock, queue):
    start_time = time.time()
    status = 'FAILURE'
    sample_id = data['sample_id']
    data_type = data['type']
    
    try:
        if data_type == 'BIOQUIMIC':
            analysis_result = await analyze_biochem_data(data)
        elif data_type == 'FISIC':
            analysis_result = await analyze_physical_data(data)
        else:
            analysis_result = {}

        safe_write_to_results(shared_results, lock, sample_id, analysis_result)

        save_result = await save_io_data(data)
        
        safe_write_to_results(shared_results, lock, sample_id, save_result)
        
        status = 'SUCCESS'
        
    except Exception as e:
        print(f"[ERROR-I/O-WORKER] Fallo en {sample_id}: {e}")
    finally:
        end_time = time.time()
        metrics_log.append({
            'sample_id': sample_id, 'type': data_type,
            'start_time': start_time, 'end_time': end_time, 'status': status
        })
        queue.task_done()

async def consumer(queue: asyncio.Queue, cpu_pool: ProcessPoolExecutor, shared_results: Dict, lock: multiprocessing.Lock):
    loop = asyncio.get_running_loop()
    
    while True:
        try:
            data = await queue.get()
            print(f"Consumidor --> Dato {data['sample_id']} recibido.")

            if data['type'] == 'GENETIC':
                print(f"-> Delegando {data['sample_id']} al Pool de CPU")
                asyncio.create_task(
                    cpu_worker_wrapper(loop, cpu_pool, data, shared_results, lock, queue)
                )
                
            elif data['type'] in ['BIOQUIMIC', 'FISIC']:
                print(f"-> Creando tarea I/O para {data['sample_id']}")
                asyncio.create_task(
                    io_worker_wrapper(data, shared_results, lock, queue)
                )
                
            else:
                print(f"[WARN] type de dato desconocido: {data.get('type')}")
                queue.task_done()

        except (AttributeError, KeyError) as e:
            print(f"[ERROR-CONSUMIDOR] Dato malformado: {data}. Error: {e}")
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
        lock = manager.Lock()
        
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
                asyncio.create_task(consumer(queue, cpu_pool, shared_results, lock)) for _ in range(3)
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

                generate_report(metrics_log, alert_latencies, critical_alerts_log)

def generate_report(metrics_log: List[Dict[str, Any]], alert_latencies: List[float], critical_alerts: List[Dict[str, Any]]):
    
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
    
    print("\n2. Log de Alertas Críticas Procesadas:")
    if critical_alerts:
        print(f"Se procesaron un total de {len(critical_alerts)} alertas críticas:")
        try:
            alert_df = pd.DataFrame(critical_alerts)
            alert_df['payload_data'] = alert_df['payload'].astype(str)
            print(alert_df[['sample_id', 'type', 'timestamp', 'payload_data']].to_string())
        except Exception as e:
            print(f"No se pudo formatear el log de alertas (imprimiendo lista cruda): {e}")
            for alert in critical_alerts:
                print(alert)
    else:
        print("No se procesó ninguna alerta crítica durante la simulación.")

    print("\n3. Latencia a la detección y envío de alertas (ms):")
    if alert_latencies:
        avg_alert_latency = (sum(alert_latencies) / len(alert_latencies)) * 1000
        print(f"Latencia media de alertas: {avg_alert_latency:.2f} ms")
    else:
        print("No se genero latencia entre alertas.")

    print("\n4. Tasa de errores / fallos:")
    total_ops = len(metrics_log)
    total_errors = total_ops - len(df)
    error_rate_per_million = (total_errors / total_ops) * 1_000_000 if total_ops > 0 else 0
    print(f"Operaciones totales: {total_ops}")
    print(f"Fallos totales: {total_errors}")
    print(f"Tasa de fallos por millón de operaciones: {error_rate_per_million:.2f}")

    print("\n5. [Visual] Tabla de eventos procesados por tipo:")
    summary_table = df.groupby('type').agg(
        total_procesados=('sample_id', 'count'),
        latencia_media_ms=('latency_ms', 'mean')
    )
    print(summary_table.to_string())

    try:
        print("\n6. [Visual] Generando gráfico 'rendimiento_latencia.png'...")
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