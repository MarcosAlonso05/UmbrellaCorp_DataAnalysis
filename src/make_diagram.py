from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.queue import Kafka
from diagrams.onprem.compute import Server
from diagrams.onprem.inmemory import Redis
from diagrams.programming.language import Python
from diagrams.generic.database import SQL

with Diagram("Sistema de Análisis Concurrente (Umbrella)", filename="docs/arquitectura_final", show=False, direction="LR"):

    with Cluster("Ingesta (Sensores/Fuentes)"):
        s_gen = Python("GeneticService")
        s_bio = Python("BioquimicService")
        s_fis = Python("FisicService")
        ingest_sources = [s_gen, s_bio, s_fis]

    with Cluster("Productor y Colas (Asyncio)"):
        normalizer = Server("Normalización")
        main_queue = Kafka("Cola de Trabajo")
        alert_queue = Kafka("Cola de Alertas")

        ingest_sources >> normalizer >> main_queue
        normalizer >> Edge(color="red", style="dashed") >> alert_queue

    with Cluster("Procesamiento Concurrente (Workers)"):
        with Cluster("Pool de CPU (multiprocessing)"):
            cpu_workers = [Server("Worker CPU 1"), Server("Worker CPU N")]

        with Cluster("Pool de I/O (asyncio tasks)"):
            io_workers = [Python("Worker I/O 1"), Python("Worker I/O N")]

        alert_worker = Python("Worker Alertas")

    with Cluster("Resultados"):
        aggregator = Redis("Agregador (Manager.dict)")
        storage = SQL("Almacenamiento (BBDD)")

    main_queue >> cpu_workers
    main_queue >> io_workers

    cpu_workers >> aggregator
    io_workers >> storage

    storage >> aggregator

    alert_queue >> alert_worker