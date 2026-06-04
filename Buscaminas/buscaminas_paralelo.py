"""Lógica de Buscaminas con paralelismo via Multiprocessing y distribución via PySpark.

Arquitectura:
  - PySpark: distribuye la inicialización del tablero (colocación de minas y
             cálculo de números) entre workers/particiones.
  - Multiprocessing (mp): paraleliza el cómputo intensivo de celdas dentro de
                          cada worker (conteo de vecinos, expansión BFS).
  - In-process: las acciones de juego (revelar, bandera) se ejecutan directamente
                para dar respuesta inmediata al usuario.
"""

import random
import time
import multiprocessing as mp
from collections import deque

import numpy as np

# ---------------------------------------------------------------------------
# Funciones de nivel de módulo para multiprocessing (requerido en Windows)
# ---------------------------------------------------------------------------

def _calcular_para_celda_mp(args):
    """Calcula las minas adyacentes a una celda usando la matriz global (proceso)."""
    r, c, col_inicio, tablero_global = args
    global_col = col_inicio + c

    if tablero_global[r, global_col] == -1:
        return (r, c, -1)

    minas = 0
    for dr in [-1, 0, 1]:
        for dc in [-1, 0, 1]:
            if dr == 0 and dc == 0:
                continue
            nr, nc = r + dr, global_col + dc
            if 0 <= nr < tablero_global.shape[0] and 0 <= nc < tablero_global.shape[1]:
                if tablero_global[nr, nc] == -1:
                    minas += 1
    return (r, c, minas)


def _evaluar_vecinos_mp(args):
    """Obtiene vecinos válidos y sus valores para BFS paralelo (proceso)."""
    f, c, filas, columnas, tablero = args
    vecinos_validos = []
    for dr in [-1, 0, 1]:
        for dc in [-1, 0, 1]:
            if dr == 0 and dc == 0:
                continue
            nf, nc = f + dr, c + dc
            if 0 <= nf < filas and 0 <= nc < columnas:
                vecinos_validos.append((nf, nc, int(tablero[nf, nc])))
    return vecinos_validos


# ---------------------------------------------------------------------------
# Funciones de nivel de módulo para PySpark (requerido para serialización RDD)
# ---------------------------------------------------------------------------

def _init_worker_spark(args):
    """Inicializa un worker de forma distribuida en un executor de Spark."""
    idx, region_id, filas, columnas, minas_region, primera_fila, primera_col, col_inicio = args
    import random
    import numpy as np

    tablero = np.zeros((filas, columnas), dtype=int)
    revelado = np.zeros((filas, columnas), dtype=bool)
    marcado = np.zeros((filas, columnas), dtype=bool)

    random.seed(region_id * 100)
    col_fin = col_inicio + columnas
    evitar_col_local = None
    if primera_fila is not None and col_inicio <= primera_col < col_fin:
        evitar_col_local = primera_col - col_inicio

    minas_colocadas = 0
    while minas_colocadas < minas_region:
        r = random.randint(0, filas - 1)
        c = random.randint(0, columnas - 1)
        if evitar_col_local is not None and r == primera_fila and c == evitar_col_local:
            continue
        if tablero[r, c] != -1:
            tablero[r, c] = -1
            minas_colocadas += 1

    return (region_id, tablero, revelado, marcado)


def _calc_numeros_spark(args):
    """Calcula los números de cada celda de un worker usando la matriz global (Spark)."""
    region_id, filas, columnas, tablero_local, tablero_global, col_inicio = args
    import numpy as np

    for r in range(filas):
        for c in range(columnas):
            global_col = col_inicio + c
            if tablero_global[r, global_col] == -1:
                continue
            minas = 0
            for dr in [-1, 0, 1]:
                for dc in [-1, 0, 1]:
                    if dr == 0 and dc == 0:
                        continue
                    nr, nc = r + dr, global_col + dc
                    if 0 <= nr < tablero_global.shape[0] and 0 <= nc < tablero_global.shape[1]:
                        if tablero_global[nr, nc] == -1:
                            minas += 1
            tablero_local[r, c] = minas

    return (region_id, tablero_local)


# ---------------------------------------------------------------------------
# Singleton de SparkContext
# ---------------------------------------------------------------------------

_spark_session = None


def obtener_contexto_spark():
    """Retorna o crea un SparkContext de forma segura (Singleton) si PySpark está disponible."""
    global _spark_session
    if _spark_session is None:
        import os
        import sys
        # Forzar a PySpark a usar el ejecutable del entorno virtual actual
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
        os.environ["SPARK_LOCAL_HOSTNAME"] = "127.0.0.1"

        try:
            from pyspark.sql import SparkSession
        except ModuleNotFoundError:
            return None

        _spark_session = SparkSession.builder \
            .appName("BuscaminasDistribuidoSpark") \
            .master("local[*]") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.ui.enabled", "false") \
            .getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")
    return _spark_session.sparkContext


# ---------------------------------------------------------------------------
# WorkerRegion — gestiona una partición horizontal del tablero
# ---------------------------------------------------------------------------

class WorkerRegion:
    """Worker independiente que administra una región del tablero."""

    def __init__(self, region_id, filas, columnas, minas_region):
        self.region_id = region_id
        self.filas = filas
        self.columnas = columnas
        self.minas_region = minas_region
        self.tablero = np.zeros((filas, columnas), dtype=int)
        self.revelado = np.zeros((filas, columnas), dtype=bool)
        self.marcado = np.zeros((filas, columnas), dtype=bool)
        self.stats = {
            'cell_calculations': 0,
            'expansion_tasks': 0,
        }

    def expandir_vacio_paralelo(self, fila, col):
        """Expande áreas vacías con BFS, evaluando vecinos via multiprocessing.Pool."""
        if self.tablero[fila, col] != 0:
            return []

        cola = deque([(fila, col)])
        visitados = {(fila, col)}
        por_revelar = [(fila, col)]

        num_procesos = min(4, mp.cpu_count())
        with mp.Pool(processes=num_procesos) as pool:
            while cola:
                nivel_actual = []
                while cola:
                    nivel_actual.append(cola.popleft())

                args_list = [(f, c, self.filas, self.columnas, self.tablero)
                             for f, c in nivel_actual]
                resultados_vecinos = pool.map(_evaluar_vecinos_mp, args_list)

                for vecinos in resultados_vecinos:
                    self.stats['expansion_tasks'] += len(vecinos)
                    for nf, nc, valor in vecinos:
                        if (nf, nc) not in visitados and not self.marcado[nf, nc]:
                            visitados.add((nf, nc))
                            if valor == 0:
                                cola.append((nf, nc))
                            if valor != -1:
                                por_revelar.append((nf, nc))

        for f, c in por_revelar:
            if not self.marcado[f, c]:
                self.revelado[f, c] = True

        return por_revelar


# ---------------------------------------------------------------------------
# BuscaminasDistribuido — coordinador principal
# ---------------------------------------------------------------------------

class BuscaminasDistribuido:
    """
    Coordinador que usa PySpark para la inicialización distribuida del tablero
    y lógica in-process para las acciones de juego (revelar/bandera).
    """

    def __init__(self, filas=12, columnas=12, minas=20, num_workers=4):
        self.filas = filas
        self.columnas = columnas
        self.minas = minas
        self.num_workers = num_workers

        # Dividir el tablero en regiones horizontales
        self.workers = []
        self.worker_offsets = []
        minas_por_worker = minas // num_workers
        minas_resto = minas % num_workers

        col_offset = 0
        for i in range(num_workers):
            cols_worker = columnas // num_workers
            if i == num_workers - 1:
                cols_worker = columnas - i * cols_worker

            minas_worker = minas_por_worker + (1 if i < minas_resto else 0)
            worker = WorkerRegion(i, filas, cols_worker, minas_worker)
            self.workers.append(worker)
            self.worker_offsets.append(col_offset)
            col_offset += cols_worker

        self.game_over = False
        self.victoria = False
        self.primera_jugada = True
        self.metrics = {
            'paralelo_times': [],
            'distributed_ops': 0,
            'workers_used': num_workers,
            'initialization_time': 0.0,
            'last_move_time': 0.0,
            'spark_stages': 0,
        }

        # Inicializar SparkContext al crear el juego
        self.sc = obtener_contexto_spark()
        self.usa_spark = self.sc is not None

    # -----------------------------------------------------------------------
    # Inicialización distribuida con PySpark
    # -----------------------------------------------------------------------

    def inicializar_partida(self, primera_fila, primera_col):
        """
        Usa PySpark RDDs para distribuir:
          1) La colocación de minas en cada región.
          2) El cálculo de números de cada celda (considerando fronteras).
        Multiprocessing se usa dentro de cada tarea Spark para paralelizar
        el conteo de minas por celda.
        """
        start_time = time.time()

        if not self.usa_spark:
            for idx, worker in enumerate(self.workers):
                col_inicio = self.worker_offsets[idx]
                col_fin = col_inicio + worker.columnas
                evitar_col_local = None
                if primera_fila is not None and col_inicio <= primera_col < col_fin:
                    evitar_col_local = primera_col - col_inicio
                worker.inicializar(primera_fila, evitar_col_local)

            tablero_global = self._construir_tablero_minas_global()
            for idx, worker in enumerate(self.workers):
                col_inicio = self.worker_offsets[idx]
                for r in range(worker.filas):
                    for c in range(worker.columnas):
                        if worker.tablero[r, c] == -1:
                            continue
                        global_col = col_inicio + c
                        minas = 0
                        for dr in [-1, 0, 1]:
                            for dc in [-1, 0, 1]:
                                if dr == 0 and dc == 0:
                                    continue
                                nr, nc = r + dr, global_col + dc
                                if 0 <= nr < tablero_global.shape[0] and 0 <= nc < tablero_global.shape[1]:
                                    if tablero_global[nr, nc] == -1:
                                        minas += 1
                        worker.tablero[r, c] = minas
                worker.stats['cell_calculations'] += worker.filas * worker.columnas

            elapsed = time.time() - start_time
            self.metrics['paralelo_times'].append(elapsed)
            self.metrics['initialization_time'] = elapsed
            self.metrics['spark_stages'] = 0
            self.primera_jugada = False
            return elapsed

        # --- Etapa 1 Spark: inicialización de regiones (colocación de minas) ---
        init_data = [
            (idx, w.region_id, w.filas, w.columnas, w.minas_region,
             primera_fila, primera_col, self.worker_offsets[idx])
            for idx, w in enumerate(self.workers)
        ]
        rdd_init = self.sc.parallelize(init_data, numSlices=self.num_workers)
        resultados_init = sorted(
            rdd_init.map(_init_worker_spark).collect(),
            key=lambda x: x[0]   # ordenar por region_id
        )

        # Aplicar resultados al driver
        for region_id, tablero, revelado, marcado in resultados_init:
            w = self.workers[region_id]
            w.tablero = tablero
            w.revelado = revelado
            w.marcado = marcado

        self.metrics['spark_stages'] += 1

        # --- Construir tablero global de minas para calcular fronteras ---
        tablero_global = self._construir_tablero_minas_global()

        # --- Etapa 2 Spark: cálculo de números por celda ---
        calc_data = [
            (w.region_id, w.filas, w.columnas, w.tablero.copy(),
             tablero_global, self.worker_offsets[idx])
            for idx, w in enumerate(self.workers)
        ]
        rdd_calc = self.sc.parallelize(calc_data, numSlices=self.num_workers)
        resultados_calc = sorted(
            rdd_calc.map(_calc_numeros_spark).collect(),
            key=lambda x: x[0]   # ordenar por region_id
        )

        for region_id, tablero_calculado in resultados_calc:
            self.workers[region_id].tablero = tablero_calculado
            self.workers[region_id].stats['cell_calculations'] += (
                self.workers[region_id].filas * self.workers[region_id].columnas
            )

        self.metrics['spark_stages'] += 1

        elapsed = time.time() - start_time
        self.metrics['paralelo_times'].append(elapsed)
        self.metrics['initialization_time'] = elapsed
        self.primera_jugada = False
        return elapsed

    def _construir_tablero_minas_global(self):
        """Construye la matriz global con solo minas, para calcular fronteras."""
        tablero_global = np.zeros((self.filas, self.columnas), dtype=int)
        for idx, worker in enumerate(self.workers):
            inicio = self.worker_offsets[idx]
            fin = inicio + worker.columnas
            tablero_global[:, inicio:fin] = worker.tablero
        return tablero_global

    # -----------------------------------------------------------------------
    # Acciones de juego — in-process para respuesta inmediata
    # -----------------------------------------------------------------------

    def _mapear_a_worker(self, fila, columna):
        """Mapea coordenadas globales al worker y coordenadas locales."""
        col_actual = 0
        for i, worker in enumerate(self.workers):
            if columna < col_actual + worker.columnas:
                return worker, fila, columna - col_actual, i
            col_actual += worker.columnas
        return None, None, None, None

    def revelar_todas_las_minas(self):
        """Revela todas las minas al perder."""
        for worker in self.workers:
            worker.revelado = np.logical_or(worker.revelado, worker.tablero == -1)

    def revelar_celda(self, fila, columna):
        """Revela una celda. Usa PySpark para la inicialización en el primer clic."""
        if self.game_over or self.victoria:
            return False, 'finalizado'

        if self.primera_jugada:
            self.inicializar_partida(fila, columna)
            # Llamada recursiva única después de inicializar
            return self.revelar_celda(fila, columna)

        start_time = time.time()

        worker, fl, cl, _ = self._mapear_a_worker(fila, columna)
        if not worker:
            return False, 'invalido'

        if worker.revelado[fl, cl] or worker.marcado[fl, cl]:
            return False, 'ignorado'

        if worker.tablero[fl, cl] == -1:
            # Mina: revelar todas y terminar
            self.game_over = True
            self.revelar_todas_las_minas()
            elapsed = time.time() - start_time
            self.metrics['paralelo_times'].append(elapsed)
            self.metrics['last_move_time'] = elapsed
            return True, 'mina'

        if worker.tablero[fl, cl] == 0:
            # Expansión BFS paralela con multiprocessing
            expandidas = worker.expandir_vacio_paralelo(fl, cl)
            self.metrics['distributed_ops'] += len(expandidas)
        else:
            worker.revelado[fl, cl] = True
            self.metrics['distributed_ops'] += 1

        elapsed = time.time() - start_time
        self.metrics['paralelo_times'].append(elapsed)
        self.metrics['last_move_time'] = elapsed

        # Verificar victoria
        total_reveladas = sum(np.sum(w.revelado) for w in self.workers)
        total_sin_minas = self.filas * self.columnas - self.minas

        if total_reveladas >= total_sin_minas:
            self.victoria = True
            self.game_over = True
            return True, 'victoria'

        return True, 'ok'

    def marcar_bandera(self, fila, columna):
        """Marca o desmarca una bandera de forma inmediata."""
        if self.game_over or self.victoria:
            return None

        if self.primera_jugada:
            self.inicializar_partida(fila, columna)

        worker, fl, cl, _ = self._mapear_a_worker(fila, columna)
        if worker and not worker.revelado[fl, cl]:
            worker.marcado[fl, cl] = not worker.marcado[fl, cl]
            return bool(worker.marcado[fl, cl])
        return None

    # -----------------------------------------------------------------------
    # Visualización y métricas
    # -----------------------------------------------------------------------

    def obtener_tablero_completo(self):
        """Reconstruye el tablero completo para enviar al frontend."""
        tablero = np.full((self.filas, self.columnas), 'hidden', dtype=object)

        col_offset = 0
        for worker in self.workers:
            if worker.revelado is None or worker.marcado is None or worker.tablero is None:
                col_offset += worker.columnas
                continue

            for i in range(worker.filas):
                for j in range(worker.columnas):
                    if worker.revelado[i, j]:
                        val = worker.tablero[i, j]
                        if val == -1:
                            tablero[i, col_offset + j] = 'mine'
                        elif val == 0:
                            tablero[i, col_offset + j] = 'empty'
                        else:
                            tablero[i, col_offset + j] = str(val)
                    elif worker.marcado[i, j]:
                        tablero[i, col_offset + j] = 'flag'

            col_offset += worker.columnas

        return tablero

    def get_metrics(self):
        """Retorna métricas de rendimiento."""
        total_tasks = sum(w.stats['expansion_tasks'] for w in self.workers)
        total_cell_calcs = sum(w.stats['cell_calculations'] for w in self.workers)
        regiones = [
            {
                'worker': idx,
                'columnas': worker.columnas,
                'minas_asignadas': worker.minas_region,
            }
            for idx, worker in enumerate(self.workers)
        ]
        return {
            'total_ops_distribuidas': self.metrics['distributed_ops'],
            'num_workers': self.metrics['workers_used'],
            'tiempo_promedio_paralelo': (
                float(np.mean(self.metrics['paralelo_times']))
                if self.metrics['paralelo_times'] else 0.0
            ),
            'tiempo_inicializacion': self.metrics['initialization_time'],
            'tiempo_ultima_jugada': self.metrics['last_move_time'],
            'tareas_paralelas': total_tasks,
            'calculos_celdas': total_cell_calcs,
            'regiones_distribuidas': regiones,
            'tablero_size': f"{self.filas}x{self.columnas}",
            'total_minas': self.minas,
            'spark_stages': self.metrics['spark_stages'],
        }