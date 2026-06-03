"""Lógica de Buscaminas con simulación paralela y distribuida."""

import random
import time
from concurrent.futures import ThreadPoolExecutor
from threading import RLock

import numpy as np

class WorkerRegion:
    """Worker independiente que maneja una región del tablero"""
    
    def __init__(self, region_id, filas, columnas, minas_region):
        self.region_id = region_id
        self.filas = filas
        self.columnas = columnas
        self.minas_region = minas_region
        self.tablero = None
        self.revelado = None
        self.marcado = None
        self.lock = RLock()
        self.stats = {
            'cell_calculations': 0,
            'expansion_tasks': 0,
        }
        
    def inicializar(self, evitar_fila=None, evitar_col=None):
        """Inicializa la región con minas aleatorias."""
        random.seed(self.region_id * 100)
        
        self.tablero = np.zeros((self.filas, self.columnas), dtype=int)
        self.revelado = np.zeros((self.filas, self.columnas), dtype=bool)
        self.marcado = np.zeros((self.filas, self.columnas), dtype=bool)
        
        # Colocar minas en la región.
        minas_colocadas = 0
        while minas_colocadas < self.minas_region:
            r = random.randint(0, self.filas - 1)
            c = random.randint(0, self.columnas - 1)
            
            if evitar_fila is not None and r == evitar_fila and c == evitar_col:
                continue
                
            if self.tablero[r, c] != -1:
                self.tablero[r, c] = -1
                minas_colocadas += 1
        
        return self.tablero
    
    def calcular_numeros_desde_global(self, tablero_global, col_inicio):
        """Calcula números de la región leyendo un tablero global en paralelo."""
        def calcular_para_celda(args):
            r, c = args
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

        celdas = [(r, c) for r in range(self.filas) for c in range(self.columnas)]
        max_workers = min(8, max(1, len(celdas) // 8))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            resultados = list(executor.map(calcular_para_celda, celdas))

        for r, c, valor in resultados:
            if valor != -1:
                self.tablero[r, c] = valor

        self.stats['cell_calculations'] += len(celdas)
    
    def expandir_vacio_paralelo(self, fila, col):
        """Expande áreas vacías usando BFS con evaluación paralela de vecinos."""
        if self.tablero[fila, col] != 0:
            return []
        
        from collections import deque
        cola = deque()
        cola.append((fila, col))
        visitados = set()
        visitados.add((fila, col))
        por_revelar = [(fila, col)]
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            while cola:
                actual_f, actual_c = cola.popleft()
                
                # Generar vecinos
                vecinos = []
                for dr in [-1, 0, 1]:
                    for dc in [-1, 0, 1]:
                        if dr == 0 and dc == 0:
                            continue
                        nf, nc = actual_f + dr, actual_c + dc
                        if 0 <= nf < self.filas and 0 <= nc < self.columnas:
                            vecinos.append((nf, nc))
                
                def procesar_vecino(vecino):
                    nf, nc = vecino
                    with self.lock:
                        if (nf, nc) not in visitados and not self.marcado[nf, nc]:
                            visitados.add((nf, nc))
                            if self.tablero[nf, nc] == 0:
                                cola.append((nf, nc))
                            if self.tablero[nf, nc] != -1:
                                por_revelar.append((nf, nc))
                            return True
                    return False
                
                futures = [executor.submit(procesar_vecino, v) for v in vecinos]
                for f in futures:
                    f.result()
                self.stats['expansion_tasks'] += len(vecinos)
        
        # Revelar todas las celdas
        with self.lock:
            for f, c in por_revelar:
                if not self.marcado[f, c]:
                    self.revelado[f, c] = True
        
        return por_revelar


class BuscaminasDistribuido:
    """Coordinador principal que maneja el sistema distribuido"""
    
    def __init__(self, filas=12, columnas=12, minas=20, num_workers=4):
        self.filas = filas
        self.columnas = columnas
        self.minas = minas
        self.num_workers = num_workers
        
        # Dividir el tablero en regiones (distribución horizontal).
        self.workers = []
        self.worker_offsets = []
        minas_por_worker = minas // num_workers
        minas_resto = minas % num_workers
        
        col_offset = 0
        for i in range(num_workers):
            cols_worker = columnas // num_workers
            if i == num_workers - 1:
                cols_worker = columnas - i * cols_worker
            
            minas_worker = minas_por_worker
            if i < minas_resto:
                minas_worker += 1
            
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
        }
    
    def _mapear_a_worker(self, fila, columna):
        """Mapea coordenadas globales al worker correspondiente"""
        col_actual = 0
        for i, worker in enumerate(self.workers):
            if columna < col_actual + worker.columnas:
                return worker, fila, columna - col_actual, i
            col_actual += worker.columnas
        return None, None, None, None
    
    def inicializar_partida(self, primera_fila, primera_col):
        """Inicializa workers y calcula números respetando fronteras entre regiones."""
        start_time = time.time()

        def init_worker(item):
            idx, worker = item
            col_inicio = self.worker_offsets[idx]
            col_fin = col_inicio + worker.columnas
            evitar_col_local = None
            if primera_fila is not None and col_inicio <= primera_col < col_fin:
                evitar_col_local = primera_col - col_inicio
            worker.inicializar(primera_fila, evitar_col_local)

        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            list(executor.map(init_worker, list(enumerate(self.workers))))

        tablero_global = self._construir_tablero_minas_global()

        def calc_worker(item):
            idx, worker = item
            worker.calcular_numeros_desde_global(tablero_global, self.worker_offsets[idx])

        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            list(executor.map(calc_worker, list(enumerate(self.workers))))
        
        elapsed = time.time() - start_time
        self.metrics['paralelo_times'].append(elapsed)
        self.metrics['initialization_time'] = elapsed
        
        self.primera_jugada = False
        return elapsed

    def _construir_tablero_minas_global(self):
        """Construye matriz global de minas para cálculos entre fronteras de workers."""
        tablero_global = np.zeros((self.filas, self.columnas), dtype=int)
        for idx, worker in enumerate(self.workers):
            inicio = self.worker_offsets[idx]
            fin = inicio + worker.columnas
            tablero_global[:, inicio:fin] = worker.tablero
        return tablero_global

    def revelar_todas_las_minas(self):
        """Revela todas las minas al perder para un comportamiento clásico."""
        for worker in self.workers:
            with worker.lock:
                worker.revelado = np.logical_or(worker.revelado, worker.tablero == -1)
    
    def revelar_celda(self, fila, columna):
        """Revela una celda usando el worker correspondiente"""
        if self.game_over or self.victoria:
            return False, 'finalizado'
        
        worker, fl, cl, _ = self._mapear_a_worker(fila, columna)
        if not worker:
            return False, 'invalido'
        
        # Inicializar en primera jugada
        if self.primera_jugada:
            self.inicializar_partida(fila, columna)
            return self.revelar_celda(fila, columna)
        
        with worker.lock:
            if worker.revelado[fl, cl] or worker.marcado[fl, cl]:
                return False, 'ignorado'
            
            if worker.tablero[fl, cl] == -1:
                self.game_over = True
                self.revelar_todas_las_minas()
                return True, 'mina'
        
        # Expansión paralela si es vacío
        start_par = time.time()
        if worker.tablero[fl, cl] == 0:
            expandidas = worker.expandir_vacio_paralelo(fl, cl)
            self.metrics['distributed_ops'] += len(expandidas)
        else:
            with worker.lock:
                worker.revelado[fl, cl] = True
            self.metrics['distributed_ops'] += 1
        
        paralelo_time = time.time() - start_par
        self.metrics['paralelo_times'].append(paralelo_time)
        self.metrics['last_move_time'] = paralelo_time
        
        # Verificar victoria
        total_reveladas = sum(np.sum(w.revelado) for w in self.workers)
        total_sin_minas = self.filas * self.columnas - self.minas
        
        if total_reveladas >= total_sin_minas:
            self.victoria = True
            self.game_over = True
            return True, 'victoria'
        
        return True, 'ok'
    
    def marcar_bandera(self, fila, columna):
        """Marca o desmarca una bandera"""
        if self.game_over or self.victoria:
            return None

        if self.primera_jugada:
            self.inicializar_partida(fila, columna)
        
        worker, fl, cl, _ = self._mapear_a_worker(fila, columna)
        if worker and not worker.revelado[fl, cl]:
            with worker.lock:
                worker.marcado[fl, cl] = not worker.marcado[fl, cl]
                return worker.marcado[fl, cl]
        return None
    
    def obtener_tablero_completo(self):
        """Reconstruye el tablero completo para visualización"""
        tablero = np.full((self.filas, self.columnas), 'hidden', dtype=object)
        
        col_offset = 0
        for worker in self.workers:
            if worker.revelado is None or worker.marcado is None or worker.tablero is None:
                col_offset += worker.columnas
                continue

            for i in range(worker.filas):
                for j in range(worker.columnas):
                    if worker.revelado[i, j]:
                        if worker.tablero[i, j] == -1:
                            tablero[i, col_offset + j] = 'mine'
                        elif worker.tablero[i, j] == 0:
                            tablero[i, col_offset + j] = 'empty'
                        else:
                            tablero[i, col_offset + j] = str(worker.tablero[i, j])
                    else:
                        if worker.marcado[i, j]:
                            tablero[i, col_offset + j] = 'flag'
            col_offset += worker.columnas
        
        return tablero
    
    def get_metrics(self):
        """Retorna métricas de rendimiento"""
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
            'tiempo_promedio_paralelo': float(np.mean(self.metrics['paralelo_times'])) if self.metrics['paralelo_times'] else 0.0,
            'tiempo_inicializacion': self.metrics['initialization_time'],
            'tiempo_ultima_jugada': self.metrics['last_move_time'],
            'tareas_paralelas': total_tasks,
            'calculos_celdas': total_cell_calcs,
            'regiones_distribuidas': regiones,
            'tablero_size': f"{self.filas}x{self.columnas}",
            'total_minas': self.minas,
        }