# Buscaminas

### José Nicolás Lesmes - Juan Camilo Camacho

Aplicacion web de Buscaminas hecha con Flask y Python. La interfaz se abre en el navegador y el juego expone una API interna para crear partidas, revelar celdas y marcar banderas.

## Requisitos

- Windows
- Python 3.13 o superior
- PowerShell

## Estructura del proyecto

- `app.py`: servidor Flask y rutas del juego
- `buscaminas_paralelo.py`: logica del tablero, ejecucion paralela y distribucion por workers
- `templates/index.html`: interfaz web
- `static/style.css`: estilos de la pagina
- `requirements.txt`: dependencias de Python

## Instalacion

Abre PowerShell en la carpeta del proyecto y ejecuta:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
```

Si todavia no existe el entorno virtual, crealo primero:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Luego instala dependencias:

```powershell
pip install -r requirements.txt
```

## Como ejecutar la aplicacion

Con el entorno virtual activado, inicia Flask con:

```powershell
python app.py
```

Tambien puedes usar la ruta directa al ejecutable de la venv:

```powershell
.\.venv\Scripts\python.exe app.py
```

## URL de acceso

Cuando el servidor arranque, abre esta direccion en el navegador:

```text
http://127.0.0.1:5000/
```

El juego usa este puerto por defecto en `app.py`.

## Uso del juego

1. Abre la URL local en el navegador.
2. Ajusta filas, columnas, minas y workers.
3. Pulsa `Nuevo juego`.
4. Click izquierdo: revelar celda.
5. Click derecho: colocar o quitar bandera.

## Parte paralela y distribuida

### Paralelismo

La ejecucion paralela esta en `buscaminas_paralelo.py` dentro de la clase `WorkerRegion`.

- Usa `ThreadPoolExecutor` para inicializar regiones y calcular numeros al mismo tiempo.
- Calcula los valores de las celdas en paralelo cuando se construye el tablero.
- Expande zonas vacias con BFS y tareas paralelas sobre vecinos.
- Mantiene estadisticas como `cell_calculations`, `expansion_tasks` y tiempos promedio.

### Distribucion

La parte distribuida tambien esta en `buscaminas_paralelo.py`, en `BuscaminasDistribuido`.

- Divide el tablero en regiones horizontales.
- Asigna una region a cada worker.
- Cada worker conserva su propio tablero parcial, minas, celdas reveladas y banderas.
- El coordinador mapea coordenadas globales a la region correcta.
- Al terminar una jugada, reconstruye el tablero completo para la interfaz.

### Flujo del juego

- `app.py` recibe la solicitud del navegador.
- `/new_game` crea una instancia de `BuscaminasDistribuido`.
- `/reveal` revela una celda y devuelve el tablero actualizado.
- `/flag` marca o desmarca una bandera y devuelve el tablero actualizado.
- La pagina en `templates/index.html` pinta el tablero y consume esos endpoints con `fetch`.

## Solucion rapida de problemas

- Si no cargan los estilos, verifica que la pagina use `static/style.css`.
- Si ves errores al iniciar, confirma que la venv esta activada antes de correr `python app.py`.
- Si el navegador muestra cambios viejos, haz una recarga dura con `Ctrl + F5`.

## Comandos rapidos

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py
```
