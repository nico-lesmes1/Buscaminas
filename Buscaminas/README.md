# Buscaminas Distribuido

Aplicacion web de Buscaminas hecha con Flask y Python. La interfaz vive en el navegador y consume una API interna para crear partidas, revelar celdas y marcar banderas.

## Requisitos

- Windows
- Python 3.13 o superior
- PowerShell

## Como ejecutar en PowerShell

Abre PowerShell dentro de la carpeta del proyecto y ejecuta estos pasos en orden:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py
```

Si ya tienes creada la venv, puedes omitir el paso de `python -m venv .venv`.

## URL del juego

Cuando el servidor arranque, abre esta direccion en el navegador:

```text
http://127.0.0.1:5000/
```

El puerto por defecto es `5000`, definido en `app.py`.

## Flujo de funcionamiento

1. `app.py` levanta Flask y sirve la pagina principal.
2. La vista `templates/index.html` carga la interfaz y hace peticiones `fetch` al backend.
3. Al pulsar `Nuevo juego`, el frontend llama a `/new_game`.
4. El motor crea la partida, inicializa el tablero y devuelve el estado completo.
5. Click izquierdo llama a `/reveal` para descubrir una celda.
6. Click derecho llama a `/flag` para poner o quitar una bandera.
7. El backend responde con el tablero actualizado y las metricas.

## Por que es paralelo

La aplicacion es paralela porque reparte trabajo de CPU dentro del motor de juego:

- La inicializacion del tablero puede ejecutarse con PySpark cuando esta disponible.
- El calculo de numeros de las celdas se hace por regiones y puede distribuirse entre tareas.
- La expansion de zonas vacias usa evaluacion concurrente de vecinos con multiprocessing.
- Las metricas del juego registran tiempos, celdas calculadas y tareas paralelas ejecutadas.

En esta implementacion, la parte paralela no cambia la experiencia del usuario: solo acelera los calculos internos.

## Por que es distribuido

La aplicacion es distribuida porque el tablero no se maneja como una sola pieza monolitica:

- `BuscaminasDistribuido` divide el tablero en regiones horizontales.
- Cada region se comporta como un worker con su propio tablero parcial.
- El coordinador mapea coordenadas globales a coordenadas locales de cada worker.
- Al final de cada accion, el estado completo se reconstruye para enviarlo a la interfaz.

Si PySpark esta instalado, la inicializacion puede usar una etapa distribuida con Spark. Si no esta disponible, la aplicacion usa el mismo modelo de regiones, pero con ejecucion local para no romper el juego.

## Estructura del proyecto

### `app.py`

Servidor Flask y punto de entrada de la aplicacion.

Funciones importantes:

- `index()`: renderiza la pagina principal.
- `new_game()`: crea una partida nueva y devuelve tablero y metricas.
- `reveal()`: revela una celda y devuelve el tablero actualizado.
- `flag()`: marca o desmarca una bandera.

### `buscaminas_paralelo.py`

Motor principal del juego, con la logica de distribucion, paralelismo y reconstruccion del tablero.

Funciones y clases importantes:

- `WorkerRegion`: representa una region del tablero.
- `WorkerRegion.expandir_vacio_paralelo()`: expande zonas vacias con evaluacion concurrente.
- `obtener_contexto_spark()`: crea el contexto Spark si PySpark esta instalado.
- `BuscaminasDistribuido.__init__()`: divide el tablero en workers y prepara el juego.
- `BuscaminasDistribuido.inicializar_partida()`: inicializa minas y numeros, con Spark si esta disponible o con fallback local.
- `BuscaminasDistribuido.revelar_celda()`: revela una celda y controla victoria o derrota.
- `BuscaminasDistribuido.marcar_bandera()`: pone o quita una bandera.
- `BuscaminasDistribuido.obtener_tablero_completo()`: reconstruye el estado visible para el frontend.
- `BuscaminasDistribuido.get_metrics()`: devuelve metricas de rendimiento y distribucion.

### `templates/index.html`

Vista principal del juego. Contiene el tablero, los controles y el JavaScript que consume la API.

Funciones importantes:

- `newGame()`: crea una nueva partida desde la interfaz.
- `handleCellClick()`: revela una celda al hacer click izquierdo.
- `handleFlag()`: marca o desmarca una bandera con click derecho.
- `createBoard()`: pinta el tablero segun los datos recibidos.
- `updateMetrics()`: renderiza las metricas del motor.

### `static/style.css`

Hoja de estilos visuales de la aplicacion.

Responsabilidades principales:

- Define colores, fondos, tarjetas y animaciones.
- Da estilo al tablero, celdas, banderas y minas.
- Ajusta el diseño responsive para pantallas pequenas.

### `requirements.txt`

Lista de dependencias de Python necesarias para ejecutar la aplicacion.

## Uso del juego

1. Abre `http://127.0.0.1:5000/`.
2. Ajusta filas, columnas, minas y workers.
3. Pulsa `Nuevo juego`.
4. Click izquierdo para revelar una celda.
5. Click derecho para poner o quitar una bandera.

## Solucion rapida de problemas

- Si no cargan los estilos, confirma que la pagina use `static/style.css`.
- Si aparece error de red, verifica que Flask siga corriendo en `http://127.0.0.1:5000/`.
- Si cambiaste codigo y no ves diferencias, recarga la pagina con `Ctrl + F5`.

## Comandos rapidos

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py
```
