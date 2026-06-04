from flask import Flask, render_template, request, jsonify, session
from buscaminas_paralelo import BuscaminasDistribuido
import uuid

app = Flask(__name__)
app.secret_key = 'tu_clave_secreta_aqui'

# Almacenar juegos activos (simulación de sesiones distribuidas)
games = {}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/new_game', methods=['POST'])
def new_game():
    game_id = str(uuid.uuid4())
    data = request.json
    
    filas = data.get('filas', 10)
    columnas = data.get('columnas', 10)
    minas = data.get('minas', 15)
    workers = data.get('workers', 4)
    
    game = BuscaminasDistribuido(filas, columnas, minas, workers)
    games[game_id] = game
    
    return jsonify({
        'game_id': game_id,
        'filas': filas,
        'columnas': columnas,
        'minas': minas,
        'workers': workers,
        'tablero': game.obtener_tablero_completo().tolist(),
        'metrics': game.get_metrics()
    })

@app.route('/reveal', methods=['POST'])
def reveal():
    data = request.json
    game_id = data.get('game_id')
    fila = data.get('fila')
    columna = data.get('columna')
    
    game = games.get(game_id)
    if not game:
        return jsonify({'error': 'Juego no encontrado'}), 404
    
    success, result = game.revelar_celda(fila, columna)
    
    return jsonify({
        'success': success,
        'result': result,
        'game_over': game.game_over,
        'victoria': game.victoria,
        'tablero': game.obtener_tablero_completo().tolist(),
        'metrics': game.get_metrics()
    })

@app.route('/flag', methods=['POST'])
def flag():
    data = request.json
    game_id = data.get('game_id')
    fila = data.get('fila')
    columna = data.get('columna')
    
    game = games.get(game_id)
    if not game:
        return jsonify({'error': 'Juego no encontrado'}), 404
    
    marcado = game.marcar_bandera(fila, columna)
    marcado = None if marcado is None else bool(marcado)
    
    return jsonify({
        'success': marcado is not None,
        'marcado': marcado,
        'tablero': game.obtener_tablero_completo().tolist(),
        'metrics': game.get_metrics()
    })

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False, port=5000)