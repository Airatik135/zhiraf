# server.py — Полная рабочая версия для Scooter Tracker
from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
import sqlite3, os, json, math, logging, csv, subprocess, sys
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'database', 'wifi_base.db')
KML_PROCESSOR_PATH = os.path.join(BASE_DIR, 'kml_processor.py')
PORT = int(os.environ.get('PORT', 5000))
HOST = os.environ.get('HOST', '0.0.0.0')

MIN_MATCHES = 2
RSSI_WEIGHT_POWER = 2
MAX_ACCURACY = 200

# ================= KML PROCESSOR =================
def run_kml_processor():
    """Запускает kml_processor.py для обновления базы данных"""
    if not os.path.exists(KML_PROCESSOR_PATH):
        logger.warning(f"⚠️  kml_processor.py не найден: {KML_PROCESSOR_PATH}")
        logger.warning("   Пропускаю обновление базы из KML")
        return False
    
    logger.info(" Запускаю kml_processor.py для обновления базы...")
    try:
        result = subprocess.run(
            [sys.executable, KML_PROCESSOR_PATH],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode == 0:
            logger.info("✅ kml_processor.py завершён успешно")
            if result.stdout:
                for line in result.stdout.strip().split('\n'):
                    if line:
                        logger.info(f"   [KML] {line}")
            return True
        else:
            logger.error(f"❌ kml_processor.py вернул код {result.returncode}")
            if result.stderr:
                logger.error(f"   Ошибка: {result.stderr.strip()}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("❌ kml_processor.py превысил таймаут (300 сек)")
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка при запуске kml_processor.py: {str(e)}")
        return False

# ================= CSV IMPORT =================
def import_csv_if_empty():
    csv_path = os.path.join(BASE_DIR, 'processed_csv', 'wifi_base_clean.csv')
    if not os.path.exists(csv_path):
        logger.warning(f"CSV не найден: {csv_path}. Пропускаю импорт.")
        return
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    try:
        c.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='wifi_ap'")
        if c.fetchone()[0] == 0:
            logger.error("Таблица wifi_ap отсутствует! Пропускаю импорт CSV.")
            conn.close()
            return
    except Exception as e:
        logger.error(f"Ошибка проверки таблицы: {e}")
        conn.close()
        return
    
    count = c.execute('SELECT COUNT(*) FROM wifi_ap').fetchone()[0]
    logger.info(f"В базе сейчас {count} записей")
    
    if count == 0:
        logger.info("Импортирую из CSV...")
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                imported = 0
                for row in reader:
                    try:
                        c.execute('''INSERT INTO wifi_ap 
                            (mac, ssid, latitude, longitude, avg_rssi, sample_count)
                            VALUES (?, ?, ?, ?, ?, ?)''',
                            (row['mac'].strip().lower(), row['ssid'], 
                             float(row['latitude']), float(row['longitude']), 
                             float(row['avg_rssi']), int(row['samples'])))
                        imported += 1
                    except Exception as e:
                        logger.warning(f"Пропуск строки: {e}")
                        continue
                conn.commit()
                logger.info(f"Импортировано {imported} WiFi точек")
        except Exception as e:
            logger.error(f"Ошибка при чтении CSV: {e}")
    else:
        logger.info("База уже содержит записи, импорт пропущен")
    
    conn.close()

# ================= DATABASE INIT =================
def init_db():
    """Гарантированно создаёт все необходимые таблицы"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        logger.info("🔧 Создаю/проверяю таблицы...")
        
        # Таблица WiFi точек
        c.execute('''CREATE TABLE IF NOT EXISTS wifi_ap (
            id INTEGER PRIMARY KEY, mac TEXT NOT NULL, ssid TEXT,
            latitude REAL, longitude REAL, avg_rssi REAL, sample_count INTEGER,
            first_seen TEXT, last_seen TEXT, frequency INTEGER, encryption TEXT)''')
        
        # Таблица местоположений устройств
        c.execute('''CREATE TABLE IF NOT EXISTS device_locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            device_id TEXT NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            accuracy INTEGER,
            matching_aps INTEGER)''')
        
        # Таблица команд для устройств
        c.execute('''CREATE TABLE IF NOT EXISTS device_commands (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT NOT NULL,
            command TEXT NOT NULL,
            params TEXT,
            created_at TEXT NOT NULL,
            executed INTEGER DEFAULT 0)''')
        
        # Индексы для ускорения поиска
        c.execute('CREATE INDEX IF NOT EXISTS idx_mac ON wifi_ap(mac)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_dev_time ON device_locations(device_id, timestamp)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_cmd_device ON device_commands(device_id, executed)')
        
        conn.commit()
        conn.close()
        logger.info("✅ Таблицы успешно созданы/проверены")
        return True
    except Exception as e:
        logger.error(f"❌ Критическая ошибка инициализации БД: {e}")
        return False

def verify_db():
    """Проверяет наличие всех таблиц перед запуском сервера"""
    if not os.path.exists(DB_PATH):
        logger.error("❌ Файл БД не найден после инициализации!")
        return False
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in c.fetchall()]
    conn.close()
    
    logger.info(f"📊 Таблицы в БД: {tables}")
    required = {'wifi_ap', 'device_locations', 'device_commands'}
    
    if required.issubset(set(tables)):
        logger.info("✅ Все необходимые таблицы присутствуют")
        return True
    else:
        missing = required - set(tables)
        logger.error(f"❌ Отсутствуют таблицы: {missing}")
        return False

# ================= LOCATION FINDING =================
def find_location(networks):
    """Ищет местоположение по WiFi сетям"""
    if not os.path.exists(DB_PATH):
        return {'found': False, 'error': 'Database not found'}
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    matches = []
    scanned_macs = []
    
    for net in networks:
        mac = net.get('mac', '').lower().replace(':', '').replace('-', '')
        scanned_macs.append(mac)
        rssi = net.get('rssi', -100)
        
        # Ищем с разными форматами MAC
        c.execute('''SELECT mac, ssid, latitude, longitude, avg_rssi, sample_count 
                     FROM wifi_ap WHERE LOWER(REPLACE(REPLACE(mac,':',''),'-',''))=?''', (mac,))
        row = c.fetchone()
        
        if row:
            _, _, lat, lon, db_rssi, samples = row
            weight = max(0.1, (100 - abs(rssi - db_rssi)) ** RSSI_WEIGHT_POWER) * math.log(samples + 1)
            matches.append({'lat': lat, 'lon': lon, 'weight': weight})
    
    conn.close()
    
    logger.info(f"Scan: {len(networks)} APs, {len(matches)} matched")
    
    if len(matches) < MIN_MATCHES:
        return {'found': False, 'reason': f'Мало совпадений ({len(matches)}/{MIN_MATCHES})'}
    
    total_weight = sum(m['weight'] for m in matches)
    if total_weight == 0:
        return {'found': False, 'reason': 'Zero weight'}
    
    lat = sum(m['lat']*m['weight'] for m in matches) / total_weight
    lon = sum(m['lon']*m['weight'] for m in matches) / total_weight
    accuracy = min(MAX_ACCURACY, max(10, 300 // len(matches)))
    
    return {
        'found': True,
        'latitude': round(lat, 6),
        'longitude': round(lon, 6),
        'accuracy': accuracy,
        'matching_aps': len(matches)
    }

# ================= API ROUTES =================
@app.route('/scan', methods=['POST'])
def receive_scan():
    """Получает скан WiFi сетей от ESP32"""
    try:
        data = request.get_json(force=True, silent=True) or {}
        networks = data.get('networks', [])
        device_id = data.get('device_id', 'unknown')
        
        logger.info(f"Scan from {device_id}: {len(networks)} networks")
        loc = find_location(networks)
        
        if loc.get('found'):
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''INSERT INTO device_locations 
                         (timestamp, device_id, latitude, longitude, accuracy, matching_aps)
                         VALUES (?,?,?,?,?,?)''',
                      (datetime.now().isoformat(), device_id, 
                       loc['latitude'], loc['longitude'], 
                       loc['accuracy'], loc['matching_aps']))
            conn.commit()
            conn.close()
            logger.info(f"✅ Saved location for {device_id} at {loc['latitude']}, {loc['longitude']}")
        else:
            logger.warning(f"❌ Location NOT saved for {device_id}: {loc.get('reason')}")
        
        return jsonify({
            'status': 'ok',
            'device_id': device_id,
            'location': loc,
            'ts': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error in /scan: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/devices', methods=['GET'])
def get_devices():
    """Возвращает список всех устройств"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''SELECT device_id, latitude, longitude, MAX(timestamp) as last_ts 
                     FROM device_locations GROUP BY device_id''')
        devs = [{'id': r[0], 'lat': r[1], 'lon': r[2], 'last_ts': r[3]} for r in c.fetchall()]
        conn.close()
        return jsonify(devs)
    except sqlite3.OperationalError as e:
        logger.warning(f"Table missing in /api/devices: {e}")
        return jsonify([])
    except Exception as e:
        logger.error(f"Error in /api/devices: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/history/<device_id>', methods=['GET'])
def get_history(device_id):
    """Возвращает историю перемещений устройства"""
    try:
        hours = request.args.get('hours', 24, type=int)
        cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''SELECT timestamp, latitude, longitude, accuracy FROM device_locations 
                     WHERE device_id=? AND timestamp>? ORDER BY timestamp ASC''', 
                  (device_id, cutoff))
        history = [{'ts': r[0], 'lat': r[1], 'lon': r[2], 'acc': r[3]} for r in c.fetchall()]
        conn.close()
        return jsonify(history)
    except sqlite3.OperationalError as e:
        logger.warning(f"Table missing in /api/history: {e}")
        return jsonify([])
    except Exception as e:
        logger.error(f"Error in /api/history: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/commands/<device_id>', methods=['GET'])
def get_commands(device_id):
    """ESP32 запрашивает команды с сервера"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''SELECT id, command, params FROM device_commands 
                     WHERE device_id=? AND executed=0 ORDER BY created_at ASC''', (device_id,))
        commands = [{'id': r[0], 'command': r[1], 'params': json.loads(r[2]) if r[2] else {}} 
                    for r in c.fetchall()]
        conn.close()
        
        return jsonify({
            'status': 'ok',
            'commands': commands,
            'interval': 60
        })
    except Exception as e:
        logger.error(f"Error in /api/commands GET: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/commands/<device_id>', methods=['POST'])
def acknowledge_command(device_id):
    """ESP32 подтверждает выполнение команды"""
    try:
        data = request.get_json(force=True, silent=True) or {}
        command_id = data.get('command_id')
        
        if command_id:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''UPDATE device_commands SET executed=1 
                         WHERE id=? AND device_id=?''', (command_id, device_id))
            conn.commit()
            conn.close()
            logger.info(f"Command {command_id} acknowledged by {device_id}")
        
        return jsonify({'status': 'ok'})
    except Exception as e:
        logger.error(f"Error acknowledging command: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/trigger/<device_id>', methods=['POST', 'GET'])
def trigger_force_scan(device_id):
    """Принудительно запускает force_scan для устройства"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        c.execute('''INSERT INTO device_commands 
                     (device_id, command, params, created_at, executed)
                     VALUES (?, ?, ?, ?, 0)''',
                  (device_id, 'force_scan', '{}', datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        logger.info(f"🎯 Force scan triggered for {device_id}")
        return jsonify({
            'status': 'ok',
            'message': f'Command queued for {device_id}',
            'note': 'ESP32 получит команду в течение 30 секунд'
        })
    except Exception as e:
        logger.error(f"Error triggering scan: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/config/<device_id>', methods=['GET'])
def get_config(device_id):
    """ESP32 запрашивает конфигурацию"""
    return jsonify({
        'status': 'ok',
        'config': {
            'scan_interval': 60,
            'sleep_interval': 300,
            'similarity_threshold': 40,
            'min_aps': MIN_MATCHES
        }
    })

@app.route('/api/debug/<device_id>', methods=['GET'])
def debug_device(device_id):
    """Отладочная информация по устройству"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        c.execute('''SELECT timestamp, latitude, longitude, accuracy 
                     FROM device_locations WHERE device_id=? ORDER BY timestamp DESC LIMIT 10''', 
                  (device_id,))
        locations = [{'ts': r[0], 'lat': r[1], 'lon': r[2], 'acc': r[3]} for r in c.fetchall()]
        
        c.execute('SELECT COUNT(*) FROM wifi_ap')
        total_aps = c.fetchone()[0]
        
        c.execute('SELECT COUNT(*) FROM device_locations WHERE device_id=?', (device_id,))
        total_records = c.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'device_id': device_id,
            'last_locations': locations,
            'total_aps_in_db': total_aps,
            'total_records': total_records,
            'note': 'Если locations пустой — MAC-адреса из ESP32 не найдены в базе'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    """Проверка здоровья сервера"""
    return jsonify({
        'status': 'healthy',
        'db_exists': os.path.exists(DB_PATH),
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/stats', methods=['GET'])
def api_stats():
    """Статистика системы"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        total_aps = c.execute('SELECT COUNT(*) FROM wifi_ap').fetchone()[0]
        total_devices = c.execute('SELECT COUNT(DISTINCT device_id) FROM device_locations').fetchone()[0]
        total_records = c.execute('SELECT COUNT(*) FROM device_locations').fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'wifi_access_points': total_aps,
            'active_devices': total_devices,
            'location_records': total_records
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

      
HTML = """
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>Жираф GO | Fleet Control</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<style>
:root {
  --bg-dark: #0b0f19; --bg-panel: rgba(15, 20, 35, 0.92);
  --accent: #3b82f6; --success: #39ff14; --danger: #ef4444;
  --text: #f8fafc; --text-muted: #94a3b8; --border: rgba(255,255,255,0.08);
  --glass: rgba(20, 28, 45, 0.7); --radius: 14px;
  --track-color: #ff8c00; --marker-color: #39ff14;
}
[data-theme="light"] {
  --bg-dark: #f1f5f9; --bg-panel: rgba(255, 255, 255, 0.92);
  --text: #0f172a; --text-muted: #64748b; --border: rgba(0,0,0,0.08);
  --glass: rgba(255, 255, 255, 0.7);
}
* { box-sizing: border-box; }
body { margin: 0; font-family: 'Inter', system-ui, sans-serif; background: var(--bg-dark); color: var(--text); overflow: hidden; transition: background 0.3s; }
#map { height: 100vh; width: 100vw; z-index: 1; }

/* Sidebar */
.sidebar {
  position: fixed; top: 16px; left: 16px; bottom: 16px; width: 300px;
  background: var(--bg-panel); backdrop-filter: blur(12px);
  border: 1px solid var(--border); border-radius: var(--radius);
  z-index: 1000; display: flex; flex-direction: column;
  box-shadow: 0 8px 32px rgba(0,0,0,0.5); transition: transform 0.3s;
}
.sidebar-header { padding: 16px; border-bottom: 1px solid var(--border); display: flex; justify-content: space-between; align-items: center; }
.sidebar-title { font-size: 1.1rem; font-weight: 700; margin: 0; display: flex; align-items: center; gap: 8px; }
.search-box {
  width: 100%; padding: 10px; margin-top: 12px; background: var(--glass);
  border: 1px solid var(--border); border-radius: 8px; color: var(--text);
  font-size: 0.9rem; outline: none; transition: border-color 0.2s;
}
.search-box:focus { border-color: var(--accent); }
.device-list { flex: 1; overflow-y: auto; padding: 12px; display: flex; flex-direction: column; gap: 8px; }
.device-card {
  background: var(--glass); border: 1px solid var(--border); border-radius: 10px;
  padding: 12px; cursor: pointer; transition: all 0.2s; position: relative;
}
.device-card:hover { transform: translateX(4px); border-color: var(--accent); }
.device-card.active { border-color: var(--accent); background: rgba(59, 130, 246, 0.15); }
.device-card.offline { opacity: 0.6; border-left: 3px solid var(--danger); }
.device-card.online { border-left: 3px solid var(--marker-color); }
.device-id { font-weight: 600; font-size: 0.95rem; margin-bottom: 4px; }
.device-meta { font-size: 0.7rem; color: var(--text-muted); display: flex; justify-content: space-between; }

/* Map Controls */
.map-controls {
  position: absolute; top: 16px; right: 16px; z-index: 1001;
  display: flex; gap: 8px;
}
.ctrl-btn {
  background: var(--bg-panel); backdrop-filter: blur(8px);
  border: 1px solid var(--border); color: var(--text);
  width: 40px; height: 40px; border-radius: 10px; cursor: pointer;
  display: flex; align-items: center; justify-content: center; font-size: 1.1rem;
  box-shadow: 0 4px 12px rgba(0,0,0,0.3); transition: all 0.2s;
}
.ctrl-btn:hover { transform: scale(1.05); border-color: var(--accent); }

/* Modal/Panel */
.side-panel {
  position: fixed; top: 16px; right: -340px; width: 320px;
  background: var(--bg-panel); backdrop-filter: blur(14px);
  border: 1px solid var(--border); border-radius: var(--radius);
  z-index: 1001; padding: 18px; box-shadow: -8px 0 32px rgba(0,0,0,0.6);
  transition: right 0.3s cubic-bezier(0.4, 0, 0.2, 1);
}
.side-panel.open { right: 16px; }
.panel-close { 
  position: absolute; top: 12px; right: 12px; 
  background: transparent; border: none; color: var(--text-muted); 
  font-size: 1.2rem; cursor: pointer;
}
.panel-header { margin-bottom: 14px; }
.panel-title { font-size: 1.3rem; font-weight: 800; letter-spacing: -0.5px; }
.panel-meta { font-size: 0.75rem; color: var(--text-muted); margin-top: 4px; }

/* Commands */
.cmd-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin: 14px 0; }
.cmd-btn {
  background: var(--glass); border: 1px solid var(--border); border-radius: 10px;
  padding: 10px; color: var(--text); cursor: pointer; font-weight: 500; font-size: 0.8rem;
  text-align: center; transition: all 0.2s;
}
.cmd-btn:hover { border-color: var(--accent); background: rgba(255,255,255,0.05); }
.cmd-btn.scan { background: var(--accent); border-color: var(--accent); color: white; }
.cmd-btn.danger { border-color: rgba(239, 68, 68, 0.3); color: var(--danger); }
.cmd-btn.danger:hover { background: var(--danger); color: white; }

/* History */
.history-panel { margin-top: 18px; padding-top: 14px; border-top: 1px solid var(--border); }
.history-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }
.timeline-slider { width: 100%; margin: 10px 0; accent-color: var(--track-color); height: 4px; }
.time-display { 
  text-align: center; font-family: monospace; font-size: 1.1rem; 
  background: var(--glass); padding: 6px; border-radius: 6px; border: 1px solid var(--border);
}
.playback-controls { display: flex; gap: 6px; justify-content: center; margin-top: 10px; }
.play-btn { 
  background: var(--glass); border: 1px solid var(--border); color: var(--text); 
  padding: 6px 12px; border-radius: 6px; cursor: pointer; font-size: 0.8rem;
}
.play-btn.active { background: var(--success); border-color: var(--success); color: #000; }

/* Map Elements */
.leaflet-control-zoom { display: none; }
.track-line { stroke: var(--track-color) !important; stroke-width: 3 !important; stroke-linecap: round; stroke-linejoin: round; }
.current-marker { 
  background: var(--marker-color); 
  border: 3px solid white; 
  border-radius: 50%; 
  box-shadow: 0 0 15px rgba(57, 255, 20, 0.8); 
  animation: pulse 2s infinite;
}
@keyframes pulse { 0% { box-shadow: 0 0 15px rgba(57, 255, 20, 0.5); } 50% { box-shadow: 0 0 25px rgba(57, 255, 20, 1); } 100% { box-shadow: 0 0 15px rgba(57, 255, 20, 0.5); } }

@media (max-width: 768px) {
  .sidebar { width: 100%; left: 0; bottom: 0; top: auto; height: 40vh; border-radius: 20px 20px 0 0; }
  .side-panel { width: 100%; right: -100%; top: auto; bottom: 0; border-radius: 20px 20px 0 0; }
  .side-panel.open { right: 0; bottom: 0; }
  .map-controls { top: 10px; right: 10px; }
}
</style>
</head>
<body>
<div id="map"></div>

<div class="map-controls">
  <button class="ctrl-btn" onclick="toggleTheme()" title="Сменить тему"></button>
  <button class="ctrl-btn" onclick="centerMap()" title="В центр Туймазов">📍</button>
</div>

<div class="sidebar">
  <div class="sidebar-header">
    <div class="sidebar-title"> Жираф GO</div>
  </div>
  <div style="padding: 0 12px;">
    <input type="text" class="search-box" id="search-input" placeholder="🔍 Поиск по ID (напр. 187)...">
  </div>
  <div class="device-list" id="device-list">
    <div style="text-align:center; color: var(--text-muted); padding: 20px;">Загрузка парка...</div>
  </div>
</div>

<div class="side-panel" id="side-panel">
  <button class="panel-close" onclick="closePanel()">×</button>
  <div class="panel-header">
    <div class="panel-title"><span id="panel-id">--</span></div>
    <div class="panel-meta" id="panel-meta">--</div>
  </div>

  <div class="cmd-grid">
    <button class="cmd-btn scan" onclick="triggerScan()">📡 Scan</button>
    <button class="cmd-btn" onclick="refreshHistory()"> Трек</button>
    <button class="cmd-btn danger" onclick="rebootDevice()"> Reboot</button>
    <button class="cmd-btn" id="follow-btn" onclick="toggleFollow()" style="grid-column: span 2;">👁 Следить</button>
  </div>

  <div class="history-panel">
    <div class="history-header">
      <span style="font-weight:600; font-size:0.9rem;"> История</span>
      <button class="play-btn" id="play-btn" onclick="togglePlayback()">▶ Play</button>
    </div>
    <div class="time-display" id="time-display">--:--:--</div>
    <input type="range" class="timeline-slider" id="track-slider" min="0" max="100" value="100">
    <div class="playback-controls">
      <button class="play-btn" onclick="setRange(1)">1ч</button>
      <button class="play-btn" onclick="setRange(6)">6ч</button>
      <button class="play-btn" onclick="setRange(24)">24ч</button>
    </div>
  </div>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
// ================= MAP INIT =================
const map = L.map('map', { zoomControl: false }).setView([54.60, 53.68], 13);
let tileLayer = null;
let currentTileUrl = '';

// ================= THEME SYSTEM =================
const DARK_TILE = 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png';
const LIGHT_TILE = 'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png';

function applyTheme(isLight) {
  document.documentElement.setAttribute('data-theme', isLight ? 'light' : 'dark');
  localStorage.setItem('theme', isLight ? 'light' : 'dark');
  const newUrl = isLight ? LIGHT_TILE : DARK_TILE;
  if (currentTileUrl !== newUrl) {
    if (tileLayer) map.removeLayer(tileLayer);
    tileLayer = L.tileLayer(newUrl, { attribution: '© CartoDB' }).addTo(map);
    currentTileUrl = newUrl;
  }
}
function toggleTheme() { applyTheme(document.documentElement.getAttribute('data-theme') !== 'light'); }
function centerMap() { map.setView([54.60, 53.68], 13); }
applyTheme(localStorage.getItem('theme') === 'light');

// ================= STATE =================
let devices = [], markers = {}, trackPolyline = null, trackMarker = null;
let selectedDevice = null, historyData = [], playbackInterval = null;
let isPlaying = false, currentSliderIndex = 0;
let followMode = false;
let pollTimer = null;

// ================= UTILS =================
const $ = id => document.getElementById(id);
const fmtTime = iso => new Date(iso).toLocaleTimeString('ru-RU', { hour:'2-digit', minute:'2-digit', second:'2-digit' });
const fmtDate = iso => new Date(iso).toLocaleDateString('ru-RU');

// ================= AUTO-POLLING =================
function startPolling() {
  if (pollTimer) clearInterval(pollTimer);
  const interval = selectedDevice ? 3000 : 8000;
  pollTimer = setInterval(loadDevices, interval);
}

// ================= API =================
function loadDevices() {
  fetch('/api/devices').then(r => r.json()).then(devs => {
    devices = devs;
    $('device-list').innerHTML = '';
    if(devs.length === 0) { $('device-list').innerHTML = '<div style="text-align:center; color:var(--text-muted);">Нет устройств</div>'; return; }
    
    devs.forEach(d => {
      const lastSeen = new Date(d.last_ts).getTime();
      const diffMin = (Date.now() - lastSeen) / 60000;
      const statusClass = diffMin < 30 ? 'online' : 'offline';
      
      const card = document.createElement('div');
      card.className = `device-card ${statusClass} ${selectedDevice === d.id ? 'active' : ''}`;
      card.dataset.id = d.id.toLowerCase();
      card.innerHTML = `
        <div class="device-id">${d.id}</div>
        <div class="device-meta"><span>${fmtDate(d.last_ts)}</span><span>${fmtTime(d.last_ts)}</span></div>`;
      card.onclick = () => openPanel(d.id, d.lat, d.lon, d.last_ts);
      $('device-list').appendChild(card);
      
      // 🔄 Плавное обновление маркера
      if(!markers[d.id]) {
        markers[d.id] = L.circleMarker([d.lat, d.lon], { radius: 6, fillColor: '#39ff14', color: '#fff', weight: 2, fillOpacity: 0.9 }).addTo(map);
        markers[d.id].bindPopup(`<b>${d.id}</b><br>${fmtTime(d.last_ts)}`);
      } else {
        markers[d.id].setLatLng([d.lat, d.lon]);
      }
      
      // 👁 Режим слежения: карта плавно следует за выбранным самокатом
      if(followMode && selectedDevice === d.id) {
        map.panTo([d.lat, d.lon], { animate: true, duration: 0.5 });
      }
    });
  });
}

// Search Filter
$('search-input').addEventListener('input', e => {
  const val = e.target.value.toLowerCase();
  document.querySelectorAll('.device-card').forEach(c => {
    c.style.display = c.dataset.id.includes(val) ? 'block' : 'none';
  });
});

// ================= PANEL & COMMANDS =================
function openPanel(id, lat, lon, ts) {
  selectedDevice = id;
  $('side-panel').classList.add('open');
  $('panel-id').textContent = id;
  $('panel-meta').textContent = `${fmtDate(ts)} • ${fmtTime(ts)}`;
  $('track-slider').value = 100;
  loadHistory(24);
  map.setView([lat, lon], 16);
  startPolling();
}
function closePanel() { $('side-panel').classList.remove('open'); stopPlayback(); selectedDevice = null; startPolling(); }

function triggerScan() {
  if(!selectedDevice) return;
  fetch(`/api/trigger/${selectedDevice}`, {method:'POST'}).then(r=>r.json()).then(d=>alert(d.message||'OK'));
}
function rebootDevice() { 
  if(!selectedDevice) return;
  fetch(`/api/trigger/${selectedDevice}/reboot`, {method:'POST'}).catch(()=>alert('Команда отправлена'));
}
function toggleFollow() {
  followMode = !followMode;
  $('follow-btn').textContent = followMode ? '👁 Выкл' : '👁 Следить';
  $('follow-btn').style.background = followMode ? 'var(--accent)' : '';
  if(followMode && selectedDevice && markers[selectedDevice]) {
    map.panTo(markers[selectedDevice].getLatLng(), { animate: true });
  }
}

// ================= HISTORY & TRACK =================
function loadHistory(hours=24) {
  if(!selectedDevice) return;
  if(trackPolyline) map.removeLayer(trackPolyline);
  if(trackMarker) map.removeLayer(trackMarker);
  trackPolyline = null; trackMarker = null; historyData = [];
  
  fetch(`/api/history/${selectedDevice}?hours=${hours}`).then(r=>r.json()).then(hist=>{
    historyData = hist;
    if(hist.length < 2) { $('time-display').textContent = 'Нет данных'; return; }
    
    const coords = hist.map(p => [p.lat, p.lon]);
    trackPolyline = L.polyline(coords, { color: '#ff8c00', weight: 3, opacity: 0.8, className: 'track-line' }).addTo(map);
    
    currentSliderIndex = hist.length - 1;
    $('track-slider').max = hist.length - 1;
    $('track-slider').value = currentSliderIndex;
    updateTrackMarker(currentSliderIndex);
  });
}
function refreshHistory() { loadHistory(parseInt($('track-slider').dataset.hours || 24)); }

function updateTrackMarker(idx) {
  if(!historyData[idx]) return;
  currentSliderIndex = idx;
  $('time-display').textContent = fmtTime(historyData[idx].ts);
  const p = historyData[idx];
  
  if(trackMarker) trackMarker.setLatLng([p.lat, p.lon]);
  else {
    trackMarker = L.marker([p.lat, p.lon], { draggable: true, icon: L.divIcon({ className: 'current-marker', iconSize: [18, 18] }) }).addTo(map);
    trackMarker.on('drag', function(e) {
      const nearest = findNearestIndex(e.target.getLatLng());
      $('track-slider').value = nearest;
      updateTrackMarker(nearest);
    });
  }
}

function findNearestIndex(targetLatLng) {
  let minDist = Infinity, idx = 0;
  historyData.forEach((p, i) => {
    const dist = L.latLng([p.lat, p.lon]).distanceTo(targetLatLng);
    if(dist < minDist) { minDist = dist; idx = i; }
  });
  return idx;
}

// ================= PLAYBACK =================
function togglePlayback() {
  if(isPlaying) { stopPlayback(); $('play-btn').textContent = '▶ Play'; $('play-btn').classList.remove('active'); }
  else {
    if(historyData.length < 2) return;
    isPlaying = true;
    $('play-btn').textContent = '⏸ Pause'; $('play-btn').classList.add('active');
    if(currentSliderIndex >= historyData.length - 1) { currentSliderIndex = 0; $('track-slider').value = 0; updateTrackMarker(0); }
    
    playbackInterval = setInterval(() => {
      currentSliderIndex++;
      if(currentSliderIndex >= historyData.length) { stopPlayback(); return; }
      $('track-slider').value = currentSliderIndex;
      updateTrackMarker(currentSliderIndex);
    }, 400);
  }
}
function stopPlayback() { clearInterval(playbackInterval); isPlaying = false; }
function setRange(h) { loadHistory(h); $('track-slider').dataset.hours = h; }

$('track-slider').addEventListener('input', e => { updateTrackMarker(parseInt(e.target.value)); if(isPlaying) togglePlayback(); });

// ================= INIT =================
startPolling();
</script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML)

if __name__ == '__main__':
    # 1. Первичная инициализация (на случай если БД нет вообще)
    init_db()
    
    # 2. Запуск KML процессора (он может пересоздать БД!)
    run_kml_processor()
    
    # 3. 🔥 КРИТИЧЕСКИ ВАЖНО: повторно создаём таблицы после kml_processor!
    #    Потому что kml_processor.py мог удалить и пересоздать базу
    init_db()
    
    # 4. Импорт CSV если база пустая
    import_csv_if_empty()
    
    # 5. Финальная проверка
    if not verify_db():
        logger.warning("⚠️ База не прошла проверку, но сервер запустится")
    
    # 6. Запуск сервера
    logger.info(f"🚀 Fleet Tracker ready on {HOST}:{PORT}")
    logger.info(f"📁 Database: {DB_PATH}")
    app.run(host=HOST, port=PORT, debug=False, threaded=True)