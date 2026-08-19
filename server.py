# server.py — Упрощённая версия для Scooter Tracker
from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
import sqlite3, os, json, math, logging, csv, subprocess, sys, glob
from datetime import datetime, timedelta, timezone

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
    """Запускает kml_processor.py для добавления новых данных"""
    if not os.path.exists(KML_PROCESSOR_PATH):
        logger.warning(f"⚠️ kml_processor.py не найден: {KML_PROCESSOR_PATH}")
        return False
    
    # 🔍 Проверяем, есть ли новые KML файлы для обработки
    kml_files = glob.glob(os.path.join(BASE_DIR, 'kml', '*.kml'))
    if not kml_files:
        logger.info("📁 Новых KML файлов не найдено — пропускаю обработку")
        return False
    
    logger.info(f"📦 Найдено {len(kml_files)} KML файлов, запускаю обработку...")
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
    """Импортирует CSV только если база полностью пустая"""
    csv_path = os.path.join(BASE_DIR, 'processed_csv', 'wifi_base_clean.csv')
    if not os.path.exists(csv_path):
        logger.info(f"📄 CSV не найден: {csv_path} — пропускаю")
        return
    
    if not os.path.exists(DB_PATH):
        logger.info("🗄️ База данных не существует — импорт CSV отложен")
        return
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    try:
        # Проверяем, есть ли записи в wifi_ap
        count = c.execute('SELECT COUNT(*) FROM wifi_ap').fetchone()[0]
        
        if count == 0:
            logger.info("📥 База пустая — импортирую из CSV...")
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
                            logger.warning(f"⚠️ Пропуск строки CSV: {e}")
                            continue
                    conn.commit()
                    logger.info(f"✅ Импортировано {imported} WiFi точек из CSV")
            except Exception as e:
                logger.error(f"❌ Ошибка при чтении CSV: {e}")
        else:
            logger.info(f"💾 В базе уже {count} записей — импорт CSV пропущен")
    except sqlite3.OperationalError:
        logger.info("🗄️ Таблица wifi_ap ещё не создана — импорт CSV отложен")
    finally:
        conn.close()

# ================= DATABASE INIT =================
def init_db():
    """Создаёт таблицы если их нет — НЕ УДАЛЯЕТ данные"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        logger.info("🔧 Проверяю таблицы БД...")
        
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
        logger.info("✅ Таблицы готовы")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")
        return False

def show_db_stats():
    """Показывает статистику по базе данных"""
    if not os.path.exists(DB_PATH):
        logger.info("🗄️ База данных ещё не создана")
        return
    
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        aps = c.execute('SELECT COUNT(*) FROM wifi_ap').fetchone()[0]
        devices = c.execute('SELECT COUNT(DISTINCT device_id) FROM device_locations').fetchone()[0]
        records = c.execute('SELECT COUNT(*) FROM device_locations').fetchone()[0]
        
        conn.close()
        
        logger.info("📊 Статистика базы данных:")
        logger.info(f"   • WiFi точек: {aps:,}")
        logger.info(f"   • Устройств: {devices}")
        logger.info(f"   • Записей треков: {records:,}")
        
    except Exception as e:
        logger.warning(f"⚠️ Не удалось получить статистику БД: {e}")

# ================= LOCATION FINDING =================
def find_location(networks):
    """Ищет местоположение по WiFi сетям"""
    if not os.path.exists(DB_PATH):
        return {'found': False, 'error': 'Database not found'}
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    matches = []
    
    for net in networks:
        mac = net.get('mac', '').lower().replace(':', '').replace('-', '')
        rssi = net.get('rssi', -100)
        
        c.execute('''SELECT mac, ssid, latitude, longitude, avg_rssi, sample_count 
                     FROM wifi_ap WHERE LOWER(REPLACE(REPLACE(mac,':',''),'-',''))=?''', (mac,))
        row = c.fetchone()
        
        if row:
            _, _, lat, lon, db_rssi, samples = row
            weight = max(0.1, (100 - abs(rssi - db_rssi)) ** RSSI_WEIGHT_POWER) * math.log(samples + 1)
            matches.append({'lat': lat, 'lon': lon, 'weight': weight})
    
    conn.close()
    
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
            timestamp = datetime.now(timezone.utc).isoformat()
            c.execute('''INSERT INTO device_locations 
                         (timestamp, device_id, latitude, longitude, accuracy, matching_aps)
                         VALUES (?,?,?,?,?,?)''',
                      (timestamp, device_id, 
                       loc['latitude'], loc['longitude'], 
                       loc['accuracy'], loc['matching_aps']))
            conn.commit()
            conn.close()
            logger.info(f"✅ Saved location for {device_id}")
        
        return jsonify({
            'status': 'ok',
            'device_id': device_id,
            'location': loc,
            'ts': datetime.now(timezone.utc).isoformat()
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
    except sqlite3.OperationalError:
        return jsonify([])
    except Exception as e:
        logger.error(f"Error in /api/devices: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/history/<device_id>', methods=['GET'])
def get_history(device_id):
    """Возвращает историю перемещений устройства"""
    try:
        hours = request.args.get('hours', 24, type=int)
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''SELECT timestamp, latitude, longitude, accuracy FROM device_locations 
                     WHERE device_id=? AND timestamp>? ORDER BY timestamp ASC''', 
                  (device_id, cutoff))
        history = [{'ts': r[0], 'lat': r[1], 'lon': r[2], 'acc': r[3]} for r in c.fetchall()]
        conn.close()
        return jsonify(history)
    except sqlite3.OperationalError:
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
        return jsonify({'status': 'ok', 'commands': commands, 'interval': 60})
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
                  (device_id, 'force_scan', '{}', datetime.now(timezone.utc).isoformat()))
        conn.commit()
        conn.close()
        logger.info(f"🎯 Force scan triggered for {device_id}")
        return jsonify({'status': 'ok', 'message': f'Command queued for {device_id}'})
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

@app.route('/health', methods=['GET'])
def health():
    """Проверка здоровья сервера"""
    return jsonify({
        'status': 'healthy',
        'db_exists': os.path.exists(DB_PATH),
        'timestamp': datetime.now(timezone.utc).isoformat()
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
<html lang="ru" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
<meta name="theme-color" content="#0a0f1c">
<title>Жираф GO | Fleet Control — Туймазы</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=Unbounded:wght@500;700;900&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css"/>
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css"/>
<style>
/* ============================================================
   Жираф GO — телематика проката самокатов (г. Туймазы)
   Тёмная/светлая тема через CSS-переменные, без мерцания
   ============================================================ */
:root{
  --bg:#0a0f1c;
  --panel:rgba(13,20,36,.9);
  --panel-2:rgba(10,16,30,.97);
  --glass:rgba(23,33,58,.52);
  --glass-2:rgba(31,43,73,.42);
  --border:rgba(151,170,209,.14);
  --border-2:rgba(151,170,209,.32);
  --text:#e9efff;
  --muted:#8aa0c6;
  --faint:#5f7398;
  --amber:#ffb020;
  --amber-2:#ff8a00;
  --green:#39ff14;
  --red:#ef4444;
  --idle:#f59e0b;
  --shadow:0 24px 60px rgba(2,6,18,.55);
  --shadow-s:0 8px 24px rgba(2,6,18,.4);
  --r:18px;
  --nav-h:0px;
}
[data-theme="light"]{
  --bg:#e8edf6;
  --panel:rgba(255,255,255,.93);
  --panel-2:rgba(255,255,255,.985);
  --glass:rgba(255,255,255,.66);
  --glass-2:rgba(238,243,252,.75);
  --border:rgba(22,38,74,.12);
  --border-2:rgba(22,38,74,.27);
  --text:#0d1830;
  --muted:#57688c;
  --faint:#8b9ab8;
  --shadow:0 24px 50px rgba(30,45,80,.18);
  --shadow-s:0 8px 20px rgba(30,45,80,.12);
}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html,body{height:100%}
body{
  margin:0;overflow:hidden;background:var(--bg);color:var(--text);
  font-family:'Manrope',system-ui,sans-serif;touch-action:manipulation;
  transition:background .35s ease,color .35s ease;
}
button{font-family:inherit}
.disp{font-family:'Unbounded','Manrope',sans-serif}
#map{position:fixed;inset:0;z-index:1;background:var(--bg)}

/* ---------- Левая панель (десктоп) / bottom-sheet (мобильный) ---------- */
.sidebar{
  position:fixed;z-index:1000;top:14px;left:14px;bottom:14px;width:326px;
  display:flex;flex-direction:column;overflow:hidden;
  background:var(--panel);backdrop-filter:blur(14px);-webkit-backdrop-filter:blur(14px);
  border:1px solid var(--border);border-radius:var(--r);box-shadow:var(--shadow);
}
.sheet-handle{display:none}
.brand{display:flex;align-items:center;gap:11px;padding:15px 16px 13px;position:relative}
.brand::after{ /* узор «пятна жирафа» */
  content:'';position:absolute;inset:0;pointer-events:none;opacity:.35;
  background-image:radial-gradient(rgba(255,176,32,.35) 1.6px,transparent 1.7px),
                   radial-gradient(rgba(255,138,0,.28) 1.3px,transparent 1.4px);
  background-size:28px 28px,38px 38px;background-position:4px 6px,19px 21px;
}
.logo{
  width:42px;height:42px;flex:none;border-radius:13px;display:grid;place-items:center;color:#1b1200;
  background:linear-gradient(145deg,var(--amber),var(--amber-2));
  box-shadow:0 6px 18px rgba(255,138,0,.35);
}
.logo svg{width:24px;height:24px}
.brand-name{font-family:'Unbounded';font-weight:900;font-size:15px;letter-spacing:.04em;line-height:1}
.brand-name em{font-style:normal;color:var(--amber)}
.brand-sub{font-size:8.5px;letter-spacing:.26em;text-transform:uppercase;color:var(--faint);margin-top:5px;font-weight:700}
.brand-right{margin-left:auto;text-align:right;position:relative;z-index:1}
.clock{font-family:'Unbounded';font-size:12px;font-weight:700;letter-spacing:.05em}
.live{display:inline-flex;align-items:center;gap:5px;font-size:8.5px;font-weight:800;letter-spacing:.2em;color:var(--green);margin-top:4px;text-transform:uppercase}
.live i{width:7px;height:7px;border-radius:50%;background:var(--green);box-shadow:0 0 8px var(--green);animation:blink 1.6s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.25}}

.stats{display:grid;grid-template-columns:1fr 1fr;gap:8px;padding:0 14px 12px}
.stat{background:var(--glass);border:1px solid var(--border);border-radius:13px;padding:9px 11px;transition:border-color .2s}
.stat:hover{border-color:var(--border-2)}
.stat b{display:block;font-family:'Unbounded';font-size:16px;font-weight:700;line-height:1.15}
.stat span{font-size:8.5px;letter-spacing:.14em;text-transform:uppercase;color:var(--faint);font-weight:800}
.stat.on b{color:var(--green)}
.stat.bump b{animation:bump .4s}
@keyframes bump{40%{transform:scale(1.16)}}

.search{position:relative;margin:0 14px 10px}
.search>svg{position:absolute;left:11px;top:50%;transform:translateY(-50%);width:15px;height:15px;color:var(--faint);pointer-events:none}
.search input{
  width:100%;height:42px;padding:0 36px;background:var(--glass);
  border:1px solid var(--border);border-radius:12px;color:var(--text);
  font:600 13px 'Manrope';outline:none;transition:border-color .2s,box-shadow .2s;
}
.search input::placeholder{color:var(--faint)}
.search input:focus{border-color:var(--amber);box-shadow:0 0 0 3px rgba(255,176,32,.16)}
.search .clr{position:absolute;right:5px;top:50%;transform:translateY(-50%);width:32px;height:32px;border:0;background:none;color:var(--faint);cursor:pointer;border-radius:9px;display:none;font-size:17px;line-height:1}
.search .clr:hover{color:var(--text);background:var(--glass)}
.search.has .clr{display:block}

.chips{display:flex;gap:6px;padding:0 14px 10px}
.chip{
  flex:1;height:32px;display:flex;align-items:center;justify-content:center;gap:5px;
  border:1px solid var(--border);background:var(--glass);color:var(--muted);border-radius:999px;
  font:700 11px 'Manrope';cursor:pointer;transition:all .18s;
}
.chip b{font-family:'Unbounded';font-size:9.5px;font-weight:700}
.chip:hover{border-color:var(--border-2);color:var(--text)}
.chip.active{background:var(--amber);border-color:var(--amber);color:#1b1200}

.device-list{flex:1;overflow-y:auto;padding:2px 14px 14px;display:flex;flex-direction:column;gap:8px;scrollbar-width:thin}
.device-list::-webkit-scrollbar{width:8px}
.device-list::-webkit-scrollbar-thumb{background:var(--border-2);border-radius:8px;border:2px solid transparent;background-clip:padding-box}
.dcard{
  position:relative;background:var(--glass);border:1px solid var(--border);border-radius:13px;
  padding:11px 12px 10px 16px;cursor:pointer;
  transition:transform .18s ease,border-color .18s,background .18s;
}
.dcard.boot{animation:cardIn .35s both}
@keyframes cardIn{from{opacity:0;transform:translateY(7px)}}
.dcard::before{content:'';position:absolute;left:6px;top:12px;bottom:12px;width:3.5px;border-radius:4px;background:var(--faint)}
.dcard.online::before{background:var(--green);box-shadow:0 0 8px rgba(57,255,20,.55)}
.dcard.idle::before{background:var(--idle)}
.dcard.offline::before{background:var(--red)}
.dcard-top{display:flex;align-items:center;justify-content:space-between;gap:8px}
.dcard-id{font-weight:800;font-size:14px;letter-spacing:.01em}
.dcard-id mark{background:none;color:var(--amber);border-bottom:2px solid var(--amber);padding:0}
.dcard-meta{display:flex;justify-content:space-between;gap:8px;margin-top:5px;font-size:10.5px;color:var(--muted);font-weight:600;font-variant-numeric:tabular-nums}
@media (hover:hover){
  .dcard:hover{transform:translateX(4px);border-color:var(--border-2)}
}
.dcard.active{border-color:var(--amber);background:rgba(255,176,32,.1)}

.pill{display:inline-flex;align-items:center;gap:5px;font-size:9.5px;font-weight:800;letter-spacing:.06em;text-transform:uppercase;padding:3.5px 8px;border-radius:999px;flex:none}
.pill i{width:6px;height:6px;border-radius:50%;background:currentColor}
.pill.online{color:var(--green);background:rgba(57,255,20,.1)}
.pill.idle{color:var(--idle);background:rgba(245,158,11,.13)}
.pill.offline{color:var(--red);background:rgba(239,68,68,.12)}
.pill.online i{animation:blink 1.8s infinite}

.skel-card{border:1px solid var(--border);border-radius:13px;padding:13px;background:var(--glass)}
.skel-card .sk{height:11px;border-radius:6px;background:linear-gradient(100deg,var(--glass-2) 40%,var(--border) 50%,var(--glass-2) 60%);background-size:200% 100%;animation:shim 1.3s infinite}
.skel-card .sk+.sk{margin-top:9px}
.skel-card .w60{width:60%}
.skel-card .w40{width:40%}
@keyframes shim{to{background-position:-200% 0}}

.empty{padding:26px 10px;text-align:center;color:var(--muted)}
.empty svg{width:42px;height:42px;color:var(--faint);margin-bottom:8px}
.empty b{display:block;font-size:13px;color:var(--text);margin-bottom:3px}
.empty span{font-size:11.5px;line-height:1.45;display:block}
.empty .btn{margin:12px auto 0;min-width:150px}

.side-foot{display:flex;align-items:center;gap:7px;padding:10px 16px;border-top:1px solid var(--border);font-size:10px;color:var(--faint);font-weight:700;letter-spacing:.08em}
.dot-ok{width:7px;height:7px;border-radius:50%;background:var(--green);box-shadow:0 0 8px rgba(57,255,20,.6);transition:background .2s}
.side-foot .v{margin-left:auto;font-family:'Unbounded';font-size:8.5px}

/* ---------- Кнопки над картой ---------- */
.map-ui{position:fixed;top:14px;right:14px;z-index:900;display:flex;flex-direction:column;gap:8px}
.ctrl-btn{
  width:42px;height:42px;border-radius:13px;display:grid;place-items:center;cursor:pointer;
  background:var(--panel);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);
  border:1px solid var(--border);color:var(--text);box-shadow:var(--shadow-s);
  transition:transform .16s,border-color .16s,color .16s;
}
.ctrl-btn svg{width:18px;height:18px}
@media (hover:hover){
  .ctrl-btn:hover{border-color:var(--amber);color:var(--amber);transform:translateY(-1px)}
}
.ctrl-btn:active{transform:scale(.94)}

.legend{
  position:fixed;left:14px;bottom:14px;z-index:900;display:flex;gap:14px;align-items:center;
  padding:9px 15px;background:var(--panel);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);
  border:1px solid var(--border);border-radius:999px;box-shadow:var(--shadow-s);
  font-size:11px;font-weight:700;color:var(--muted);
}
.legend b{color:var(--text);font-family:'Unbounded';font-size:10.5px;margin-right:1px}
.legend i{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:5px}

.fab{
  position:fixed;right:16px;bottom:22px;z-index:950;width:56px;height:56px;border-radius:18px;
  display:grid;place-items:center;cursor:pointer;color:#1b1200;
  border:1px solid rgba(255,176,32,.5);background:linear-gradient(145deg,var(--amber),var(--amber-2));
  box-shadow:0 12px 30px rgba(255,138,0,.4);transition:transform .18s,box-shadow .25s,background .25s;
}
.fab svg{width:22px;height:22px}
@media (hover:hover){.fab:hover{transform:translateY(-2px)}}
.fab:active{transform:scale(.93)}
.fab.active{background:linear-gradient(145deg,#52ff2e,#16c400);border-color:rgba(57,255,20,.6);box-shadow:0 12px 30px rgba(57,255,20,.35);animation:fabPulse 2s infinite}
@keyframes fabPulse{50%{box-shadow:0 12px 42px rgba(57,255,20,.55)}}

/* ---------- Правая панель деталей ---------- */
.side-panel{
  position:fixed;z-index:1001;top:14px;right:14px;bottom:14px;width:356px;
  display:flex;flex-direction:column;overflow:hidden;
  background:var(--panel);backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);
  border:1px solid var(--border);border-radius:var(--r);box-shadow:var(--shadow);
  transform:translateX(calc(100% + 26px));transition:transform .38s cubic-bezier(.32,.72,.28,1);
}
.side-panel.open{transform:none}
.panel-scroll{overflow-y:auto;flex:1;padding:18px;scrollbar-width:thin}
.panel-close{
  position:absolute;top:12px;right:12px;z-index:2;width:34px;height:34px;border-radius:10px;
  display:grid;place-items:center;cursor:pointer;background:var(--glass);
  border:1px solid var(--border);color:var(--muted);transition:all .15s;
}
.panel-close:hover{color:var(--text);border-color:var(--border-2)}
.panel-close svg{width:15px;height:15px}

/* 🔥 НОВАЯ КНОПКА СВОРАЧИВАНИЯ */
.panel-minimize{
  position:absolute;top:12px;right:52px;z-index:2;width:34px;height:34px;border-radius:10px;
  display:grid;place-items:center;cursor:pointer;background:var(--glass);
  border:1px solid var(--border);color:var(--muted);transition:all .15s;
}
.panel-minimize:hover{color:var(--text);border-color:var(--border-2)}
.panel-minimize svg{width:15px;height:15px;transition:transform .2s}
.panel-minimize.minimized svg{transform:rotate(180deg)}

.panel-id{font-family:'Unbounded';font-weight:900;font-size:21px;letter-spacing:.01em;padding-right:42px;word-break:break-all}
.panel-status{margin:9px 0 5px}
.panel-meta{font-size:11.5px;color:var(--muted);font-weight:600;display:flex;flex-wrap:wrap;gap:4px 12px;align-items:center}
.coord-btn{border:0;background:none;padding:0;color:var(--amber);font:700 11.5px 'Manrope';cursor:pointer;border-bottom:1px dashed rgba(255,176,32,.5);font-variant-numeric:tabular-nums}
.coord-btn:hover{color:var(--amber-2)}

.cmd-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin:16px 0 4px}
.btn{
  display:flex;align-items:center;justify-content:center;gap:7px;min-height:42px;padding:0 10px;
  border-radius:12px;border:1px solid var(--border);background:var(--glass);color:var(--text);
  font:700 12.5px 'Manrope';cursor:pointer;transition:all .16s;
}
.btn svg{width:15px;height:15px;flex:none}
@media (hover:hover){.btn:hover{border-color:var(--amber);color:var(--amber)}}
.btn:active{transform:scale(.96)}
.btn.primary{grid-column:1/-1;background:linear-gradient(145deg,var(--amber),var(--amber-2));border-color:transparent;color:#1b1200;font-weight:800;box-shadow:0 8px 20px rgba(255,138,0,.28)}
@media (hover:hover){.btn.primary:hover{color:#1b1200;filter:brightness(1.06)}}
.btn.danger{color:var(--red);border-color:rgba(239,68,68,.32)}
@media (hover:hover){.btn.danger:hover{background:var(--red);border-color:var(--red);color:#fff}}
.btn.on{background:rgba(57,255,20,.14);border-color:rgba(57,255,20,.5);color:var(--green)}
.btn.loading{opacity:.7;pointer-events:none}
.btn.loading svg{animation:spin 1s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}

.hist{margin-top:16px;padding-top:15px;border-top:1px solid var(--border)}
.hist-head{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:10px}
.hist-title{font-size:11px;font-weight:800;letter-spacing:.16em;text-transform:uppercase;color:var(--muted)}
.seg{display:flex;background:var(--glass);border:1px solid var(--border);border-radius:10px;padding:3px;gap:2px}
.seg button{border:0;background:none;color:var(--muted);font:800 10.5px 'Manrope';padding:5px 8px;border-radius:7px;cursor:pointer;transition:all .15s;letter-spacing:.03em}
.seg button:hover{color:var(--text)}
.seg button.active{background:var(--amber);color:#1b1200}

.time-display{text-align:center;background:var(--glass);border:1px solid var(--border);border-radius:13px;padding:10px 8px 8px}
.time-display .t{font-family:'Unbounded';font-size:23px;font-weight:700;letter-spacing:.04em;display:block;line-height:1.15;font-variant-numeric:tabular-nums}
.time-display .d{font-size:10px;color:var(--muted);font-weight:700;letter-spacing:.1em;text-transform:uppercase}

.slider{width:100%;appearance:none;-webkit-appearance:none;height:28px;background:none;cursor:pointer;margin:4px 0 0}
.slider::-webkit-slider-runnable-track{height:5px;border-radius:4px;background:linear-gradient(90deg,#ffc24d,#ff5c33);opacity:.9}
.slider::-webkit-slider-thumb{-webkit-appearance:none;width:18px;height:18px;margin-top:-6.5px;border-radius:50%;background:#fff;border:4px solid var(--amber);box-shadow:0 2px 8px rgba(0,0,0,.4);transition:transform .12s}
.slider::-webkit-slider-thumb:hover{transform:scale(1.15)}
.slider::-moz-range-track{height:5px;border-radius:4px;background:linear-gradient(90deg,#ffc24d,#ff5c33)}
.slider::-moz-range-thumb{width:11px;height:11px;border-radius:50%;background:#fff;border:4px solid var(--amber)}
.slider:disabled{opacity:.35;cursor:default}
.slider-ends{display:flex;justify-content:space-between;font-size:9.5px;color:var(--faint);font-weight:700;font-variant-numeric:tabular-nums}

.play-row{display:flex;align-items:center;gap:7px;margin-top:10px}
.pbtn{
  height:38px;min-width:38px;padding:0 9px;border-radius:11px;display:grid;place-items:center;cursor:pointer;
  border:1px solid var(--border);background:var(--glass);color:var(--text);
  font:800 11px 'Manrope';transition:all .15s;
}
.pbtn svg{width:15px;height:15px}
@media (hover:hover){.pbtn:hover{border-color:var(--amber);color:var(--amber)}}
.pbtn:active{transform:scale(.94)}
.pbtn.main{width:46px;height:46px;border-radius:14px;background:linear-gradient(145deg,var(--amber),var(--amber-2));color:#1b1200;border-color:transparent;box-shadow:0 8px 18px rgba(255,138,0,.3)}
@media (hover:hover){.pbtn.main:hover{color:#1b1200;filter:brightness(1.06)}}
.pbtn.main.playing{background:linear-gradient(145deg,#52ff2e,#17c902);box-shadow:0 8px 18px rgba(57,255,20,.3)}
.pbtn.active{background:var(--amber);color:#1b1200;border-color:transparent}
.play-row .sp{margin-left:auto;display:flex;gap:5px}

.tstats{display:grid;grid-template-columns:repeat(3,1fr);gap:7px;margin-top:13px}
.tstat{background:var(--glass);border:1px solid var(--border);border-radius:11px;padding:8px 5px;text-align:center}
.tstat b{display:block;font-family:'Unbounded';font-size:12px;font-weight:700}
.tstat span{font-size:8px;letter-spacing:.13em;text-transform:uppercase;color:var(--faint);font-weight:800}

.recent{margin-top:14px}
.recent h4{margin:0 0 4px;font-size:10px;letter-spacing:.16em;text-transform:uppercase;color:var(--muted)}
.rrow{display:flex;align-items:center;gap:9px;padding:7px 2px;border-top:1px dashed var(--border);font-size:11.5px;font-weight:600}
.rrow .rt{font-variant-numeric:tabular-nums}
.rrow .rd{margin-left:auto;color:var(--faint);font-size:10.5px;font-variant-numeric:tabular-nums}
.rbar{width:36px;height:4px;border-radius:3px;background:var(--glass-2);overflow:hidden;flex:none}
.rbar i{display:block;height:100%;background:var(--amber);border-radius:3px}
.empty-row{padding:12px 2px;font-size:11.5px;color:var(--faint);border-top:1px dashed var(--border)}

/* 🔥 МИНИМИЗИРОВАННОЕ СОСТОЯНИЕ ПАНЕЛИ */
.side-panel.minimized .panel-scroll{
  opacity:0;pointer-events:none;
}
.side-panel.minimized .panel-minimize svg{
  transform:rotate(180deg);
}

/* ---------- Toast-уведомления ---------- */
#toasts{position:fixed;top:14px;left:50%;transform:translateX(-50%);z-index:3000;display:flex;flex-direction:column;gap:8px;pointer-events:none;width:max-content;max-width:92vw}
.toast{
  pointer-events:auto;display:flex;align-items:flex-start;gap:9px;max-width:min(92vw,360px);
  background:var(--panel-2);border:1px solid var(--border);border-left:3px solid var(--amber);
  border-radius:13px;padding:11px 12px;box-shadow:var(--shadow-s);
  font-size:12.5px;font-weight:600;line-height:1.35;animation:tIn .3s cubic-bezier(.2,.9,.3,1.2);
}
.toast svg{width:16px;height:16px;flex:none;margin-top:1px}
.toast.t-ok{border-left-color:var(--green)}
.toast.t-ok svg{color:var(--green)}
.toast.t-err{border-left-color:var(--red)}
.toast.t-err svg{color:var(--red)}
.toast.t-info svg{color:var(--amber)}
.toast.out{animation:tOut .25s forwards}
.toast-x{margin-left:auto;border:0;background:none;color:var(--faint);font-size:16px;cursor:pointer;padding:0 0 0 8px;line-height:1}
@keyframes tIn{from{opacity:0;transform:translateY(-16px)}}
@keyframes tOut{to{opacity:0;transform:translateY(-10px)}}

#net-banner{
  position:fixed;top:14px;left:50%;z-index:2900;display:flex;align-items:center;gap:8px;
  transform:translate(-50%,-180%);background:rgba(239,68,68,.96);color:#fff;
  font:700 12px 'Manrope';padding:9px 16px;border-radius:999px;
  box-shadow:0 10px 24px rgba(239,68,68,.4);transition:transform .3s;
}
#net-banner.show{transform:translate(-50%,0)}
#net-banner svg{width:15px;height:15px}

/* ---------- Маркеры и трек ---------- */
.scooter-pin{position:relative;width:34px;height:34px}
.scooter-pin.online{color:#39ff14}
.scooter-pin.idle{color:#f59e0b}
.scooter-pin.offline{color:#ef4444}
.scooter-pin .halo{position:absolute;inset:-5px;border-radius:50%;border:2px solid currentColor;opacity:0}
.scooter-pin.online .halo{animation:ping 2.2s cubic-bezier(.2,.6,.4,1) infinite}
@keyframes ping{0%{transform:scale(.55);opacity:.9}80%,100%{transform:scale(1.5);opacity:0}}
.scooter-pin .core{
  position:absolute;inset:2px;border-radius:50%;display:grid;place-items:center;
  background:var(--panel-2);border:2.5px solid currentColor;
  box-shadow:0 4px 12px rgba(0,0,0,.45);transition:transform .2s;
}
.scooter-pin .core svg{width:16px;height:16px;color:currentColor}
.scooter-pin.sel .core{transform:scale(1.28);box-shadow:0 0 0 5px rgba(255,176,32,.28),0 6px 16px rgba(0,0,0,.5)}
.scooter-pin.sel::after{content:'';position:absolute;inset:-9px;border-radius:50%;border:1.5px dashed #ffb020;animation:spin 9s linear infinite}
.cluster-pin{
  width:40px;height:40px;border-radius:50%;display:grid;place-items:center;
  background:linear-gradient(145deg,#ffb020,#ff8a00);color:#1b1200;
  border:3px solid rgba(255,255,255,.85);box-shadow:0 6px 16px rgba(0,0,0,.4);
  font-family:'Unbounded';font-weight:900;font-size:12px;
}
.track-dot{width:16px;height:16px;border-radius:50%;background:#fff;border:4.5px solid #ff8a00;box-shadow:0 0 14px rgba(255,138,0,.85),0 2px 6px rgba(0,0,0,.5)}

.leaflet-container{font-family:'Manrope',sans-serif;background:var(--bg)}
.leaflet-control-attribution{background:var(--glass)!important;color:var(--faint)!important;font-size:9px!important;backdrop-filter:blur(6px)}
.leaflet-control-attribution a{color:var(--muted)!important}
.leaflet-tooltip{background:var(--panel-2);color:var(--text);border:1px solid var(--border);border-radius:9px;font:700 11.5px 'Manrope';box-shadow:var(--shadow-s)}

/* ---------- Модальное окно настроек ---------- */
.modal{position:fixed;inset:0;z-index:2000;display:grid;place-items:center;padding:18px;background:rgba(4,8,18,.55);backdrop-filter:blur(6px);opacity:0;pointer-events:none;transition:opacity .25s}
.modal.open{opacity:1;pointer-events:auto}
.modal-card{width:min(390px,100%);background:var(--panel-2);border:1px solid var(--border);border-radius:20px;box-shadow:var(--shadow);padding:20px;transform:translateY(14px) scale(.97);transition:transform .3s cubic-bezier(.2,.9,.3,1.15)}
.modal.open .modal-card{transform:none}
.modal-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:6px}
.modal-head h3{margin:0;font-family:'Unbounded';font-size:15px;font-weight:900}
.m-row{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:12px 0;border-top:1px dashed var(--border);font-size:13px;font-weight:700}
.m-row small{display:block;color:var(--faint);font-size:10.5px;font-weight:600;margin-top:2px}
.switch{position:relative;width:46px;height:27px;flex:none;display:inline-block}
.switch input{position:absolute;opacity:0;inset:0;margin:0;cursor:pointer;z-index:2}
.switch i{position:absolute;inset:0;border-radius:999px;background:var(--glass-2);border:1px solid var(--border);transition:background .2s;display:block}
.switch i::after{content:'';position:absolute;top:3px;left:3px;width:19px;height:19px;border-radius:50%;background:#fff;box-shadow:0 2px 5px rgba(0,0,0,.35);transition:transform .2s}
.switch input:checked+i{background:var(--amber)}
.switch input:checked+i::after{transform:translateX(19px)}
.sel-wrap select{
  appearance:none;-webkit-appearance:none;background:var(--glass);border:1px solid var(--border);color:var(--text);
  font:700 12px 'Manrope';padding:9px 28px 9px 11px;border-radius:10px;cursor:pointer;
  background-image:linear-gradient(45deg,transparent 49%,var(--muted) 50%),linear-gradient(135deg,var(--muted) 50%,transparent 51%);
  background-position:calc(100% - 15px) 55%,calc(100% - 10px) 55%;background-size:5px 5px;background-repeat:no-repeat;
}
.about{margin-top:14px;padding-top:12px;border-top:1px dashed var(--border);font-size:10.5px;color:var(--faint);line-height:1.55;font-weight:600}

/* ---------- Мобильная навигация ---------- */
.bottom-nav{display:none}
.bn-btn{display:flex;flex-direction:column;align-items:center;gap:3.5px;border:0;background:none;color:var(--faint);font:800 9.5px 'Manrope';letter-spacing:.06em;text-transform:uppercase;padding:6px 0;border-radius:11px;cursor:pointer;min-height:46px;transition:color .15s}
.bn-btn svg{width:20px;height:20px}
.bn-btn.active{color:var(--amber)}

/* На десктопе освобождаем место под открытую панель деталей */
@media (min-width:861px){
  .map-ui,.fab{transition:right .38s cubic-bezier(.32,.72,.28,1),transform .18s,box-shadow .25s,background .25s}
  body:has(.side-panel.open) .map-ui{right:384px}
  body:has(.side-panel.open) .fab{right:384px}
  
  /* 🔥 Минимизация на десктопе */
  .side-panel.minimized{
    transform:translateX(calc(100% + 26px));
  }
}

/* ---------- Адаптив: мобильный (bottom-sheet + nav) ---------- */
@media (max-width:860px){
  :root{--nav-h:calc(60px + env(safe-area-inset-bottom))}
  .sidebar{
    top:auto;left:0;right:0;bottom:var(--nav-h);width:auto;max-height:72vh;max-height:72dvh;
    border-radius:22px 22px 0 0;border-left:0;border-right:0;
    transform:translateY(calc(100% - 150px));
    transition:transform .34s cubic-bezier(.32,.72,.28,1);
    box-shadow:0 -14px 44px rgba(2,6,18,.5);
  }
  .sidebar.sheet-open{transform:none}
  .sheet-handle{display:flex;justify-content:center;padding:9px 0 2px;cursor:grab;touch-action:none}
  .sheet-handle i{width:44px;height:4.5px;border-radius:4px;background:var(--border-2)}
  .map-ui{top:10px;right:10px}
  .ctrl-btn{width:44px;height:44px}
  .legend{left:10px;bottom:calc(var(--nav-h) + 160px);padding:7px 12px;gap:10px;font-size:10px}
  .fab{right:12px;bottom:calc(var(--nav-h) + 12px);width:58px;height:58px}
  .bottom-nav{
    display:grid;grid-template-columns:repeat(4,1fr);gap:2px;
    position:fixed;left:0;right:0;bottom:0;z-index:1200;
    background:var(--panel-2);backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);
    border-top:1px solid var(--border);
    padding:7px 6px calc(7px + env(safe-area-inset-bottom));
  }
  .side-panel{
    top:auto;left:0;right:0;bottom:0;width:auto;max-height:84vh;max-height:84dvh;
    border-radius:22px 22px 0 0;border-left:0;border-right:0;
    transform:translateY(105%);box-shadow:0 -14px 44px rgba(2,6,18,.5);
  }
  .side-panel.open{transform:none}
  
  /* 🔥 Минимизация на мобильном - панель сворачивается до небольшой полоски */
  .side-panel.minimized{
    transform:translateY(calc(100% - 90px));
  }
  
  .btn{min-height:46px}
  .pbtn{min-height:44px}
  .pbtn.main{width:52px;height:52px}
  .search input{height:46px}
  #toasts{top:auto;bottom:calc(var(--nav-h) + 90px)}
}
@media (max-width:380px){
  .stats{gap:6px}
  .stat b{font-size:14px}
  .brand-sub{display:none}
  .legend{gap:8px}
}
@media (hover:none){
  .dcard:hover,.ctrl-btn:hover,.btn:hover,.fab:hover{transform:none}
}
@media (prefers-reduced-motion:reduce){
  *,*::before,*::after{animation-duration:.01ms!important;animation-iteration-count:1!important;transition-duration:.01ms!important}
}
</style>
</head>
<body>

<div id="map"></div>

<!-- Баннер потери связи с сервером -->
<div id="net-banner">
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M2 8.8A15 15 0 0 1 12 5c1.6 0 3.1.25 4.6.7M22 8.8a15 15 0 0 0-3-2M5.3 12.5A10 10 0 0 1 12 10c.9 0 1.8.13 2.7.38M18.7 12.5c-.7-.55-1.5-1-2.3-1.35M8.5 16.2A5.5 5.5 0 0 1 12 15c.6 0 1.2.1 1.8.3M12 20h.01"/><path d="M3 3l18 18"/></svg>
  <span>Нет связи с сервером — повторяю запрос…</span>
</div>

<!-- Кнопки управления картой -->
<div class="map-ui">
  <button class="ctrl-btn" onclick="map.zoomIn()" title="Приблизить">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><path d="M12 5v14M5 12h14"/></svg>
  </button>
  <button class="ctrl-btn" onclick="map.zoomOut()" title="Отдалить">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><path d="M5 12h14"/></svg>
  </button>
  <button class="ctrl-btn" onclick="fitAll()" title="Показать весь парк">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8 3H5a2 2 0 0 0-2 2v3M16 3h3a2 2 0 0 1 2 2v3M8 21H5a2 2 0 0 1-2-2v-3M16 21h3a2 2 0 0 0 2-2v-3"/></svg>
  </button>
  <button class="ctrl-btn" onclick="centerMap()" title="Центр Туймазов">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 10c0 6-8 12-8 12s-8-6-8-12a8 8 0 0 1 16 0Z"/><circle cx="12" cy="10" r="3"/></svg>
  </button>
  <button class="ctrl-btn" id="theme-btn" onclick="toggleTheme()" title="Сменить тему"></button>
</div>

<!-- Легенда статусов -->
<div class="legend">
  <span><i style="background:var(--green)"></i><b id="lg-on">0</b>онлайн</span>
  <span><i style="background:var(--idle)"></i><b id="lg-idle">0</b>ожидание</span>
  <span><i style="background:var(--red)"></i><b id="lg-off">0</b>оффлайн</span>
</div>

<!-- FAB: следить / центрировать -->
<button class="fab" id="fab" onclick="fabClick()" title="Следить за самокатом"></button>

<!-- Сайдбар / bottom-sheet -->
<aside class="sidebar" id="sidebar">
  <div class="sheet-handle" id="sheet-handle"><i></i></div>
  <div class="brand">
    <div class="logo">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><circle cx="5" cy="18.4" r="2.3"/><circle cx="18.9" cy="18.4" r="2.3"/><path d="M5 18.4h6.4l5.5-12.2"/><path d="M14.6 6.2h4.6"/><path d="M16.9 6.2l1.9 10.1"/></svg>
    </div>
    <div>
      <div class="brand-name">ЖИРАФ<em>&nbsp;GO</em></div>
      <div class="brand-sub">Туймазы • Fleet Control</div>
    </div>
    <div class="brand-right">
      <div class="clock disp" id="clock">--:--:--</div>
      <div class="live"><i></i>live</div>
    </div>
  </div>

  <div class="stats">
    <div class="stat"><b id="st-total">–</b><span>Самокаты</span></div>
    <div class="stat on"><b id="st-online">–</b><span>Онлайн</span></div>
    <div class="stat"><b id="st-aps">–</b><span>WiFi точек</span></div>
    <div class="stat"><b id="st-records">–</b><span>Замеров</span></div>
  </div>

  <div class="search" id="search-wrap">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><circle cx="11" cy="11" r="7"/><path d="m21 21-4.3-4.3"/></svg>
    <input type="text" id="search-input" placeholder="Поиск по ID — например, 187" autocomplete="off">
    <button class="clr" onclick="clearSearch()" title="Очистить">✕</button>
  </div>

  <div class="chips">
    <button class="chip active" data-f="all" onclick="setStatusFilter('all')">Все <b id="c-all">0</b></button>
    <button class="chip" data-f="on" onclick="setStatusFilter('on')">Онлайн <b id="c-on">0</b></button>
    <button class="chip" data-f="off" onclick="setStatusFilter('off')">Оффлайн <b id="c-off">0</b></button>
  </div>

  <div class="device-list" id="device-list"></div>

  <div class="side-foot">
    <span class="dot-ok" id="api-dot"></span>
    <span id="api-state">API подключено</span>
    <span class="v">v2.0</span>
  </div>
</aside>

<!-- Панель деталей устройства -->
<section class="side-panel" id="side-panel">
  <button class="panel-close" onclick="closePanel()" title="Закрыть">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round"><path d="M18 6 6 18M6 6l12 12"/></svg>
  </button>
  <!-- 🔥 НОВАЯ КНОПКА СВОРАЧИВАНИЯ -->
  <button class="panel-minimize" onclick="togglePanelMinimize()" title="Свернуть/Развернуть">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round"><path d="M8 15l4-4 4 4M8 9l4 4 4-4"/></svg>
  </button>
  <div class="panel-scroll">
    <div class="panel-head">
      <div class="panel-id" id="panel-id">—</div>
      <div class="panel-status"><span class="pill online" id="status-pill"><i></i><span id="status-text">—</span></span></div>
      <div class="panel-meta">
        <span id="panel-meta">—</span>
        <button class="coord-btn" id="coord-btn" onclick="copyCoords()" title="Скопировать координаты">—</button>
      </div>
    </div>

    <div class="cmd-grid">
      <button class="btn primary" id="scan-btn" onclick="triggerScan()">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M4.5 12A7.5 7.5 0 0 1 12 4.5M12 4.5A7.5 7.5 0 0 1 19.5 12M7 12a5 5 0 0 1 5-5M12 7a5 5 0 0 1 5 5"/><circle cx="12" cy="12" r="1.6" fill="currentColor" stroke="none"/><path d="M12 14v6"/></svg>
        Скан WiFi
      </button>
      <button class="btn danger" onclick="rebootDevice()">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M12 2v10"/><path d="M18.4 6.6a9 9 0 1 1-12.77.04"/></svg>
        Reboot
      </button>
      <button class="btn" onclick="refreshHistory()">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-2.64-6.36"/><path d="M21 3v6h-6"/></svg>
        Трек
      </button>
      <button class="btn" id="follow-btn" onclick="toggleFollow()" style="grid-column:1/-1"></button>
    </div>

    <div class="hist">
      <div class="hist-head">
        <span class="hist-title">История трека</span>
        <div class="seg" id="seg-range">
          <button data-h="1" onclick="setRange(1)">1ч</button>
          <button data-h="6" onclick="setRange(6)">6ч</button>
          <button data-h="24" class="active" onclick="setRange(24)">24ч</button>
          <button data-h="168" onclick="setRange(168)">7д</button>
        </div>
      </div>

      <div class="time-display">
        <span class="t" id="time-display">--:--:--</span>
        <span class="d" id="date-display">нет данных</span>
      </div>
      <input type="range" class="slider" id="track-slider" min="0" max="100" value="100" disabled>
      <div class="slider-ends"><span id="sl-min">--:--</span><span id="sl-max">--:--</span></div>

      <div class="play-row">
        <button class="pbtn" onclick="restartPlayback()" title="В начало">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 20 9 12l10-8v16Z"/><path d="M5 19V5"/></svg>
        </button>
        <button class="pbtn main" id="play-btn" onclick="togglePlayback()" title="Воспроизвести трек"></button>
        <div class="sp">
          <button class="pbtn spd active" data-s="1" onclick="setPlaybackSpeed(1)">1×</button>
          <button class="pbtn spd" data-s="2" onclick="setPlaybackSpeed(2)">2×</button>
          <button class="pbtn spd" data-s="4" onclick="setPlaybackSpeed(4)">4×</button>
        </div>
      </div>

      <div class="tstats">
        <div class="tstat"><b id="ts-points">–</b><span>Точек</span></div>
        <div class="tstat"><b id="ts-dist">–</b><span>Дистанция</span></div>
        <div class="tstat"><b id="ts-dur">–</b><span>В пути</span></div>
      </div>

      <div class="recent">
        <h4>Последние замеры</h4>
        <div id="recent-list"></div>
      </div>
    </div>
  </div>
</section>

<!-- Настройки -->
<div class="modal" id="settings-modal" onclick="if(event.target===this)closeSettings()">
  <div class="modal-card">
    <div class="modal-head">
      <h3>Настройки</h3>
      <button class="panel-close" style="position:static" onclick="closeSettings()" title="Закрыть">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round"><path d="M18 6 6 18M6 6l12 12"/></svg>
      </button>
    </div>
    <div class="m-row">
      <div>Тёмная тема<small>Тайлы карты переключаются автоматически</small></div>
      <label class="switch"><input type="checkbox" id="theme-switch" onchange="applyTheme(!this.checked)"><i></i></label>
    </div>
    <div class="m-row">
      <div>Интервал опроса<small>Как часто обновляются позиции (вне панели — всегда)</small></div>
      <div class="sel-wrap">
        <select id="poll-select" onchange="setPollInterval(this.value)">
          <option value="5000">5 сек</option>
          <option value="8000" selected>8 сек</option>
          <option value="15000">15 сек</option>
        </select>
      </div>
    </div>
    <div class="m-row">
      <div>Центр карты<small>Вернуться к центру Туймазов</small></div>
      <button class="btn" style="min-width:110px" onclick="centerMap();closeSettings()">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 10c0 6-8 12-8 12s-8-6-8-12a8 8 0 0 1 16 0Z"/><circle cx="12" cy="10" r="3"/></svg>
        Карта
      </button>
    </div>
    <div class="about">
      Жираф GO • Fleet Control v2.0<br>
      WiFi-триангуляция позиций • г. Туймазы • Leaflet 1.9.4
    </div>
  </div>
</div>

<!-- Нижняя навигация (мобильный) -->
<nav class="bottom-nav" id="bottom-nav">
  <button class="bn-btn active" data-tab="map" onclick="switchTab('map')">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m9 3-6 2v16l6-2 6 2 6-2V3l-6 2-6-2Z"/><path d="M9 3v16M15 5v16"/></svg>
    <span>Карта</span>
  </button>
  <button class="bn-btn" data-tab="fleet" onclick="switchTab('fleet')">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><circle cx="5" cy="18.4" r="2.3"/><circle cx="18.9" cy="18.4" r="2.3"/><path d="M5 18.4h6.4l5.5-12.2"/><path d="M14.6 6.2h4.6"/><path d="M16.9 6.2l1.9 10.1"/></svg>
    <span>Флот</span>
  </button>
  <button class="bn-btn" data-tab="history" onclick="switchTab('history')">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 3"/></svg>
    <span>История</span>
  </button>
  <button class="bn-btn" data-tab="settings" onclick="switchTab('settings')">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.7 1.7 0 0 0 .34 1.87l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.7 1.7 0 0 0-1.87-.34 1.7 1.7 0 0 0-1 1.55V21a2 2 0 1 1-4 0v-.09a1.7 1.7 0 0 0-1-1.55 1.7 1.7 0 0 0-1.87.34l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.7 1.7 0 0 0 .34-1.87 1.7 1.7 0 0 0-1.55-1H3a2 2 0 1 1 0-4h.09a1.7 1.7 0 0 0 1.55-1 1.7 1.7 0 0 0-.34-1.87l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.7 1.7 0 0 0-1.87.34h.09a1.7 1.7 0 0 0 1-1.55V3a2 2 0 1 1 4 0v.09a1.7 1.7 0 0 0 1 1.55 1.7 1.7 0 0 0 1.87-.34l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.7 1.7 0 0 0-.34 1.87v.09a1.7 1.7 0 0 0 1.55 1H21a2 2 0 1 1 0 4h-.09a1.7 1.7 0 0 0-1.55 1Z"/></svg>
    <span>Настройки</span>
  </button>
</nav>

<div id="toasts"></div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
<script>
/* ============================================================
   Жираф GO — логика интерфейса (Vanilla JS)
   Структура API-вызовов сохранена: /api/devices, /api/history,
   /api/trigger, /api/stats. Добавлены ретраи при 5xx и toasts.
   ============================================================ */

// ================= КОНСТАНТЫ И СОСТОЯНИЕ =================
var CITY = [54.6000, 53.6920];                 // центр г. Туймазы
var DARK_TILE  = 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png';
var LIGHT_TILE = 'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png';
var ATTR = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/">CARTO</a>';

var devices = [], markers = {};
var trackLayer = null, trackPolyline = null, progressLine = null, trackMarker = null;
var selectedDevice = null, historyData = [], playbackInterval = null;
var isPlaying = false, currentSliderIndex = 0;
var followMode = false, pollTimer = null, panelCoord = '';
var currentRange = 24, histToken = 0, welcomed = false, onlineState = true;
var playRAF = null, playT0 = 0, playSpeed = 1, BASE_DUR = 22000;
var stFilter = 'all', stQuery = '', listBooted = false, statsCache = null;
var panelMinimized = false; // 🔥 СОСТОЯНИЕ МИНИМИЗАЦИИ ПАНЕЛИ

var LABEL = { online:'Онлайн', idle:'Ожидание', offline:'Оффлайн' };

// Инлайн-иконки (без внешних шрифтов иконок)
var ICON = {
  play:  '<svg viewBox="0 0 24 24" fill="currentColor" stroke="none"><path d="M8 5.5v13l11-6.5-11-6.5Z"/></svg>',
  pause: '<svg viewBox="0 0 24 24" fill="currentColor" stroke="none"><rect x="6" y="5" width="4" height="14" rx="1.2"/><rect x="14" y="5" width="4" height="14" rx="1.2"/></svg>',
  eye:   '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7-10-7-10-7Z"/><circle cx="12" cy="12" r="3"/></svg>',
  cross: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="7.5"/><path d="M12 2v4M12 18v4M2 12h4M18 12h4"/></svg>',
  sun:   '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="4.5"/><path d="M12 2.5v2.5M12 19v2.5M2.5 12H5M19 12h2.5M4.9 4.9l1.8 1.8M17.3 17.3l1.8 1.8M19.1 4.9l-1.8 1.8M6.7 17.3l-1.8 1.8"/></svg>',
  moon:  '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20.5 14.5A8.5 8.5 0 1 1 9.5 3.5a7 7 0 0 0 11 11Z"/></svg>',
  ok:    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="m4.5 12.5 5 5 10-11"/></svg>',
  info:  '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><circle cx="12" cy="12" r="9"/><path d="M12 11v5"/><circle cx="12" cy="8" r="0.6" fill="currentColor"/></svg>',
  err:   '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><circle cx="12" cy="12" r="9"/><path d="M12 7.5V13"/><circle cx="12" cy="16.3" r="0.6" fill="currentColor"/></svg>',
  scooter:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><circle cx="5" cy="18.4" r="2.3"/><circle cx="18.9" cy="18.4" r="2.3"/><path d="M5 18.4h6.4l5.5-12.2"/><path d="M14.6 6.2h4.6"/><path d="M16.9 6.2l1.9 10.1"/></svg>',
  minimize: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round"><path d="M8 15l4-4 4 4M8 9l4 4 4-4"/></svg>'
};

// ================= УТИЛИТЫ =================
var $ = function(id){ return document.getElementById(id); };
// Безопасная подписка: если элемента нет — молча пропускаем, скрипт не падает
function onEl(id, ev, fn){
  var el = $(id);
  if (el) el.addEventListener(ev, fn);
}
// Безопасная запись текста в элемент
function setText(id, val){
  var el = $(id);
  if (el && el.textContent !== String(val)) el.textContent = val;
}

function esc(s){
  return String(s).split('&').join('&amp;').split('<').join('&lt;').split('>').join('&gt;');
}
function fmtTime(iso){
  var d = new Date(iso);
  return isNaN(d.getTime()) ? '--:--:--' : d.toLocaleTimeString('ru-RU', { hour:'2-digit', minute:'2-digit', second:'2-digit' });
}
function fmtDate(iso){
  var d = new Date(iso);
  return isNaN(d.getTime()) ? '—' : d.toLocaleDateString('ru-RU', { day:'2-digit', month:'short' });
}
function fmtAgo(iso){
  var diff = Date.now() - new Date(iso).getTime();
  if (isNaN(diff)) return '—';
  var m = Math.floor(diff / 60000);
  if (m < 1)  return 'только что';
  if (m < 60) return m + ' мин назад';
  var h = Math.floor(m / 60);
  if (h < 24) return h + ' ч назад';
  return Math.floor(h / 24) + ' дн назад';
}
function fmtDur(ms){
  if (!ms || ms <= 0) return '—';
  var h = Math.floor(ms / 3600000), m = Math.floor(ms / 60000) % 60;
  if (h > 0) return h + ' ч ' + m + ' мин';
  return Math.max(1, m) + ' мин';
}
function haversine(a, b){
  var R = 6371000, toRad = Math.PI / 180;
  var dLat = (b.lat - a.lat) * toRad, dLon = (b.lon - a.lon) * toRad;
  var s = Math.sin(dLat/2) * Math.sin(dLat/2) +
          Math.cos(a.lat*toRad) * Math.cos(b.lat*toRad) * Math.sin(dLon/2) * Math.sin(dLon/2);
  return 2 * R * Math.asin(Math.sqrt(s));
}
function debounce(fn, ms){
  var t = null;
  return function(){
    var args = arguments, ctx = this;
    clearTimeout(t);
    t = setTimeout(function(){ fn.apply(ctx, args); }, ms);
  };
}
function statusOf(d){
  var min = (Date.now() - new Date(d.last_ts).getTime()) / 60000;
  if (min < 30)  return 'online';
  if (min < 360) return 'idle';
  return 'offline';
}
// Подсветка совпадения поиска (без регэкспов — безопасно для любых ID)
function highlight(text, q){
  var safe = esc(text);
  if (!q) return safe;
  var i = String(text).toLowerCase().indexOf(q.toLowerCase());
  if (i < 0) return safe;
  return esc(text.slice(0, i)) + '<mark>' + esc(text.slice(i, i + q.length)) + '</mark>' + esc(text.slice(i + q.length));
}

// ================= СЕТЕВОЙ СЛОЙ (ретраи при 5xx, индикатор связи) =================
function sleep(ms){ return new Promise(function(r){ setTimeout(r, ms); }); }
function setOnline(ok){
  if (ok === onlineState) return;
  onlineState = ok;
  var nb = $('net-banner');  if (nb)  nb.classList.toggle('show', !ok);
  var dot = $('api-dot');    if (dot) dot.style.background = ok ? 'var(--green)' : 'var(--red)';
  var st = $('api-state');   if (st)  st.textContent = ok ? 'API подключено' : 'Нет связи с сервером';
  if (!ok) toast('Ошибка сети — повторяю запросы', 'err');
}
async function fetchJSON(url, opts, retries){
  retries = (retries === undefined) ? 2 : retries;
  var lastErr = null;
  for (var a = 0; a <= retries; a++){
    try {
      var r = await fetch(url, opts);
      if (r.ok){ setOnline(true); return await r.json(); }
      lastErr = new Error('HTTP ' + r.status);
      if (r.status >= 500 && a < retries){ await sleep(450 * (a + 1)); continue; }
      break;                                  // 4xx не ретраим
    } catch (e){
      lastErr = e;
      if (a < retries) await sleep(450 * (a + 1));
    }
  }
  setOnline(false);
  throw lastErr;
}

// ================= TOAST-УВЕДОМЛЕНИЯ (замена alert) =================
function toast(msg, type){
  type = type || 'info';
  var box = $('toasts');
  if (!box) return;                            // контейнер отсутствует — не критично
  var t = document.createElement('div');
  t.className = 'toast t-' + type;
  t.innerHTML = (ICON[type] || ICON.info) + '<div>' + esc(msg) + '</div><button class="toast-x" title="Закрыть">✕</button>';
  box.appendChild(t);
  var to = setTimeout(kill, 3800);
  function kill(){ clearTimeout(to); t.classList.add('out'); setTimeout(function(){ t.remove(); }, 260); }
  t.querySelector('.toast-x').onclick = kill;
  while (box.children.length > 4) box.firstChild.remove();
}

// ================= КАРТА =================
// 🔥 ИСПРАВЛЕНО: добавлен maxZoom: 19 для MarkerCluster
var map = L.map('map', { zoomControl:false, attributionControl:true, maxZoom: 19 }).setView(CITY, 13);
var tileLayer = null, currentTileUrl = '';

// Кластеризация при зуме < 12 (leaflet.markercluster)
var cluster = L.markerClusterGroup({
  showCoverageOnHover:false,
  maxClusterRadius:58,
  disableClusteringAtZoom:12,
  iconCreateFunction:function(c){
    return L.divIcon({ className:'', html:'<div class="cluster-pin"><b>' + c.getChildCount() + '</b></div>', iconSize:[40,40], iconAnchor:[20,20] });
  }
});
map.addLayer(cluster);
trackLayer = L.layerGroup().addTo(map);

// ================= ТЕМА (dark/light, кроссфейд тайлов) =================
function setTiles(isLight){
  var url = isLight ? LIGHT_TILE : DARK_TILE;
  if (currentTileUrl === url) return;
  var next = L.tileLayer(url, { attribution:ATTR, maxZoom:19 });
  next.addTo(map);
  var prev = tileLayer;
  tileLayer = next; currentTileUrl = url;
  if (prev){
    next.on('load', function(){ if (map.hasLayer(prev)) map.removeLayer(prev); });
    setTimeout(function(){ if (map.hasLayer(prev)) map.removeLayer(prev); }, 1500); // страховка
  }
}
function applyTheme(isLight){
  document.documentElement.setAttribute('data-theme', isLight ? 'light' : 'dark');
  try { localStorage.setItem('theme', isLight ? 'light' : 'dark'); } catch (e){}
  setTiles(isLight);
  $('theme-btn').innerHTML = isLight ? ICON.sun : ICON.moon;
  var sw = $('theme-switch'); if (sw) sw.checked = !isLight;
  var mc = document.querySelector('meta[name="theme-color"]');
  if (mc) mc.setAttribute('content', isLight ? '#e8edf6' : '#0a0f1c');
}
function toggleTheme(){
  applyTheme(document.documentElement.getAttribute('data-theme') !== 'light');
}
function centerMap(){ map.flyTo(CITY, 13, { duration:.8 }); }
function fitAll(){
  var ms = Object.keys(markers).map(function(k){ return markers[k]; });
  if (!ms.length){ toast('Парк пока пуст', 'info'); return; }
  map.flyToBounds(L.featureGroup(ms).getBounds().pad(0.2), { maxZoom:16, duration:.7 });
}

// ================= МАРКЕРЫ =================
function scooterIcon(status, selected){
  var html = '<div class="scooter-pin ' + status + (selected ? ' sel' : '') + '">' +
             '<span class="halo"></span><span class="core">' + ICON.scooter + '</span></div>';
  return L.divIcon({ className:'', html:html, iconSize:[34,34], iconAnchor:[17,17] });
}
function panFollow(ll){
  if (!map.getBounds().pad(-0.15).contains(ll)) map.flyTo(ll, Math.max(map.getZoom(), 15), { duration:.6 });
  else map.panTo(ll, { animate:true, duration:.6 });
}

// ================= АВТООПРОС =================
function startPolling(){
  if (pollTimer) clearInterval(pollTimer);
  var sel = $('poll-select');
  var base = sel ? (parseInt(sel.value, 10) || 8000) : 8000;
  var interval = selectedDevice ? 3000 : base;   // рядом с выбранной панелью — чаще
  pollTimer = setInterval(loadDevices, interval);
}
function setPollInterval(){ startPolling(); toast('Интервал опроса обновлён', 'ok'); }

// ================= ЗАГРУЗКА УСТРОЙСТВ =================
function loadDevices(){
  fetchJSON('/api/devices')
    .then(function(devs){
      devices = Array.isArray(devs) ? devs : [];
      renderDevices();
      if (!welcomed){ welcomed = true; toast('Система подключена: ' + devices.length + ' устр.', 'ok'); }
    })
    .catch(function(){
      if (!welcomed){
        var list = $('device-list');
        if (list){
          list.innerHTML =
            '<div class="empty">' + ICON.err +
            '<b>Не удалось загрузить парк</b><span>Проверьте подключение к серверу</span>' +
            '<button class="btn" onclick="loadDevices()">Повторить</button></div>';
        }
      }
    });
}

function renderDevices(){
  // Маркеры пересобираются: выбранный — поверх кластера, с кольцом
  cluster.clearLayers();
  Object.keys(markers).forEach(function(k){ map.removeLayer(markers[k]); });
  markers = {};
  var counts = { online:0, idle:0, offline:0 };

  devices.forEach(function(d){
    var st = statusOf(d);
    counts[st]++;
    var m = L.marker([d.lat, d.lon], { icon:scooterIcon(st, d.id === selectedDevice) });
    m.on('click', function(){ openPanel(d.id, d.lat, d.lon, d.last_ts); });
    markers[d.id] = m;
    if (d.id === selectedDevice) m.addTo(map);
    else cluster.addLayer(m);
  });

  setText('lg-on', counts.online);
  setText('lg-idle', counts.idle);
  setText('lg-off', counts.offline);
  setText('c-all', devices.length);
  setText('c-on', counts.online);
  setText('c-off', counts.idle + counts.offline);

  setStat('st-total', String(devices.length));
  setStat('st-online', String(counts.online));

  renderList();
  updateFab();

  // Плавное слежение за выбранным самокатом
  if (followMode && selectedDevice && markers[selectedDevice]){
    panFollow(markers[selectedDevice].getLatLng());
  }
}

function setStat(id, val){
  var el = $(id);
  if (!el || el.textContent === val) return;
  el.textContent = val;
  var card = el.parentElement;
  card.classList.remove('bump'); void card.offsetWidth; card.classList.add('bump');
}

// ================= СПИСОК (поиск + фильтр + скелетоны) =================
function renderList(){
  var list = $('device-list');
  if (!list) return;                           // список отсутствует — рендер не критичен
  var scrollTop = list.scrollTop;
  var q = stQuery.trim();

  var shown = devices.filter(function(d){
    if (q && d.id.toLowerCase().indexOf(q.toLowerCase()) < 0) return false;
    if (stFilter === 'on')  return statusOf(d) === 'online';
    if (stFilter === 'off') return statusOf(d) !== 'online';
    return true;
  });

  if (!devices.length){
    list.innerHTML = '<div class="empty">' + ICON.scooter +
      '<b>Парк пуст</b><span>Устройства появятся после первого скана WiFi (POST /scan)</span></div>';
    return;
  }
  if (!shown.length){
    list.innerHTML = '<div class="empty">' + ICON.info +
      '<b>Ничего не найдено</b><span>Попробуйте другой ID или сбросьте фильтры</span>' +
      '<button class="btn" onclick="clearSearch();setStatusFilter(&#39;all&#39;)">Сбросить</button></div>';
    return;
  }

  var html = '';
  shown.forEach(function(d, i){
    var st = statusOf(d);
    var cls = 'dcard ' + st + (d.id === selectedDevice ? ' active' : '') + (listBooted ? '' : ' boot');
    var delay = listBooted ? '' : ' style="animation-delay:' + Math.min(i * 45, 400) + 'ms"';
    html += '<article class="' + cls + '" data-raw="' + esc(d.id) + '"' + delay + '>' +
      '<div class="dcard-top"><span class="dcard-id">' + highlight(d.id, q) + '</span>' +
      '<span class="pill ' + st + '"><i></i>' + LABEL[st] + '</span></div>' +
      '<div class="dcard-meta"><span>' + fmtAgo(d.last_ts) + '</span>' +
      '<span>' + d.lat.toFixed(4) + ', ' + d.lon.toFixed(4) + '</span></div></article>';
  });
  list.innerHTML = html;
  list.scrollTop = scrollTop;
  listBooted = true;
}

onEl('device-list', 'click', function(e){
  var card = e.target.closest('.dcard');
  if (!card) return;
  var d = null;
  devices.forEach(function(x){ if (x.id === card.dataset.raw) d = x; });
  if (d) openPanel(d.id, d.lat, d.lon, d.last_ts);
});

// Поиск с debounce 300 мс
onEl('search-input', 'input', debounce(function(e){
  stQuery = e.target.value;
  $('search-wrap').classList.toggle('has', !!stQuery);
  renderList();
}, 300));
function clearSearch(){
  $('search-input').value = ''; stQuery = '';
  $('search-wrap').classList.remove('has');
  renderList();
}
function setStatusFilter(f){
  stFilter = f;
  document.querySelectorAll('.chip').forEach(function(c){
    c.classList.toggle('active', c.dataset.f === f);
  });
  renderList();
}

// ================= СТАТИСТИКА (/api/stats) =================
function loadStats(){
  fetchJSON('/api/stats').then(function(s){
    statsCache = s;
    setStat('st-aps', Number(s.wifi_access_points || 0).toLocaleString('ru-RU'));
    setStat('st-records', Number(s.location_records || 0).toLocaleString('ru-RU'));
  }).catch(function(){});
}

// ================= ПАНЕЛЬ УСТРОЙСТВА И КОМАНДЫ =================
function openPanel(id, lat, lon, ts) {
    console.log('🎯 openPanel:', id, lat, lon, ts);  // ← ДОБАВИТЬ ЛОГ
    
    selectedDevice = id;
    $('side-panel').classList.add('open');
    //  При открытии панели сбрасываем минимизацию
    if (panelMinimized) togglePanelMinimize();
    $('panel-id').textContent = id;
    $('panel-meta').textContent = `${fmtDate(ts)} • ${fmtTime(ts)}`;
    $('track-slider').value = 100;
    
    // 🔥 ВАЖНО: не закрывать панель при обновлении!
    loadHistory(24);
    map.setView([lat, lon], 16);
    startPolling();
    
    console.log('✅ Панель открыта');  // ← ДОБАВИТЬ
}
function closePanel(){
  $('side-panel').classList.remove('open');
  stopPlayback();
  selectedDevice = null;
  followMode = false;
  syncFollowBtn();
  renderDevices();
  markTab('map');
  startPolling();
}

// 🔥 НОВАЯ ФУНКЦИЯ СВОРАЧИВАНИЯ/РАЗВОРАЧИВАНИЯ ПАНЕЛИ
function togglePanelMinimize(){
  panelMinimized = !panelMinimized;
  var panel = $('side-panel');
  var btn = document.querySelector('.panel-minimize');
  if (panel && btn){
    panel.classList.toggle('minimized', panelMinimized);
    btn.classList.toggle('minimized', panelMinimized);
  }
  if (panelMinimized){
    toast('Панель свёрнута', 'info');
  } else {
    toast('Панель развёрнута', 'ok');
  }
}

function triggerScan(){
  if (!selectedDevice) return;
  var b = $('scan-btn');
  b.classList.add('loading');
  fetchJSON('/api/trigger/' + encodeURIComponent(selectedDevice), { method:'POST' })
    .then(function(d){ toast(d.message || 'Команда отправлена', 'ok'); })
    .catch(function(){ toast('Не удалось отправить команду', 'err'); })
    .finally(function(){ setTimeout(function(){ b.classList.remove('loading'); }, 600); });
}
function rebootDevice(){
  if (!selectedDevice) return;
  fetchJSON('/api/trigger/' + encodeURIComponent(selectedDevice) + '/reboot', { method:'POST' })
    .then(function(){ toast('Команда Reboot отправлена', 'ok'); })
    .catch(function(){ toast('Команда Reboot поставлена в очередь', 'info'); });
}

function toggleFollow(){
  if (!selectedDevice){ toast('Сначала выберите самокат', 'info'); return; }
  followMode = !followMode;
  if (followMode && markers[selectedDevice]){
    panFollow(markers[selectedDevice].getLatLng());
    // 🔥 Автоматически сворачиваем панель на мобильной версии при включении отслеживания
    if (window.innerWidth <= 860 && !panelMinimized){
      togglePanelMinimize();
    }
  }
  syncFollowBtn(); updateFab();
  toast(followMode ? 'Слежение включено' : 'Слежение выключено', followMode ? 'ok' : 'info');
}
function syncFollowBtn(){
  var b = $('follow-btn');
  b.classList.toggle('on', followMode);
  b.innerHTML = ICON.eye + '<span>' + (followMode ? 'Слежение: вкл' : 'Следить за самокатом') + '</span>';
}
function updateFab(){
  var f = $('fab');
  var act = followMode && !!selectedDevice;
  f.classList.toggle('active', act);
  f.innerHTML = act ? ICON.eye : ICON.cross;
  f.title = act ? 'Выключить слежение' : 'Следить / центрировать';
}
function fabClick(){
  if (!selectedDevice){ centerMap(); toast('Центрирую на Туймазах', 'info'); }
  else toggleFollow();
}
function copyCoords(){
  if (!panelCoord) return;
  if (navigator.clipboard && navigator.clipboard.writeText){
    navigator.clipboard.writeText(panelCoord)
      .then(function(){ toast('Координаты скопированы', 'ok'); })
      .catch(function(){ toast(panelCoord, 'info'); });
  } else toast(panelCoord, 'info');
}

// ================= ИСТОРИЯ И ТРЕК =================
function clearTrack(){
  trackLayer.clearLayers();
  if (progressLine){ map.removeLayer(progressLine); progressLine = null; }
  if (trackMarker){ map.removeLayer(trackMarker); trackMarker = null; }
  trackPolyline = null;
}
function markRange(h){
  document.querySelectorAll('#seg-range button').forEach(function(b){
    b.classList.toggle('active', parseInt(b.dataset.h, 10) === h);
  });
}

function loadHistory(hours){
  hours = parseInt(hours, 10) || 24;
  if (!selectedDevice) return;
  currentRange = hours;
  markRange(hours);
  var my = ++histToken;
  clearTrack();
  $('time-display').textContent = '…';
  $('date-display').textContent = 'загрузка трека';

  fetchJSON('/api/history/' + encodeURIComponent(selectedDevice) + '?hours=' + hours)
    .then(function(hist){
      if (my !== histToken) return;            // ответ устарел — игнорируем
      historyData = Array.isArray(hist) ? hist : [];
      var n = historyData.length;
      var slider = $('track-slider');

      if (n === 0){
        slider.max = 0; slider.value = 0; slider.disabled = true;
        $('time-display').textContent = '--:--:--';
        $('date-display').textContent = 'нет точек за период';
        $('sl-min').textContent = '--:--'; $('sl-max').textContent = '--:--';
        setTStats(0, 0, 0);
        $('recent-list').innerHTML = '<div class="empty-row">Нет данных</div>';
        toast('За выбранный период точек нет', 'info');
        return;
      }

      slider.max = n - 1; slider.value = n - 1; slider.disabled = false;
      $('sl-min').textContent = fmtTime(historyData[0].ts).slice(0, 5);
      $('sl-max').textContent = fmtTime(historyData[n-1].ts).slice(0, 5);

      // Градиентная полилиния: сегменты с плавным переходом цвета от начала к концу
      if (n >= 2){
        var step = Math.max(1, Math.floor(n / 240));
        for (var i = 0; i < n - 1; i += step){
          var j = Math.min(i + step, n - 1);
          var t = j / (n - 1);
          var hue = 45 - 33 * t;               // янтарный -> горячий оранжево-красный
          var lig = 60 - 10 * t;
          trackPolyline = L.polyline(
            [[historyData[i].lat, historyData[i].lon], [historyData[j].lat, historyData[j].lon]],
            { color:'hsl(' + hue + ', 98%, ' + lig + '%)', weight:4, opacity:.88, lineCap:'round', lineJoin:'round' }
          ).addTo(trackLayer);
        }
        L.circleMarker([historyData[0].lat, historyData[0].lon],
          { radius:5, color:'#8aa0c6', weight:2.5, fillColor:'#0a0f1c', fillOpacity:1 }).addTo(trackLayer);
        L.circleMarker([historyData[n-1].lat, historyData[n-1].lon],
          { radius:5, color:'#ff5c33', weight:2, fillColor:'#ff5c33', fillOpacity:1 }).addTo(trackLayer);
      }

      // Прогресс-линия (пройденная часть) и перетаскиваемый маркер позиции
      progressLine = L.polyline([], { color:'#e07000', weight:5.5, opacity:.95, lineCap:'round', lineJoin:'round' }).addTo(map);
      trackMarker = L.marker([historyData[n-1].lat, historyData[n-1].lon], {
        icon:L.divIcon({ className:'', html:'<div class="track-dot"></div>', iconSize:[16,16], iconAnchor:[8,8] }),
        draggable:true, zIndexOffset:600
      }).addTo(map);
      trackMarker.on('drag', function(e){
        if (isPlaying) stopPlayback();
        var near = findNearestIndex(e.target.getLatLng());
        $('track-slider').value = near;
        updateTrackMarker(near);
      });

      var dist = 0;
      for (var k = 1; k < n; k++) dist += haversine(historyData[k-1], historyData[k]);
      var durMs = Date.parse(historyData[n-1].ts) - Date.parse(historyData[0].ts);
      setTStats(n, dist, durMs);
      renderRecent();

      currentSliderIndex = n - 1;
      updateTrackMarker(n - 1);

      if (!followMode){
        try { map.flyToBounds(trackLayer.getBounds().pad(0.18), { maxZoom:16, duration:.7 }); } catch (e){}
      }
    })
    .catch(function(){
      if (my === histToken){
        $('time-display').textContent = '--:--:--';
        $('date-display').textContent = 'ошибка загрузки';
      }
    });
}
function refreshHistory(){ loadHistory(currentRange); }
function setRange(h){
  h = parseInt(h, 10) || 24;
  $('track-slider').dataset.hours = h;
  loadHistory(h);
}

function setTStats(n, distM, durMs){
  $('ts-points').textContent = n ? n : '–';
  $('ts-dist').textContent = !n ? '–' : (distM < 1000 ? Math.round(distM) + ' м' : (distM / 1000).toFixed(1) + ' км');
  $('ts-dur').textContent = !n ? '–' : fmtDur(durMs);
}
function renderRecent(){
  var box = $('recent-list');
  var rows = historyData.slice(-6).reverse();
  if (!rows.length){ box.innerHTML = '<div class="empty-row">Нет данных</div>'; return; }
  box.innerHTML = rows.map(function(p){
    var quality = Math.max(8, Math.min(100, 105 - (p.acc || 50)));
    return '<div class="rrow"><span class="rt">' + fmtTime(p.ts) + '</span>' +
           '<span class="rbar"><i style="width:' + quality + '%"></i></span>' +
           '<span class="rd">±' + (p.acc == null ? '—' : p.acc) + ' м</span></div>';
  }).join('');
}

function updateTrackMarker(idx){
  if (!historyData.length) return;
  idx = Math.max(0, Math.min(historyData.length - 1, idx));
  currentSliderIndex = idx;
  var p = historyData[idx];
  $('time-display').textContent = fmtTime(p.ts);
  $('date-display').textContent = fmtDate(p.ts) + ' • ' + fmtAgo(p.ts);
  if (trackMarker) trackMarker.setLatLng([p.lat, p.lon]);
  if (progressLine){
    progressLine.setLatLngs(historyData.slice(0, idx + 1).map(function(q){ return [q.lat, q.lon]; }));
  }
}

function findNearestIndex(targetLatLng){
  var minDist = Infinity, idx = 0;
  historyData.forEach(function(p, i){
    var dist = L.latLng([p.lat, p.lon]).distanceTo(targetLatLng);
    if (dist < minDist){ minDist = dist; idx = i; }
  });
  return idx;
}

onEl('track-slider', 'input', function(e){
  if (isPlaying) stopPlayback();               // скраббинг ставит playback на паузу
  updateTrackMarker(parseInt(e.target.value, 10));
});

// ================= PLAYBACK (плавный, requestAnimationFrame) =================
function playFrame(now){
  var n = historyData.length;
  if (n < 2){ stopPlayback(); return; }
  var dur = BASE_DUR / playSpeed;
  var f = (now - playT0) / dur;
  if (f > 1) f = 1;
  var pos = f * (n - 1);
  var i = Math.min(n - 2, Math.floor(pos));
  var k = pos - i;
  var a = historyData[i], b = historyData[i + 1];
  var lat = a.lat + (b.lat - a.lat) * k;
  var lon = a.lon + (b.lon - a.lon) * k;
  var ts = Date.parse(a.ts) + (Date.parse(b.ts) - Date.parse(a.ts)) * k;

  if (trackMarker) trackMarker.setLatLng([lat, lon]);
  if (progressLine){
    var seg = historyData.slice(0, i + 1).map(function(p){ return [p.lat, p.lon]; });
    seg.push([lat, lon]);
    progressLine.setLatLngs(seg);
  }
  var iso = new Date(ts).toISOString();
  $('time-display').textContent = fmtTime(iso);
  $('date-display').textContent = fmtDate(iso) + ' • воспроизведение';
  $('track-slider').value = Math.round(pos);
  currentSliderIndex = Math.round(pos);

  if (f >= 1){ stopPlayback(); toast('Трек воспроизведён', 'info'); return; }
  playRAF = requestAnimationFrame(playFrame);
}
function togglePlayback(){
  if (isPlaying){ stopPlayback(); return; }
  if (historyData.length < 2){ toast('Недостаточно точек для воспроизведения', 'info'); return; }
  if (currentSliderIndex >= historyData.length - 1){
    currentSliderIndex = 0;
    updateTrackMarker(0);
  }
  isPlaying = true;
  syncPlayBtn();
  var f0 = currentSliderIndex / (historyData.length - 1);
  playT0 = performance.now() - f0 * (BASE_DUR / playSpeed);
  playRAF = requestAnimationFrame(playFrame);
}
function stopPlayback(){
  if (playRAF) cancelAnimationFrame(playRAF);
  playRAF = null;
  if (playbackInterval) clearInterval(playbackInterval); // совместимость со старой логикой
  isPlaying = false;
  syncPlayBtn();
}
function syncPlayBtn(){
  var b = $('play-btn');
  b.classList.toggle('playing', isPlaying);
  b.innerHTML = isPlaying ? ICON.pause : ICON.play;
  b.title = isPlaying ? 'Пауза' : 'Воспроизвести трек';
}
function restartPlayback(){
  if (historyData.length < 2) return;
  var was = isPlaying;
  stopPlayback();
  updateTrackMarker(0);
  if (was) togglePlayback();
}
function setPlaybackSpeed(s){
  playSpeed = s;
  document.querySelectorAll('.spd').forEach(function(b){
    b.classList.toggle('active', parseInt(b.dataset.s, 10) === s);
  });
  if (isPlaying && historyData.length > 1){
    var f = currentSliderIndex / (historyData.length - 1);
    playT0 = performance.now() - f * (BASE_DUR / playSpeed); // бесшовная смена скорости
  }
}

// ================= МОБИЛЬНЫЙ bottom-sheet И НАВИГАЦИЯ =================
function openSheet(){ $('sidebar').classList.add('sheet-open'); }
function closeSheet(){ $('sidebar').classList.remove('sheet-open'); }
(function initSheet(){
  var hd = $('sheet-handle'), startY = null;
  if (!hd) return;                             // на десктопе ручка скрыта, но обязана существовать; страховка от падения
  hd.addEventListener('pointerdown', function(e){ startY = e.clientY; });
  hd.addEventListener('pointerup', function(e){
    if (startY === null) return;
    var dy = e.clientY - startY; startY = null;
    if (Math.abs(dy) < 8){ $('sidebar').classList.toggle('sheet-open'); }  // тап
    else if (dy < 0) openSheet();                                          // swipe вверх
    else closeSheet();                                                     // swipe вниз
  });
})();

function markTab(t){
  document.querySelectorAll('.bn-btn').forEach(function(b){
    b.classList.toggle('active', b.dataset.tab === t);
  });
}
function switchTab(tab){
  markTab(tab);
  if (tab === 'map'){
    closeSheet();
  } else if (tab === 'fleet'){
    openSheet();
    setTimeout(function(){ $('search-input').focus({ preventScroll:true }); }, 380);
  } else if (tab === 'history'){
    if (selectedDevice){
      closeSheet();
      $('side-panel').classList.add('open');
      var h = document.querySelector('.hist');
      if (h) setTimeout(function(){ h.scrollIntoView({ behavior:'smooth', block:'start' }); }, 350);
    } else {
      toast('Сначала выберите самокат из списка', 'info');
      openSheet();
    }
  } else if (tab === 'settings'){
    openSettings();
  }
}
function openSettings(){ $('settings-modal').classList.add('open'); }
function closeSettings(){ $('settings-modal').classList.remove('open'); }

// ================= ЧАСЫ И ИНИЦИАЛИЗАЦИЯ =================
function tickClock(){
  var c = $('clock');
  if (c) c.textContent = new Date().toLocaleTimeString('ru-RU', { hour:'2-digit', minute:'2-digit', second:'2-digit' });
}
(function init(){
  // 1) Тема — до первого рендера, без мерцания
  applyTheme(localStorage.getItem('theme') === 'light');

  // 2) Скелетоны на время первой загрузки
  var dl = $('device-list');
  if (dl){
    var sk = '';
    for (var i = 0; i < 5; i++){
      sk += '<div class="skel-card"><div class="sk w60"></div><div class="sk w40"></div></div>';
    }
    dl.innerHTML = sk;
  }

  // 3) КРИТИЧЕСКИЙ КОНТУР: данные и поллинг запускаются первыми и независимо
  //    от декоративных биндингов ниже. Даже если какой-то элемент отсутствует,
  //    список устройств продолжит обновляться.
  loadDevices();
  loadStats();
  setInterval(loadStats, 25000);
  startPolling();
  setInterval(tickClock, 1000);
  tickClock();

  // 4) Декоративные состояния кнопок — не должны ломать инициализацию
  try {
    syncFollowBtn();
    updateFab();
    syncPlayBtn();
  } catch (err) {
    console.warn('Жираф GO: не удалось инициализировать часть UI', err);
  }
})();
</script>
</body>
</html>
"""                 

@app.route('/')
def index():
    return render_template_string(HTML)

# ================= ЗАПУСК СЕРВЕРА =================
if __name__ == '__main__':
    logger.info("🚀 Запуск Scooter Tracker Server...")
    
    # 1. Инициализация БД (создаёт таблицы если нет)
    init_db()
    
    # 2. Проверка и обработка новых KML файлов
    run_kml_processor()
    
    # 3. Импорт CSV только если база пустая
    import_csv_if_empty()
    
    # 4. 🔥 Показываем статистику — что уже есть в базе
    show_db_stats()
    
    # 5. Запуск сервера
    logger.info(f"✅ Сервер готов на {HOST}:{PORT}")
    logger.info(f"📁 База: {DB_PATH}")
    app.run(host=HOST, port=PORT, debug=False, threaded=True)
