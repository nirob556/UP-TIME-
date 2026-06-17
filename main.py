import os
import time
import json
import asyncio
import httpx
from typing import List, Dict
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager

# --- DB MANAGEMENT SYSTEM ---
DB_FILE = "monitors_db.json"
GLOBAL_SETTINGS_FILE = "global_settings.json"

def load_db() -> list:
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return []
    return []

def save_db(data: list):
    try:
        with open(DB_FILE, "w") as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f"[!] Database Save Error: {e}")

def load_settings() -> dict:
    default_settings = {"global_timeout": 12, "auto_refresh": True}
    if os.path.exists(GLOBAL_SETTINGS_FILE):
        try:
            with open(GLOBAL_SETTINGS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return default_settings
    return default_settings

def save_settings(settings: dict):
    try:
        with open(GLOBAL_SETTINGS_FILE, "w") as f:
            json.dump(settings, f, indent=4)
    except Exception as e:
        print(f"[!] Settings Save Error: {e}")

# Global In-Memory Sync
monitors: List[Dict] = load_db()
global_config = load_settings()
total_global_checks = sum(m.get('total_checks', 0) for m in monitors)

class MonitorRequest(BaseModel):
    name: str
    url: str
    interval: int = 5

class SettingsRequest(BaseModel):
    global_timeout: int
    auto_refresh: bool

async def check_api_status(monitor: Dict):
    global total_global_checks
    start_time = time.time()
    
    url = monitor['url']
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    timeout_val = float(global_config.get("global_timeout", 12))

    async with httpx.AsyncClient(timeout=timeout_val, follow_redirects=True) as client:
        try:
            response = await client.get(url)
            response_time = int((time.time() - start_time) * 1000)
            
            if 200 <= response.status_code < 400:
                monitor['status'] = 'UP'
                monitor['success_checks'] += 1
            else:
                monitor['status'] = 'DOWN'
            
            monitor['response_time'] = response_time
            monitor['status_code'] = response.status_code

        except Exception:
            monitor['status'] = 'DOWN'
            monitor['response_time'] = 0
            monitor['status_code'] = "ERR"
            
        finally:
            monitor['total_checks'] += 1
            total_global_checks += 1
            monitor['last_check'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            if monitor['total_checks'] > 0:
                monitor['uptime'] = round((monitor['success_checks'] / monitor['total_checks']) * 100, 2)
            save_db(monitors)

# Keep-Alive Engine to prevent Render/Koyeb sleeping
async def keep_alive_pulse():
    await asyncio.sleep(30)
    app_url = os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("KOYEB_APP_URL") or "http://localhost:8080"
    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            try:
                await client.get(app_url)
            except Exception:
                pass
            await asyncio.sleep(300)

# Background Monitoring Network Loop
async def monitor_loop():
    while True:
        tasks = []
        current_time = time.time()
        
        for monitor in monitors:
            if monitor.get('is_active', True):
                last_run = monitor.get('_last_run_timestamp', 0)
                interval_seconds = monitor.get('interval', 5) * 60
                
                if current_time - last_run >= interval_seconds:
                    monitor['_last_run_timestamp'] = current_time
                    tasks.append(check_api_status(monitor))
                    
        if tasks:
            await asyncio.gather(*tasks)
            save_db(monitors)
            
        await asyncio.sleep(10)

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(monitor_loop())
    asyncio.create_task(keep_alive_pulse())
    yield

app = FastAPI(title="VIP Uptime Monitor By SPEED_X", lifespan=lifespan)

# --- API ENDPOINTS ---

@app.get("/api/stats")
async def get_stats():
    total = len(monitors)
    active = len([m for m in monitors if m['is_active']])
    up = len([m for m in monitors if m['is_active'] and m['status'] == 'UP'])
    down = len([m for m in monitors if m['is_active'] and m['status'] == 'DOWN'])
    return {
        "total": total,
        "active": active,
        "up": up,
        "down": down,
        "total_checks": total_global_checks,
        "settings": global_config
    }

@app.get("/api/monitors")
async def get_monitors():
    cleaned_monitors = []
    for m in monitors:
        copy_m = m.copy()
        copy_m.pop('_last_run_timestamp', None)
        cleaned_monitors.append(copy_m)
    return cleaned_monitors

@app.post("/api/settings")
async def update_settings(req: SettingsRequest):
    global global_config
    global_config["global_timeout"] = req.global_timeout
    global_config["auto_refresh"] = req.auto_refresh
    save_settings(global_config)
    return {"success": True, "settings": global_config}

@app.post("/api/monitors")
async def add_monitor(req: MonitorRequest):
    new_monitor = {
        "id": str(int(time.time() * 1000)),
        "name": req.name,
        "url": req.url,
        "interval": req.interval,
        "status": "PENDING",
        "response_time": 0,
        "status_code": "N/A",
        "uptime": 100.0,
        "success_checks": 0,
        "total_checks": 0,
        "is_active": True,
        "last_check": "Never",
        "_last_run_timestamp": 0
    }
    monitors.append(new_monitor)
    await check_api_status(new_monitor) 
    save_db(monitors)
    return new_monitor

@app.post("/api/monitors/{monitor_id}/ping")
async def force_ping_monitor(monitor_id: str):
    for m in monitors:
        if m['id'] == monitor_id:
            if not m['is_active']:
                raise HTTPException(status_code=400, detail="Cannot pulse suspended node")
            await check_api_status(m)
            save_db(monitors)
            return m
    raise HTTPException(status_code=404, detail="Monitor not found")

@app.post("/api/monitors/{monitor_id}/toggle")
async def toggle_monitor(monitor_id: str):
    for m in monitors:
        if m['id'] == monitor_id:
            m['is_active'] = not m['is_active']
            if not m['is_active']:
                m['status'] = 'STOPPED'
            else:
                m['status'] = 'PENDING'
                m['_last_run_timestamp'] = 0
            save_db(monitors)
            return m
    raise HTTPException(status_code=404, detail="Monitor not found")

@app.post("/api/global/suspend")
async def suspend_all_monitors():
    for m in monitors:
        m['is_active'] = False
        m['status'] = 'STOPPED'
    save_db(monitors)
    return {"success": True}

@app.post("/api/global/resume")
async def resume_all_monitors():
    for m in monitors:
        m['is_active'] = True
        m['status'] = 'PENDING'
        m['_last_run_timestamp'] = 0
    save_db(monitors)
    return {"success": True}

@app.post("/api/global/purge")
async def purge_all_monitors():
    global monitors
    monitors.clear()
    save_db(monitors)
    return {"success": True}

@app.delete("/api/monitors/{monitor_id}")
async def delete_monitor(monitor_id: str):
    global monitors
    monitors = [m for m in monitors if m['id'] != monitor_id]
    save_db(monitors)
    return {"success": True}


# --- FRONTEND UI (100% English Cyber Architecture) ---

@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>MATRIX UPTIME mainframe PRO • SPEED_X</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://cdn.jsdelivr.net/npm/particles.js@2.0.0/particles.min.js"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;600;900&family=Rajdhani:wght@500;700&display=swap');
            body { background-color: #020205; color: #f1f5f9; font-family: 'Rajdhani', sans-serif; overflow-x: hidden; }
            .font-orbitron { font-family: 'Orbitron', sans-serif; }
            .neon-shadow-cyan { box-shadow: 0 0 25px rgba(6, 182, 212, 0.25); }
            .neon-text-cyan { text-shadow: 0 0 15px rgba(6, 182, 212, 0.8), 0 0 30px rgba(6, 182, 212, 0.4); }
            .neon-text-green { text-shadow: 0 0 10px rgba(16, 185, 129, 0.6); }
            .neon-text-rose { text-shadow: 0 0 10px rgba(244, 63, 94, 0.6); }
            .vip-card { background: linear-gradient(135deg, rgba(8, 8, 20, 0.9) 0%, rgba(3, 3, 8, 0.98) 100%); border: 1px solid rgba(6, 182, 212, 0.12); backdrop-filter: blur(16px); }
            .vip-card:hover { border-color: rgba(6, 182, 212, 0.4); box-shadow: 0 0 25px rgba(6, 182, 212, 0.15); transform: translateY(-1px); }
            #particles-js { position: fixed; width: 100%; height: 100%; z-index: -1; top: 0; left: 0; }
            
            .cyber-scanner { height: 2px; background: linear-gradient(90deg, transparent, #06b6d4, transparent); width: 100%; position: absolute; animation: scan 4s linear infinite; }
            @keyframes scan { 0% { top: 0%; } 50% { top: 100%; } 100% { top: 0%; } }
            
            #toast-container { position: fixed; bottom: 20px; right: 20px; z-index: 50; display: flex; flex-direction: column; gap: 10px; }
            
            ::-webkit-scrollbar { width: 5px; height: 5px; }
            ::-webkit-scrollbar-track { background: #020205; }
            ::-webkit-scrollbar-thumb { background: linear-gradient(#06b6d4, #3b82f6); border-radius: 10px; }
            
            .terminal-box { background: rgba(2, 2, 5, 0.95); font-family: monospace; border: 1px solid rgba(6, 182, 212, 0.2); height: 160px; overflow-y: auto; box-shadow: inset 0 0 15px rgba(0,0,0,0.8); }
        </style>
    </head>
    <body class="relative min-h-screen antialiased">
        
        <div id="particles-js"></div>
        <div id="toast-container"></div>

        <div class="max-w-6xl mx-auto px-4 py-8 relative z-10">
            
            <!-- Global Control Deck Header -->
            <div class="flex flex-wrap justify-between items-center gap-3 mb-6 bg-black/50 border border-cyan-950/30 px-4 py-3 rounded-xl">
                <div class="flex flex-wrap items-center gap-2">
                    <button onclick="toggleAudio()" id="audio-btn" class="px-3 py-1.5 bg-cyan-950/30 border border-cyan-800/40 hover:border-cyan-500/50 text-cyan-400 font-bold text-xs rounded-lg transition-all flex items-center gap-1.5 font-mono">
                        <i class="fa-solid fa-volume-high" id="audio-icon"></i> SFX: ENABLED
                    </button>
                    <div class="h-4 w-[1px] bg-cyan-950"></div>
                    <span id="auto-refresh-badge" class="px-3 py-1 bg-blue-950/30 border border-blue-900/40 text-blue-400 font-bold text-xs rounded-lg font-mono">
                        AUTO REFRESH: ON
                    </span>
                </div>
                <div class="flex flex-wrap gap-2">
                    <button onclick="globalAction('resume')" class="px-3 py-1.5 bg-emerald-950/40 border border-emerald-800/40 hover:border-emerald-500/50 text-emerald-400 font-bold text-xs rounded-lg transition-all font-mono">
                        <i class="fa-solid fa-play"></i> RESUME ALL NODES
                    </button>
                    <button onclick="globalAction('suspend')" class="px-3 py-1.5 bg-amber-950/40 border border-amber-800/40 hover:border-amber-500/50 text-amber-400 font-bold text-xs rounded-lg transition-all font-mono">
                        <i class="fa-solid fa-pause"></i> SUSPEND ALL NODES
                    </button>
                    <button onclick="globalPurge()" class="px-3 py-1.5 bg-rose-950/40 border border-rose-800/40 hover:border-rose-500/50 text-rose-400 font-bold text-xs rounded-lg transition-all font-mono">
                        <i class="fa-solid fa-skull-crossbones"></i> PURGE ALL
                    </button>
                </div>
            </div>

            <!-- Master branding header -->
            <header class="text-center mb-10 border-b border-cyan-950/20 pb-8 relative">
                <div class="inline-block px-4 py-1.5 bg-gradient-to-r from-cyan-950/60 to-blue-950/60 border border-cyan-500/20 text-cyan-400 text-xs font-mono tracking-widest uppercase rounded-md mb-4">
                    <i class="fa-solid fa-microchip animate-pulse mr-1.5 text-cyan-400"></i> SYSTEM LAYER FRAMEWORK ACTIVE
                </div>
                <h1 class="text-5xl md:text-6xl font-black tracking-wider text-cyan-400 neon-text-cyan font-orbitron">VIP UPTIME MATRIX</h1>
                <p class="text-sm text-gray-500 uppercase tracking-widest mt-3 font-mono">ENGINEERED BY <span class="text-cyan-400 font-bold">SPEED_X</span> • SECURE ARCHITECTURE v4.0</p>
            </header>

            <!-- Live Metric Status Engine Grid -->
            <div class="grid grid-cols-2 md:grid-cols-5 gap-4 mb-8">
                <div class="vip-card p-4 rounded-xl text-center border-b-2 border-cyan-500 relative overflow-hidden">
                    <div class="cyber-scanner opacity-10"></div>
                    <h3 class="text-xs font-bold text-cyan-500/70 tracking-wider uppercase mb-1 font-mono">Total Targets</h3>
                    <p id="stat-total" class="text-4xl font-black text-cyan-400 font-mono tracking-tighter">0</p>
                </div>
                <div class="vip-card p-4 rounded-xl text-center border-b-2 border-blue-500">
                    <h3 class="text-xs font-bold text-blue-500/70 tracking-wider uppercase mb-1 font-mono">Active Rails</h3>
                    <p id="stat-active" class="text-4xl font-black text-blue-400 font-mono tracking-tighter">0</p>
                </div>
                <div class="vip-card p-4 rounded-xl text-center border-b-2 border-emerald-500">
                    <h3 class="text-xs font-bold text-emerald-500/70 tracking-wider uppercase mb-1 font-mono">Status UP</h3>
                    <p id="stat-up" class="text-4xl font-black text-emerald-400 font-mono tracking-tighter neon-text-green">0</p>
                </div>
                <div class="vip-card p-4 rounded-xl text-center border-b-2 border-rose-500">
                    <h3 class="text-xs font-bold text-rose-500/70 tracking-wider uppercase mb-1 font-mono">Status DOWN</h3>
                    <p id="stat-down" class="text-4xl font-black text-rose-400 font-mono tracking-tighter neon-text-rose">0</p>
                </div>
                <div class="vip-card p-4 rounded-xl text-center border-b-2 border-amber-500 col-span-2 md:col-span-1">
                    <h3 class="text-xs font-bold text-amber-500/70 tracking-wider uppercase mb-1 font-mono">Total Transmissions</h3>
                    <p id="stat-checks" class="text-4xl font-black text-amber-400 font-mono tracking-tighter">0</p>
                </div>
            </div>

            <!-- Double Column Layout: Form Configuration and Settings Control panel -->
            <div class="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
                
                <!-- Main Link Injector Matrix Form (Takes 2 Columns) -->
                <div class="vip-card p-6 rounded-2xl border border-cyan-500/10 lg:col-span-2 shadow-xl">
                    <h2 class="text-md font-bold text-cyan-400 mb-5 flex items-center gap-2 font-mono uppercase tracking-widest font-orbitron">
                        <i class="fa-solid fa-plus-node text-cyan-400 animate-bounce"></i> INJECT TARGET MATRIX ENDPOINT
                    </h2>
                    
                    <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
                        <div>
                            <label class="block text-xs font-bold text-cyan-400/80 mb-1 font-mono uppercase tracking-wider">Node Alias Identifier</label>
                            <div class="relative">
                                <i class="fa-solid fa-tag absolute left-3.5 top-3.5 text-cyan-600/50 text-xs"></i>
                                <input type="text" id="mon-name" placeholder="e.g. Main Framework Bot" class="w-full bg-black/60 border border-cyan-950 rounded-lg pl-10 pr-4 py-2.5 text-white placeholder-gray-700 focus:outline-none focus:border-cyan-500 text-xs font-mono tracking-wide">
                            </div>
                        </div>
                        <div>
                            <label class="block text-xs font-bold text-cyan-400/80 mb-1 font-mono uppercase tracking-wider">Target Endpoint URL</label>
                            <div class="relative">
                                <i class="fa-solid fa-network-wired absolute left-3.5 top-3.5 text-cyan-600/50 text-xs"></i>
                                <input type="url" id="mon-url" placeholder="https://api.domain.com/pulse" class="w-full bg-black/60 border border-cyan-950 rounded-lg pl-10 pr-4 py-2.5 text-white placeholder-gray-700 focus:outline-none focus:border-cyan-500 text-xs font-mono tracking-wide">
                            </div>
                        </div>
                    </div>

                    <div class="mb-5">
                        <label class="block text-xs font-bold text-cyan-400/80 mb-1 font-mono uppercase tracking-wider">Interval Cycle Control Sequence</label>
                        <select id="mon-interval" class="w-full bg-black/60 border border-cyan-950 rounded-lg px-4 py-2.5 text-white focus:outline-none focus:border-cyan-500 text-xs font-mono cursor-pointer">
                            <option value="1">Hyper Blast Mode (Every 1 Minute)</option>
                            <option value="5" selected>Standard VIP Framework Cycle (Every 5 Minutes)</option>
                            <option value="15">Optimized Operations Sequence (Every 15 Minutes)</option>
                            <option value="30">Deep Matrix System Save (Every 30 Minutes)</option>
                        </select>
                    </div>

                    <button onclick="deployMonitor()" class="w-full bg-gradient-to-r from-cyan-500 via-blue-600 to-indigo-600 hover:from-cyan-400 hover:to-indigo-500 text-black font-black py-3 rounded-xl tracking-widest uppercase text-xs font-orbitron transition-all shadow-lg shadow-cyan-500/10">
                        <i class="fa-solid fa-circle-nodes mr-1 text-sm"></i> Deploy Main Target Node
                    </button>
                </div>

                <!-- Global Settings Core Configuration Panel (Takes 1 Column) -->
                <div class="vip-card p-6 rounded-2xl border border-cyan-500/10 flex flex-col justify-between">
                    <div>
                        <h2 class="text-md font-bold text-cyan-400 mb-5 flex items-center gap-2 font-mono uppercase tracking-widest font-orbitron">
                            <i class="fa-solid fa-sliders text-cyan-400"></i> CONFIG RUNTIME
                        </h2>

                        <div class="mb-4">
                            <label class="block text-xs font-bold text-cyan-400/80 mb-1.5 font-mono uppercase tracking-wider">Network Timeout (Seconds)</label>
                            <input type="number" id="settings-timeout" min="2" max="60" class="w-full bg-black/60 border border-cyan-950 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-cyan-500 text-xs font-mono">
                        </div>

                        <div class="mb-4 flex items-center justify-between bg-black/40 border border-cyan-950/40 p-2.5 rounded-lg">
                            <span class="text-xs font-bold text-cyan-400/80 font-mono uppercase tracking-wider">UI Auto Synchronizer</span>
                            <input type="checkbox" id="settings-refresh" class="w-4 h-4 accent-cyan-500 cursor-pointer">
                        </div>
                    </div>

                    <button onclick="saveGlobalConfig()" class="w-full bg-cyan-950/50 border border-cyan-700/60 hover:bg-cyan-900/60 hover:border-cyan-400 text-cyan-400 font-bold py-2 rounded-xl tracking-widest uppercase text-xs font-mono transition-all">
                        <i class="fa-solid fa-floppy-disk mr-1"></i> Sync Config Core
                    </button>
                </div>
            </div>

            <!-- Advanced Search Filters, Sorting and Grid Controller Desk -->
            <div class="bg-black/40 border border-cyan-950/40 p-4 rounded-xl mb-6 flex flex-col md:flex-row gap-4 items-center justify-between">
                
                <!-- Search & Filters Container -->
                <div class="flex flex-wrap items-center gap-3 w-full md:w-auto">
                    <div class="relative w-full sm:w-64">
                        <i class="fa-solid fa-magnifying-glass absolute left-3 top-3 text-cyan-600/70 text-xs"></i>
                        <input type="text" id="search-bar" oninput="applyFiltersAndSorting()" placeholder="Search active node matrices..." class="w-full bg-black border border-cyan-950 rounded-lg pl-9 pr-4 py-2 text-xs focus:outline-none focus:border-cyan-500 text-gray-300 font-mono">
                    </div>

                    <!-- Status Categorization Filter Grid Tabs -->
                    <div class="flex bg-black border border-cyan-950 p-1 rounded-lg text-xs font-mono">
                        <button onclick="setStatusFilter('ALL')" id="tab-ALL" class="px-3 py-1 rounded bg-cyan-950 text-cyan-400 font-bold">ALL</button>
                        <button onclick="setStatusFilter('UP')" id="tab-UP" class="px-3 py-1 rounded text-gray-500 hover:text-cyan-400">UP</button>
                        <button onclick="setStatusFilter('DOWN')" id="tab-DOWN" class="px-3 py-1 rounded text-gray-500 hover:text-cyan-400">DOWN</button>
                        <button onclick="setStatusFilter('STOPPED')" id="tab-STOPPED" class="px-3 py-1 rounded text-gray-500 hover:text-cyan-400">SUSPENDED</button>
                    </div>
                </div>

                <!-- Sorting Selection Engine -->
                <div class="flex items-center gap-2 w-full md:w-auto justify-end">
                    <span class="text-xs text-gray-500 font-mono uppercase tracking-wider"><i class="fa-solid fa-arrow-down-sort-alphabet"></i> Sort By:</span>
                    <select id="sort-engine" onchange="applyFiltersAndSorting()" class="bg-black border border-cyan-950 rounded-lg px-3 py-2 text-xs font-mono text-gray-300 focus:outline-none focus:border-cyan-500 cursor-pointer">
                        <option value="name">Identifier Name</option>
                        <option value="uptime">Health Rate (Uptime)</option>
                        <option value="response_time">Response Latency</option>
                    </select>
                    <button onclick="downloadBackup()" class="px-3 py-2 bg-cyan-950/20 border border-cyan-900/50 hover:border-cyan-500/50 text-cyan-400 font-bold text-xs rounded-lg transition-all font-mono">
                        <i class="fa-solid fa-file-export"></i> BACKUP
                    </button>
                </div>
            </div>

            <div class="flex items-center justify-between mb-4">
                <h2 class="text-md font-bold font-mono tracking-wider text-gray-300 flex items-center gap-2 font-orbitron uppercase"><i class="fa-solid fa-server text-cyan-500"></i> Main Target Framework Logs</h2>
                <span id="sync-clock" class="text-[10px] font-mono bg-cyan-950/40 text-cyan-400 border border-cyan-800/40 px-3 py-1 rounded-full font-bold shadow-md">Sync Interval Standby</span>
            </div>

            <!-- Active Channels Core Mainframe Grid Loader -->
            <div id="monitors-grid" class="space-y-4 mb-8"></div>

            <!-- Real-time Simulation Shell Terminal Console View Module -->
            <div class="mb-4">
                <h2 class="text-xs font-bold font-mono tracking-wider text-cyan-500 uppercase mb-2 flex items-center gap-1.5"><i class="fa-solid fa-terminal text-[10px]"></i> Active Framework Shell Terminal Output Log Console</h2>
                <div id="terminal-log" class="terminal-box p-4 text-xs font-mono text-emerald-400 space-y-1">
                    <div>[SYSTEM INITIALIZING] Main secure tracking framework operational sequence online...</div>
                </div>
            </div>

            <footer class="text-center text-xs font-mono text-gray-600 border-t border-cyan-950/20 pt-6">
                <p>&copy; 2026 <span class="text-cyan-500/50 font-bold">SPEED_X</span> • AUTHORIZED QUANTUM FRAMEWORK LABS. ALL POWER SECURED.</p>
            </footer>
        </div>

        <script>
            let currentCachedMonitors = [];
            let activeStatusFilter = 'ALL';
            let isAudioEnabled = true;
            let autoRefreshIntervalId = null;

            const audioCtx = new (window.AudioContext || window.webkitAudioContext)();
            function playSoundFx(type) {
                if (!isAudioEnabled) return;
                const osc = audioCtx.createOscillator();
                const gain = audioCtx.createGain();
                osc.connect(gain); gain.connect(audioCtx.destination);

                if (type === 'success') {
                    osc.type = 'sine'; osc.frequency.setValueAtTime(880, audioCtx.currentTime);
                    gain.gain.setValueAtTime(0.06, audioCtx.currentTime);
                    gain.gain.exponentialRampToValueAtTime(0.00001, audioCtx.currentTime + 0.12);
                    osc.start(); osc.stop(audioCtx.currentTime + 0.12);
                } else if (type === 'alert') {
                    osc.type = 'sawtooth'; osc.frequency.setValueAtTime(160, audioCtx.currentTime);
                    gain.gain.setValueAtTime(0.1, audioCtx.currentTime);
                    gain.gain.exponentialRampToValueAtTime(0.00001, audioCtx.currentTime + 0.25);
                    osc.start(); osc.stop(audioCtx.currentTime + 0.25);
                } else if (type === 'click') {
                    osc.type = 'square'; osc.frequency.setValueAtTime(550, audioCtx.currentTime);
                    gain.gain.setValueAtTime(0.03, audioCtx.currentTime);
                    gain.gain.exponentialRampToValueAtTime(0.00001, audioCtx.currentTime + 0.04);
                    osc.start(); osc.stop(audioCtx.currentTime + 0.04);
                }
            }

            function printTerminalLog(msg) {
                const term = document.getElementById('terminal-log');
                const timestamp = new Date().toISOString().slice(11, 19);
                const logNode = document.createElement('div');
                logNode.innerHTML = `<span class="text-cyan-600">[${timestamp}]</span> <span class="text-gray-400">-></span> ${msg}`;
                term.appendChild(logNode);
                term.scrollTop = term.scrollHeight;
            }

            function toggleAudio() {
                isAudioEnabled = !isAudioEnabled;
                const btn = document.getElementById('audio-btn');
                if (isAudioEnabled) {
                    btn.className = "px-3 py-1.5 bg-cyan-950/30 border border-cyan-800/40 hover:border-cyan-500/50 text-cyan-400 font-bold text-xs rounded-lg transition-all flex items-center gap-1.5 font-mono";
                    btn.innerHTML = `<i class="fa-solid fa-volume-high"></i> SFX: ENABLED`;
                    playSoundFx('success');
                } else {
                    btn.className = "px-3 py-1.5 bg-gray-950/40 border border-gray-800 text-gray-500 font-bold text-xs rounded-lg transition-all flex items-center gap-1.5 font-mono";
                    btn.innerHTML = `<i class="fa-solid fa-volume-xmark"></i> SFX: MUTED`;
                }
            }

            function showToast(message, type = 'info') {
                const toast = document.createElement('div');
                let borderTheme = 'border-cyan-500 text-cyan-400';
                if (type === 'success') borderTheme = 'border-emerald-500 text-emerald-400';
                if (type === 'error') borderTheme = 'border-rose-500 text-rose-400';
                
                toast.className = `vip-card px-4 py-2.5 rounded-lg border-l-4 ${borderTheme} shadow-lg font-mono text-xs flex items-center gap-2 transition-all duration-300`;
                toast.innerHTML = `<i class="fa-solid fa-terminal"></i> <span>${message}</span>`;
                
                document.getElementById('toast-container').appendChild(toast);
                setTimeout(() => { toast.style.opacity = '0'; setTimeout(() => toast.remove(), 300); }, 3500);
            }

            particlesJS('particles-js', {
                "particles": {
                    "number": { "value": 45, "density": { "enable": true, "value_area": 900 } },
                    "color": { "value": "#06b6d4" },
                    "opacity": { "value": 0.15, "random": true },
                    "size": { "value": 2 },
                    "line_linked": { "enable": true, "distance": 120, "color": "#0891b2", "opacity": 0.06, "width": 1 },
                    "move": { "enable": true, "speed": 0.8, "direction": "none" }
                }
            });

            async function refreshDashboard(isSilent = false) {
                try {
                    const statsRes = await fetch('/api/stats');
                    const stats = await statsRes.json();
                    document.getElementById('stat-total').innerText = stats.total;
                    document.getElementById('stat-active').innerText = stats.active;
                    document.getElementById('stat-up').innerText = stats.up;
                    document.getElementById('stat-down').innerText = stats.down;
                    document.getElementById('stat-checks').innerText = stats.total_checks;

                    document.getElementById('settings-timeout').value = stats.settings.global_timeout;
                    document.getElementById('settings-refresh').checked = stats.settings.auto_refresh;
                    
                    const refreshBadge = document.getElementById('auto-refresh-badge');
                    if(stats.settings.auto_refresh) {
                        refreshBadge.innerText = "AUTO REFRESH: ON";
                        refreshBadge.className = "px-3 py-1 bg-blue-950/30 border border-blue-900/40 text-blue-400 font-bold text-xs rounded-lg font-mono";
                        initAutoRefreshLoop(true);
                    } else {
                        refreshBadge.innerText = "AUTO REFRESH: OFF";
                        refreshBadge.className = "px-3 py-1 bg-gray-950/40 border border-gray-800 text-gray-500 font-bold text-xs rounded-lg font-mono";
                        initAutoRefreshLoop(false);
                    }

                    const listRes = await fetch('/api/monitors');
                    currentCachedMonitors = await listRes.json();
                    applyFiltersAndSorting();
                    
                    if(!isSilent) {
                        printTerminalLog("Main mainframe cluster metrics verified and fully synchronized.");
                    }
                } catch (err) { 
                    printTerminalLog("CRITICAL ERROR: Failed to request connection to matrix framework gateway."); 
                }
            }

            function setStatusFilter(status) {
                playSoundFx('click');
                activeStatusFilter = status;
                ['ALL', 'UP', 'DOWN', 'STOPPED'].forEach(st => {
                    const el = document.getElementById(`tab-${st}`);
                    if(st === status) {
                        el.className = "px-3 py-1 rounded bg-cyan-950 text-cyan-400 font-bold";
                    } else {
                        el.className = "px-3 py-1 rounded text-gray-500 hover:text-cyan-400";
                    }
                });
                applyFiltersAndSorting();
            }

            function applyFiltersAndSorting() {
                const searchQuery = document.getElementById('search-bar').value.toLowerCase().trim();
                const sortBy = document.getElementById('sort-engine').value;

                let data = [...currentCachedMonitors];

                if(activeStatusFilter !== 'ALL') {
                    data = data.filter(m => m.status === activeStatusFilter);
                }

                if(searchQuery) {
                    data = data.filter(m => m.name.toLowerCase().includes(searchQuery) || m.url.toLowerCase().includes(searchQuery));
                }

                data.sort((a, b) => {
                    if (sortBy === 'name') return a.name.localeCompare(b.name);
                    if (sortBy === 'uptime') return b.uptime - a.uptime;
                    if (sortBy === 'response_time') return b.response_time - a.response_time;
                    return 0;
                });

                renderMatrixGrid(data);
            }

            function renderMatrixGrid(dataList) {
                const container = document.getElementById('monitors-grid');
                container.innerHTML = '';

                if(dataList.length === 0) {
                    container.innerHTML = `<div class="vip-card p-12 rounded-xl text-center text-xs font-mono text-gray-600 tracking-widest uppercase">No target operational sequences detected matching criteria inside mainframe memory core.</div>`;
                    return;
                }

                dataList.forEach(m => {
                    let badgeColor = "bg-yellow-500/10 text-yellow-400 border-yellow-500/30";
                    let pulseIndicator = "text-yellow-400";
                    if(m.status === 'UP') { badgeColor = "bg-emerald-500/10 text-emerald-400 border-emerald-500/20"; pulseIndicator = "text-emerald-400"; }
                    if(m.status === 'DOWN') { badgeColor = "bg-rose-500/10 text-rose-400 border-rose-500/20"; pulseIndicator = "text-rose-500"; }
                    if(m.status === 'STOPPED') { badgeColor = "bg-gray-900 text-gray-500 border-gray-800"; pulseIndicator = "text-gray-600"; }

                    const card = document.createElement('div');
                    card.className = `vip-card p-5 rounded-xl border-l-4 transition-all ${m.status === 'UP' ? 'border-l-emerald-500' : m.status === 'DOWN' ? 'border-l-rose-500' : 'border-l-gray-600'}`;
                    
                    card.innerHTML = `
                        <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-2 mb-3">
                            <div>
                                <h3 class="text-md font-black text-gray-100 tracking-wide flex items-center gap-2 font-mono">${m.name} <span class="text-xs font-bold text-gray-600">[HTTP STATUS: ${m.status_code}]</span></h3>
                                <p class="text-xs font-mono text-cyan-600/70 mt-0.5 break-all">${m.url}</p>
                            </div>
                            <span class="px-3 py-1 font-mono text-[10px] font-black rounded border self-start sm:self-center uppercase tracking-widest ${badgeColor}"><i class="fa-solid fa-circle text-[7px] mr-1.5 ${pulseIndicator} animate-pulse"></i>${m.status}</span>
                        </div>

                        <div class="grid grid-cols-2 sm:grid-cols-4 gap-4 bg-black/40 p-3 rounded-lg text-[11px] font-mono text-gray-400 mb-4 border border-cyan-950/20">
                            <div class="flex items-center gap-1.5"><i class="fa-solid fa-gauge-high text-cyan-400"></i> <span>Latency: <b class="text-white">${m.response_time}ms</b></span></div>
                            <div class="flex items-center gap-1.5"><i class="fa-solid fa-heart-pulse text-emerald-400"></i> <span>Core Health: <b class="text-white">${m.uptime}%</b></span></div>
                            <div class="flex items-center gap-1.5"><i class="fa-solid fa-chart-bar text-amber-500"></i> <span>Pulses: <b class="text-white">${m.success_checks}/${m.total_checks}</b></span></div>
                            <div class="flex items-center gap-1.5"><i class="fa-solid fa-rotate text-blue-400"></i> <span>Routine: <b class="text-white">${m.interval}m</b></span></div>
                        </div>
                        
                        <div class="flex flex-col sm:flex-row justify-between text-[10px] font-mono text-gray-600 mb-4 gap-2">
                            <div><i class="fa-solid fa-microchip"></i> Last Evaluation Sequence: <span class="text-gray-400">${m.last_check}</span></div>
                            <div class="w-24 bg-gray-950 h-1.5 rounded-full overflow-hidden self-center border border-gray-950">
                                <div class="bg-gradient-to-r from-cyan-500 to-blue-500 h-full rounded-full" style="width: ${m.uptime}%"></div>
                            </div>
                        </div>

                        <div class="flex flex-wrap items-center gap-2">
                            <button onclick="toggleChannel('${m.id}')" class="px-3 py-1.5 bg-black/50 border border-gray-800/80 hover:border-gray-600 text-gray-300 font-bold rounded-lg text-xs flex items-center gap-1.5 transition-all font-mono">
                                <i class="fa-solid fa-${m.is_active ? 'pause text-amber-500' : 'play text-emerald-500'}"></i> ${m.is_active ? 'Suspend Node' : 'Activate Node'}
                            </button>
                            <button onclick="forcePing('${m.id}')" ${!m.is_active ? 'disabled class="opacity-30 cursor-not-allowed px-3 py-1.5 bg-black/50 border border-gray-800 text-gray-600 font-bold rounded-lg text-xs flex items-center gap-1.5 font-mono"' : 'class="px-3 py-1.5 bg-cyan-950/30 border border-cyan-900/40 hover:border-cyan-400 text-cyan-400 font-bold rounded-lg text-xs flex items-center gap-1.5 transition-all font-mono"'}>
                                <i class="fa-solid fa-bolt"></i> Manual Pulse Trigger
                            </button>
                            <button onclick="destroyChannel('${m.id}')" class="px-3 py-1.5 bg-rose-950/10 border border-rose-950/30 hover:bg-rose-900/40 text-rose-400 font-bold rounded-lg text-xs flex items-center gap-1.5 transition-all font-mono ml-auto">
                                <i class="fa-solid fa-trash-can"></i> Terminate Channel
                            </button>
                        </div>
                    `;
                    container.appendChild(card);
                });
            }

            async function deployMonitor() {
                playSoundFx('click');
                const name = document.getElementById('mon-name').value.trim();
                const url = document.getElementById('mon-url').value.trim();
                const interval = document.getElementById('mon-interval').value;

                if(!name || !url) { 
                    playSoundFx('alert');
                    showToast('Required payload parameters are missing!', 'error'); 
                    return; 
                }

                printTerminalLog(`Attempting injection sequence for target alias: ${name}`);
                const res = await fetch('/api/monitors', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name, url, interval: parseInt(interval) })
                });

                if(res.ok) {
                    showToast('Target core node injected successfully!', 'success');
                    printTerminalLog(`Target metadata injected successfully to mainframe cluster: [${name}]`);
                    playSoundFx('success');
                }
                document.getElementById('mon-name').value = '';
                document.getElementById('mon-url').value = '';
                refreshDashboard(true);
            }

            async function saveGlobalConfig() {
                playSoundFx('click');
                const timeout = parseInt(document.getElementById('settings-timeout').value);
                const autoRefresh = document.getElementById('settings-refresh').checked;

                const res = await fetch('/api/settings', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ global_timeout: timeout, auto_refresh: autoRefresh })
                });

                if(res.ok) {
                    showToast('Global network environment sync completed!', 'success');
                    printTerminalLog(`Mainframe architecture variables updated. Sync interval refreshed.`);
                    playSoundFx('success');
                    refreshDashboard(true);
                }
            }

            function initAutoRefreshLoop(status) {
                if(autoRefreshIntervalId) clearInterval(autoRefreshIntervalId);
                if(status) {
                    autoRefreshIntervalId = setInterval(() => refreshDashboard(true), 10000);
                }
            }

            async function forcePing(id) {
                playSoundFx('click');
                printTerminalLog(`Triggering immediate diagnostic transmission payload to target ID: ${id}`);
                const res = await fetch(`/api/monitors/${id}/ping`, { method: 'POST' });
                if (res.ok) {
                    showToast('Diagnostic manual pulse routing complete!', 'success');
                    printTerminalLog(`Manual pulse diagnostic routing acknowledged. Memory tables updated.`);
                    playSoundFx('success');
                } else {
                    showToast('Node transmission tracking error!', 'error');
                    playSoundFx('alert');
                }
                refreshDashboard(true);
            }

            async function toggleChannel(id) {
                playSoundFx('click');
                await fetch(`/api/monitors/${id}/toggle`, { method: 'POST' });
                showToast('Target structural runtime execution changed', 'info');
                printTerminalLog(`Structural execution variable altered for allocation node ID: ${id}`);
                refreshDashboard(true);
            }

            async function globalAction(action) {
                playSoundFx('click');
                const res = await fetch(`/api/global/${action}`, { method: 'POST' });
                if (res.ok) {
                    showToast(`All structural core matrices successfully ${action === 'resume' ? 'activated' : 'suspended'}`, 'success');
                    printTerminalLog(`Global execution directive invoked: [${action.toUpperCase()} ALL TRACKING TERMINALS]`);
                    playSoundFx('success');
                }
                refreshDashboard(true);
            }

            async function globalPurge() {
                playSoundFx('alert');
                if(confirm('CRITICAL WARN: Are you sure you want to terminate and wipe ALL active target matrices inside mainframe registry memory?')) {
                    const res = await fetch('/api/global/purge', { method: 'POST' });
                    if(res.ok) {
                        showToast('Mainframe records fully wiped!', 'error');
                        printTerminalLog('SYSTEM COMMAND COMPLETED: Registry dataset wiped out completely.');
                        refreshDashboard(true);
                    }
                }
            }

            async function destroyChannel(id) {
                playSoundFx('alert');
                if(confirm('Are you absolute sure to wipe out this target monitoring node terminal from mainframe registry?')) {
                    await fetch(`/api/monitors/${id}`, { method: 'DELETE' });
                    showToast('Target monitoring terminal wiped!', 'error');
                    printTerminalLog(`Database alteration sequence initialized: Node ID [${id}] completely destroyed.`);
                    refreshDashboard(true);
                }
            }

            function downloadBackup() {
                playSoundFx('click');
                if(currentCachedMonitors.length === 0) return showToast('No dataset rows loaded inside memory cluster to dump!', 'error');
                const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(currentCachedMonitors, null, 2));
                const downloadAnchor = document.createElement('a');
                downloadAnchor.setAttribute("href", dataStr);
                downloadAnchor.setAttribute("download", "matrix_uptime_dump.json");
                document.body.appendChild(downloadAnchor);
                downloadAnchor.click(); downloadAnchor.remove();
                showToast('Database JSON backup downloaded to client storage.', 'success');
            }

            window.onload = () => refreshDashboard(false);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

# --- PRODUCTION RUNNER ENGINE ---
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
