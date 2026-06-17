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

# Global In-Memory Sync
monitors: List[Dict] = load_db()
total_global_checks = sum(m.get('total_checks', 0) for m in monitors)

class MonitorRequest(BaseModel):
    name: str
    url: str
    interval: int = 5

async def check_api_status(monitor: Dict):
    global total_global_checks
    start_time = time.time()
    
    url = monitor['url']
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    async with httpx.AsyncClient(timeout=12.0, follow_redirects=True) as client:
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
    await asyncio.sleep(30) # Delay initial start
    app_url = os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("KOYEB_APP_URL") or "http://localhost:8080"
    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            try:
                await client.get(app_url)
                print(f"[+] Keep-Alive Heartbeat Sent to {app_url}")
            except Exception as e:
                print(f"[-] Keep-Alive Pulse failed: {e}")
            await asyncio.sleep(300) # Self ping every 5 minutes

# Background Monitoring Network Loop
async def monitor_loop():
    while True:
        tasks = []
        current_time = time.time()
        
        for monitor in monitors:
            if monitor.get('is_active', True):
                # Checking interval configuration mapping
                last_run = monitor.get('_last_run_timestamp', 0)
                interval_seconds = monitor.get('interval', 5) * 60
                
                if current_time - last_run >= interval_seconds:
                    monitor['_last_run_timestamp'] = current_time
                    tasks.append(check_api_status(monitor))
                    
        if tasks:
            await asyncio.gather(*tasks)
            save_db(monitors)
            
        await asyncio.sleep(10) # Scanner ticks every 10 seconds checking internal matrices

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Fire core background threads safely
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
        "total_checks": total_global_checks
    }

@app.get("/api/monitors")
async def get_monitors():
    # Strip runtime dynamic fields before sending data to client
    cleaned_monitors = []
    for m in monitors:
        copy_m = m.copy()
        copy_m.pop('_last_run_timestamp', None)
        cleaned_monitors.append(copy_m)
    return cleaned_monitors

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
                raise HTTPException(status_code=400, detail="Cannot ping a suspended channel")
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
                m['_last_run_timestamp'] = 0 # Force immediate loop execution
            save_db(monitors)
            return m
    raise HTTPException(status_code=404, detail="Monitor not found")

@app.delete("/api/monitors/{monitor_id}")
async def delete_monitor(monitor_id: str):
    global monitors
    monitors = [m for m in monitors if m['id'] != monitor_id]
    save_db(monitors)
    return {"success": True}


# --- FRONTEND UI (Natively Embedded HTML Architecture) ---

@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>VIP UPTIME PRO • SPEED_X</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://cdn.jsdelivr.net/npm/particles.js@2.0.0/particles.min.js"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
        <style>
            body { background-color: #030307; color: #f1f5f9; }
            .neon-shadow-cyan { box-shadow: 0 0 20px rgba(6, 182, 212, 0.15); }
            .neon-text-cyan { text-shadow: 0 0 12px rgba(6, 182, 212, 0.7); }
            .vip-card { background: rgba(10, 10, 18, 0.8); border: 1px solid rgba(6, 182, 212, 0.1); backdrop-filter: blur(12px); }
            .vip-card:hover { border-color: rgba(6, 182, 212, 0.3); box-shadow: 0 0 15px rgba(6, 182, 212, 0.1); }
            #particles-js { position: fixed; width: 100%; height: 100%; z-index: -1; top: 0; left: 0; }
            ::-webkit-scrollbar { width: 6px; }
            ::-webkit-scrollbar-track { background: #030307; }
            ::-webkit-scrollbar-thumb { background: #0891b2; border-radius: 10px; }
        </style>
    </head>
    <body class="relative min-h-screen font-sans antialiased">
        
        <div id="particles-js"></div>

        <div class="max-w-5xl mx-auto px-4 py-8 relative z-10">
            <header class="text-center mb-10 border-b border-cyan-950/40 pb-6">
                <div class="inline-block px-3 py-1 bg-cyan-500/10 border border-cyan-500/30 text-cyan-400 text-[10px] font-mono tracking-widest uppercase rounded-full mb-3 shadow-sm">
                    <i class="fa-solid fa-shield-halved animate-pulse mr-1"></i> Centralized Control Core v2.0
                </div>
                <h1 class="text-4xl md:text-5xl font-black tracking-tighter text-cyan-400 neon-text-cyan">VIP UPTIME PRO</h1>
                <p class="text-[11px] text-gray-500 uppercase tracking-widest mt-2 font-mono">Engineered By <span class="text-cyan-400 font-bold">SPEED_X</span> • ROOT CYBER TEAM</p>
            </header>

            <div class="grid grid-cols-2 md:grid-cols-5 gap-4 mb-8">
                <div class="vip-card p-4 rounded-xl text-center border-b-2 border-cyan-500 transition-all duration-300">
                    <h3 class="text-[10px] font-bold text-gray-500 tracking-wider uppercase mb-1 font-mono">Total Matrix</h3>
                    <p id="stat-total" class="text-3xl font-black text-cyan-400 font-mono">0</p>
                </div>
                <div class="vip-card p-4 rounded-xl text-center border-b-2 border-blue-500 transition-all duration-300">
                    <h3 class="text-[10px] font-bold text-gray-500 tracking-wider uppercase mb-1 font-mono">Active Rails</h3>
                    <p id="stat-active" class="text-3xl font-black text-blue-400 font-mono">0</p>
                </div>
                <div class="vip-card p-4 rounded-xl text-center border-b-2 border-emerald-500 transition-all duration-300">
                    <h3 class="text-[10px] font-bold text-gray-500 tracking-wider uppercase mb-1 font-mono">Nodes UP</h3>
                    <p id="stat-up" class="text-3xl font-black text-emerald-400 font-mono">0</p>
                </div>
                <div class="vip-card p-4 rounded-xl text-center border-b-2 border-rose-500 transition-all duration-300">
                    <h3 class="text-[10px] font-bold text-gray-500 tracking-wider uppercase mb-1 font-mono">Nodes DOWN</h3>
                    <p id="stat-down" class="text-3xl font-black text-rose-400 font-mono">0</p>
                </div>
                <div class="vip-card p-4 rounded-xl text-center border-b-2 border-amber-500 col-span-2 md:col-span-1 transition-all duration-300">
                    <h3 class="text-[10px] font-bold text-gray-500 tracking-wider uppercase mb-1 font-mono">Total Pulses</h3>
                    <p id="stat-checks" class="text-3xl font-black text-amber-400 font-mono">0</p>
                </div>
            </div>

            <div class="vip-card p-6 rounded-2xl border border-cyan-500/20 shadow-xl neon-shadow-cyan mb-8">
                <h2 class="text-md font-bold text-cyan-400 mb-4 flex items-center gap-2 font-mono uppercase tracking-wider">
                    <i class="fa-solid fa-circle-plus text-xs text-cyan-400 animate-pulse"></i> Inject Target Monitor Node
                </h2>
                
                <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
                    <div>
                        <label class="block text-[10px] font-bold text-cyan-500/70 mb-1 font-mono uppercase tracking-wider">Identity Alias</label>
                        <input type="text" id="mon-name" placeholder="e.g. Free Fire Automator Bot" class="w-full bg-black/60 border border-gray-800/80 rounded-lg px-4 py-2.5 text-white placeholder-gray-600 focus:outline-none focus:border-cyan-500 text-xs font-mono">
                    </div>
                    <div>
                        <label class="block text-[10px] font-bold text-cyan-500/70 mb-1 font-mono uppercase tracking-wider">Target API Endpoint URL</label>
                        <input type="url" id="mon-url" placeholder="https://my-api-server.onrender.com" class="w-full bg-black/60 border border-gray-800/80 rounded-lg px-4 py-2.5 text-white placeholder-gray-600 focus:outline-none focus:border-cyan-500 text-xs font-mono">
                    </div>
                </div>

                <div class="mb-5">
                    <label class="block text-[10px] font-bold text-cyan-500/70 mb-1 font-mono uppercase tracking-wider">Pinging Frequency Engine Routine</label>
                    <select id="mon-interval" class="w-full bg-black/60 border border-gray-800/80 rounded-lg px-4 py-2.5 text-white focus:outline-none focus:border-cyan-500 text-xs font-mono">
                        <option value="1">Hyper Pulse (Every 1 Minute)</option>
                        <option value="5" selected>Standard VIP Cycle (Every 5 Minutes)</option>
                        <option value="15">Optimized Safe Cycle (Every 15 Minutes)</option>
                        <option value="30">Deep Sleep Standby (Every 30 Minutes)</option>
                    </select>
                </div>

                <button onclick="deployMonitor()" class="w-full bg-gradient-to-r from-cyan-500 to-blue-600 hover:from-cyan-400 hover:to-blue-500 text-black font-black py-3 rounded-xl tracking-widest uppercase text-xs shadow-lg shadow-cyan-500/20 transition-all duration-300">
                    <i class="fa-solid fa-satellite-dish mr-1"></i> Initialize Link Injector
                </button>
            </div>

            <div class="flex flex-col sm:flex-row gap-3 items-center justify-between bg-black/40 border border-cyan-950/30 p-4 rounded-xl mb-6">
                <div class="w-full sm:w-80 relative">
                    <i class="fa-solid fa-fingerprint absolute left-3.5 top-3.5 text-cyan-600/70 text-xs"></i>
                    <input type="text" id="search-bar" oninput="searchMatrix()" placeholder="Scan matrix by identity / url..." class="w-full bg-black border border-gray-950 rounded-lg pl-9 pr-4 py-2 text-xs focus:outline-none focus:border-cyan-500 text-gray-300 font-mono">
                </div>
                <button onclick="downloadBackup()" class="w-full sm:w-auto px-4 py-2 bg-cyan-950/20 border border-cyan-900/50 hover:bg-cyan-900/40 text-cyan-400 font-bold text-xs rounded-md flex items-center justify-center gap-1.5 transition-all font-mono">
                    <i class="fa-solid fa-file-code"></i> Export JSON Dataset
                </button>
            </div>

            <div class="flex items-center justify-between mb-4">
                <h2 class="text-lg font-bold font-mono tracking-wide text-gray-300 flex items-center gap-2"><i class="fa-solid fa-network-wired text-cyan-500"></i> Active Core Nodes</h2>
                <span class="text-[10px] font-mono bg-cyan-950/40 text-cyan-400 border border-cyan-800/40 px-3 py-1 rounded-full font-bold shadow-sm">Sync Status: Online (10s)</span>
            </div>

            <div id="monitors-grid" class="space-y-4"></div>

            <footer class="mt-16 text-center text-[11px] font-mono text-gray-600 border-t border-cyan-950/20 pt-6">
                <p>&copy; 2026 <span class="text-cyan-500/50 font-bold">SPEED_X</span> • ROOT CYBER LABS. CORE RUNTIME IS PERSISTENT.</p>
            </footer>
        </div>

        <script>
            let currentCachedMonitors = [];

            particlesJS('particles-js', {
                "particles": {
                    "number": { "value": 45, "density": { "enable": true, "value_area": 900 } },
                    "color": { "value": "#06b6d4" },
                    "shape": { "type": "circle" },
                    "opacity": { "value": 0.2, "random": true },
                    "size": { "value": 2 },
                    "line_linked": { "enable": true, "distance": 110, "color": "#0891b2", "opacity": 0.1, "width": 1 },
                    "move": { "enable": true, "speed": 1.0, "direction": "none" }
                }
            });

            async function refreshDashboard() {
                try {
                    const statsRes = await fetch('/api/stats');
                    const stats = await statsRes.json();
                    document.getElementById('stat-total').innerText = stats.total;
                    document.getElementById('stat-active').innerText = stats.active;
                    document.getElementById('stat-up').innerText = stats.up;
                    document.getElementById('stat-down').innerText = stats.down;
                    document.getElementById('stat-checks').innerText = stats.total_checks;

                    const listRes = await fetch('/api/monitors');
                    currentCachedMonitors = await listRes.json();
                    renderMatrixGrid(currentCachedMonitors);
                } catch (err) { console.error("Synchronization Failure: ", err); }
            }

            function renderMatrixGrid(dataList) {
                const container = document.getElementById('monitors-grid');
                container.innerHTML = '';

                if(dataList.length === 0) {
                    container.innerHTML = `<div class="vip-card p-10 rounded-xl text-center text-xs font-mono text-gray-500 tracking-wider">No nodes deployed inside current operational sequence.</div>`;
                    return;
                }

                dataList.forEach(m => {
                    let badgeColor = "bg-yellow-500/10 text-yellow-400 border-yellow-500/30";
                    let pulseIndicator = "text-yellow-400";
                    if(m.status === 'UP') { badgeColor = "bg-emerald-500/10 text-emerald-400 border-emerald-500/20"; pulseIndicator = "text-emerald-400"; }
                    if(m.status === 'DOWN') { badgeColor = "bg-rose-500/10 text-rose-400 border-rose-500/20"; pulseIndicator = "text-rose-500"; }
                    if(m.status === 'STOPPED') { badgeColor = "bg-gray-900 text-gray-500 border-gray-800"; pulseIndicator = "text-gray-600"; }

                    const card = document.createElement('div');
                    card.className = `vip-card p-5 rounded-xl border-l-4 transition-all duration-300 ${m.status === 'UP' ? 'border-l-emerald-500' : m.status === 'DOWN' ? 'border-l-rose-500' : 'border-l-gray-600'}`;
                    
                    card.innerHTML = `
                        <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-2 mb-3">
                            <div>
                                <h3 class="text-md font-black text-gray-100 tracking-wide flex items-center gap-2 font-mono">${m.name} <span class="text-xs font-bold font-mono text-gray-600">[HTTP: ${m.status_code}]</span></h3>
                                <p class="text-xs font-mono text-cyan-600/70 mt-0.5 break-all">${m.url}</p>
                            </div>
                            <span class="px-3 py-1 font-mono text-[10px] font-black rounded border self-start sm:self-center uppercase tracking-widest ${badgeColor}"><i class="fa-solid fa-circle text-[7px] mr-1.5 ${pulseIndicator} animate-pulse"></i>${m.status}</span>
                        </div>

                        <div class="grid grid-cols-2 sm:grid-cols-4 gap-4 bg-black/40 p-3 rounded-lg text-[11px] font-mono text-gray-400 mb-4 border border-gray-950">
                            <div class="flex items-center gap-1.5"><i class="fa-solid fa-gauge-high text-cyan-400"></i> <span>Pulse: <b class="text-white">${m.response_time}ms</b></span></div>
                            <div class="flex items-center gap-1.5"><i class="fa-solid fa-shield-heart text-emerald-400"></i> <span>Health: <b class="text-white">${m.uptime}%</b></span></div>
                            <div class="flex items-center gap-1.5"><i class="fa-solid fa-square-poll-vertical text-amber-500"></i> <span>Checks: <b class="text-white">${m.success_checks}/${m.total_checks}</b></span></div>
                            <div class="flex items-center gap-1.5"><i class="fa-solid fa-clock-history text-blue-400"></i> <span>Routine: <b class="text-white">${m.interval}m</b></span></div>
                        </div>
                        
                        <div class="text-[10px] font-mono text-gray-600 mb-3 flex items-center gap-1"><i class="fa-solid fa-timeline"></i> Last Evaluation Check: <span class="text-gray-400">${m.last_check}</span></div>

                        <div class="flex flex-wrap items-center gap-2">
                            <button onclick="toggleChannel('${m.id}')" class="px-3 py-1.5 bg-black/50 border border-gray-800 hover:border-gray-700 text-gray-300 font-bold rounded-lg text-xs flex items-center gap-1.5 transition-all font-mono">
                                <i class="fa-solid fa-${m.is_active ? 'pause text-amber-500' : 'play text-emerald-500'}"></i> ${m.is_active ? 'Suspend' : 'Resume'}
                            </button>
                            <button onclick="forcePing('${m.id}')" ${!m.is_active ? 'disabled class="opacity-30 cursor-not-allowed px-3 py-1.5 bg-black/50 border border-gray-800 text-gray-600 font-bold rounded-lg text-xs flex items-center gap-1.5 font-mono"' : 'class="px-3 py-1.5 bg-cyan-950/30 border border-cyan-900/40 hover:bg-cyan-900/60 text-cyan-400 font-bold rounded-lg text-xs flex items-center gap-1.5 transition-all font-mono"'}>
                                <i class="fa-solid fa-bolt"></i> Manual Pulse
                            </button>
                            <button onclick="destroyChannel('${m.id}')" class="px-3 py-1.5 bg-rose-950/20 border border-rose-950/40 hover:bg-rose-900/30 text-rose-400 font-bold rounded-lg text-xs flex items-center gap-1.5 transition-all font-mono ml-auto">
                                <i class="fa-solid fa-trash-can"></i> Terminate
                            </button>
                        </div>
                    `;
                    container.appendChild(card);
                });
            }

            function searchMatrix() {
                const query = document.getElementById('search-bar').value.toLowerCase().trim();
                const filtered = currentCachedMonitors.filter(m => m.name.toLowerCase().includes(query) || m.url.toLowerCase().includes(query));
                renderMatrixGrid(filtered);
            }

            async function deployMonitor() {
                const name = document.getElementById('mon-name').value.trim();
                const url = document.getElementById('mon-url').value.trim();
                const interval = document.getElementById('mon-interval').value;

                if(!name || !url) { alert('সবগুলো রিকোয়ার্ড ফিল্ড ইনপুট করুন!'); return; }

                await fetch('/api/monitors', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name, url, interval: parseInt(interval) })
                });

                document.getElementById('mon-name').value = '';
                document.getElementById('mon-url').value = '';
                refreshDashboard();
            }

            async function forcePing(id) {
                await fetch(`/api/monitors/${id}/ping`, { method: 'POST' });
                refreshDashboard();
            }

            async function toggleChannel(id) {
                await fetch(`/api/monitors/${id}/toggle`, { method: 'POST' });
                refreshDashboard();
            }

            async function destroyChannel(id) {
                if(confirm('Are you absolute sure to delete this target node?')) {
                    await fetch(`/api/monitors/${id}`, { method: 'DELETE' });
                    refreshDashboard();
                }
            }

            function downloadBackup() {
                if(currentCachedMonitors.length === 0) return alert('No logs found to export!');
                const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(currentCachedMonitors, null, 2));
                const downloadAnchor = document.createElement('a');
                downloadAnchor.setAttribute("href", dataStr);
                downloadAnchor.setAttribute("download", "vip_uptime_backup.json");
                document.body.appendChild(downloadAnchor);
                downloadAnchor.click();
                downloadAnchor.remove();
            }

            setInterval(refreshDashboard, 10000);
            window.onload = refreshDashboard;
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
