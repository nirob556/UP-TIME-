import os
import time
import asyncio
import httpx
from typing import List, Dict
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager

app = FastAPI(title="VIP Uptime Monitor By SPEED_X")

# Database Simulation
monitors: List[Dict] = []
total_global_checks = 0

class MonitorRequest(BaseModel):
    name: str
    url: str
    interval: int = 5

async def check_api_status(monitor: Dict):
    global total_global_checks
    start_time = time.time()
    
    # URL Format clean up
    url = monitor['url']
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        try:
            response = await client.get(url)
            response_time = int((time.time() - start_time) * 1000)
            
            if response.status_code >= 200 and response.status_code < 400:
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
            if monitor['total_checks'] > 0:
                monitor['uptime'] = round((monitor['success_checks'] / monitor['total_checks']) * 100, 2)

# Background Task to Background-ping target loops
async def monitor_loop():
    while True:
        tasks = []
        for monitor in monitors:
            if monitor.get('is_active', True):
                tasks.append(check_api_status(monitor))
        if tasks:
            await asyncio.gather(*tasks)
        await asyncio.sleep(60) # Run system status checks every 1 minute

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start loop task on startup
    asyncio.create_task(monitor_loop())
    yield

app.router.lifespan_context = lifespan

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
    return monitors

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
        "is_active": True
    }
    monitors.append(new_monitor)
    await check_api_status(new_monitor) # Run instant first check
    return new_monitor

@app.post("/api/monitors/{monitor_id}/toggle")
async def toggle_monitor(monitor_id: str):
    for m in monitors:
        if m['id'] == monitor_id:
            m['is_active'] = not m['is_active']
            if not m['is_active']:
                m['status'] = 'STOPPED'
            return m
    raise HTTPException(status_code=404, detail="Monitor not found")

@app.delete("/api/monitors/{monitor_id}")
async def delete_monitor(monitor_id: str):
    global monitors
    monitors = [m for m in monitors if m['id'] != monitor_id]
    return {"success": True}


# --- FRONTEND UI (HTML embedded natively) ---

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
            body { background-color: #06060c; color: #f1f5f9; }
            .neon-shadow-cyan { box-shadow: 0 0 15px rgba(6, 182, 212, 0.3); }
            .neon-text-cyan { text-shadow: 0 0 10px rgba(6, 182, 212, 0.6); }
            .vip-card { background: rgba(15, 15, 27, 0.85); border: 1px solid rgba(255,255,255,0.05); backdrop-filter: blur(8px); }
            #particles-js { position: fixed; width: 100%; height: 100%; z-index: -1; top: 0; left: 0; }
        </style>
    </head>
    <body class="relative min-h-screen font-sans">
        
        <div id="particles-js"></div>

        <div class="max-w-4xl mx-auto px-4 py-8 relative z-10">
            <header class="text-center mb-10 border-b border-gray-800 pb-6">
                <h1 class="text-4xl font-extrabold tracking-wider text-cyan-400 neon-text-cyan">VIP UPTIME PRO</h1>
                <p class="text-xs text-gray-500 uppercase tracking-widest mt-2">Engineered by <span class="text-cyan-400 font-bold">SPEED_X</span></p>
            </header>

            <div class="grid grid-cols-2 md:grid-cols-5 gap-4 mb-8">
                <div class="vip-card p-4 rounded-xl text-center border-b-2 border-cyan-500 transition-all duration-300 hover:scale-105">
                    <h3 class="text-xs font-bold text-gray-500 tracking-wider uppercase mb-1">Total</h3>
                    <p id="stat-total" class="text-2xl font-black text-cyan-400">0</p>
                </div>
                <div class="vip-card p-4 rounded-xl text-center border-b-2 border-blue-500 transition-all duration-300 hover:scale-105">
                    <h3 class="text-xs font-bold text-gray-500 tracking-wider uppercase mb-1">Active</h3>
                    <p id="stat-active" class="text-2xl font-black text-blue-400">0</p>
                </div>
                <div class="vip-card p-4 rounded-xl text-center border-b-2 border-green-500 transition-all duration-300 hover:scale-105">
                    <h3 class="text-xs font-bold text-gray-500 tracking-wider uppercase mb-1">System UP</h3>
                    <p id="stat-up" class="text-2xl font-black text-green-400">0</p>
                </div>
                <div class="vip-card p-4 rounded-xl text-center border-b-2 border-red-500 transition-all duration-300 hover:scale-105">
                    <h3 class="text-xs font-bold text-gray-500 tracking-wider uppercase mb-1">System DOWN</h3>
                    <p id="stat-down" class="text-2xl font-black text-red-500">0</p>
                </div>
                <div class="vip-card p-4 rounded-xl text-center border-b-2 border-yellow-500 col-span-2 md:col-span-1 transition-all duration-300 hover:scale-105">
                    <h3 class="text-xs font-bold text-gray-500 tracking-wider uppercase mb-1">Total Checks</h3>
                    <p id="stat-checks" class="text-2xl font-black text-yellow-400">0</p>
                </div>
            </div>

            <div class="vip-card p-6 rounded-2xl border border-cyan-500/20 shadow-xl neon-shadow-cyan mb-10">
                <h2 class="text-lg font-bold text-cyan-400 mb-4 flex items-center gap-2"><i class="fa-solid fa-plus-cube"></i> Deploy New Live Monitor</h2>
                
                <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
                    <div>
                        <label class="block text-xs font-bold text-cyan-500/70 mb-1 uppercase">Monitor Identity</label>
                        <input type="text" id="mon-name" placeholder="e.g. Free Fire API Bot" class="w-full bg-black/50 border border-gray-800 rounded-lg px-4 py-2.5 text-white placeholder-gray-600 focus:outline-none focus:border-cyan-400 text-sm">
                    </div>
                    <div>
                        <label class="block text-xs font-bold text-cyan-500/70 mb-1 uppercase">Target Target Endpoint (URL)</label>
                        <input type="url" id="mon-url" placeholder="https://like-api.onrender.com" class="w-full bg-black/50 border border-gray-800 rounded-lg px-4 py-2.5 text-white placeholder-gray-600 focus:outline-none focus:border-cyan-400 text-sm">
                    </div>
                </div>

                <div class="mb-5">
                    <label class="block text-xs font-bold text-cyan-500/70 mb-1 uppercase">Pinging Routine Frequency</label>
                    <select id="mon-interval" class="w-full bg-black/50 border border-gray-800 rounded-lg px-4 py-2.5 text-white focus:outline-none focus:border-cyan-400 text-sm">
                        <option value="1">Continuous Pulse (Every 1 Minute)</option>
                        <option value="5" selected>Standard VIP Loop (Every 5 Minutes)</option>
                        <option value="15">Idle Safe Mode (Every 15 Minutes)</option>
                    </select>
                </div>

                <button onclick="deployMonitor()" class="w-full bg-gradient-to-r from-cyan-500 to-blue-600 hover:from-cyan-400 hover:to-blue-500 text-black font-black py-3 rounded-xl tracking-wider uppercase text-xs shadow-lg shadow-cyan-500/20 transition-all">
                    <i class="fa-solid fa-satellite-dish mr-2"></i> Inject Monitor Channel
                </button>
            </div>

            <div class="flex items-center justify-between mb-4">
                <h2 class="text-xl font-black text-gray-300 flex items-center gap-2"><i class="fa-solid fa-network-wired text-cyan-400"></i> Active Core Matrices</h2>
                <span class="text-xs bg-cyan-900/40 text-cyan-400 border border-cyan-800 px-3 py-1 rounded-full font-bold">Auto-Sync Active (10s)</span>
            </div>

            <div id="monitors-grid" class="space-y-4">
                </div>

            <footer class="mt-16 text-center text-xs text-gray-600 border-t border-gray-950 pt-6">
                <p>&copy; 2026 <span class="text-cyan-500/60 font-bold">SPEED_X</span> • ROOT CYBER LABS. ALL CODE IS SECURED.</p>
            </footer>
        </div>

        <script>
            // Particle system background matrix loop
            particlesJS('particles-js', {
                "particles": {
                    "number": { "value": 35, "density": { "enable": true, "value_area": 800 } },
                    "color": { "value": "#06b6d4" },
                    "shape": { "type": "circle" },
                    "opacity": { "value": 0.25, "random": true },
                    "size": { "value": 2.5 },
                    "line_linked": { "enable": true, "distance": 120, "color": "#0891b2", "opacity": 0.12, "width": 1 },
                    "move": { "enable": true, "speed": 1.2, "direction": "none" }
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
                    const monitors = await listRes.json();
                    const container = document.getElementById('monitors-grid');
                    container.innerHTML = '';

                    if(monitors.length === 0) {
                        container.innerHTML = `<div class="vip-card p-8 rounded-xl text-center text-sm text-gray-500 tracking-wide">No internal target nodes deployed inside runtime network.</div>`;
                        return;
                    }

                    monitors.forEach(m => {
                        let badgeColor = "bg-yellow-500/10 text-yellow-400 border-yellow-500/30";
                        if(m.status === 'UP') badgeColor = "bg-green-500/10 text-green-400 border-green-500/30";
                        if(m.status === 'DOWN') badgeColor = "bg-red-500/10 text-red-500/30 border-red-500/40";
                        if(m.status === 'STOPPED') badgeColor = "bg-gray-800 text-gray-500 border-gray-700";

                        const card = document.createElement('div');
                        card.className = `vip-card p-5 rounded-xl border-l-4 shadow-md transition-all duration-300 ${m.status === 'UP' ? 'border-l-green-500' : m.status === 'DOWN' ? 'border-l-red-500' : 'border-l-gray-600'}`;
                        
                        card.innerHTML = `
                            <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-2 mb-3">
                                <div>
                                    <h3 class="text-md font-extrabold text-white tracking-wide flex items-center gap-2">${m.name} <span class="text-xs font-mono text-gray-600">[HTTP: ${m.status_code}]</span></h3>
                                    <p class="text-xs font-mono text-cyan-600/80 mt-0.5 break-all">${m.url}</p>
                                </div>
                                <span class="px-3 py-1 font-mono text-xs font-black rounded-md border self-start sm:self-center ${badgeColor}">${m.status}</span>
                            </div>

                            <div class="grid grid-cols-2 sm:grid-cols-4 gap-4 bg-black/30 p-3 rounded-lg text-xs font-mono text-gray-400 mb-4">
                                <div class="flex items-center gap-1.5"><i class="fa-solid fa-gauge-high text-cyan-400"></i> <span>Pulse: <b class="text-white">${m.response_time}ms</b></span></div>
                                <div class="flex items-center gap-1.5"><i class="fa-solid fa-shield-heart text-green-400"></i> <span>Uptime: <b class="text-white">${m.uptime}%</b></span></div>
                                <div class="flex items-center gap-1.5"><i class="fa-solid fa-rotate text-yellow-500"></i> <span>Checks: <b class="text-white">${m.success_checks}/${m.total_checks}</b></span></div>
                                <div class="flex items-center gap-1.5"><i class="fa-solid fa-hourglass-start text-blue-400"></i> <span>Loop: <b class="text-white">${m.interval}m</b></span></div>
                            </div>

                            <div class="flex items-center gap-3">
                                <button onclick="toggleChannel('${m.id}')" class="px-4 py-1.5 bg-gray-900 border border-gray-800 hover:border-gray-700 text-gray-300 font-bold rounded-lg text-xs flex items-center gap-1.5 transition-all">
                                    <i class="fa-solid fa-${m.is_active ? 'pause text-amber-500' : 'play text-green-500'}"></i> ${m.is_active ? 'Suspend' : 'Resume'}
                                </button>
                                <button onclick="destroyChannel('${m.id}')" class="px-4 py-1.5 bg-red-950/40 border border-red-900/30 hover:bg-red-900/50 text-red-400 font-bold rounded-lg text-xs flex items-center gap-1.5 transition-all">
                                    <i class="fa-solid fa-trash-can"></i> Terminate
                                </button>
                            </div>
                        `;
                        container.appendChild(card);
                    });
                } catch (err) { console.error("Sync Error: ", err); }
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

            setInterval(refreshDashboard, 10000);
            window.onload = refreshDashboard;
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)
