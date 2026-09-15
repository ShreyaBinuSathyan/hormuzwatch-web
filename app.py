import streamlit as st
import asyncio
import websockets
import json
import threading
import time
from collections import deque
from datetime import datetime, timezone
import pandas as pd
import plotly.express as px
import base64
import os

st.set_page_config(page_title="HormuzWatch", page_icon="⚓", layout="wide")

# ============================================================
# CONFIG
# ============================================================
API_KEY = st.secrets["AISSTREAM_KEY"]
DEMO_BOX = [[[1.0, 103.4], [1.35, 104.2]]]  # Singapore Strait — validated corridor
DAY_RATE = 50000
BARRELS = 2_000_000
INTEREST = 0.06
OIL_PRICE = 70
BASELINE_SPEED = 8.7
MAX_HISTORY = 5000

# ============================================================
# PERSISTENT BACKGROUND COLLECTOR
# Starts once, keeps running forever in a background thread,
# independent of page loads. This is the real "live backend".
# ============================================================
@st.cache_resource
def get_shared_store():
    return {"positions": deque(maxlen=MAX_HISTORY), "status": "starting", "started_at": datetime.now(timezone.utc)}

store = get_shared_store()

def collector_loop(store):
    async def run():
        url = "wss://stream.aisstream.io/v0/stream"
        while True:
            try:
                async with websockets.connect(url) as ws:
                    sub = {"APIKey": API_KEY, "BoundingBoxes": DEMO_BOX, "FilterMessageTypes": ["PositionReport"]}
                    await ws.send(json.dumps(sub))
                    store["status"] = "connected"
                    async for message in ws:
                        data = json.loads(message)
                        if data.get("MessageType") != "PositionReport":
                            continue
                        r = data["Message"]["PositionReport"]
                        m = data.get("MetaData", {})
                        store["positions"].append({
                            "time_utc": datetime.now(timezone.utc),
                            "ship_name": (m.get("ShipName") or "Unnamed").strip(),
                            "lat": r.get("Latitude"),
                            "lon": r.get("Longitude"),
                            "speed_knots": r.get("Sog"),
                        })
            except Exception as e:
                store["status"] = f"reconnecting ({e})"
                await asyncio.sleep(8)
    asyncio.run(run())

@st.cache_resource
def start_collector_thread():
    t = threading.Thread(target=collector_loop, args=(store,), daemon=True)
    t.start()
    return t

start_collector_thread()

# ============================================================
# STYLING
# ============================================================
def img_b64(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()

ICONS = {k: img_b64(f"icon_{k}.png") for k in ["ship","cost","gauge","barrel","live"]}

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Manrope:wght@500;700;800&family=Inter:wght@400;500;600&display=swap');
html, body, [class*="css"] {{ font-family: 'Inter', sans-serif; }}
.stApp {{ background-color: #F7F5F0; }}
#MainMenu, footer, header {{visibility: hidden;}}

.hero {{
    background: linear-gradient(135deg, #14202E 0%, #1F3347 100%);
    padding: 2.6rem 2.8rem; border-radius: 16px; margin-bottom: 1.6rem;
    box-shadow: 0 10px 28px rgba(20,32,46,0.28);
    display: flex; align-items: center; justify-content: space-between;
}}
.hero h1 {{ font-family: 'Manrope', sans-serif; color: #FFFFFF; font-size: 2.5rem; font-weight: 800; margin: 0; letter-spacing: -0.5px; }}
.hero p {{ color: #C9A34E; font-size: 1.05rem; margin-top: 0.3rem; font-weight: 500; }}
.hero .tagline {{ color: #A8B5C2; font-size: 0.88rem; margin-top: 0.6rem; font-style: italic; max-width: 480px; }}
.hero-icon img {{ width: 90px; opacity: 0.92; }}

.kpi-row {{ display: flex; gap: 1rem; margin-bottom: 1.5rem; }}
.kpi-card {{
    background: white; border-radius: 14px; padding: 1.3rem 1.4rem; flex: 1;
    border-top: 3px solid #B8894A; box-shadow: 0 3px 12px rgba(20,32,46,0.07);
    display: flex; align-items: center; gap: 0.9rem;
}}
.kpi-card.highlight {{ background: linear-gradient(135deg, #14202E, #2C4459); }}
.kpi-icon img {{ width: 38px; height: 38px; }}
.kpi-label {{ font-family: 'Manrope', sans-serif; font-size: 0.7rem; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; color: #8A94A3; }}
.kpi-card.highlight .kpi-label {{ color: #C9A34E; }}
.kpi-value {{ font-family: 'Manrope', sans-serif; font-size: 1.85rem; font-weight: 800; color: #14202E; margin-top: 0.1rem; }}
.kpi-card.highlight .kpi-value {{ color: #FFFFFF; }}

.section-title {{ font-family: 'Manrope', sans-serif; font-weight: 700; font-size: 1.15rem; color: #14202E; border-left: 4px solid #B8894A; padding-left: 0.7rem; margin: 1.6rem 0 0.8rem 0; }}
.panel {{ background: white; border-radius: 14px; padding: 1.4rem; box-shadow: 0 3px 12px rgba(20,32,46,0.07); }}

.live-badge {{ display: inline-flex; align-items: center; background: #E8F3EC; color: #2E7D4F; font-size: 0.8rem; font-weight: 700; padding: 5px 14px; border-radius: 20px; }}
.live-badge img {{ width: 16px; margin-right: 6px; }}

.status-pill {{ display:inline-block; background:#EEF1F8; color:#14202E; font-size:0.75rem; font-weight:600; padding:4px 10px; border-radius:14px; margin-left:0.6rem; }}
</style>
""", unsafe_allow_html=True)

# ============================================================
# HERO
# ============================================================
st.markdown(f"""
<div class="hero">
    <div>
        <h1>HormuzWatch</h1>
        <p>A real-time cost-of-disruption index for the world's chokepoints</p>
        <div class="tagline">Free vessel data, converted into one decision-ready number — collected continuously by a live background process, not on demand.</div>
    </div>
    <div class="hero-icon"><img src="data:image/png;base64,{ICONS['ship']}"></div>
</div>
""", unsafe_allow_html=True)

positions = list(store["positions"])
uptime = (datetime.now(timezone.utc) - store["started_at"]).total_seconds()
uptime_str = f"{int(uptime//60)} min" if uptime < 3600 else f"{uptime/3600:.1f} hr"

st.markdown(f"""
<span class="live-badge"><img src="data:image/png;base64,{ICONS['live']}">LIVE BACKEND — {store['status']}</span>
<span class="status-pill">Collector uptime: {uptime_str}</span>
<span class="status-pill">{len(positions)} positions in memory</span>
""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ============================================================
# COMPUTE METRICS
# ============================================================
if len(positions) < 5:
    st.info("Backend is still collecting its first live positions — refresh in a few seconds.")
else:
    df = pd.DataFrame(positions)
    clean = df[(df['speed_knots'] < 40) & (df['speed_knots'] > 0.5)]
    recent = clean.tail(200)
    current_speed = recent['speed_knots'].median() if len(recent) else 0
    score = max(0, min(1, (BASELINE_SPEED - current_speed) / BASELINE_SPEED))
    n_ships = df['ship_name'].nunique()

    charter_hr = DAY_RATE / 24
    holding_hr = (BARRELS * OIL_PRICE * INTEREST) / 365 / 24
    cost_hr = n_ships * (charter_hr + holding_hr) * score

    st.markdown(f"""
    <div class="kpi-row">
        <div class="kpi-card highlight">
            <div class="kpi-icon"><img src="data:image/png;base64,{ICONS['cost']}"></div>
            <div><div class="kpi-label">COST OF DISRUPTION</div><div class="kpi-value">${cost_hr:,.0f}/hr</div></div>
        </div>
        <div class="kpi-card">
            <div class="kpi-icon"><img src="data:image/png;base64,{ICONS['gauge']}"></div>
            <div><div class="kpi-label">SLOWDOWN SCORE</div><div class="kpi-value">{score:.2f}</div></div>
        </div>
        <div class="kpi-card">
            <div class="kpi-icon"><img src="data:image/png;base64,{ICONS['ship']}"></div>
            <div><div class="kpi-label">VESSELS TRACKED</div><div class="kpi-value">{n_ships}</div></div>
        </div>
        <div class="kpi-card">
            <div class="kpi-icon"><img src="data:image/png;base64,{ICONS['barrel']}"></div>
            <div><div class="kpi-label">OIL PRICE (assumed)</div><div class="kpi-value">${OIL_PRICE}/bbl</div></div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ---------------- MAP ----------------
    st.markdown('<div class="section-title">Live vessel positions</div>', unsafe_allow_html=True)
    map_df = df.drop_duplicates(subset="ship_name", keep="last")
    map_df = map_df.dropna(subset=["lat", "lon"])
    if len(map_df) > 0:
        st.map(map_df.rename(columns={"lat": "latitude", "lon": "longitude"}),
               latitude="latitude", longitude="longitude", size=20, color="#B8894A")
    else:
        st.info("Waiting for enough live positions to plot the map.")

    # ---------------- TREND ----------------
    st.markdown('<div class="section-title">Score trend since collector started</div>', unsafe_allow_html=True)
    trend = clean.set_index("time_utc").resample("1min")["speed_knots"].median().dropna()
    trend_score = ((BASELINE_SPEED - trend) / BASELINE_SPEED).clip(0, 1)
    fig2 = px.line(trend_score, height=280)
    fig2.update_traces(line_color="#B8894A", line_width=3)
    fig2.update_layout(showlegend=False, paper_bgcolor="white", plot_bgcolor="white",
                        margin=dict(l=0,r=0,t=10,b=0), yaxis_title="Slowdown Score", xaxis_title="")
    st.plotly_chart(fig2, use_container_width=True)

    st.markdown(f"""
    <div class="section-title">Method & honest limitations</div>
    <div class="panel">
    A background process connects to aisstream.io and has been collecting continuously since this
    app started — independent of page views. Demonstrated on the Singapore Strait because free AIS
    coverage over the Gulf proved too sparse during testing (a finding documented in our report).
    The same code applies directly to the Strait of Hormuz once a commercial or satellite AIS feed
    is added.
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br><div style='text-align:center; color:#8A94A3; font-size:0.8rem;'>HormuzWatch — MBA Digital Transformation Project, IIM Jammu</div>", unsafe_allow_html=True)
