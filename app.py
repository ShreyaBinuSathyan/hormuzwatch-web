import streamlit as st
import asyncio
import websockets
import json
import threading
from collections import deque
from datetime import datetime, timezone
import pandas as pd
import base64

st.set_page_config(page_title="Waypoint", page_icon="⚓", layout="wide")

# ============================================================
# CORRIDOR DEFINITIONS — organized by region, tested against real data
# Each box is [[lat1,lon1],[lat2,lon2]] — ONE box, used in its OWN
# connection (the pattern proven to work throughout testing).
# ============================================================
CORRIDORS_BY_REGION = {
    "Europe": {
        "Dover Strait": {"box": [[50.7, 1.2], [51.2, 2.0]], "live": True},
        "Strait of Gibraltar": {"box": [[35.8, -5.8], [36.2, -5.1]], "live": True},
        "Kiel Canal": {"box": [[54.3, 9.6], [54.4, 10.2]], "live": True},
        "Öresund Strait": {"box": [[55.5, 12.6], [56.0, 12.9]], "live": True},
        "Bosphorus Strait": {"box": [[40.9, 28.9], [41.3, 29.2]], "live": False},
        "Dardanelles Strait": {"box": [[40.0, 26.0], [40.4, 26.7]], "live": False},
    },
    "Middle East / Gulf": {
        "Strait of Hormuz": {"box": [[26.0, 56.0], [26.6, 56.6]], "live": False},
        "Bab-el-Mandeb": {"box": [[12.3, 43.2], [12.8, 43.6]], "live": False},
        "Suez Canal": {"box": [[29.8, 32.4], [31.3, 32.6]], "live": False},
    },
    "Asia-Pacific": {
        "Singapore Strait": {"box": [[1.0, 103.4], [1.35, 104.2]], "live": True},
        "Strait of Malacca": {"box": [[2.0, 100.0], [5.0, 101.5]], "live": False},
        "Sunda Strait": {"box": [[-6.2, 105.5], [-5.5, 106.2]], "live": False},
        "Lombok Strait": {"box": [[-9.0, 115.7], [-8.3, 116.1]], "live": False},
        "Taiwan Strait": {"box": [[23.5, 118.5], [25.5, 120.0]], "live": False},
        "Korea Strait": {"box": [[34.0, 128.5], [35.2, 129.5]], "live": False},
        "Tsugaru Strait": {"box": [[41.3, 139.8], [41.7, 141.0]], "live": False},
    },
    "Americas": {
        "Panama Canal": {"box": [[8.8, -79.8], [9.4, -79.4]], "live": False},
        "Strait of Magellan": {"box": [[-53.8, -72.0], [-52.4, -68.5]], "live": False},
        "Bering Strait": {"box": [[64.5, -169.5], [66.2, -168.0]], "live": False},
    },
    "Africa": {
        "Mozambique Channel": {"box": [[-26.0, 32.5], [-25.5, 33.2]], "live": False},
    },
}

LIVE_CORRIDORS = {name: c for region in CORRIDORS_BY_REGION.values()
                   for name, c in region.items() if c["live"]}

BARRELS = 2_000_000
INTEREST = 0.06
MAX_HISTORY_PER_CORRIDOR = 1500

# ============================================================
# PERSISTENT BACKGROUND COLLECTOR
# One independent connection per live corridor, each using the
# exact single-box subscription pattern already proven to work.
# ============================================================
@st.cache_resource
def get_shared_store():
    return {
        "positions": {name: deque(maxlen=MAX_HISTORY_PER_CORRIDOR) for name in LIVE_CORRIDORS},
        "status": {name: "starting" for name in LIVE_CORRIDORS},
        "started_at": datetime.now(timezone.utc),
    }

store = get_shared_store()

def collector_loop(store, api_key):
    async def watch_corridor(name, box):
        url = "wss://stream.aisstream.io/v0/stream"
        single_box = [box]  # wrap as list-of-one-box — the proven working shape
        while True:
            try:
                async with websockets.connect(url) as ws:
                    sub = {"APIKey": api_key, "BoundingBoxes": single_box,
                           "FilterMessageTypes": ["PositionReport"]}
                    await ws.send(json.dumps(sub))
                    store["status"][name] = "connected"
                    async for message in ws:
                        data = json.loads(message)
                        if data.get("MessageType") != "PositionReport":
                            continue
                        r = data["Message"]["PositionReport"]
                        m = data.get("MetaData", {})
                        store["positions"][name].append({
                            "time_utc": datetime.now(timezone.utc),
                            "ship_name": (m.get("ShipName") or "Unnamed").strip(),
                            "lat": r.get("Latitude"), "lon": r.get("Longitude"),
                            "speed_knots": r.get("Sog"),
                        })
            except Exception as e:
                store["status"][name] = f"reconnecting"
                await asyncio.sleep(6)

    async def run_all():
        await asyncio.gather(*[
            watch_corridor(name, cdata["box"]) for name, cdata in LIVE_CORRIDORS.items()
        ])

    asyncio.run(run_all())

@st.cache_resource
def start_collector_thread():
    api_key = st.secrets["AISSTREAM_KEY"]
    t = threading.Thread(target=collector_loop, args=(store, api_key), daemon=True)
    t.start()
    return t

start_collector_thread()

# ============================================================
# STYLING — explicit, high-specificity rules for text visibility,
# including Streamlit's selectbox internals (data-baseweb targets)
# ============================================================
def img_b64(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()

ICONS = {k: img_b64(f"icon_{k}.png") for k in ["ship", "cost", "gauge", "barrel", "live"]}
INK = "#14202E"

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Manrope:wght@500;700;800&family=Inter:wght@400;500;600&display=swap');
html, body, [class*="css"] {{ font-family: 'Inter', sans-serif; }}
.stApp {{ background-color: #F7F5F0; }}
#MainMenu, footer, header {{visibility: hidden;}}

/* base text color everywhere */
p, span, div, li, label {{ color: {INK}; }}

/* --- fix: selectbox closed value + open menu options (both light & dark theme) --- */
div[data-baseweb="select"] > div {{ background-color: #FFFFFF !important; border: 1px solid #D6DBE8 !important; }}
div[data-baseweb="select"] * {{ color: {INK} !important; }}
div[data-baseweb="popover"] {{ background-color: #FFFFFF !important; }}
ul[data-baseweb="menu"] {{ background-color: #FFFFFF !important; }}
ul[data-baseweb="menu"] li {{ color: {INK} !important; background-color: #FFFFFF !important; }}
ul[data-baseweb="menu"] li:hover {{ background-color: #EEF1F8 !important; }}

/* --- fix: hero title, isolated inline-safe rule --- */
.hero-title {{ font-family:'Manrope',sans-serif !important; color:#FFFFFF !important;
    font-size:2.3rem !important; font-weight:800 !important; margin:0 !important; }}
.hero-sub {{ color:#C9A34E !important; font-size:1rem !important; margin-top:0.3rem !important; font-weight:500 !important; }}
.hero-tag {{ color:#A8B5C2 !important; font-size:0.85rem !important; margin-top:0.5rem !important;
    font-style:italic !important; max-width:480px !important; }}

.hero {{ background: linear-gradient(135deg, #14202E 0%, #1F3347 100%); padding: 2.2rem 2.5rem;
    border-radius: 16px; margin-bottom: 1.1rem; box-shadow: 0 10px 28px rgba(20,32,46,0.28);
    display: flex; align-items: center; justify-content: space-between; }}
.hero-icon img {{ width: 65px; opacity: 0.92; }}

.section-title {{ font-family: 'Manrope', sans-serif; font-weight: 700; font-size: 1.1rem; color: {INK} !important;
    border-left: 4px solid #B8894A; padding-left: 0.7rem; margin: 1.4rem 0 0.7rem 0; }}

.kpi-row {{ display: flex; gap: 1rem; margin-bottom: 0.5rem; }}
.kpi-card {{ background: white; border-radius: 14px; padding: 1.1rem 1.2rem; flex: 1;
    border-top: 3px solid #B8894A; box-shadow: 0 3px 12px rgba(20,32,46,0.07);
    display: flex; align-items: center; gap: 0.8rem; }}
.kpi-card.highlight {{ background: linear-gradient(135deg, #14202E, #2C4459); }}
.kpi-icon img {{ width: 30px; height: 30px; }}
.kpi-label {{ font-family: 'Manrope', sans-serif; font-size: 0.65rem; font-weight: 700; letter-spacing: 1px;
    text-transform: uppercase; color: #8A94A3 !important; }}
.kpi-card.highlight .kpi-label {{ color: #C9A34E !important; }}
.kpi-value {{ font-family: 'Manrope', sans-serif; font-size: 1.5rem; font-weight: 800; color: {INK} !important; }}
.kpi-card.highlight .kpi-value {{ color: #FFFFFF !important; }}

.panel {{ background: white; border-radius: 14px; padding: 1.3rem; box-shadow: 0 3px 12px rgba(20,32,46,0.07); }}
.panel, .panel p {{ color: {INK} !important; }}
.panel.amber {{ background: #FBF3E4; border: 1px solid #E0C48A; }}
.panel.amber, .panel.amber p {{ color: #7A4A18 !important; }}

.live-badge {{ display: inline-flex; align-items: center; background: #E8F3EC; color: #2E7D4F !important;
    font-size: 0.78rem; font-weight: 700; padding: 5px 14px; border-radius: 20px; }}
.live-badge img {{ width: 15px; margin-right: 6px; }}
.status-pill {{ display:inline-block; background:#EEF1F8; color:{INK} !important; font-size:0.73rem;
    font-weight:600; padding:4px 10px; border-radius:14px; margin-left:0.5rem; }}
.ship-count {{ font-family:'Manrope',sans-serif; font-weight:700; color:#B8894A !important; font-size:0.95rem; }}
</style>
""", unsafe_allow_html=True)

# ============================================================
# HERO
# ============================================================
st.markdown(f"""
<div class="hero">
    <div>
        <div class="hero-title">Waypoint</div>
        <div class="hero-sub">A real-time cost-of-disruption index for the world's chokepoints</div>
        <div class="hero-tag">Free vessel data, converted into one decision-ready number for the mid-market
        shippers, insurers and traders enterprise platforms price out.</div>
    </div>
    <div class="hero-icon"><img src="data:image/png;base64,{ICONS['ship']}"></div>
</div>
""", unsafe_allow_html=True)

uptime = (datetime.now(timezone.utc) - store["started_at"]).total_seconds()
uptime_str = f"{int(uptime//60)} min" if uptime < 3600 else f"{uptime/3600:.1f} hr"
total_positions = sum(len(v) for v in store["positions"].values())
connected_count = sum(1 for s in store["status"].values() if s == "connected")
st.markdown(f"""
<span class="live-badge"><img src="data:image/png;base64,{ICONS['live']}">LIVE BACKEND — {connected_count}/{len(LIVE_CORRIDORS)} corridors connected</span>
<span class="status-pill">Collector uptime: {uptime_str}</span>
<span class="status-pill">{total_positions} positions across {len(LIVE_CORRIDORS)} live corridors</span>
""", unsafe_allow_html=True)

# ============================================================
# STEP 1 — REGION, THEN CORRIDOR
# ============================================================
st.markdown('<div class="section-title">1. Choose a region, then a corridor</div>', unsafe_allow_html=True)
c1, c2 = st.columns(2)
with c1:
    region = st.selectbox("Region", list(CORRIDORS_BY_REGION.keys()))
with c2:
    corridor_opts = CORRIDORS_BY_REGION[region]
    labels = [f"{name}  {'🟢 LIVE' if c['live'] else '⚪ Coming soon'}" for name, c in corridor_opts.items()]
    label_to_name = dict(zip(labels, corridor_opts.keys()))
    chosen_label = st.selectbox("Corridor", labels)
    corridor = label_to_name[chosen_label]

info = corridor_opts[corridor]

# ============================================================
# STEP 2 — LIVE INDEX (or explainer)
# ============================================================
st.markdown(f'<div class="section-title">2. {corridor} — current status</div>', unsafe_allow_html=True)

df = None
ship_list = []
live_score = 0.35
live_ships = 25
charter_hr = 50000 / 24
oil_price = 70
holding_hr = (BARRELS * oil_price * INTEREST) / 365 / 24

if not info["live"]:
    st.markdown(f"""
    <div class="panel amber">
    <b>Coming soon.</b> Free community AIS coverage doesn't reach this corridor — a full global scan found
    zero live positions here, matching a clear pattern: dense coverage over Europe and Southeast Asia,
    near-zero over the Gulf, Suez, and most of Asia-Pacific outside Singapore. The same engine activates
    the moment a licensed or satellite AIS feed is connected. Use the scenario calculator below, or switch
    to a 🟢 LIVE corridor above to see the real thing.
    </div>
    """, unsafe_allow_html=True)
    default_ships, default_score = 25, 0.35
else:
    positions = list(store["positions"][corridor])
    corridor_status = store["status"][corridor]
    if len(positions) < 5:
        st.info(f"Connecting to {corridor} — status: {corridor_status}. First live positions should appear within a minute.")
        default_ships, default_score = 10, 0.2
    else:
        df = pd.DataFrame(positions)
        clean = df[(df['speed_knots'] < 40) & (df['speed_knots'] > 0.5)]
        baseline = clean['speed_knots'].quantile(0.75) if len(clean) else 10
        recent = clean.tail(150)
        current_speed = recent['speed_knots'].median() if len(recent) else baseline
        live_score = max(0, min(1, (baseline - current_speed) / baseline)) if baseline else 0
        live_ships = df['ship_name'].nunique()
        live_cost = live_ships * (charter_hr + holding_hr) * live_score

        st.markdown(f"""
        <div class="kpi-row">
            <div class="kpi-card highlight">
                <div class="kpi-icon"><img src="data:image/png;base64,{ICONS['cost']}"></div>
                <div><div class="kpi-label">COST OF DISRUPTION</div><div class="kpi-value">${live_cost:,.0f}/hr</div></div>
            </div>
            <div class="kpi-card">
                <div class="kpi-icon"><img src="data:image/png;base64,{ICONS['gauge']}"></div>
                <div><div class="kpi-label">SLOWDOWN SCORE</div><div class="kpi-value">{live_score:.2f}</div></div>
            </div>
            <div class="kpi-card">
                <div class="kpi-icon"><img src="data:image/png;base64,{ICONS['ship']}"></div>
                <div><div class="kpi-label">VESSELS TRACKED</div><div class="kpi-value">{live_ships}</div></div>
            </div>
            <div class="kpi-card">
                <div class="kpi-icon"><img src="data:image/png;base64,{ICONS['barrel']}"></div>
                <div><div class="kpi-label">OIL PRICE (assumed)</div><div class="kpi-value">${oil_price}/bbl</div></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        map_df = df.drop_duplicates(subset="ship_name", keep="last").dropna(subset=["lat", "lon"])
        st.markdown(f'<div class="ship-count">🗺️ {len(map_df)} ships currently visible on the map below — zoom and pan to explore</div>', unsafe_allow_html=True)
        if len(map_df) > 0:
            st.map(map_df.rename(columns={"lat": "latitude", "lon": "longitude"}),
                   latitude="latitude", longitude="longitude", size=25, color="#B8894A", zoom=9)

        default_ships, default_score = live_ships, live_score
        ship_list = sorted(df['ship_name'].dropna().unique().tolist())

# ============================================================
# STEP 3 — TRACK A SPECIFIC SHIP
# ============================================================
st.markdown('<div class="section-title">3. Track a specific ship</div>', unsafe_allow_html=True)
if info["live"] and ship_list:
    picked_ship = st.selectbox("Search or select a vessel currently in this corridor", ship_list)
    if df is not None:
        ship_rows = df[df['ship_name'] == picked_ship].sort_values("time_utc")
        if len(ship_rows):
            last = ship_rows.iloc[-1]
            est_cost_per_hr = (charter_hr + holding_hr) * live_score
            st.markdown(f"""
            <div class="panel">
            <b>{picked_ship}</b> — last seen at {last['speed_knots']:.1f} knots in {corridor}.<br>
            This corridor is currently scoring <b>{live_score:.2f}</b> — estimated added cost for this vessel's
            transit right now: <b>${est_cost_per_hr:,.0f}/hour</b> above normal.
            </div>
            """, unsafe_allow_html=True)
elif not info["live"]:
    st.markdown('<div class="panel">Vessel search activates once this corridor is live. Switch to a 🟢 LIVE corridor above to try it now.</div>', unsafe_allow_html=True)
else:
    st.markdown('<div class="panel">No vessels in memory yet for this corridor — check back shortly.</div>', unsafe_allow_html=True)

# ============================================================
# STEP 4 — RUN YOUR OWN SCENARIO
# ============================================================
st.markdown('<div class="section-title">4. Run your own scenario</div>', unsafe_allow_html=True)
st.markdown('<div class="panel">', unsafe_allow_html=True)
c1, c2, c3 = st.columns(3)
with c1:
    sc_vessels = st.slider("Vessels in corridor", 5, 60, int(default_ships), 1)
with c2:
    sc_score = st.slider("Slowdown severity (0=normal, 1=stopped)", 0.0, 1.0, float(round(default_score, 2)), 0.05)
with c3:
    sc_oil = st.slider("Oil price ($/bbl)", 40, 150, 70, 5)
c4, c5 = st.columns(2)
with c4:
    sc_rate = st.slider("Charter day rate ($'000/day)", 20, 100, 50, 5) * 1000
with c5:
    sc_interest = st.slider("Cost of capital (%/yr)", 2.0, 12.0, 6.0, 0.5) / 100

sc_charter_hr = sc_rate / 24
sc_holding_hr = (BARRELS * sc_oil * sc_interest) / 365 / 24
sc_cost = sc_vessels * (sc_charter_hr + sc_holding_hr) * sc_score

st.markdown(f"""
<div class="kpi-row" style="margin-top:1rem;">
    <div class="kpi-card highlight">
        <div class="kpi-icon"><img src="data:image/png;base64,{ICONS['cost']}"></div>
        <div><div class="kpi-label">YOUR SCENARIO COST</div><div class="kpi-value">${sc_cost:,.0f}/hr</div></div>
    </div>
    <div class="kpi-card">
        <div class="kpi-icon"><img src="data:image/png;base64,{ICONS['gauge']}"></div>
        <div><div class="kpi-label">PER DAY</div><div class="kpi-value">${sc_cost*24:,.0f}</div></div>
    </div>
</div>
""", unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)

st.markdown("<br><div style='text-align:center; color:#8A94A3; font-size:0.8rem;'>Waypoint — MBA Digital Transformation Project, IIM Jammu</div>", unsafe_allow_html=True)
