import streamlit as st
import asyncio
import websockets
import json
import threading
from collections import deque
from datetime import datetime, timezone
import pandas as pd
import base64
import os

st.set_page_config(page_title="HormuzWatch", page_icon="⚓", layout="wide")

# ============================================================
# CORRIDOR DEFINITIONS — organized by region, tested against real data
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
# PERSISTENT BACKGROUND COLLECTOR — all live corridors, one connection
# ============================================================
@st.cache_resource
def get_shared_store():
    return {
        "positions": {name: deque(maxlen=MAX_HISTORY_PER_CORRIDOR) for name in LIVE_CORRIDORS},
        "status": "starting",
        "started_at": datetime.now(timezone.utc),
    }

store = get_shared_store()

def point_in_box(lat, lon, box):
    (lat1, lon1), (lat2, lon2) = box
    return lat is not None and lon is not None and lat1 <= lat <= lat2 and lon1 <= lon <= lon2

def collector_loop(store):
    async def run():
        url = "wss://stream.aisstream.io/v0/stream"
        all_boxes = [c["box"] for c in LIVE_CORRIDORS.values()]
        while True:
            try:
                async with websockets.connect(url) as ws:
                    sub = {"APIKey": API_KEY, "BoundingBoxes": all_boxes,
                           "FilterMessageTypes": ["PositionReport"]}
                    await ws.send(json.dumps(sub))
                    store["status"] = "connected"
                    async for message in ws:
                        data = json.loads(message)
                        if data.get("MessageType") != "PositionReport":
                            continue
                        r = data["Message"]["PositionReport"]
                        m = data.get("MetaData", {})
                        lat, lon = r.get("Latitude"), r.get("Longitude")
                        for cname, cdata in LIVE_CORRIDORS.items():
                            if point_in_box(lat, lon, cdata["box"]):
                                store["positions"][cname].append({
                                    "time_utc": datetime.now(timezone.utc),
                                    "ship_name": (m.get("ShipName") or "Unnamed").strip(),
                                    "lat": lat, "lon": lon,
                                    "speed_knots": r.get("Sog"),
                                })
            except Exception as e:
                store["status"] = f"reconnecting ({e})"
                await asyncio.sleep(8)
    asyncio.run(run())

API_KEY = st.secrets["AISSTREAM_KEY"]

@st.cache_resource
def start_collector_thread():
    t = threading.Thread(target=collector_loop, args=(store,), daemon=True)
    t.start()
    return t

start_collector_thread()

# ============================================================
# STYLING — explicit colors everywhere, no white-on-white
# ============================================================
def img_b64(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()

ICONS = {k: img_b64(f"icon_{k}.png") for k in ["ship", "cost", "gauge", "barrel", "live"]}
INK = "#14202E"

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Manrope:wght@500;700;800&family=Inter:wght@400;500;600&display=swap');
html, body, [class*="css"] {{ font-family: 'Inter', sans-serif; color: {INK} !important; }}
.stApp {{ background-color: #F7F5F0; }}
#MainMenu, footer, header {{visibility: hidden;}}
p, span, div, li, label {{ color: {INK}; }}

.hero {{ background: linear-gradient(135deg, #14202E 0%, #1F3347 100%); padding: 2.4rem 2.6rem;
    border-radius: 16px; margin-bottom: 1.2rem; box-shadow: 0 10px 28px rgba(20,32,46,0.28);
    display: flex; align-items: center; justify-content: space-between; }}
.hero h1 {{ font-family: 'Manrope', sans-serif; color: #FFFFFF !important; font-size: 2.3rem; font-weight: 800; margin: 0; }}
.hero p {{ color: #C9A34E !important; font-size: 1rem; margin-top: 0.3rem; font-weight: 500; }}
.hero .tagline {{ color: #A8B5C2 !important; font-size: 0.85rem; margin-top: 0.5rem; font-style: italic; max-width: 480px; }}
.hero-icon img {{ width: 70px; opacity: 0.92; }}

.section-title {{ font-family: 'Manrope', sans-serif; font-weight: 700; font-size: 1.1rem; color: {INK} !important;
    border-left: 4px solid #B8894A; padding-left: 0.7rem; margin: 1.5rem 0 0.7rem 0; }}

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
.panel, .panel p, .panel li {{ color: {INK} !important; }}
.panel.dark {{ background: #14202E; }}
.panel.dark, .panel.dark p, .panel.dark li {{ color: #E6EAF0 !important; }}
.panel.amber {{ background: #FBF3E4; border: 1px solid #E0C48A; }}
.panel.amber, .panel.amber p {{ color: #7A4A18 !important; }}

.live-badge {{ display: inline-flex; align-items: center; background: #E8F3EC; color: #2E7D4F !important;
    font-size: 0.78rem; font-weight: 700; padding: 5px 14px; border-radius: 20px; }}
.live-badge img {{ width: 15px; margin-right: 6px; }}
.status-pill {{ display:inline-block; background:#EEF1F8; color:{INK} !important; font-size:0.73rem;
    font-weight:600; padding:4px 10px; border-radius:14px; margin-left:0.5rem; }}
.soon-badge {{ display:inline-block; background:#F3EEE6; color:#9A6520 !important; font-size:0.75rem;
    font-weight:700; padding:4px 12px; border-radius:14px; }}

.seg-card {{ background: white; border-radius: 14px; padding: 1.2rem; box-shadow: 0 3px 12px rgba(20,32,46,0.07);
    border-top: 3px solid #B8894A; height: 100%; }}
.seg-card, .seg-card p {{ color:{INK} !important; }}
.seg-card .stat {{ font-family:'Manrope',sans-serif; font-size:1.35rem; font-weight:800; color:#B8894A !important; }}

.price-card {{ border-radius: 14px; padding: 1.3rem; text-align:center; height: 100%; }}
.price-card.free {{ background:#F7F9FB; border:1px solid #DCE2EA; }}
.price-card.pro {{ background:#FBF3E4; border:2px solid #B8894A; }}
.price-card.ent {{ background:#14202E; }}
.price-card.free, .price-card.free p {{ color:{INK} !important; }}
.price-card.pro, .price-card.pro p {{ color:{INK} !important; }}
.price-card.ent, .price-card.ent p {{ color:#FFFFFF !important; }}
.price-name {{ font-family:'Manrope',sans-serif; font-weight:700; font-size:0.82rem; letter-spacing:1px; text-transform:uppercase; }}
.price-amt {{ font-family:'Manrope',sans-serif; font-weight:800; font-size:1.7rem; margin:0.4rem 0; }}
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
        <div class="tagline">Free vessel data, converted into one decision-ready number for the mid-market
        shippers, insurers and traders enterprise platforms price out.</div>
    </div>
    <div class="hero-icon"><img src="data:image/png;base64,{ICONS['ship']}"></div>
</div>
""", unsafe_allow_html=True)

uptime = (datetime.now(timezone.utc) - store["started_at"]).total_seconds()
uptime_str = f"{int(uptime//60)} min" if uptime < 3600 else f"{uptime/3600:.1f} hr"
total_positions = sum(len(v) for v in store["positions"].values())
st.markdown(f"""
<span class="live-badge"><img src="data:image/png;base64,{ICONS['live']}">LIVE BACKEND — {store['status']}</span>
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
# STEP 2 — LIVE INDEX (or explainer for coming-soon)
# ============================================================
st.markdown(f'<div class="section-title">2. {corridor} — current status</div>', unsafe_allow_html=True)

if not info["live"]:
    st.markdown(f"""
    <div class="panel amber">
    <b>Coming soon.</b> Free community AIS coverage doesn't reach this corridor — our global scan found
    zero live positions here across a full test window, matching the pattern documented in our Solution
    Framework: dense coverage over Europe and Southeast Asia, near-zero over the Gulf, Suez, and most of
    Asia-Pacific outside Singapore. The same Slowdown-Score engine activates the moment a licensed or
    satellite AIS feed is connected. Use the scenario calculator below with your own assumptions, or
    switch to a 🟢 LIVE corridor above to see the real thing.
    </div>
    """, unsafe_allow_html=True)
    default_ships, default_score, ship_list = 25, 0.35, []
else:
    positions = list(store["positions"][corridor])
    if len(positions) < 10:
        st.info(f"Collecting live data for {corridor} — still building a baseline, check back shortly.")
        default_ships, default_score, ship_list = 10, 0.2, []
    else:
        df = pd.DataFrame(positions)
        clean = df[(df['speed_knots'] < 40) & (df['speed_knots'] > 0.5)]
        baseline = clean['speed_knots'].quantile(0.75)
        recent = clean.tail(150)
        current_speed = recent['speed_knots'].median() if len(recent) else baseline
        live_score = max(0, min(1, (baseline - current_speed) / baseline)) if baseline else 0
        live_ships = df['ship_name'].nunique()
        oil_price = 70
        charter_hr = 50000 / 24
        holding_hr = (BARRELS * oil_price * INTEREST) / 365 / 24
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
        if len(map_df) > 0:
            st.map(map_df.rename(columns={"lat": "latitude", "lon": "longitude"}),
                   latitude="latitude", longitude="longitude", size=20, color="#B8894A")

        default_ships, default_score = live_ships, live_score
        ship_list = sorted(df['ship_name'].dropna().unique().tolist())

# ============================================================
# STEP 3 — TRACK A SPECIFIC SHIP (live corridors only)
# ============================================================
st.markdown('<div class="section-title">3. Track a specific ship</div>', unsafe_allow_html=True)
if info["live"] and ship_list:
    picked_ship = st.selectbox("Search or select a vessel currently in this corridor", ship_list)
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

# ============================================================
# STEP 5 — WHO THIS IS FOR (4 tabs now)
# ============================================================
st.markdown('<div class="section-title">5. Who this is built for</div>', unsafe_allow_html=True)
t1, t2, t3, t4 = st.tabs(["🚢 Shippers", "🛡️ Insurers", "📈 Traders", "🔎 Track a Vessel"])
with t1:
    st.markdown('<div class="seg-card"><div class="stat">Wait, sail, or reroute?</div><p>Regional shippers use the live corridor index to decide whether to hold position or enter — the exact decision our cost figure is built to inform.</p></div>', unsafe_allow_html=True)
with t2:
    st.markdown('<div class="seg-card"><div class="stat">An independent reference point</div><p>Small P&amp;I correspondents use the corridor score as a neutral benchmark in premium discussions.</p></div>', unsafe_allow_html=True)
with t3:
    st.markdown('<div class="seg-card"><div class="stat">Anticipate landed cost</div><p>Importers and traders watch multiple corridors to see disruption cost building before it hits their invoice.</p></div>', unsafe_allow_html=True)
with t4:
    st.markdown('<div class="seg-card"><div class="stat">Where is my shipment, really?</div><p>Anyone with a stake in one specific vessel can search it directly and see its live speed and estimated added cost — not just the corridor average.</p></div>', unsafe_allow_html=True)

# ============================================================
# STEP 6 — PRICING
# ============================================================
st.markdown('<div class="section-title">6. Pricing — the free number is the marketing</div>', unsafe_allow_html=True)
p1, p2, p3 = st.columns(3)
with p1:
    st.markdown('<div class="price-card free"><div class="price-name">PUBLIC</div><div class="price-amt">Free</div><p>Headline index number only</p></div>', unsafe_allow_html=True)
with p2:
    st.markdown('<div class="price-card pro"><div class="price-name">PROFESSIONAL</div><div class="price-amt">$49/mo</div><p>History · alerts · vessel search</p></div>', unsafe_allow_html=True)
with p3:
    st.markdown('<div class="price-card ent"><div class="price-name">ENTERPRISE / API</div><div class="price-amt">$249/mo</div><p>Raw API · multi-corridor feed</p></div>', unsafe_allow_html=True)

with st.form("waitlist"):
    st.markdown('<div class="section-title">Request early access</div>', unsafe_allow_html=True)
    email = st.text_input("Work email")
    role = st.selectbox("I am a...", ["Shipper / Forwarder", "Marine Insurer", "Trader / Importer", "Tracking a specific vessel", "Other"])
    submitted = st.form_submit_button("Request access")
    if submitted and email:
        st.success(f"Thanks — we'll reach out to {email} when paid tiers open.")

st.markdown(f"""
<div class="section-title">Method &amp; honest limitations</div>
<div class="panel dark">
Five corridors (Dover Strait, Gibraltar, Kiel Canal, Öresund Strait, Singapore Strait) run on a live,
continuously-collecting background process against free AIS data. Fifteen more — including the Strait of
Hormuz, Bab-el-Mandeb, and Suez — are marked Coming Soon because a full global scan found zero free
coverage there. That gap is not a flaw in this prototype; it is the market gap the business is built on.
</div>
""", unsafe_allow_html=True)

st.markdown("<br><div style='text-align:center; color:#8A94A3; font-size:0.8rem;'>HormuzWatch — MBA Digital Transformation Project, IIM Jammu</div>", unsafe_allow_html=True)
