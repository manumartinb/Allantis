# -*- coding: utf-8 -*-
r"""
SHOT NOW V4 — Foto estática AHORA (VIX) + Postprocesado (IV_BS, r y griegas BS) + Modo PERMA cada 5'
----------------------------------------------------------------------------------------------------------
- Conserva íntegro el flujo de SHOT NOW: snapshot + r (yfinance) + IV_BS + delta_BS/theta_BS/vega_BS + columnas finales.
- Añade ÚNICAMENTE el mecanismo de ejecución recurrente de PERMA STREAM:
    * Días de mercado: salta fines de semana (no incluye festivos US).
    * Ventana RTH: 09:30–16:00 ET.
    * Cadencia: cada 5 minutos, alineado a múltiplos (...:00, ...:05, ...:10, ...:15, ...:20, etc.).
- No hay ZIPs ni otras funciones de PERMA STREAM.

Columnas finales (sin cambios):
date;ms_of_day;underlying_price;root;expiration;right;strike;bid;ask;mid;delta;theta;vega;implied_vol;delta_BS;theta_BS;vega_BS;IV_BS;r
"""

from __future__ import annotations

from io import StringIO
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Optional, Tuple
import time

import math
import warnings

import httpx
import numpy as np
import pandas as pd

# yfinance para r en tiempo real
try:
    import yfinance as yf
except Exception:
    yf = None

# ================== RUTAS ==================
BASE_DIR = Path(r"C:\Users\Administrator\Desktop\FINAL DATA")
OUT_DIR  = BASE_DIR / r"HIST AND STREAMING DATA\STREAMING"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OLD_DIR = OUT_DIR / "Old"
OLD_DIR.mkdir(parents=True, exist_ok=True)

# ================== CONFIG ==================
BASE_URL    = "http://127.0.0.1:8000/v2"   # ajusta si usas otro puerto (p.ej. 25510)
ROOTS       = ("VIX",)
TIMEOUT_S   = 90.0
PRETTY_TIME = True
HEADERS = {
    "User-Agent": "TD-Snapshot-ONCE/4.0 (+httpx)",
    "Accept-Encoding": "gzip, deflate"
}

# Cadencia PERMA (minutos)
CADENCE_MIN = 5

# Ventana RTH US (PERMA)
ET = ZoneInfo("America/New_York")
RTH_OPEN  = (9, 30)   # 09:30 ET
RTH_CLOSE = (16, 0)   # 16:00 ET (exclusivo)

# Columnas máximas a conservar del endpoint antes del post
KEEP_COLS = [
    "date","time","ms_of_day",
    "root","expiration","right","strike",
    "bid","ask","mid",
    "implied_vol","iv_error",
    "delta","gamma","theta","vega","rho",
    "underlying_price","openInterest","volume"
]

FINAL_COLS = [
    "date","ms_of_day","underlying_price","root","expiration","right","strike",
    "bid","ask","mid","delta","theta","vega","implied_vol",
    "delta_BS","theta_BS","vega_BS","IV_BS","r"
]

# ================== Utilidades de tiempo ==================
def et_now() -> datetime:
    return datetime.now(ET)

def timestamp_now() -> str:
    """Devuelve timestamp local para logging."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def ymd(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def hm(dt: datetime) -> str:
    return dt.strftime("%H%M")

def within_rth(dt: datetime) -> bool:
    h, m = dt.hour, dt.minute
    open_dt  = dt.replace(hour=RTH_OPEN[0], minute=RTH_OPEN[1], second=0, microsecond=0)
    close_dt = dt.replace(hour=RTH_CLOSE[0], minute=RTH_CLOSE[1], second=0, microsecond=0)
    return (dt >= open_dt) and (dt < close_dt)

def is_weekend(dt: datetime) -> bool:
    return dt.weekday() >= 5  # 5=Sat, 6=Sun

def next_business_day_930(dt: datetime) -> datetime:
    d = dt
    while True:
        d += timedelta(days=1)
        if not is_weekend(d):
            return d.replace(hour=RTH_OPEN[0], minute=RTH_OPEN[1], second=0, microsecond=0)

def align_next_cadence(dt: datetime, cadence_min: int) -> datetime:
    """Alinea al siguiente múltiplo de 'cadence_min' (nunca se queda en el mismo slot)."""
    minute = dt.minute
    remainder = minute % cadence_min
    add = cadence_min - remainder if remainder != 0 else cadence_min
    aligned = (dt + timedelta(minutes=add)).replace(second=0, microsecond=0)
    return aligned

def archive_existing_snapshots(latest_csv: Path) -> None:
    """Move prior snapshots out of OUT_DIR into the Old subfolder, leaving the latest in place."""
    for csv_file in OUT_DIR.glob("*.csv"):
        try:
            if csv_file.resolve() == latest_csv.resolve():
                continue
        except FileNotFoundError:
            continue
        target = OLD_DIR / csv_file.name
        if target.exists():
            stem = csv_file.stem
            suffix = csv_file.suffix
            counter = 1
            while True:
                candidate = OLD_DIR / f"{stem}_{counter}{suffix}"
                if not candidate.exists():
                    target = candidate
                    break
                counter += 1
        csv_file.replace(target)
        print(f"[{timestamp_now()}] [info] CSV previo movido a Old: {target.name}")

# ================== Descarga paginada CSV ==================
def paged_get_csv(cli: httpx.Client, url: str, params: dict) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    cur_url, p = url, params
    attempts = 0
    while cur_url:
        try:
            r = cli.get(cur_url, params=p, timeout=TIMEOUT_S)
        except httpx.RequestError as e:
            attempts += 1
            if attempts <= 2:
                continue
            print(f"[{timestamp_now()}] [warn] Error de red persistente: {e}")
            return pd.DataFrame()

        if r.status_code in (472,):  # sin datos
            return pd.DataFrame()
        if r.status_code in (572,) or r.status_code >= 500:
            attempts += 1
            if attempts <= 2:
                continue
            print(f"[{timestamp_now()}] [warn] Backend ocupado (status {r.status_code}).")
            return pd.DataFrame()

        r.raise_for_status()
        txt = r.text
        if txt.strip():
            frames.append(pd.read_csv(StringIO(txt)))
        nxt = r.headers.get("Next-Page") or r.headers.get("next-page")
        cur_url, p = (nxt, None) if nxt and nxt != "null" else (None, None)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def snapshot_root(cli: httpx.Client, root: str) -> pd.DataFrame:
    url = f"{BASE_URL}/bulk_snapshot/option/greeks"
    params = {
        "root": root,
        "exp": "0",
        "use_csv": "true",
        "pretty_time": "true" if PRETTY_TIME else "false"
    }
    df = paged_get_csv(cli, url, params)
    if df.empty:
        return df
    keep = [c for c in KEEP_COLS if c in df.columns]
    return df[keep] if keep else df

# ================== r en tiempo real (yfinance) ==================
def get_realtime_r_continuous() -> float:
    """
    Intenta obtener r (continuo) desde yfinance:
    1) ^IRX (13-week T-bill, % a/ño)
    2) ^TNX (10y, % a/ño) como último recurso
    Devuelve ln(1 + y/100). Si falla, 0.0
    """
    if yf is None:
        print(f"[{timestamp_now()}] [warn] yfinance no disponible; r=0.0")
        return 0.0

    def _fetch_y(symbol: str) -> Optional[float]:
        try:
            assert yf is not None, "yfinance module is required"
            t = yf.Ticker(symbol)
            h = t.history(period="5d", interval="1d", auto_adjust=False, actions=False)
            if h is None or h.empty:
                return None
            y = float(h["Close"].dropna().iloc[-1])
            if not math.isfinite(y):
                return None
            return y
        except Exception:
            return None

    y = _fetch_y("^IRX")
    if y is None:
        y = _fetch_y("^TNX")
    if y is None:
        print(f"[{timestamp_now()}] [warn] No pude obtener ^IRX/^TNX; r=0.0")
        return 0.0

    try:
        r = math.log(1.0 + y / 100.0)
        return float(r) if math.isfinite(r) else 0.0
    except Exception:
        return 0.0

# ================== Helpers numéricos ==================
SQRT_2PI = math.sqrt(2.0 * math.pi)

def _ensure_float_array(s: pd.Series) -> np.ndarray:
    s2 = (s.astype(str)
            .str.replace('%', '', regex=False)
            .str.replace('\u00A0','', regex=False)
            .str.replace("'", "", regex=False)
            .str.strip())
    has_dot = s2.str.contains(r'\.', regex=True)
    s2 = s2.where(has_dot, s2.str.replace(',', '.', regex=False))
    return pd.to_numeric(s2, errors='coerce').to_numpy()

def norm_cdf(x: np.ndarray) -> np.ndarray:
    return 0.5 * (1.0 + np.vectorize(math.erf)(x / math.sqrt(2.0)))

def norm_pdf(x: np.ndarray) -> np.ndarray:
    return np.exp(-0.5 * x * x) / SQRT_2PI

def ms_of_day_to_timedelta(ms_series: pd.Series) -> pd.Series:
    num = pd.to_numeric(ms_series, errors='coerce')
    td = pd.to_timedelta(num, unit='ms')
    mask = num.isna()
    if mask.any():
        td_text = pd.to_timedelta(ms_series[mask], errors='coerce')
        td[mask] = td_text
    return td  # type: ignore[return-value]

def compute_T_years(df: pd.DataFrame) -> np.ndarray:
    """
    T en años ACT/365 usando la mejor combinación disponible:
    d0 = date + ms_of_day (o + snapshot_time_et, o solo date)
    d1 = expiration (+ 16:00 si viene solo fecha)
    """
    if "date" in df.columns:
        d0 = pd.to_datetime(df["date"], errors="coerce", utc=False)
        if "ms_of_day" in df.columns:
            td = ms_of_day_to_timedelta(df["ms_of_day"].astype(str))
            d0 = d0 + td
        elif "snapshot_time_et" in df.columns:
            d0 = d0 + pd.to_timedelta(df["snapshot_time_et"].astype(str), errors="coerce")
    else:
        snap_date = df.get("snapshot_date_et", pd.NaT)
        d0 = pd.to_datetime(snap_date, errors="coerce", utc=False)  # type: ignore[arg-type]
        if "snapshot_time_et" in df.columns:
            d0 = d0 + pd.to_timedelta(df["snapshot_time_et"].astype(str), errors="coerce")

    d1 = pd.to_datetime(df["expiration"], errors="coerce", utc=False)
    if (d1.dt.normalize() == d1).all():
        d1 = d1 + pd.to_timedelta("16:00:00")

    dt_days = (d1 - d0).dt.total_seconds() / 86400.0
    return (dt_days / 365.0).to_numpy()

# ================== BS: precio, vega, solver IV ==================
def bs_price(right: str, S: float, K: float, r: float, T: float, sigma: float) -> float:
    if sigma <= 0 or T <= 0 or S <= 0 or K <= 0:
        intrinsic = max(0.0, S - K) if right == "C" else max(0.0, K - S)
        return intrinsic
    sqrtT = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
    if right == "C":
        return S * 0.5*(1+math.erf(d1/math.sqrt(2))) - K * math.exp(-r*T) * 0.5*(1+math.erf(d2/math.sqrt(2)))
    else:
        return K * math.exp(-r*T) * 0.5*(1+math.erf(-d2/math.sqrt(2))) - S * 0.5*(1+math.erf(-d1/math.sqrt(2)))

def vega_annual(S: float, K: float, r: float, T: float, sigma: float) -> float:
    if sigma <= 0 or T <= 0 or S <= 0 or K <= 0:
        return 0.0
    sqrtT = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrtT)
    return S * (math.exp(-0.5*d1*d1) / math.sqrt(2*math.pi)) * sqrtT

def implied_vol_NR_bisect(right: str, S: float, K: float, r: float, T: float,
                          price: float, max_iter: int = 50, tol: float = 1e-6) -> float:
    intrinsic = max(0.0, S - K) if right == "C" else max(0.0, K - S)
    if not math.isfinite(price) or price <= intrinsic:
        return float("nan")
    if S <= 0 or K <= 0 or T <= 0:
        return float("nan")

    try:
        sigma = math.sqrt(2 * math.pi / max(T, 1e-8)) * price / max(S + K, 1e-8)
    except Exception:
        sigma = 0.2
    sigma = max(1e-4, min(5.0, sigma))

    for _ in range(max_iter):
        theo = bs_price(right, S, K, r, T, sigma)
        diff = theo - price
        if abs(diff) < tol:
            return sigma
        v = vega_annual(S, K, r, T, sigma)
        if v <= 1e-12:
            break
        sigma -= diff / v
        sigma = max(1e-4, min(5.0, sigma))

    lo, hi = 1e-4, 5.0
    for _ in range(60):
        mid = 0.5 * (lo + hi)
        theo = bs_price(right, S, K, r, T, mid)
        d = theo - price
        if abs(d) < tol:
            return mid
        if d > 0:
            hi = mid
        else:
            lo = mid
    return 0.5 * (lo + hi)

# ================== Griegas BS vectorizadas (q=0) ==================
def greeks_bs_q0(right: np.ndarray, S: np.ndarray, K: np.ndarray,
                 r: np.ndarray, sigma: np.ndarray, T: np.ndarray
                 ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    invalid = (S <= 0) | (K <= 0) | (sigma <= 0) | (T <= 0)
    with np.errstate(divide='ignore', invalid='ignore'):
        sqrtT = np.sqrt(T)
        d1 = (np.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrtT)
        d2 = d1 - sigma * sqrtT

    Nd1 = norm_cdf(d1)
    Nd2 = norm_cdf(d2)
    phid1 = norm_pdf(d1)
    disc_r = np.exp(-r * T)

    first = np.array([(s.strip().upper()[:1] if isinstance(s, str) else '') for s in right], dtype='<U1')
    is_call = first == 'C'
    is_put  = first == 'P'

    delta = np.where(is_call, Nd1, np.where(is_put, Nd1 - 1.0, np.nan))
    vega  = S * phid1 * sqrtT
    theta_call = (-S * phid1 * sigma / (2.0 * sqrtT)) - r * K * disc_r * Nd2
    theta_put  = (-S * phid1 * sigma / (2.0 * sqrtT)) + r * K * disc_r * norm_cdf(-d2)
    theta = np.where(is_call, theta_call, np.where(is_put, theta_put, np.nan))

    delta = np.where(invalid, np.nan, delta)
    vega  = np.where(invalid, np.nan, vega)
    theta = np.where(invalid, np.nan, theta)
    return delta, theta, vega

# ================== Post-proceso sobre el DataFrame ==================
def normalize_strike_inplace(df: pd.DataFrame) -> None:
    """Divide strike por 1000 si parece escalado (mediana > 50.000)."""
    if "strike" not in df.columns:
        return
    try:
        k = pd.to_numeric(df["strike"], errors="coerce")
        med = float(k.median(skipna=True))
        if math.isfinite(med) and med > 50000:
            df["strike"] = k / 1000.0
            print(f"[{timestamp_now()}] [info] Strike parecía escalado; dividido por 1000.")
        else:
            df["strike"] = k
    except Exception:
        pass

def ensure_mid_inplace(df: pd.DataFrame) -> None:
    if "mid" not in df.columns:
        df["mid"] = np.nan
    if "bid" in df.columns and "ask" in df.columns:
        b = pd.to_numeric(df["bid"], errors="coerce")
        a = pd.to_numeric(df["ask"], errors="coerce")
        df["mid"] = df["mid"].where(df["mid"].notna(), (b + a) / 2.0)

def compute_T_and_inputs(df: pd.DataFrame, r_val: float):
    T = compute_T_years(df)
    S = _ensure_float_array(df["underlying_price"]) if "underlying_price" in df.columns else np.full(len(df), np.nan)
    K = _ensure_float_array(df["strike"]) if "strike" in df.columns else np.full(len(df), np.nan)
    r = np.full(len(df), float(r_val))
    sigma_snap = _ensure_float_array(df["implied_vol"]) if "implied_vol" in df.columns else np.full(len(df), np.nan)
    right = df["right"].astype(str).str.strip().str.upper().replace({"CALL": "C", "PUT": "P"}).to_numpy()
    price = pd.to_numeric(df["mid"], errors="coerce").to_numpy()
    return T, S, K, r, sigma_snap, right, price

def add_ivbs_column(df: pd.DataFrame, right: np.ndarray, S: np.ndarray, K: np.ndarray,
                    r: np.ndarray, T: np.ndarray, price: np.ndarray) -> pd.Series:
    iv = np.full(len(df), np.nan, dtype=float)
    for i in range(len(df)):
        rt = right[i] if isinstance(right[i], str) else ""
        s, k, rr, tt, p = S[i], K[i], r[i], T[i], price[i]
        if rt not in ("C","P") or not all(map(lambda x: (x is not None) and math.isfinite(x), [s,k,rr,tt,p])):
            continue
        if tt <= 0:
            continue
        try:
            iv[i] = implied_vol_NR_bisect(rt, float(s), float(k), float(rr), float(tt), float(p))
        except Exception:
            iv[i] = np.nan
    return pd.Series(iv)

def add_bs_greeks_columns(df: pd.DataFrame, right: np.ndarray, S: np.ndarray, K: np.ndarray,
                          r: np.ndarray, T: np.ndarray, sigma: np.ndarray) -> Tuple[pd.Series, pd.Series, pd.Series]:
    delta, theta_annual, vega_annual = greeks_bs_q0(right, S, K, r, sigma, T)
    theta_per_day = theta_annual / 365.0
    vega_per_pct  = vega_annual / 100.0  # por +1% vol
    return (pd.Series(delta), pd.Series(theta_per_day), pd.Series(vega_per_pct))

def postprocess_snapshot_csv(path: Path) -> Optional[Path]:
    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception as e:
        print(f"[{timestamp_now()}] [ERROR] No pude leer {path.name}: {e}")
        return None

    if df.empty:
        print(f"[{timestamp_now()}] [INFO] CSV vacío; no hay nada que postprocesar.")
        return path

    # 1) Normalizar strike si procede
    normalize_strike_inplace(df)

    # 2) Asegurar mid
    ensure_mid_inplace(df)

    # 3) Obtener r en continuo
    r_val = get_realtime_r_continuous()
    df["r"] = float(r_val)

    # 4) Inputs para T, IV_BS y griegas
    T, S, K, r_arr, sigma_snap, right, price = compute_T_and_inputs(df, r_val)

    # 5) IV_BS (BS solver sin dividendos)
    print(f"[{timestamp_now()}] [i] Calculando IV_BS (Black-Scholes sin dividendo)...")
    df["IV_BS"] = add_ivbs_column(df, right, S, K, r_arr, T, price)

    # 6) Griegas BS con q=0 usando **IV_BS** (si falta, usa implied_vol del snapshot)
    sigma_bs = np.where(np.isfinite(df["IV_BS"].to_numpy(dtype=float)),
                        df["IV_BS"].to_numpy(dtype=float),
                        sigma_snap)

    print(f"[{timestamp_now()}] [i] Calculando griegas BS (delta_BS, theta_BS por día, vega_BS por +1%)...")
    d_bs, th_bs, vg_bs = add_bs_greeks_columns(df, right, S, K, r_arr, T, sigma_bs)
    df["delta_BS"] = d_bs
    df["theta_BS"] = th_bs
    df["vega_BS"]  = vg_bs

    # 7) Eliminar snapshot_date_et / snapshot_time_et si existen
    for col in ("snapshot_date_et", "snapshot_time_et"):
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    # 8) Orden final de columnas
    for c in FINAL_COLS:
        if c not in df.columns:
            df[c] = np.nan

    # 9) Reordenar y escribir in-place
    df_out = df[FINAL_COLS].copy()

    num_cols = ["underlying_price","strike","bid","ask","mid","delta","theta","vega","implied_vol",
                "delta_BS","theta_BS","vega_BS","IV_BS","r"]
    for c in num_cols:
        df_out[c] = pd.to_numeric(df_out[c], errors="coerce")

    tmp = path.with_suffix(".csv.tmp")
    df_out.to_csv(tmp, index=False, encoding="utf-8-sig", lineterminator="\n")
    tmp.replace(path)
    print(f"[{timestamp_now()}] [OK] Post-proceso aplicado -> {path}")
    return path

# ================== Flujo principal: foto + post (SHOT NOW original) ==================
def take_snapshot_once() -> Optional[Path]:
    now = et_now()
    tag = f"{ymd(now)}_{hm(now)}_ET"
    out_path = OUT_DIR / f"SNAPSHOT_CHAIN_{'_'.join(ROOTS)}_{tag}.csv"

    with httpx.Client(headers=HEADERS) as cli:
        frames: List[pd.DataFrame] = []
        for root in ROOTS:
            print(f"[{timestamp_now()}] [i] Pidiendo snapshot NOW de {root} (todas las expiraciones)...")
            df = snapshot_root(cli, root)
            if df.empty:
                print(f"[{timestamp_now()}]    [info] {root}: sin filas devueltas.")
            else:
                frames.append(df)

    if not frames:
        print(f"[{timestamp_now()}] [INFO] No llegó data para ningún root. No se genera archivo.")
        return None

    df = pd.concat(frames, ignore_index=True)

    # Orden y dedup
    sort_keys = [k for k in ("date", "time", "ms_of_day") if k in df.columns]
    if sort_keys:
        df = df.sort_values(sort_keys)
    df = df.drop_duplicates(subset=["root","expiration","right","strike"], keep="last")

    # Metadatos de snapshot (se eliminan al final, pero ayudan a T si falta ms_of_day)
    now_et_time = now.strftime("%H:%M:%S")
    now_et_date = now.strftime("%Y-%m-%d")
    df.insert(0, "snapshot_time_et", now_et_time)
    df.insert(0, "snapshot_date_et", now_et_date)

    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[{timestamp_now()}] [OK] Foto estática escrita -> {out_path}")
    print(f"[{timestamp_now()}] [OK] Filas: {len(df):,}")

    # Post-procesado requerido
    postprocess_snapshot_csv(out_path)
    archive_existing_snapshots(out_path)
    return out_path

# ================== Bucle PERMA RTH cada 5' (clonado de PERMA STREAM, sin compresión) ==================
def run_perma_rth_every_5min():
    print(f"[{timestamp_now()}] === SHOT NOW V4 VIX — Modo PERMA {CADENCE_MIN}min (RTH 09:30–16:00 ET; sin fines de semana) ===")
    while True:
        now = et_now()

        # Fines de semana → dormir hasta el próximo lunes 09:30 ET
        if is_weekend(now):
            nxt = next_business_day_930(now).replace(second=0, microsecond=0)
            print(f"[{timestamp_now()}] [info] Fin de semana. Durmiendo hasta {nxt} ET")
            time.sleep((nxt - now).total_seconds())
            continue

        # Determinar siguiente slot válido
        if within_rth(now):
            slot = align_next_cadence(now, CADENCE_MIN)
        else:
            # Antes de apertura → primer slot del día; después de cierre → mañana 09:30
            if now.hour < RTH_OPEN[0] or (now.hour == RTH_OPEN[0] and now.minute < RTH_OPEN[1]):
                slot = align_next_cadence(
                    now.replace(hour=RTH_OPEN[0], minute=RTH_OPEN[1], second=0, microsecond=0),
                    CADENCE_MIN
                )
            else:
                slot = align_next_cadence(next_business_day_930(now), CADENCE_MIN)

        # Dormir hasta el slot
        now2 = et_now()
        if slot > now2:
            wait_s = (slot - now2).total_seconds()
            print(f"[{timestamp_now()}] [sleep] Esperando hasta {slot} ET (~{int(wait_s)}s)")
            time.sleep(wait_s)

        # Revalidar ventana
        shot_time = et_now().replace(second=0, microsecond=0)
        if not within_rth(shot_time):
            # Si se pasó de 16:00 durante la espera, saltar al siguiente día hábil 09:30
            continue

        # Disparar snapshot (SHOT NOW original)
        try:
            take_snapshot_once()
        except httpx.HTTPStatusError as e:
            print(f"[{timestamp_now()}] [ERROR] HTTP {e.response.status_code}: {e}")
        except Exception as e:
            print(f"[{timestamp_now()}] [ERROR] Excepción no controlada en snapshot: {e}")

        # Pequeña espera de seguridad para evitar drift entre slots
        time.sleep(0.5)

def main():
    warnings.filterwarnings("ignore", category=FutureWarning)
    # Ejecuta SHOT NOW en modo PERMA (cada 5' dentro de RTH, saltando fines de semana)
    run_perma_rth_every_5min()

if __name__ == "__main__":
    main()