# -*- coding: utf-8 -*-
"""
ALLANTIS SPX V1 — SISTEMA FWD (Forward Testing) con Mediana Intraday — LOCAL DATA
====================================================================================
VERSIÓN: Allantis V1 — BWB de Puts + Calendar Call superpuesto

ESTRUCTURA ALLANTIS:
====================
PATAS y VENCIMIENTOS:
- Puts: todo en DTE1 (aprox 160 DTE) → BWB de puts: +4P@UL : -8P@Shorts : +4P@LL
  * Upper Line (UL): Long 4P aprox Δ40 (establece rango superior BWB)
  * Shorts: Short 8P aprox Δ30 (shorts centrales BWB)
  * Lower Line (LL): Long 4P aprox Δ20 (deep OTM, establece rango inferior BWB)

- Calls: Calendar Call superpuesto
  * Short 2C @ DTE1 (Δ≈30)
  * Long 2C @ DTE2 (aprox 220 DTE), mismo strike que la short call

TOTAL: 5 grupos de patas con 2 expiraciones (DTE1 para puts + short calls, DTE2 para long calls)

GRUPOS DE STRIKES (basados en rangos delta):
1. UL_DELTA: Upper Line puts (Δ40 puts) - Rango delta para long puts superiores BWB
2. SHORT_P_DELTA: Short puts centrales (Δ30 puts) - Rango delta para shorts BWB
3. LL_DELTA: Lower Line puts (Δ20 puts) - Rango delta para long puts deep OTM BWB
4. SHORT_C_DELTA: Short calls DTE1 (Δ30 calls) - Rango delta para short calls calendario
5. LONG_C_DELTA: Long calls DTE2 (Δ30 calls) - Rango delta para long calls calendario

EXPIRACIONES:
- RANGE_A: DTE1 (aprox 160 DTE) para BWB puts completo + short calls
- RANGE_B: DTE2 (aprox 220 DTE) para long calls calendario

NOVEDADES V1 (base from Batman V23):
1) **Estructura Allantis 5-leg**:
   - BWB de puts: +4P:-8P:+4P (3 strikes, DTE1)
   - Calendar call: -2C@DTE1:+2C@DTE2 (mismo strike, 2 expiraciones)
   - Total: 5 grupos de patas, 20 contratos
   - Función `allantis_value_from_df`: revaloriza estructura completa
   - Cálculo de net_credit forward (5 grupos, 2 expiraciones)

2) **Sistema FWD con mediana intraday**:
   - Evalúa estructura en múltiples timestamps fijos US
   - Agrega por MEDIANA → columnas: PnL_fwd_pts_*_mediana y PnL_fwd_pct_*_mediana
   - Optimización: lee archivo FWD UNA SOLA VEZ por ventana

3) **Uso de TODAS las expiraciones en RANGE_A/RANGE_B**:
   - Se procesan todas las expiraciones dentro de cada rango
   - Ordenadas por DTE ascendente

4) **Paralelización con ProcessPoolExecutor**:
   - Worker function para cálculo paralelo
   - Múltiples workers para maximizar performance

MANTIENE:
- Canonical chain robusto
- Caché de greeks
- Filtros de liquidez y spread
- Sistema de análisis estadístico
"""

import os, math, calendar, re, random, urllib.request
import numpy as np, pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Backend sin GUI para guardar gráficos

from pathlib import Path
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional, Any
import logging
import warnings

# Imports para módulo de análisis estadístico
from scipy import stats
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import permutation_test_score
from sklearn.ensemble import RandomForestRegressor
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

# Librerías opcionales para análisis estadístico
try:
    import statsmodels.api as sm
    from statsmodels.stats.proportion import proportion_confint
    HAS_STATSMODELS = True
except ImportError:
    HAS_STATSMODELS = False

try:
    from minepy import MINE  # type: ignore[import-untyped]
    HAS_MINEPY = True
except ImportError:
    HAS_MINEPY = False

try:
    import dcor  # type: ignore[import-untyped]
    HAS_DCOR = True
except ImportError:
    HAS_DCOR = False

BASE_URL = "https://optionstrat.com/build/custom/SPX/"
TZ_US = ZoneInfo("America/New_York")
TZ_ES = ZoneInfo("Europe/Madrid")

# Configuración de logging para módulo estadístico
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Suprimir warnings
warnings.filterwarnings('ignore', category=RuntimeWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

# ================== RUTAS Y CONFIGURACIÓN ==================
DATA_DIR = r"C:\Users\Administrator\Desktop\FINAL DATA\HIST AND STREAMING DATA\STREAMING"
DESKTOP  = Path.home() / "Desktop"

# ================== CONFIG SNAPSHOT ==================
# Configuración de timestamps para el snapshot inicial de estructuras Batman
TARGET_HHMMS_US = ["10:30"]               # Lista de horas US objetivo para snapshot (formato "HH:MM")
                                           # Ejemplo: ["10:30", "14:00"] busca estructuras a las 10:30 AM y 2:00 PM
NEAREST_MINUTE_TOLERANCE = 5              # Tolerancia en minutos para encontrar timestamp más cercano
                                           # Si el timestamp exacto no existe, busca dentro de ±5 minutos
IGNORE_TARGET_MINUTE = True              # ! IMP! Desactivar en LIVE. Control de procesamiento completo del día
                                           # False: solo procesa los timestamps en TARGET_HHMMS_US
                                           # True: ignora TARGET_HHMMS_US y procesa TODOS los timestamps del CSV
                                           # ATENCIÓN: True puede generar miles de estructuras por día

# ================== CONFIG FWD (Forward Testing) ==================
# Ventanas de tiempo forward para evaluar evolución de las estructuras Batman
FORWARD_FRACS = [0.15, 0.25, 0.50]  # Fracciones del DTE1 para ventanas forward
                                                  # 0.01 = 1% del DTE1 (ej: si DTE1=50, evalúa en 0.5 días ≈ 12 horas)
                                                  # 0.05 = 5% del DTE1 (ej: si DTE1=50, evalúa en 2.5 días)
                                                  # 0.15 = 15% del DTE1 (ej: si DTE1=50, evalúa en 7.5 días)
                                                  # 0.25 = 25% del DTE1 (ej: si DTE1=50, evalúa en 12.5 días)
                                                  # 0.50 = 50% del DTE1 (ej: si DTE1=50, evalúa en 25 días)
FRAC_SUFFIXES = [f"{int(round(fr*100)):02d}" for fr in FORWARD_FRACS]  # Sufijos para nombrar columnas (01, 05, 15, 25, 50)

# Timestamps fijos de mercado US para evaluación intraday (mediana sobre múltiples snapshots)
FWD_INTRADAY_TIMESTAMPS = [
     "10:30", "12:00",  # Mañana: 10:30 AM - 12:30 PM
    "13:00", "13:30", "15:00"      # Tarde: 1:00 PM - 3:00 PM
]
# PROPÓSITO: Para cada ventana FWD, evalúa la estructura en estos 10 timestamps y calcula la MEDIANA
# Esto reduce el ruido de snapshots únicos y captura mejor el comportamiento intradiario
# NOTA: Requiere que los archivos históricos tengan datos en estos timestamps (tolerancia ±5 min)

# Control de FWD sobre winners/losers (filtra qué estructuras se evalúan en forward)
FWD_ON_WINNERS = False    # True: evalúa FWD en los mejores batmans (mayor valor según RANKING_MODE)
                          # False: NO evalúa FWD en winners
FWD_ON_LOSERS = False   # True: evalúa FWD en los peores batmans (menor valor según RANKING_MODE)
                          # False: NO evalúa FWD en losers
                          # NOTA: Puedes activar ambos para evaluar extremos (winners Y losers)

# Selección por porcentaje (controla cuántos batmans se procesan en FWD)
FWD_TOP_PCT = 0.10      #OJO ahora no elimina tantos. Deja candidato si hay distinta fecha (fix deduplicacion)
                        # Porcentaje de estructuras a evaluar (valor entre 0.0 y 1.0, o string "25%")
                        # 1.0 o "100%": evalúa TODAS las estructuras filtradas (winners/losers)
                        # 0.25 o "25%": evalúa solo el top/bottom 25% según RANKING_MODE
                        # 0.10 o "10%": evalúa solo el top/bottom 10%
                        # Ejemplo: Si tienes 1000 winners y FWD_TOP_PCT=0.25, evalúa los 250 mejores

# Selección por porcentaje para análisis W/L post-FWD (separado del corte pre-FWD)
WL_TOP_PCT = 1.00       # Porcentaje de estructuras a usar en análisis W/L post-filtros
                        # Este porcentaje se aplica ESTRATIFICADAMENTE por grupo (WIN/LOS)
                        # 0.10 o "10%": toma el 10% de WINNERS supervivientes + 10% de LOSERS supervivientes
                        # 1.0 o "100%": incluye TODOS los supervivientes de cada grupo
                        # IMPORTANTE: Se aplica sobre el dataset YA FILTRADO (ej: tras NET_CREDIT_DIFF)
                        # y respeta las etiquetas WIN/LOS originales del corte pre-FWD
                        # Ejemplo: Si sobreviven 60 WIN y 30 LOS con WL_TOP_PCT=0.10 → 6 WIN + 3 LOS

# Gráficos de promedios FWD (análisis visual de performance por ventana)    
FWD_PLOT_ENABLED = False  # True: genera gráficos PNG con promedios de PnL_fwd_pct por ventana (W01, W05, etc.)
                           # False: NO genera gráficos (ahorra tiempo y espacio)
                           # Los gráficos se guardan en Desktop con sufijo del batch

# Orden global pre-FWD (ranking de estructuras antes de aplicar filtros FWD)
ORDER_PRE_FWD_GLOBAL = False              # True: ordena TODAS las estructuras globalmente antes de FWD
                                          # False: ordena por archivo (no recomendado)
RANKING_MODE = "AQI"             # Métrica de ranking para ordenar estructuras Allantis
                                          # "theta_total": Theta total (por defecto para Allantis)
                                          # "AQI": Allantis Quality Index (balance entre LEL, UEL, picos, valle, theta, MASTER_DISTANCE)

# ================== UMBRALES PARA GRÁFICOS FILTRADOS (Subconjuntos de Alta Calidad) ==================
# Controla qué estructuras se incluyen en los gráficos 3A/3B, 4A/4B, 5A/5B y 6A/6B

FILTER_AQI_THRESHOLD = 2.0            # Umbral para gráficos de AQI (Allantis Quality Index)
                                       # AQI balancea: LEL, UEL, picos BWB/CCAL, valle, theta, MASTER_DISTANCE
                                       # Valores típicos: 1-5 (mayor es mejor)

FILTER_FF_BAT_THRESHOLD = 0.1         # Umbral para gráficos de FF_BAT (Forward Factor Batman)
                                       # Gráficos 6A/6B mostrarán solo estructuras con FF_BAT > este valor
                                       # Ejemplo: 0.1 → solo batmans con Forward Factor Batman mayor a 0.1
                                       # NOTA: FF_BAT > 0 indica wings "calientes" vs forward implícita del centro

# ================== CONFIG PROCESO ==================
NUM_RANDOM_FILES = 1     # ! Número de archivos CSV a procesar aleatoriamente del directorio DATA_DIR
                             # Útil para backtests rápidos sin procesar todo el histórico
                             # Ejemplo: 2 procesa 2 días aleatorios, 0 o None procesa TODOS los archivos

# ===== MODO DE PROCESAMIENTO =====
INCREMENTAL_MODE = True      # ! True: Modo incremental (procesa 1 a 1, bajo uso memoria, MUY detallado)
                             #   False: Modo batch paralelo (más rápido, mayor uso memoria)
                             # RECOMENDADO: True si tienes problemas de MemoryError

PARALLEL_BATCH_SIZE = 50     # ! [Solo modo batch] Tamaño de lote para procesamiento paralelo
                             # Divide las tareas en chunks más pequeños para reducir presión de memoria
                             # Ejemplo: 50 procesa 50 tareas a la vez, luego el siguiente lote
                             # Valores recomendados: 30-100 dependiendo de RAM disponible

INCREMENTAL_WORKERS = 1      # ! [Solo modo incremental] Número de workers para procesamiento incremental
                             # 1 = secuencial (mínima memoria), 2-4 = semi-paralelo (balance)
                             # RECOMENDADO: 1 para evitar MemoryError, 2-4 si tienes RAM suficiente

THETA_TO_DAILY = 100.0       # Multiplicador SPX para convertir puntos a USD
                             # Los theta_BS del snapshot ya vienen en formato diario (por día)
                             # Ejemplo: theta_BS = -2.847 puntos/día → -2.847 × 100 = -284.7 USD/día
RISK_FREE_R = 0.04           # Tasa libre de riesgo anual (4% = 0.04)
                             # Usado como fallback en cálculos Black-Scholes si no hay tasa específica

# ================== CONFIG ANÁLISIS ESTADÍSTICO ==================
STATISTICAL_ANALYSIS = False  # True: ejecuta análisis estadístico al finalizar el backtest
                             # False: omite el análisis estadístico (ahorra tiempo de procesamiento)
                             # El análisis genera reportes detallados de correlaciones y umbrales
                             # en la carpeta ANALISIS (puede tomar varios minutos en datasets grandes)

# ================== FILTROS DE LIQUIDEZ OI/VOLUMEN ==================
# Filtros para asegurar liquidez mínima en opciones (evitar strikes ilíquidos)
# Umbrales mínimos por pata:
# - FRONT (exp1): patas cortas k1 y k3 (vencimiento cercano)
# - BACK  (exp2): pata larga k2 (vencimiento lejano)
USE_LIQUIDITY_FILTERS = False  # True: aplica filtros de OI/Volumen | False: NO filtra por liquidez
MIN_OI_FRONT  = 1              # Open Interest mínimo para opciones FRONT (k1, k3)
MIN_VOL_FRONT = 1              # Volumen mínimo para opciones FRONT (k1, k3)
MIN_OI_BACK   = 1              # Open Interest mínimo para opciones BACK (k2)
MIN_VOL_BACK  = 1              # Volumen mínimo para opciones BACK (k2)
                               # NOTA: Valores bajos (1) efectivamente deshabilitan el filtro

# ================== CONFIGURACIÓN ESTRUCTURA BATMAN ==================
# Cobertura amplia de expiraciones y strikes para maximizar el espacio de búsqueda

# === DTE (Days To Expiration) - Rangos para expiraciones ===
# Se procesan TODAS las expiraciones dentro de cada rango (sin límites de cantidad)
RANGE_A = (150, 170)       # ! Rango DTE para DTE1 (aprox 160 DTE)
                           # Usado para: BWB puts completo (+4P:-8P:+4P) + short calls (-2C)
                           # Ejemplos alternativos:
                           # (140, 180): Ventana más amplia alrededor de 160 DTE
                           # (155, 165): Ventana muy estrecha para mayor precisión

RANGE_B = (210, 230)       # ! Rango DTE para DTE2 (aprox 220 DTE)
                           # Usado para: long calls del calendario (+2C)
                           # IMPORTANTE: RANGE_B debe ser > RANGE_A para estructura Allantis válida
                           # Ejemplos alternativos:
                           # (200, 240): Ventana más amplia alrededor de 220 DTE
                           # (215, 225): Ventana muy estrecha para mayor precisión

# === ESTRUCTURA ALLANTIS: 5 GRUPOS DE STRIKES (PUTS + CALLS) ===
# Estructura: +4P@UL(DTE1) : -8P@Shorts(DTE1) : +4P@LL(DTE1) : -2C@Short(DTE1) : +2C@Long(DTE2)
#
# PUTS (todos en DTE1):
# 1. UL (Upper Line): Long 4P aprox Δ40
# 2. Shorts: Short 8P aprox Δ30
# 3. LL (Lower Line): Long 4P aprox Δ20 (deep OTM)
#
# CALLS (calendario call):
# 4. Short calls: Short 2C @ DTE1 aprox Δ30
# 5. Long calls: Long 2C @ DTE2 (mismo strike que short), aprox Δ30

# === UL PUTS (Upper Line - Long 4P) - BASADO EN DELTA ===
# Upper line del BWB, establece el límite superior
# Delta de put: valor entre -1 y 0 (en magnitud: 0% a 100%)
UL_PUT_DELTA_MIN = 0.35      # ! Delta mínimo para UL puts (Δ40 target)
UL_PUT_DELTA_MAX = 0.45      # ! Delta máximo para UL puts
                             # Rango típico: [0.35, 0.45] para capturar Δ40
                             # Estos puts están más ITM que los shorts

# === SHORT PUTS (Short 8P) - BASADO EN DELTA ===
# Shorts centrales del BWB, corazón de la estructura
SHORT_PUT_DELTA_MIN = 0.25   # ! Delta mínimo para short puts (Δ30 target)
SHORT_PUT_DELTA_MAX = 0.35   # ! Delta máximo para short puts
                             # Rango típico: [0.25, 0.35] para capturar Δ30
                             # Strike constraint: UL > Shorts

# === LL PUTS (Lower Line - Long 4P) - BASADO EN DELTA ===
# Lower line del BWB, deep OTM, establece el límite inferior
LL_PUT_DELTA_MIN = 0.15      # ! Delta mínimo para LL puts (Δ20 target)
LL_PUT_DELTA_MAX = 0.25      # ! Delta máximo para LL puts
                             # Rango típico: [0.15, 0.25] para capturar Δ20
                             # Strike constraint: UL > Shorts > LL

# === SHORT CALLS (Short 2C @ DTE1) - BASADO EN DELTA ===
# Short calls del calendario, expira en DTE1
SHORT_CALL_DELTA_MIN = 0.25  # ! Delta mínimo para short calls (Δ30 target)
SHORT_CALL_DELTA_MAX = 0.35  # ! Delta máximo para short calls
                             # Rango típico: [0.25, 0.35] para capturar Δ30

# === LONG CALLS (Long 2C @ DTE2) - BASADO EN DELTA ===
# Long calls del calendario, expira en DTE2, mismo strike que short call
# NOTA: Este rango se usa para SELECCIONAR el strike, luego se busca el MISMO strike en DTE2
LONG_CALL_DELTA_MIN = 0.25   # ! Delta mínimo para long calls (Δ30 target en DTE1)
LONG_CALL_DELTA_MAX = 0.35   # ! Delta máximo para long calls
                             # Rango típico: [0.25, 0.35] para capturar Δ30
                             # IMPORTANTE: Se selecciona por delta en DTE1, luego se busca mismo K en DTE2

# === Comentarios de referencia ===
# Estructura Allantis necesita 5 strikes:
# - KUL (puts Upper Line, Δ40)
# - KShorts (puts Short, Δ30)
# - KLL (puts Lower Line, Δ20)
# - KCall (calls, Δ30, shared entre short DTE1 y long DTE2)
# Constraint: KUL > KShorts > KLL (todos puts, strikes decrecientes hacia OTM)


# === PREFILTRO NET CREDIT (Crédito inicial de la estructura Allantis) ===
# Allantis: +4P -8P +4P -2C +2C = 20 contratos totales (5x más que Batman)
# Net credit = +4*P_UL - 8*P_Shorts + 4*P_LL - 2*C_Short + 2*C_Long (en puntos SPX)
# Valores negativos = crédito recibido (favorable), positivos = débito pagado
PREFILTER_CREDIT_MIN = -20000.0     # Crédito mínimo: -200 pts = recibir máximo $20,000 total
PREFILTER_CREDIT_MAX = 20000.0      # Crédito máximo: +200 pts = pagar máximo $20,000 total
                                  # RANGO AMPLIO inicial para no filtrar agresivamente
                                  # AJUSTAR según resultados: estructura puede ser crédito o débito
                                  # Ejemplo: -50 pts = recibir $5,000 | +30 pts = pagar $3,000

# === FILTROS FINALES (Greeks y métricas de riesgo) ===
# DELTA: Allantis tiene 20 contratos (BWB puts + call calendar)
# BWB puts: delta negativo (protección bajista), Calls: delta positivo (alcista)
# Estructura debería ser aproximadamente delta-neutral o ligeramente sesgada
DELTA_MIN, DELTA_MAX = -0.05, 0.05    # Rango delta total para 20 contratos
                                       # BWB: ~4*(-0.40) - 8*(-0.30) + 4*(-0.20) = -1.6 + 2.4 - 0.8 = 0
                                       # Calls: -2*(0.30) + 2*(0.30) = 0
                                       # Total esperado: cerca de 0, permitir ±50 para variabilidad

# THETA: 20 contratos = theta 5x mayor que Batman
# Estructura mayormente theta positivo (decay favorable en BWB + calendar)
THETA_MIN, THETA_MAX = -500.0, 50000.0  # Theta diario en USD
                                         # Allantis típico: 50-200 USD/día positivo
                                         # Permitir negativo por si calendar tiene theta negativo en ciertos casos

# === FILTRO UEL_INF ===
# NO APLICA para Allantis (UEL_INF es concepto específico de Batman call butterfly)
# Allantis usa BWB de puts, no tiene "orejas" ni "death valley" tradicional
# Deshabilitado mediante rangos amplios
UEL_INF_MIN = -10000000               # Deshabilitado (rango muy amplio)
UEL_INF_MAX =  10000000               # Deshabilitado (rango muy amplio)

# === FILTRO PnLDV (Profit & Loss en Death Valley) ===
# NO APLICA para Allantis (Death Valley es concepto de Batman butterfly)
# BWB tiene perfil diferente: máxima ganancia en el centro, pérdidas en extremos
FILTER_PNLDV_ENABLED = False          # Deshabilitado para Allantis
PNLDV_MIN = -10000000                 # Rango deshabilitado
PNLDV_MAX =  10000000                 # Rango deshabilitado

# === FILTRO AQI (Allantis Quality Index) ===
# Métrica de calidad específica para BWB + Calendar
# Balancea LEL, UEL, picos BWB/CCAL, valle, theta, MASTER_DISTANCE
FILTER_AQI_ENABLED = True             # True: aplica filtro por AQI | False: no filtra
AQI_MIN = 0.5                         # AQI mínimo aceptable (valores típicos: 1-5)
AQI_MAX = 100000                      # Sin límite superior
CALCULATE_AQI = True                  # Control para cálculo de AQI

# === FILTRO LEL (Lower Expected Loss - Asíntota inferior) ===
# LEL representa la pérdida máxima en el extremo inferior (bajada extrema de precio)
# Valores en puntos SPX (multiplicar por 100 para USD)
FILTER_LEL_ENABLED = True             # True: aplica filtro por LEL | False: no filtra
LEL_MIN = -10000                      # LEL mínimo aceptable en puntos (ej: -100 pts = -$10,000)
LEL_MAX = 100000                      # LEL máximo (sin límite superior efectivo)
                                       # EJEMPLO: LEL_MIN = 150 → solo estructuras con LEL >= 150 pts

# === FILTRO UEL (Upper Expected Loss - Asíntota superior) ===
# UEL representa la pérdida máxima en el extremo superior (subida extrema de precio)
# Valores en puntos SPX (multiplicar por 100 para USD)
FILTER_UEL_ENABLED = True             # True: aplica filtro por UEL | False: no filtra
UEL_MIN = -10000                      # UEL mínimo aceptable en puntos (ej: -100 pts = -$10,000)
UEL_MAX = 100000                      # UEL máximo (sin límite superior efectivo)
                                       # EJEMPLO: UEL_MIN = 200 → solo estructuras con UEL >= 200 pts

# === FILTRO NET_CREDIT_DIFF (solo aplica en CSV Copia con mediana T+0) ===
# Filtra estructuras comparando net_credit vs net_credit_mediana (% diferencia)
FILTER_NET_CREDIT_DIFF_ENABLED = False  # True: aplica filtro | False: no filtra
NET_CREDIT_DIFF_MIN = -15              # Diferencia mínima: -15% (net_credit puede ser 15% menor que mediana)
NET_CREDIT_DIFF_MAX =  15              # Diferencia máxima: +15% (net_credit puede ser 15% mayor que mediana)
                                        # PROPÓSITO: Descarta estructuras con net_credit anómalo vs comportamiento intradiario
                                        # EJEMPLO: Si mediana=-10 pts y net_credit=-12 pts, diff=20% → RECHAZA

# === SPREAD MÁXIMO (Control de calidad de precios) ===
MAX_SPREAD_REL = 0.5                   # Spread bid-ask máximo relativo permitido (0.5 = 50% del mid)
                                        # Descarta opciones con spread muy amplio (ilíquidas o precios sospechosos)
                                        # EJEMPLO: Si mid=10, bid=7, ask=13, spread=6, spread_rel=60% → RECHAZA

# === SALIDA Y DISPLAY ===
SHOW_MAX = 0                           # Número máximo de estructuras a mostrar por consola (0 = no muestra)
S_PNL = 8000                           # Spot SPX para gráficos de P&L (valor de referencia)

# Caché greeks
GREEKS_CACHE = {}

# ================== HELPERS ==================
def safe_filename(text: str) -> str:
    return re.sub(r'[\\/:*?"<>|]+', '-', text)

def get_filter_stats(df, col_name, filter_min, filter_max):
    """
    Calcula estadísticas detalladas sobre una columna antes de aplicar un filtro.

    Args:
        df: DataFrame
        col_name: nombre de la columna a analizar
        filter_min: valor mínimo del filtro configurado
        filter_max: valor máximo del filtro configurado

    Returns:
        dict con: rango_real_min, rango_real_max, num_nan, num_inf, fuera_de_rango
    """
    if col_name not in df.columns:
        return {
            'rango_real_min': None,
            'rango_real_max': None,
            'num_nan': 0,
            'num_inf': 0,
            'fuera_de_rango': 0
        }

    col_data = pd.to_numeric(df[col_name], errors='coerce')

    # Contar NaN (incluye valores que no se pudieron convertir a numérico)
    num_nan = col_data.isna().sum()

    # Contar infinitos
    num_inf = np.isinf(col_data).sum()

    # Obtener valores válidos (ni NaN ni infinito)
    valid_data = col_data[np.isfinite(col_data)]

    if len(valid_data) > 0:
        rango_real_min = float(valid_data.min())
        rango_real_max = float(valid_data.max())

        # Contar cuántos valores válidos están fuera del rango del filtro
        fuera_de_rango = ((valid_data < filter_min) | (valid_data > filter_max)).sum()
    else:
        rango_real_min = None
        rango_real_max = None
        fuera_de_rango = 0

    return {
        'rango_real_min': rango_real_min,
        'rango_real_max': rango_real_max,
        'num_nan': int(num_nan),
        'num_inf': int(num_inf),
        'fuera_de_rango': int(fuera_de_rango)
    }

def format_filter_log(filter_name, filter_min, filter_max, filas_antes, filas_despues, stats, indent="    "):
    """
    Formatea el mensaje de log para un filtro con estadísticas detalladas.

    Args:
        filter_name: nombre del filtro
        filter_min: valor mínimo configurado
        filter_max: valor máximo configurado
        filas_antes: número de filas antes del filtro
        filas_despues: número de filas después del filtro
        stats: diccionario con estadísticas de get_filter_stats
        indent: sangría para el mensaje

    Returns:
        string formateado para imprimir
    """
    # Formatear rango del filtro
    rango_filtro = f"[{filter_min};{filter_max}]"

    # Formatear filas
    cambio_filas = f"filas {filas_antes:,} → {filas_despues:,}"
    if filas_antes == filas_despues:
        cambio_filas += " (sin cambios)"

    # Formatear rango real
    if stats['rango_real_min'] is not None and stats['rango_real_max'] is not None:
        # Formatear con 2 decimales y usar coma europea
        min_str = f"{stats['rango_real_min']:.2f}".replace('.', ',')
        max_str = f"{stats['rango_real_max']:.2f}".replace('.', ',')
        rango_real = f"rango real {filter_name}: [min={min_str};max={max_str}]"
    else:
        rango_real = f"rango real {filter_name}: [N/A]"

    # Formatear estadísticas
    estadisticas = f"NaN={stats['num_nan']:,};inf={stats['num_inf']};fuera_de_rango={stats['fuera_de_rango']:,}"

    # Construir mensaje completo
    msg = f"{indent}[FILTER] {filter_name} {rango_filtro} → {cambio_filas} | {rango_real} | {estadisticas}"

    return msg

def parse_percentage(value):
    """
    Convierte un porcentaje (string "15%" o float 0.15) a float.

    Args:
        value: None, string "15%", o float 0.15

    Returns:
        float entre 0 y 1, o None
    """
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value.endswith('%'):
            return float(value.rstrip('%')) / 100.0
        return float(value)
    return float(value)

def is_third_friday(d):
    cal = calendar.monthcalendar(d.year, d.month)
    fridays = [wk[calendar.FRIDAY] for wk in cal if wk[calendar.FRIDAY]!=0]
    return len(fridays)>=3 and d.day==fridays[2]

def root_for_exp(exp_str):
    d = datetime.strptime(exp_str, "%Y-%m-%d").date()
    if d.weekday()==4 and is_third_friday(d): return "SPX"
    if d.weekday()==3:
        nd = d + timedelta(days=1)
        if nd.weekday()==4 and is_third_friday(nd): return "SPX"
    return "SPXW"

def yyyymmdd_to_yymmdd(d): return d.strftime("%y%m%d")
def round_to_nearest_10(x): return int(round(x/10.0)*10)
def normal_cdf(x): return 0.5*(1+math.erf(x/math.sqrt(2)))
def normal_pdf(x): return math.exp(-0.5*x*x)/math.sqrt(2*math.pi)

# ================== BLACK-SCHOLES ==================
def bs_call_price(S,K,T,r,sigma,q=0.0):
    if T<=0 or sigma<=0:
        return max(S-K,0.0)
    d1=(math.log(S/K)+(r-q+0.5*sigma*sigma)*T)/(sigma*math.sqrt(T))
    d2=d1-sigma*math.sqrt(T)
    return S*math.exp(-q*T)*normal_cdf(d1) - K*math.exp(-r*T)*normal_cdf(d2)

def bs_delta_call(S,K,T,r,sigma,q=0.0):
    if T<=0 or sigma<=0:
        return 1.0 if S>K else 0.0
    d1=(math.log(S/K)+(r-q+0.5*sigma*sigma)*T)/(sigma*math.sqrt(T))
    return math.exp(-q*T)*normal_cdf(d1)

def bs_theta_call_excel(S,K,T,r,sigma):
    if T<=0 or sigma<=0:
        return 0.0
    sqrtT = math.sqrt(T)
    d1 = (math.log(S/K) + (r + 0.5*sigma*sigma)*T) / (sigma*sqrtT)
    d2 = d1 - sigma*sqrtT
    pdf = math.exp(-0.5*d1*d1)/math.sqrt(2*math.pi)
    term1 = -(S * pdf * sigma) / (2*sqrtT)
    term2 = - r * K * math.exp(-r*T) * normal_cdf(d2)
    return term1 + term2

def bs_theta_put_excel(S,K,T,r,sigma):
    """Black-Scholes theta para put (anualizado)."""
    if T<=0 or sigma<=0:
        return 0.0
    sqrtT = math.sqrt(T)
    d1 = (math.log(S/K) + (r + 0.5*sigma*sigma)*T) / (sigma*sqrtT)
    d2 = d1 - sigma*sqrtT
    pdf = math.exp(-0.5*d1*d1)/math.sqrt(2*math.pi)
    term1 = -(S * pdf * sigma) / (2*sqrtT)
    term2 = r * K * math.exp(-r*T) * normal_cdf(-d2)  # Nota: +r para put (vs -r para call)
    return term1 + term2

def bs_vega_call(S,K,T,r,sigma,q=0.0):
    if T<=0 or sigma<=0:
        return 0.0
    d1=(math.log(S/K)+(r-q+0.5*sigma*sigma)*T)/(sigma*math.sqrt(T))
    pdf=normal_pdf(d1)
    return S*math.exp(-q*T)*pdf*math.sqrt(T)

def implied_vol_call_from_price(S, K, T, r, q, target_price,
                                lo=1e-6, hi=5.0, tol=1e-7, max_iter=100):
    """IV inversa robusta: asegura precio ≥ intrínseco, bisección con fallback Newton."""
    intrinsic = max(S - K, 0.0)
    if target_price is None:
        return float('nan')
    try:
        tp = float(target_price)
    except:
        return float('nan')
    if math.isnan(tp) or tp <= 0:
        return float('nan')

    target = max(tp, intrinsic + 1e-6)
    if T <= 0:
        return float('nan')

    def f(sig):
        return bs_call_price(S, K, T, r, sig, q) - target

    f_lo, f_hi = f(lo), f(hi)
    if f_lo * f_hi > 0:
        # sin bracket: intenta Newton con vega-guard
        sigma = 0.2
        for _ in range(max_iter):
            price = bs_call_price(S, K, T, r, sigma, q)
            vega  = bs_vega_call(S, K, T, r, sigma, q)
            if vega <= 1e-12:
                break
            diff = price - target
            sigma = max(lo, min(hi, sigma - diff / max(vega, 1e-12)))
            if abs(diff) < tol:
                return sigma
        return float('nan')

    for _ in range(max_iter):
        mid = (lo + hi) / 2.0
        f_mid = f(mid)
        if abs(f_mid) < tol or (hi - lo) / 2 < tol:
            return mid
        if f_lo * f_mid <= 0:
            hi, f_hi = mid, f_mid
        else:
            lo, f_lo = mid, f_mid
    return (lo + hi) / 2.0

# ================== LECTURA DE ARCHIVOS LOCALES ==================
def list_local_files(folder: str) -> list[Path]:
    p = Path(folder)
    if not p.exists():
        raise FileNotFoundError(f"No existe la carpeta: {folder}")
    files = sorted(p.glob("SNAP*.csv"))
    uniq = sorted({f.resolve() for f in files if f.is_file()})
    return list(uniq)

# ================== MINUTOS ==================
def hhmm_to_minutes(hhmm: str) -> int:
    hh, mm = hhmm.split(":"); return int(hh)*60 + int(mm)

def minutes_to_hhmm(m: int) -> str:
    hh = m // 60; mm = m % 60; return f"{hh:02d}:{mm:02d}"

def series_hhmm(df: pd.DataFrame) -> pd.Series:
    if "time" in df.columns:
        s = df["time"].astype(str)
        hhmm = s.str.extract(r'(\b\d{2}:\d{2}\b)', expand=False)
        mask_missing = hhmm.isna()
        if mask_missing.any():
            t = pd.to_datetime(s[mask_missing], errors="coerce", utc=False)
            hhmm.loc[mask_missing] = t.dt.strftime("%H:%M")
        return hhmm
    if "ms_of_day" in df.columns:
        col = df["ms_of_day"]
        if pd.api.types.is_string_dtype(col) or col.dtype == object:
            s = col.astype(str)
            return s.str.extract(r'(\b\d{2}:\d{2}\b)', expand=False)
        ms = pd.to_numeric(col, errors="coerce")
        hh = (ms // 3_600_000).astype("Int64")
        mm = ((ms % 3_600_000) // 60_000).astype("Int64")
        return (hh.astype("float").fillna(-1).astype(int).astype(str).str.zfill(2)
                + ":" +
                mm.astype("float").fillna(-1).astype(int).astype(str).str.zfill(2))
    raise KeyError("No existe 'time' ni 'ms_of_day' en el CSV.")

def filter_df_by_hhmm(df: pd.DataFrame, hhmm: str) -> pd.DataFrame:
    if "time" in df.columns:
        s = df["time"].astype(str)
        return df[s.str.startswith(hhmm + ":")].copy()
    if "ms_of_day" in df.columns:
        col = df["ms_of_day"]
        if pd.api.types.is_string_dtype(col) or col.dtype == object:
            s = col.astype(str)
            return df[s.str.startswith(hhmm + ":")].copy()
        ms = pd.to_numeric(col, errors="coerce")
        hh = (ms // 3_600_000).astype(int)
        mm = ((ms % 3_600_000) // 60_000).astype(int)
        mask = (pd.Series(hh, index=df.index).astype(str).str.zfill(2) + ":" +
                pd.Series(mm, index=df.index).astype(str).str.zfill(2)) == hhmm
        return df[mask].copy()
    raise KeyError("No existe 'time' ni 'ms_of_day' en el CSV.")

def get_slice_at_target_hhmm(df: pd.DataFrame, target_hhmm: str, tolerance_min: int = 0):
    hhmm_series = series_hhmm(df).dropna().astype(str)
    if (hhmm_series == target_hhmm).any():
        return filter_df_by_hhmm(df, target_hhmm), target_hhmm
    if tolerance_min and tolerance_min > 0:
        valid = [x for x in pd.unique(hhmm_series) if re.match(r"^\d{2}:\d{2}$", str(x))]
        if valid:
            target_m = hhmm_to_minutes(target_hhmm)
            candidates = sorted(valid, key=lambda x: abs(hhmm_to_minutes(str(x)) - target_m))
            best = candidates[0]
            if abs(hhmm_to_minutes(best) - target_m) <= tolerance_min:
                return filter_df_by_hhmm(df, best), best
    return pd.DataFrame(), target_hhmm

def resolve_used_hhmm(used_hhmm_us: str, df_one_min: pd.DataFrame) -> str:
    try:
        if used_hhmm_us != "ALL" and re.match(r"^\d{2}:\d{2}$", str(used_hhmm_us)):
            return str(used_hhmm_us)
        if df_one_min is not None and not df_one_min.empty:
            if "ms_of_day" in df_one_min.columns:
                s = df_one_min["ms_of_day"]
                if s.dtype == object:
                    m = re.search(r"\b(\d{2}:\d{2})\b", str(s.iloc[0]))
                    if m:
                        return m.group(1)
                try:
                    ms = int(pd.to_numeric(s.iloc[0], errors="coerce"))
                    hh = ms // 3_600_000
                    mm = (ms % 3_600_000) // 60_000
                    if 0 <= hh <= 23 and 0 <= mm <= 59:
                        return f"{int(hh):02d}:{int(mm):02d}"
                except Exception:
                    pass
            if "time" in df_one_min.columns:
                val = str(df_one_min["time"].iloc[0])
                m = re.search(r"\b(\d{2}:\d{2})\b", val)
                if m:
                    return m.group(1)
        return "12:00"
    except Exception:
        return "12:00"

# ================== ESCALA STRIKE ==================
def td_detect_scale_for_strike(df: pd.DataFrame) -> int:
    try:
        med = pd.to_numeric(df.get("strike", pd.Series(dtype=float)), errors="coerce").median()
        if med and med > 100000: return 1000
    except: pass
    return 1

def td_rescale_strike_only(df: pd.DataFrame) -> pd.DataFrame:
    if "strike" in df.columns:
        scale = td_detect_scale_for_strike(df)
        if scale != 1:
            df = df.copy()
            df["strike"] = pd.to_numeric(df["strike"], errors="coerce") / float(scale)
    return df

# ================== CANONICAL CHAIN (sin descartar por spread en selección) ==================
def pick_best_row(rows: pd.DataFrame) -> pd.Series | None:
    """
    Elige la 'mejor' fila dentro del trío, **sin** descartar por spread relativo ni mid inválido.
    Criterios de ordenación (desc): volumen, open_interest, ms_of_day.
    Si no existen estas columnas, devuelve la primera fila.
    """
    if rows is None or rows.empty:
        return None
    tmp = rows.copy()
    for c in ("volume","open_interest","ms_of_day"):
        if c in tmp.columns: tmp[c] = pd.to_numeric(tmp[c], errors="coerce")
    sort_cols = []; ascending = []
    for col in ("volume","open_interest","ms_of_day"):
        if col in tmp.columns: sort_cols.append(col); ascending.append(False)
    if sort_cols:
        tmp = tmp.sort_values(by=sort_cols, ascending=ascending, kind="mergesort")
    return tmp.iloc[0]

def build_canonical_chain(df_full: pd.DataFrame) -> pd.DataFrame:
    needed = {"right","expiration","strike","bid","ask"}
    if not needed.issubset(df_full.columns):
        return pd.DataFrame()

    out = []
    for (r,e,k), sub in df_full.groupby(["right","expiration","strike"], dropna=False):
        best = pick_best_row(sub)
        if best is not None:
            out.append(best.to_dict())

    can = pd.DataFrame(out)
    if not can.empty:
        for c in ("strike","bid","ask"):
            if c in can.columns: can[c] = pd.to_numeric(can[c], errors="coerce")
        can = can.dropna(subset=["right","expiration","strike","bid","ask"]).reset_index(drop=True)

    return can

# ================== CADENAS / DTE / MID ==================
def build_chains(df_one_minute: pd.DataFrame):
    expirations = sorted(pd.unique(df_one_minute["expiration"].astype(str)))
    precache = {}
    for exp in expirations:
        sub = df_one_minute[df_one_minute["expiration"].astype(str)==exp].copy()
        if "strike" in sub.columns:
            sub["strike"] = pd.to_numeric(sub["strike"], errors="coerce")
        calls = sub[sub["right"].str.upper()=="C"].copy()
        puts  = sub[sub["right"].str.upper()=="P"].copy()
        for dfp in (calls, puts):
            dfp.dropna(subset=["strike"], inplace=True)
            dfp.sort_values("strike", inplace=True)
            dfp.reset_index(drop=True, inplace=True)
        # Crear índices por strike para O(1) lookup
        calls_idx = calls.set_index("strike", drop=False) if not calls.empty else pd.DataFrame()
        puts_idx  = puts.set_index("strike", drop=False) if not puts.empty else pd.DataFrame()
        precache[exp] = (calls, puts, calls_idx, puts_idx)
    return expirations, precache

def compute_dte_map(expirations, date_et_str: str):
    date_et = datetime.strptime(date_et_str, "%Y-%m-%d").date()
    return {e: (datetime.strptime(e,"%Y-%m-%d").date() - date_et).days for e in expirations}

def get_mid_from_row(row):
    def f(x):
        try:
            v=float(x); return v if np.isfinite(v) else np.nan
        except: return np.nan
    mv = f(row.get("mid", np.nan))
    if not np.isnan(mv) and mv>0: return mv
    b = f(row.get("bid", np.nan)); a = f(row.get("ask", np.nan))
    if not np.isnan(b) and not np.isnan(a) and a>0: return (a+b)/2.0
    lp = f(row.get("last", np.nan))
    if np.isnan(lp): lp = f(row.get("lastPrice", np.nan))
    return lp

def get_root_from_series(row):
    if row is None:
        return None
    if hasattr(row, "get"):
        for key in ("root", "root_symbol", "rootSymbol", "Root", "ROOT"):
            try:
                val = row.get(key)
            except Exception:
                val = None
            if isinstance(val, str):
                val = val.strip()
                if val:
                    return val.upper()
    return None

def filter_df_by_root_strict(df: pd.DataFrame, expected_root: str):
    """
    Filtro ESTRICTO por root con preferencia, sin fallback silencioso.

    Returns:
        tuple: (filtered_df, root_pref_applied: bool, chosen_root: str|None)
        - Si hay match exacto con expected_root → (df_preferido, True, expected_root)
        - Si NO hay match → (df_original, False, root_encontrado_o_None)
    """
    if expected_root is None or df is None or df.empty:
        return df, False, None

    try:
        expected_norm = str(expected_root).strip().upper()
    except Exception:
        return df, False, None

    if not expected_norm:
        return df, False, None

    # Buscar columna de root
    root_col = None
    for key in ("root", "root_symbol", "rootSymbol", "Root", "ROOT"):
        if key in df.columns:
            root_col = key
            break

    if root_col is None:
        return df, False, None

    # Normalizar columna root
    df_work = df.copy()
    df_work["__root_norm"] = df_work[root_col].astype(str).str.strip().str.upper()

    # Filtrar por preferencia
    pref_mask = df_work["__root_norm"] == expected_norm
    df_pref = df_work.loc[pref_mask]

    if not df_pref.empty:
        # Match exacto → aplicar preferencia
        df_result = df_pref.drop(columns=["__root_norm"])
        return df_result, True, expected_norm
    else:
        # NO match → devolver original SIN filtrar, pero señalar anomalía
        chosen = df_work["__root_norm"].iloc[0] if len(df_work) > 0 else None
        df_result = df.copy()  # Sin __root_norm
        return df_result, False, chosen


def select_best_strike_row(df_strike: pd.DataFrame, base_mid: float = None):
    """
    Selecciona la mejor fila cuando hay múltiples con el mismo strike.

    Criterios de selección (en orden):
    1. Mejor liquidez: (ask-bid)/abs(mid) más pequeño, con bid>0 & ask>0
    2. Si empate: mid más cercano al base_mid (si se proporciona)
    3. Si aún empate: primera fila

    Returns:
        tuple: (row: pd.Series, tie_break: str)
        tie_break puede ser: "unique", "liquidity", "mid_proximity", "first"
    """
    if df_strike is None or df_strike.empty:
        return None, None

    if len(df_strike) == 1:
        return df_strike.iloc[0], "unique"

    # Calcular spread relativo para liquidez
    df_work = df_strike.copy()

    def calc_spread_rel(row):
        try:
            bid = float(row.get("bid", np.nan))
            ask = float(row.get("ask", np.nan))
            if not (np.isfinite(bid) and np.isfinite(ask)):
                return np.inf
            if bid <= 0 or ask <= 0:
                return np.inf
            mid = 0.5 * (bid + ask)
            if mid <= 0:
                return np.inf
            return (ask - bid) / abs(mid)
        except Exception:
            return np.inf

    df_work["__spread_rel"] = df_work.apply(calc_spread_rel, axis=1)

    # Filtrar solo filas con spread válido
    valid = df_work.loc[df_work["__spread_rel"] < np.inf]

    if valid.empty:
        # Ninguna tiene liquidez válida → tomar primera
        return df_strike.iloc[0], "first"

    # Ordenar por spread relativo (menor = mejor liquidez)
    valid = valid.sort_values("__spread_rel")

    if len(valid) == 1:
        row = df_strike.loc[df_strike.index == valid.index[0]].iloc[0]
        return row, "liquidity"

    # Empate en liquidez → desempatar por mid más cercano a base_mid
    best_spread = valid["__spread_rel"].iloc[0]
    tied = valid.loc[valid["__spread_rel"] == best_spread]

    if len(tied) == 1:
        row = df_strike.loc[df_strike.index == tied.index[0]].iloc[0]
        return row, "liquidity"

    if base_mid is not None and np.isfinite(base_mid):
        def calc_mid(row):
            try:
                bid = float(row.get("bid", np.nan))
                ask = float(row.get("ask", np.nan))
                if np.isfinite(bid) and np.isfinite(ask):
                    return 0.5 * (bid + ask)
                return np.nan
            except Exception:
                return np.nan

        tied["__mid"] = tied.apply(calc_mid, axis=1)
        tied = tied.loc[tied["__mid"].notna()]

        if not tied.empty:
            tied["__mid_diff"] = (tied["__mid"] - base_mid).abs()
            tied = tied.sort_values("__mid_diff")
            row = df_strike.loc[df_strike.index == tied.index[0]].iloc[0]
            return row, "mid_proximity"

    # Último recurso: primera fila de las empatadas
    row = df_strike.loc[df_strike.index == tied.index[0]].iloc[0]
    return row, "liquidity"

# ================== SPOT & r ==================
FRED_DGS1MO_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS1MO"

def underlying_from_slice(df_slice: pd.DataFrame) -> float:
    if df_slice is None or df_slice.empty:
        return float('nan')
    candidates = ["underlying_price", "Underlying_Price", "underlyingPrice", "underlying", "spot", "SPX", "spx"]
    for col in candidates:
        if col in df_slice.columns:
            s = pd.to_numeric(df_slice[col], errors="coerce").dropna()
            if not s.empty:
                return float(s.median())
    return float('nan')

def fetch_risk_free_historical_fred(target_date: str, fallback: float=RISK_FREE_R) -> float:
    """
    Descarga el CSV histórico de FRED DGS1MO y devuelve el valor correspondiente
    a la fecha target_date (formato "YYYY-MM-DD"), tomando la última observación
    disponible <= target_date.

    Args:
        target_date: Fecha objetivo en formato "YYYY-MM-DD" (ej: "2023-06-15")
        fallback: Valor por defecto si no se encuentra dato válido

    Returns:
        Tasa libre de riesgo como decimal (ej: 0.05 para 5%)
    """
    try:
        with urllib.request.urlopen(FRED_DGS1MO_URL, timeout=10) as r:
            data = r.read()
        text = None
        for enc in ("utf-8-sig", "utf-8", "latin-1"):
            try:
                text = data.decode(enc); break
            except UnicodeDecodeError:
                continue
        if text is None:
            text = data.decode("utf-8", errors="ignore")

        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        pat = re.compile(r"^(\d{4}-\d{2}-\d{2}),\s*([+-]?\d+(?:\.\d+)?)\s*$")

        # Parsear target_date
        target_dt = datetime.strptime(target_date, "%Y-%m-%d").date()

        # Buscar todas las observaciones <= target_date, guardar la más reciente
        best_val = None
        best_date = None

        for ln in lines:
            m = pat.match(ln)
            if not m:
                continue
            date_str, val_str = m.groups()
            obs_date = datetime.strptime(date_str, "%Y-%m-%d").date()

            # Solo considerar fechas <= target_date
            if obs_date <= target_dt:
                val = float(val_str) / 100.0  # Convertir de % a decimal
                # Actualizar si es la primera o si encontramos fecha más reciente
                if best_date is None or obs_date > best_date:
                    best_date = obs_date
                    best_val = val

        # Validar resultado
        if best_val is None or not (best_val == best_val):  # None o NaN
            return float(fallback)
        if best_val < 0 or best_val > 1.0:  # Validación de rango razonable
            return float(fallback)

        return float(best_val)

    except Exception:
        return float(fallback)

# ================== FILTROS DE COTIZACIÓN ==================
def leg_quote_ok(df_chain: pd.DataFrame, K: float, max_spread_rel: float = MAX_SPREAD_REL) -> bool:
    if df_chain is None or df_chain.empty:
        return False
    Ks = pd.to_numeric(df_chain["strike"], errors="coerce")
    r = df_chain.loc[np.isclose(Ks, float(K), atol=1e-6)]
    if r.empty:
        return False

    # Usar selección inteligente en vez de iloc[0] ciego
    row, _ = select_best_strike_row(r, base_mid=None)
    if row is None:
        return False

    try:
        bid = float(row.get("bid", np.nan))
        ask = float(row.get("ask", np.nan))
    except Exception:
        return False
    if not (np.isfinite(bid) and np.isfinite(ask)):
        return False
    if bid <= 0:
        return False
    if ask <= 0:
        return False
    if ask < bid:
        return False
    mid = 0.5*(bid + ask)
    if not np.isfinite(mid) or mid <= 0:
        return False
    spread_rel = (ask - bid) / mid
    if not np.isfinite(spread_rel) or spread_rel > float(max_spread_rel):
        return False
    return True

# ================== UTILS BÚSQUEDA ==================
def pick_nearest_dte(exp_list, dte_map, target, nmax):
    if nmax is None or nmax<=0: return exp_list
    exps = sorted(exp_list, key=lambda e: abs(dte_map[e]-target))
    return exps[:nmax]

def nearest(lst, x):
    return min(lst, key=lambda v: abs(v-x)) if lst else None

def sample_strikes(available_strikes, spot, factors):
    out=[]
    for f in factors:
        t = spot*f
        s = nearest(available_strikes, t)
        if s is not None and s not in out:
            out.append(s)
    return out

def gen_k1_candidates(base, rng=30, step=10):
    """DEPRECATED: Función antigua basada en puntos. Usar gen_k1_candidates_by_delta() en su lugar."""
    return [base + o for o in range(-rng, rng+1, step)]

def gen_k1_candidates_by_delta(calls_idx, available_strikes, spot, dte, r,
                                delta_min=0.50, delta_max=0.60, q=0.0):
    """
    Genera candidatos de K1 (pata corta front) filtrando strikes por rango de delta.

    Args:
        calls_idx: DataFrame indexado por strike con datos de opciones
        available_strikes: Lista de strikes disponibles
        spot: Precio subyacente (SPX)
        dte: Days to expiration
        r: Risk-free rate
        delta_min: Delta mínimo aceptable (ej: 0.50 = 50%)
        delta_max: Delta máximo aceptable (ej: 0.60 = 60%)
        q: Dividend yield (default 0.0)

    Returns:
        Lista de strikes que cumplen con el rango de delta especificado
    """
    T = max(dte, 1) / 365.0
    candidates = []

    for K in available_strikes:
        row = fetch_row(calls_idx, K)
        if row is None:
            continue

        # Obtener precio mid para calcular IV
        bid = float(row.get("bid", np.nan))
        ask = float(row.get("ask", np.nan))
        last = float(row.get("lastPrice", np.nan))
        if math.isnan(last):
            last = float(row.get("last", np.nan))

        mid = (bid + ask) / 2.0 if (not math.isnan(bid) and not math.isnan(ask) and ask > 0) else last

        if math.isnan(mid) or T <= 0:
            continue

        # Calcular IV
        iv = implied_vol_call_from_price(spot, K, T, r, q, mid)
        if iv is None or (isinstance(iv, float) and (math.isnan(iv) or iv <= 0)):
            continue

        # Calcular delta
        delta = bs_delta_call(spot, K, T, r, iv, q)

        # Filtrar por rango de delta
        if delta_min <= delta <= delta_max:
            candidates.append(K)

    return candidates

def gen_k2_candidates_by_delta(calls_idx, available_strikes, spot, dte, r,
                                delta_min=0.28, delta_max=0.35, q=0.0):
    """
    Genera candidatos de K2 (pata larga back) filtrando strikes por rango de delta.

    Args:
        calls_idx: DataFrame indexado por strike con datos de opciones
        available_strikes: Lista de strikes disponibles
        spot: Precio subyacente (SPX)
        dte: Days to expiration
        r: Risk-free rate
        delta_min: Delta mínimo aceptable (ej: 0.28 = 28%)
        delta_max: Delta máximo aceptable (ej: 0.35 = 35%)
        q: Dividend yield (default 0.0)

    Returns:
        Lista de strikes que cumplen con el rango de delta especificado
    """
    T = max(dte, 1) / 365.0
    candidates = []

    for K in available_strikes:
        row = fetch_row(calls_idx, K)
        if row is None:
            continue

        # Obtener precio mid para calcular IV
        bid = float(row.get("bid", np.nan))
        ask = float(row.get("ask", np.nan))
        last = float(row.get("lastPrice", np.nan))
        if math.isnan(last):
            last = float(row.get("last", np.nan))

        mid = (bid + ask) / 2.0 if (not math.isnan(bid) and not math.isnan(ask) and ask > 0) else last

        if math.isnan(mid) or T <= 0:
            continue

        # Calcular IV
        iv = implied_vol_call_from_price(spot, K, T, r, q, mid)
        if iv is None or (isinstance(iv, float) and (math.isnan(iv) or iv <= 0)):
            continue

        # Calcular delta
        delta = bs_delta_call(spot, K, T, r, iv, q)

        # Filtrar por rango de delta
        if delta_min <= delta <= delta_max:
            candidates.append(K)

    return candidates

def gen_k3_candidates_by_delta(calls_idx, available_strikes, spot, dte, r,
                                delta_min=0.10, delta_max=0.5, q=0.0):
    """
    Genera candidatos de K3 (pata corta front superior) filtrando strikes por rango de delta.

    Args:
        calls_idx: DataFrame indexado por strike con datos de opciones
        available_strikes: Lista de strikes disponibles
        spot: Precio subyacente (SPX)
        dte: Days to expiration
        r: Risk-free rate
        delta_min: Delta mínimo aceptable (ej: 0.10 = 10%)
        delta_max: Delta máximo aceptable (ej: 0.5 = 50%)
        q: Dividend yield (default 0.0)

    Returns:
        Lista de strikes que cumplen con el rango de delta especificado
    """
    T = max(dte, 1) / 365.0
    candidates = []

    for K in available_strikes:
        row = fetch_row(calls_idx, K)
        if row is None:
            continue

        # Obtener precio mid para calcular IV
        bid = float(row.get("bid", np.nan))
        ask = float(row.get("ask", np.nan))
        last = float(row.get("lastPrice", np.nan))
        if math.isnan(last):
            last = float(row.get("last", np.nan))

        mid = (bid + ask) / 2.0 if (not math.isnan(bid) and not math.isnan(ask) and ask > 0) else last

        if math.isnan(mid) or T <= 0:
            continue

        # Calcular IV
        iv = implied_vol_call_from_price(spot, K, T, r, q, mid)
        if iv is None or (isinstance(iv, float) and (math.isnan(iv) or iv <= 0)):
            continue

        # Calcular delta
        delta = bs_delta_call(spot, K, T, r, iv, q)

        # Filtrar por rango de delta
        if delta_min <= delta <= delta_max:
            candidates.append(K)

    return candidates

# ================== ALLANTIS: FUNCIONES DE GENERACIÓN DE CANDIDATOS ==================

def bs_put_price(S, K, T, r, sigma, q=0.0):
    """Black-Scholes precio de put."""
    if T <= 0 or sigma <= 0:
        return max(K - S, 0.0)
    d1 = (math.log(S/K) + (r - q + 0.5*sigma*sigma)*T) / (sigma*math.sqrt(T))
    d2 = d1 - sigma*math.sqrt(T)
    return K*math.exp(-r*T)*normal_cdf(-d2) - S*math.exp(-q*T)*normal_cdf(-d1)

def bs_delta_put(S, K, T, r, sigma, q=0.0):
    """Black-Scholes delta de put (negativo)."""
    if T <= 0 or sigma <= 0:
        return -1.0 if S < K else 0.0
    d1 = (math.log(S/K) + (r - q + 0.5*sigma*sigma)*T) / (sigma*math.sqrt(T))
    return -math.exp(-q*T)*normal_cdf(-d1)

def implied_vol_put_from_price(S, K, T, r, q, target_price,
                                lo=1e-6, hi=5.0, tol=1e-7, max_iter=100):
    """IV inversa para put: asegura precio ≥ intrínseco, bisección con fallback Newton."""
    intrinsic = max(K - S, 0.0)
    if target_price is None:
        return float('nan')
    try:
        tp = float(target_price)
    except:
        return float('nan')
    if math.isnan(tp) or tp <= 0:
        return float('nan')

    target = max(tp, intrinsic + 1e-6)
    if T <= 0:
        return float('nan')

    def f(sig):
        return bs_put_price(S, K, T, r, sig, q) - target

    f_lo, f_hi = f(lo), f(hi)
    if f_lo * f_hi > 0:
        # sin bracket: intenta Newton con vega-guard
        sigma = 0.2
        for _ in range(max_iter):
            price = bs_put_price(S, K, T, r, sigma, q)
            vega = bs_vega_call(S, K, T, r, sigma, q)  # Vega es igual para call y put
            if vega <= 1e-12:
                break
            diff = price - target
            sigma = max(lo, min(hi, sigma - diff / max(vega, 1e-12)))
            if abs(diff) < tol:
                return sigma
        return float('nan')

    for _ in range(max_iter):
        mid = (lo + hi) / 2.0
        f_mid = f(mid)
        if abs(f_mid) < tol or (hi - lo) / 2 < tol:
            return mid
        if f_lo * f_mid <= 0:
            hi, f_hi = mid, f_mid
        else:
            lo, f_lo = mid, f_mid
    return (lo + hi) / 2.0

def gen_ul_put_candidates_by_delta(puts_idx, available_strikes, spot, dte, r,
                                    delta_min=0.35, delta_max=0.45, q=0.0):
    """
    Genera candidatos de Upper Line puts (long 4P @ DTE1) filtrando por rango de delta.

    Args:
        puts_idx: DataFrame indexado por strike con datos de puts
        available_strikes: Lista de strikes disponibles
        spot: Precio subyacente (SPX)
        dte: Days to expiration
        r: Risk-free rate
        delta_min: Delta mínimo aceptable en magnitud (ej: 0.35 = 35%)
        delta_max: Delta máximo aceptable en magnitud (ej: 0.45 = 45%)
        q: Dividend yield (default 0.0)

    Returns:
        Lista de strikes que cumplen con el rango de delta especificado
    """
    T = max(dte, 1) / 365.0
    candidates = []

    for K in available_strikes:
        row = fetch_row(puts_idx, K)
        if row is None:
            continue

        # Obtener precio mid
        bid = float(row.get("bid", np.nan))
        ask = float(row.get("ask", np.nan))
        last = float(row.get("lastPrice", np.nan))
        if math.isnan(last):
            last = float(row.get("last", np.nan))

        mid = (bid + ask) / 2.0 if (not math.isnan(bid) and not math.isnan(ask) and ask > 0) else last

        if math.isnan(mid) or T <= 0:
            continue

        # Calcular IV
        iv = implied_vol_put_from_price(spot, K, T, r, q, mid)
        if iv is None or (isinstance(iv, float) and (math.isnan(iv) or iv <= 0)):
            continue

        # Calcular delta (en magnitud, put delta es negativo)
        delta_put = bs_delta_put(spot, K, T, r, iv, q)
        delta_mag = abs(delta_put)  # Magnitud del delta

        # Filtrar por rango de delta en magnitud
        if delta_min <= delta_mag <= delta_max:
            candidates.append(K)

    return candidates

def gen_short_put_candidates_by_delta(puts_idx, available_strikes, spot, dte, r,
                                       delta_min=0.25, delta_max=0.35, q=0.0):
    """
    Genera candidatos de Short puts (short 8P @ DTE1) filtrando por rango de delta.

    Args:
        puts_idx: DataFrame indexado por strike con datos de puts
        available_strikes: Lista de strikes disponibles
        spot: Precio subyacente (SPX)
        dte: Days to expiration
        r: Risk-free rate
        delta_min: Delta mínimo aceptable en magnitud (ej: 0.25 = 25%)
        delta_max: Delta máximo aceptable en magnitud (ej: 0.35 = 35%)
        q: Dividend yield (default 0.0)

    Returns:
        Lista de strikes que cumplen con el rango de delta especificado
    """
    T = max(dte, 1) / 365.0
    candidates = []

    for K in available_strikes:
        row = fetch_row(puts_idx, K)
        if row is None:
            continue

        bid = float(row.get("bid", np.nan))
        ask = float(row.get("ask", np.nan))
        last = float(row.get("lastPrice", np.nan))
        if math.isnan(last):
            last = float(row.get("last", np.nan))

        mid = (bid + ask) / 2.0 if (not math.isnan(bid) and not math.isnan(ask) and ask > 0) else last

        if math.isnan(mid) or T <= 0:
            continue

        iv = implied_vol_put_from_price(spot, K, T, r, q, mid)
        if iv is None or (isinstance(iv, float) and (math.isnan(iv) or iv <= 0)):
            continue

        delta_put = bs_delta_put(spot, K, T, r, iv, q)
        delta_mag = abs(delta_put)

        if delta_min <= delta_mag <= delta_max:
            candidates.append(K)

    return candidates

def gen_ll_put_candidates_by_delta(puts_idx, available_strikes, spot, dte, r,
                                    delta_min=0.15, delta_max=0.25, q=0.0):
    """
    Genera candidatos de Lower Line puts (long 4P @ DTE1) filtrando por rango de delta.

    Args:
        puts_idx: DataFrame indexado por strike con datos de puts
        available_strikes: Lista de strikes disponibles
        spot: Precio subyacente (SPX)
        dte: Days to expiration
        r: Risk-free rate
        delta_min: Delta mínimo aceptable en magnitud (ej: 0.15 = 15%)
        delta_max: Delta máximo aceptable en magnitud (ej: 0.25 = 25%)
        q: Dividend yield (default 0.0)

    Returns:
        Lista de strikes que cumplen con el rango de delta especificado
    """
    T = max(dte, 1) / 365.0
    candidates = []

    for K in available_strikes:
        row = fetch_row(puts_idx, K)
        if row is None:
            continue

        bid = float(row.get("bid", np.nan))
        ask = float(row.get("ask", np.nan))
        last = float(row.get("lastPrice", np.nan))
        if math.isnan(last):
            last = float(row.get("last", np.nan))

        mid = (bid + ask) / 2.0 if (not math.isnan(bid) and not math.isnan(ask) and ask > 0) else last

        if math.isnan(mid) or T <= 0:
            continue

        iv = implied_vol_put_from_price(spot, K, T, r, q, mid)
        if iv is None or (isinstance(iv, float) and (math.isnan(iv) or iv <= 0)):
            continue

        delta_put = bs_delta_put(spot, K, T, r, iv, q)
        delta_mag = abs(delta_put)

        if delta_min <= delta_mag <= delta_max:
            candidates.append(K)

    return candidates

def gen_short_call_candidates_by_delta(calls_idx, available_strikes, spot, dte, r,
                                        delta_min=0.25, delta_max=0.35, q=0.0):
    """
    Genera candidatos de Short calls (short 2C @ DTE1) filtrando por rango de delta.

    Args:
        calls_idx: DataFrame indexado por strike con datos de calls
        available_strikes: Lista de strikes disponibles
        spot: Precio subyacente (SPX)
        dte: Days to expiration
        r: Risk-free rate
        delta_min: Delta mínimo aceptable (ej: 0.25 = 25%)
        delta_max: Delta máximo aceptable (ej: 0.35 = 35%)
        q: Dividend yield (default 0.0)

    Returns:
        Lista de strikes que cumplen con el rango de delta especificado
    """
    T = max(dte, 1) / 365.0
    candidates = []

    for K in available_strikes:
        row = fetch_row(calls_idx, K)
        if row is None:
            continue

        bid = float(row.get("bid", np.nan))
        ask = float(row.get("ask", np.nan))
        last = float(row.get("lastPrice", np.nan))
        if math.isnan(last):
            last = float(row.get("last", np.nan))

        mid = (bid + ask) / 2.0 if (not math.isnan(bid) and not math.isnan(ask) and ask > 0) else last

        if math.isnan(mid) or T <= 0:
            continue

        iv = implied_vol_call_from_price(spot, K, T, r, q, mid)
        if iv is None or (isinstance(iv, float) and (math.isnan(iv) or iv <= 0)):
            continue

        delta = bs_delta_call(spot, K, T, r, iv, q)

        if delta_min <= delta <= delta_max:
            candidates.append(K)

    return candidates

def fetch_row(idx: pd.DataFrame, K: float):
    """Devuelve la fila de índice 'K' (Series). Si duplicado, toma la primera."""
    try:
        r = idx.loc[K]
    except KeyError:
        return None
    if isinstance(r, pd.DataFrame):
        if r.empty: return None
        return r.iloc[0]
    return r

def quick_mid_idx(idx, K):
    row = fetch_row(idx, K)
    if row is None: return np.nan
    return get_mid_from_row(row)

def leg_liquidity_ok_idx(idx: pd.DataFrame, K: float, min_oi: int, min_vol: int) -> bool:
    row = fetch_row(idx, K)
    if row is None: return False
    oi  = pd.to_numeric(row.get("openInterest", np.nan), errors="coerce")
    vol = pd.to_numeric(row.get("volume", np.nan), errors="coerce")
    oi  = 0 if (not np.isfinite(oi))  else float(oi)
    vol = 0 if (not np.isfinite(vol)) else float(vol)
    return (oi >= float(min_oi)) and (vol >= float(min_vol))

# ================== URL ==================
def make_url(exp1,k1,exp2,k2,k3):
    ymd1=yyyymmdd_to_yymmdd(datetime.strptime(exp1,"%Y-%m-%d"))
    ymd2=yyyymmdd_to_yymmdd(datetime.strptime(exp2,"%Y-%m-%d"))
    r1,r2=root_for_exp(exp1),root_for_exp(exp2)
    legs=[f".{r1}{ymd1}C{int(k1)}x-1", f".{r2}{ymd2}C{int(k2)}x2", f".{r1}{ymd1}C{int(k3)}x-1"]
    return BASE_URL + ",".join(legs)

def make_allantis_url(exp1, k_ul, k_shorts, k_ll, k_call, exp2):
    """
    Genera URL de ThetaData para estructura Allantis de 5 patas.
    Estructura: +4P@k_ul : -8P@k_shorts : +4P@k_ll @ exp1 + -2C@k_call(exp1) : +2C@k_call(exp2)
    """
    ymd1 = yyyymmdd_to_yymmdd(datetime.strptime(exp1, "%Y-%m-%d"))
    ymd2 = yyyymmdd_to_yymmdd(datetime.strptime(exp2, "%Y-%m-%d"))
    r1, r2 = root_for_exp(exp1), root_for_exp(exp2)

    legs = [
        f".{r1}{ymd1}P{int(k_ul)}x4",        # +4P Upper Line
        f".{r1}{ymd1}P{int(k_shorts)}x-8",   # -8P Shorts
        f".{r1}{ymd1}P{int(k_ll)}x4",        # +4P Lower Line
        f".{r1}{ymd1}C{int(k_call)}x-2",     # -2C Short Call @ DTE1
        f".{r2}{ymd2}C{int(k_call)}x2"       # +2C Long Call @ DTE2
    ]
    return BASE_URL + ",".join(legs)

# ================== REVALORIZACIÓN BATMAN (FWD) ==================
def normalize_root_value(val):
    """Normaliza valor de root a formato estándar."""
    if val is None:
        return None
    try:
        s = str(val).strip().upper()
        return s if s else None
    except Exception:
        return None

def compare_mid_values(mid_fwd, mid_base):
    """Compara mid forward vs mid base para validación."""
    if mid_fwd is None or mid_base is None:
        return "MISSING"
    if not (np.isfinite(mid_fwd) and np.isfinite(mid_base)):
        return "NONFINITE"
    try:
        diff_pct = abs(float(mid_fwd) - float(mid_base)) / max(abs(float(mid_base)), 1e-6) * 100.0
        if diff_pct < 5.0:
            return "CERCANO"
        elif diff_pct < 15.0:
            return "MODERADO"
        else:
            return "DISTANTE"
    except Exception:
        return "ERROR"

def batman_value_from_df(one_min_df: pd.DataFrame, exp1: str, k1: float, exp2: str, k2: float, k3: float, root1=None, root2=None):
    """
    Revaloriza estructura Batman: -1C@k1(exp1) +2C@k2(exp2) -1C@k3(exp1)

    Args:
        one_min_df: DataFrame con snapshot del mercado
        exp1: Expiración de k1 y k3 (front, short legs)
        k1: Strike de primera call corta
        exp2: Expiración de k2 (back, long leg)
        k2: Strike de call larga (×2)
        k3: Strike de segunda call corta
        root1: Root esperado para exp1 (SPX/SPXW)
        root2: Root esperado para exp2 (SPX/SPXW)

    Returns:
        dict con net_credit (puntos SPX), leg1/leg2/leg3 (bid/ask/mid/root/strike)
        o None si no se puede revalorizar
    """
    def _to_float(val):
        try:
            v = float(val)
        except (TypeError, ValueError):
            return None
        return v if np.isfinite(v) else None

    def _to_index(idx_val):
        try:
            return int(idx_val)
        except (TypeError, ValueError):
            return idx_val

    def _get_root(row):
        for key in ("root", "root_symbol", "rootSymbol", "Root", "ROOT"):
            val = row.get(key) if hasattr(row, "get") else None
            if isinstance(val, str) and val.strip():
                return val.strip().upper()
        return None

    try:
        # Filtrar por right=C (calls), expiración, y root
        sub1 = one_min_df[(one_min_df["right"].str.upper()=="C") & (one_min_df["expiration"].astype(str)==exp1)]
        sub2 = one_min_df[(one_min_df["right"].str.upper()=="C") & (one_min_df["expiration"].astype(str)==exp2)]

        # Aplicar filtro estricto de root con preferencia
        sub1_filt, root1_pref_applied, root1_chosen = filter_df_by_root_strict(sub1.copy(), root1)
        sub2_filt, root2_pref_applied, root2_chosen = filter_df_by_root_strict(sub2.copy(), root2)

        # Log de anomalías de root (solo si no hubo match exacto)
        if root1 is not None and not root1_pref_applied and root1_chosen is not None:
            expected_norm = normalize_root_value(root1)
            if expected_norm != root1_chosen:
                pass  # Anomalía detectada, metadata ya registrada en leg
        if root2 is not None and not root2_pref_applied and root2_chosen is not None:
            expected_norm = normalize_root_value(root2)
            if expected_norm != root2_chosen:
                pass  # Anomalía detectada, metadata ya registrada en leg

        sub1_filt["strike"] = pd.to_numeric(sub1_filt["strike"], errors="coerce")
        sub2_filt["strike"] = pd.to_numeric(sub2_filt["strike"], errors="coerce")

        # Buscar strikes k1 y k3 en exp1
        r1 = sub1_filt.loc[np.isclose(sub1_filt["strike"], float(k1), atol=1e-6)]
        r3 = sub1_filt.loc[np.isclose(sub1_filt["strike"], float(k3), atol=1e-6)]

        # Buscar strike k2 en exp2
        r2 = sub2_filt.loc[np.isclose(sub2_filt["strike"], float(k2), atol=1e-6)]

        if r1.empty or r2.empty or r3.empty:
            return None

        # Selección inteligente de la mejor fila (sin iloc[0] ciego)
        row1, tie1 = select_best_strike_row(r1, base_mid=None)
        row2, tie2 = select_best_strike_row(r2, base_mid=None)
        row3, tie3 = select_best_strike_row(r3, base_mid=None)

        if row1 is None or row2 is None or row3 is None:
            return None

        mid1 = get_mid_from_row(row1)
        mid2 = get_mid_from_row(row2)
        mid3 = get_mid_from_row(row3)

        if not np.isfinite(mid1) or not np.isfinite(mid2) or not np.isfinite(mid3):
            return None

        def _build_leg(row, idx, mid, tie_break, root_pref_applied, root_chosen):
            leg_root = _get_root(row)
            return {
                "row_index": _to_index(idx),
                "bid": _to_float(row.get("bid")),
                "ask": _to_float(row.get("ask")),
                "mid": float(mid),
                "root": leg_root,
                "strike": _to_float(row.get("strike")),
                "tie_break": tie_break,
                "root_pref_applied": root_pref_applied,
                "root_chosen": root_chosen,
                "root_expected": normalize_root_value(root1) if idx in r1.index or idx in r3.index else normalize_root_value(root2)
            }

        leg1 = _build_leg(row1, r1.index[0] if len(r1) > 0 else None, mid1, tie1, root1_pref_applied, root1_chosen)
        leg2 = _build_leg(row2, r2.index[0] if len(r2) > 0 else None, mid2, tie2, root2_pref_applied, root2_chosen)
        leg3 = _build_leg(row3, r3.index[0] if len(r3) > 0 else None, mid3, tie3, root1_pref_applied, root1_chosen)

        # net_credit = -mid1 + 2*mid2 - mid3 (puntos SPX)
        # CREDIT negativo = cobramos (favorable)
        net_credit = float(-mid1 + 2*mid2 - mid3)

        return {
            "net_credit": net_credit,
            "leg1": leg1,
            "leg2": leg2,
            "leg3": leg3
        }
    except Exception:
        return None

def allantis_value_from_df(one_min_df: pd.DataFrame, exp1: str, k_ul: float, k_shorts: float,
                            k_ll: float, k_call: float, exp2: str, root1=None, root2=None):
    """
    Revaloriza estructura Allantis de 5 patas:
    - BWB Puts @ DTE1: +4P@k_ul : -8P@k_shorts : +4P@k_ll
    - Call Calendar: -2C@k_call(exp1) : +2C@k_call(exp2)

    Args:
        one_min_df: DataFrame con snapshot del mercado
        exp1: Expiración DTE1 (puts + short call)
        k_ul: Strike Upper Line puts (long 4P)
        k_shorts: Strike Short puts (short 8P)
        k_ll: Strike Lower Line puts (long 4P)
        k_call: Strike compartido para calls
        exp2: Expiración DTE2 (long calls)
        root1: Root esperado para exp1 (SPX/SPXW)
        root2: Root esperado para exp2 (SPX/SPXW)

    Returns:
        dict con net_credit (puntos SPX) y detalles de cada pata,
        o None si no se puede revalorizar
    """
    def _to_float(val):
        try:
            v = float(val)
        except (TypeError, ValueError):
            return None
        return v if np.isfinite(v) else None

    def _to_index(idx_val):
        try:
            return int(idx_val)
        except (TypeError, ValueError):
            return idx_val

    def _get_root(row):
        for key in ("root", "root_symbol", "rootSymbol", "Root", "ROOT"):
            val = row.get(key) if hasattr(row, "get") else None
            if isinstance(val, str) and val.strip():
                return val.strip().upper()
        return None

    try:
        # Filtrar por right y expiración
        puts1 = one_min_df[(one_min_df["right"].str.upper()=="P") & (one_min_df["expiration"].astype(str)==exp1)]
        calls1 = one_min_df[(one_min_df["right"].str.upper()=="C") & (one_min_df["expiration"].astype(str)==exp1)]
        calls2 = one_min_df[(one_min_df["right"].str.upper()=="C") & (one_min_df["expiration"].astype(str)==exp2)]

        # Aplicar filtro estricto de root
        puts1_filt, root1_pref_applied_p, root1_chosen_p = filter_df_by_root_strict(puts1.copy(), root1)
        calls1_filt, root1_pref_applied_c, root1_chosen_c = filter_df_by_root_strict(calls1.copy(), root1)
        calls2_filt, root2_pref_applied, root2_chosen = filter_df_by_root_strict(calls2.copy(), root2)

        # Convertir strikes a numérico
        puts1_filt["strike"] = pd.to_numeric(puts1_filt["strike"], errors="coerce")
        calls1_filt["strike"] = pd.to_numeric(calls1_filt["strike"], errors="coerce")
        calls2_filt["strike"] = pd.to_numeric(calls2_filt["strike"], errors="coerce")

        # Buscar strikes de puts @ exp1
        r_ul = puts1_filt.loc[np.isclose(puts1_filt["strike"], float(k_ul), atol=1e-6)]
        r_shorts = puts1_filt.loc[np.isclose(puts1_filt["strike"], float(k_shorts), atol=1e-6)]
        r_ll = puts1_filt.loc[np.isclose(puts1_filt["strike"], float(k_ll), atol=1e-6)]

        # Buscar strike de calls
        r_c_short = calls1_filt.loc[np.isclose(calls1_filt["strike"], float(k_call), atol=1e-6)]
        r_c_long = calls2_filt.loc[np.isclose(calls2_filt["strike"], float(k_call), atol=1e-6)]

        # Verificar que todos los strikes existen
        if r_ul.empty or r_shorts.empty or r_ll.empty or r_c_short.empty or r_c_long.empty:
            return None

        # Selección inteligente de la mejor fila
        row_ul, tie_ul = select_best_strike_row(r_ul, base_mid=None)
        row_shorts, tie_shorts = select_best_strike_row(r_shorts, base_mid=None)
        row_ll, tie_ll = select_best_strike_row(r_ll, base_mid=None)
        row_c_short, tie_c_short = select_best_strike_row(r_c_short, base_mid=None)
        row_c_long, tie_c_long = select_best_strike_row(r_c_long, base_mid=None)

        if any(r is None for r in [row_ul, row_shorts, row_ll, row_c_short, row_c_long]):
            return None

        # Obtener precios mid
        mid_ul = get_mid_from_row(row_ul)
        mid_shorts = get_mid_from_row(row_shorts)
        mid_ll = get_mid_from_row(row_ll)
        mid_c_short = get_mid_from_row(row_c_short)
        mid_c_long = get_mid_from_row(row_c_long)

        if not all(np.isfinite(m) for m in [mid_ul, mid_shorts, mid_ll, mid_c_short, mid_c_long]):
            return None

        def _build_leg(row, idx, mid, tie_break, root_pref_applied, root_chosen, expected_root):
            leg_root = _get_root(row)
            return {
                "row_index": _to_index(idx),
                "bid": _to_float(row.get("bid")),
                "ask": _to_float(row.get("ask")),
                "mid": float(mid),
                "root": leg_root,
                "strike": _to_float(row.get("strike")),
                "tie_break": tie_break,
                "root_pref_applied": root_pref_applied,
                "root_chosen": root_chosen,
                "root_expected": normalize_root_value(expected_root)
            }

        # Construir legs
        leg_ul = _build_leg(row_ul, r_ul.index[0] if len(r_ul) > 0 else None, mid_ul, tie_ul,
                           root1_pref_applied_p, root1_chosen_p, root1)
        leg_shorts = _build_leg(row_shorts, r_shorts.index[0] if len(r_shorts) > 0 else None, mid_shorts,
                               tie_shorts, root1_pref_applied_p, root1_chosen_p, root1)
        leg_ll = _build_leg(row_ll, r_ll.index[0] if len(r_ll) > 0 else None, mid_ll, tie_ll,
                           root1_pref_applied_p, root1_chosen_p, root1)
        leg_c_short = _build_leg(row_c_short, r_c_short.index[0] if len(r_c_short) > 0 else None, mid_c_short,
                                tie_c_short, root1_pref_applied_c, root1_chosen_c, root1)
        leg_c_long = _build_leg(row_c_long, r_c_long.index[0] if len(r_c_long) > 0 else None, mid_c_long,
                               tie_c_long, root2_pref_applied, root2_chosen, root2)

        # Calcular net_credit: +4P@UL -8P@Shorts +4P@LL -2C@Short +2C@Long (puntos SPX)
        net_credit = float(
            +4 * mid_ul
            - 8 * mid_shorts
            + 4 * mid_ll
            - 2 * mid_c_short
            + 2 * mid_c_long
        )

        return {
            "net_credit": net_credit,
            "leg_ul": leg_ul,
            "leg_shorts": leg_shorts,
            "leg_ll": leg_ll,
            "leg_c_short": leg_c_short,
            "leg_c_long": leg_c_long
        }
    except Exception:
        return None

# ================== FORWARD FACTOR BATMAN (FF_BAT) ==================
def compute_FF_BAT(IV2_K2, T2_years, IV1_K1, IV1_K3, T1_years, vega_K1, vega_K3, row_identifier=""):
    """
    Calcula el Forward Factor Batman-aware (FF_BAT) usando metodología específica para Batman.

    FF_BAT compara la IV promedio de los wings (ponderada por vega) con la IV forward
    implícita en el strike central K2.

    Args:
        IV2_K2: IV anualizada del body en K2 (back leg, decimal, ej. 0.35 para 35%)
        T2_years: Tiempo al vencimiento T2 en años (back leg)
        IV1_K1: IV anualizada del wing izquierdo en K1 (front leg, decimal)
        IV1_K3: IV anualizada del wing derecho en K3 (front leg, decimal)
        T1_years: Tiempo al vencimiento T1 en años (front legs)
        vega_K1: Vega de la opción en K1 (para ponderación)
        vega_K3: Vega de la opción en K3 (para ponderación)
        row_identifier: String identificador de la fila para logging (opcional)

    Returns:
        float: FF_BAT redondeado a 6 decimales, o np.nan si no es válido

    Formula:
        1. IV_fwd(K2) = √[(IV2(K2)² · T2 - IV1(K2)² · T1) / (T2 - T1)]
           donde IV1(K2) se estima como promedio ponderado de wings

        2. IV1_wings = [vega(K1)·IV1(K1) + vega(K3)·IV1(K3)] / [vega(K1) + vega(K3)]

        3. FF_BAT = IV1_wings / IV_fwd(K2) - 1

    Interpretación:
        - FF_BAT > 0: Los wings están "calientes" vs forward implícita del centro
        - FF_BAT < 0: Los wings están "fríos" vs forward implícita del centro
    """
    try:
        # Validar inputs básicos
        if IV2_K2 is None or T2_years is None or T1_years is None:
            return np.nan
        if IV1_K1 is None or IV1_K3 is None:
            return np.nan
        if vega_K1 is None or vega_K3 is None:
            return np.nan

        # Convertir a float
        iv2_k2 = float(IV2_K2)
        t2 = float(T2_years)
        iv1_k1 = float(IV1_K1)
        iv1_k3 = float(IV1_K3)
        t1 = float(T1_years)
        v_k1 = float(vega_K1)
        v_k3 = float(vega_K3)

        # Validar que son números finitos
        if not all(np.isfinite([iv2_k2, t2, iv1_k1, iv1_k3, t1, v_k1, v_k3])):
            return np.nan

        # Validar que IVs son positivas
        if iv2_k2 <= 0 or iv1_k1 <= 0 or iv1_k3 <= 0:
            return np.nan

        # Validar que T2 > T1
        if t2 <= t1:
            return np.nan

        # Calcular IV1_wings ponderado por vega
        sum_vega = v_k1 + v_k3
        if sum_vega <= 0:
            return np.nan

        iv1_wings = (v_k1 * iv1_k1 + v_k3 * iv1_k3) / sum_vega

        # Calcular varianza del body en T2
        var2_k2 = iv2_k2 * iv2_k2

        # Para calcular IV_fwd(K2), necesitamos estimar IV1(K2)
        # Usamos iv1_wings como proxy para IV1(K2)
        iv1_k2_proxy = iv1_wings
        var1_k2 = iv1_k2_proxy * iv1_k2_proxy

        # Calcular varianza forward en K2
        var_fwd_k2 = (var2_k2 * t2 - var1_k2 * t1) / (t2 - t1)

        # Validar que varianza forward es positiva
        if var_fwd_k2 <= 0:
            return np.nan

        # Calcular IV_fwd(K2)
        iv_fwd_k2 = math.sqrt(var_fwd_k2)

        # Calcular FF_BAT
        ff_bat = (iv1_wings / iv_fwd_k2) - 1.0

        # Redondear a 6 decimales
        return round(ff_bat, 6)

    except Exception as e:
        # En caso de cualquier error, retornar NaN silenciosamente
        return np.nan

# ================== CÁLCULO ESTRATEGIA ==================
def compute_strategy_metrics(spot,r_base,exp1,dte1,k1,exp2,dte2,k2,k3,precache):
    calls1, _, calls1_idx, _ = precache[exp1]
    calls2, _, calls2_idx, _ = precache[exp2]

    T1 = max(dte1,1)/365.0
    T2 = max(dte2,1)/365.0
    tau = max(T2 - T1, 0.0)
    r1=r_base; r2=r_base; q1=q2=0.0

    def greeks_price(idx: pd.DataFrame, exp, K, T, r, q):
        key=("call",exp,float(K),round(T,6))
        if key in GREEKS_CACHE:
            return GREEKS_CACHE[key]
        row = fetch_row(idx, K)
        if row is None:
            GREEKS_CACHE[key]=(np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan)
            return GREEKS_CACHE[key]

        # Leer precios
        bid = float(row.get("bid",np.nan))
        ask = float(row.get("ask",np.nan))
        last = float(row.get("lastPrice",np.nan))
        if math.isnan(last):
            last = float(row.get("last",np.nan))
        mid = (bid+ask)/2.0 if (not math.isnan(bid) and not math.isnan(ask) and ask>0) else (last if not math.isnan(last) else np.nan)

        if math.isnan(mid) or T<=0:
            GREEKS_CACHE[key]=(np.nan,np.nan,mid,np.nan,bid,ask,last)
            return GREEKS_CACHE[key]

        # ESTRATEGIA: Usar delta_BS y theta_BS del SNAPSHOT por defecto
        delta_snapshot = float(row.get("delta_BS", np.nan))
        theta_snapshot = float(row.get("theta_BS", np.nan))
        iv_snapshot = float(row.get("IV_BS", np.nan))

        # Si tenemos greeks válidos del snapshot, usarlos directamente
        if not math.isnan(delta_snapshot) and not math.isnan(iv_snapshot) and iv_snapshot > 0:
            # Usar greeks del snapshot (más confiables)
            delta = delta_snapshot
            # Convertir theta a USD diario (puntos × 365 días × 100 multiplicador)
            theta_daily = theta_snapshot * THETA_TO_DAILY if not math.isnan(theta_snapshot) else np.nan
            iv = iv_snapshot
            GREEKS_CACHE[key]=(delta,theta_daily,mid,iv,bid,ask,last)
            return GREEKS_CACHE[key]

        # FALLBACK: Calcular manualmente si los greeks del snapshot no están disponibles
        iv = implied_vol_call_from_price(spot, K, T, r, q, mid)
        if (iv is None) or (isinstance(iv,float) and (math.isnan(iv) or iv<=0)):
            # Último recurso: intentar usar delta del exchange
            delta_exchange = float(row.get("delta", np.nan))
            if not math.isnan(delta_exchange):
                delta = delta_exchange
                theta_daily = theta_snapshot * THETA_TO_DAILY if not math.isnan(theta_snapshot) else np.nan
                GREEKS_CACHE[key]=(delta,theta_daily,mid,iv,bid,ask,last)
                return GREEKS_CACHE[key]
            # Si no hay nada disponible, retornar NaN
            GREEKS_CACHE[key]=(np.nan,np.nan,mid,iv,bid,ask,last)
            return GREEKS_CACHE[key]

        # Calcular greeks manualmente si llegamos aquí
        delta = bs_delta_call(spot,K,T,r,iv,q)
        theta_annual_long = bs_theta_call_excel(spot,K,T,r,iv)
        theta_daily = theta_annual_long * THETA_TO_DAILY  # Convertir a USD diario
        GREEKS_CACHE[key]=(delta,theta_daily,mid,iv,bid,ask,last)
        return GREEKS_CACHE[key]

    d1,t1,p1,iv1,bid1,ask1,last1 = greeks_price(calls1_idx,exp1,k1,T1,r1,q1)   # -1C
    d2,t2,p2,iv2,bid2,ask2,last2 = greeks_price(calls2_idx,exp2,k2,T2,r2,q2)   # +2C
    d3,t3,p3,iv3,bid3,ask3,last3 = greeks_price(calls1_idx,exp1,k3,T1,r1,q1)   # -1C

    dz = lambda x: 0.0 if (x is None or (isinstance(x,float) and math.isnan(x))) else x
    delta_total = -1*dz(d1) + 2*dz(d2) - 1*dz(d3)
    theta_total = -1*dz(t1) + 2*dz(t2) - 1*dz(t3)

    p1z, p2z, p3z = dz(p1), dz(p2), dz(p3)
    net_credit = (-1*p1z) + (2*p2z) - (1*p3z)  # puntos SPX

    def clean_sigma(sig, default=0.20):
        try:
            return default if (sig is None or np.isnan(sig) or sig<=0) else float(sig)
        except:
            return default

    def bs_call_price_safe(S,K,T,r,sigma):
        return bs_call_price(S,K,T,r,sigma,q=0.0)

    def pnl_leg_at_S_T(q, K, P, S, T_rem, r, sigma):
        sigma = clean_sigma(sigma)
        if T_rem <= 1e-10:
            value = max(S - K, 0.0)
        else:
            value = bs_call_price_safe(S, K, T_rem, r, sigma)
        return q * 100.0 * (value - P)

    T_rem_long = max(T2 - T1, 0.0)
    pnl_short1 = pnl_leg_at_S_T(-1, k1, p1z, S_PNL, 0.0,        r1, iv1)
    pnl_long2  = pnl_leg_at_S_T(+2, k2, p2z, S_PNL, T_rem_long, r2, iv2)
    pnl_short3 = pnl_leg_at_S_T(-1, k3, p3z, S_PNL, 0.0,        r1, iv3)
    pnl_total  = pnl_short1 + pnl_long2 + pnl_short3

    # Death Valley + PnLDV (puntos SPX)
    death_valley = None
    pnl_dv_points = None
    tau = max(T2 - T1, 0.0)
    if tau > 0 and (iv2 is not None) and not (isinstance(iv2,float) and math.isnan(iv2)):
        sigma2 = float(iv2)
        S0 = float(k2) * math.exp(-(r2 + 0.5*sigma2*sigma2) * tau)
        k_lo, k_hi = (min(k1,k3), max(k1,k3))
        if (S0 >= k_lo) and (S0 < k_hi):
            val_short1 = -max(0.0, S0 - float(k1))
            val_short3 = -max(0.0, S0 - float(k3))
            val_long2  = 2.0 * bs_call_price_safe(S0, float(k2), tau, r2, sigma2)
            value_t1   = val_short1 + val_short3 + val_long2
            pnl_dv_points = value_t1 - net_credit
            death_valley  = S0
        else:
            value_lim = (float(k1) + float(k3)) - 2.0*float(k2)*math.exp(-r2*tau)
            pnl_dv_points = value_lim - net_credit
            death_valley  = float('nan')

    # Orejas (puntos SPX)
    iv2_clean = None if (iv2 is None or (isinstance(iv2,float) and math.isnan(iv2))) else float(iv2)
    EarL_pts = None; EarR_pts = None
    if tau > 0 and iv2_clean is not None:
        EarL_pts = 2.0*bs_call_price_safe(float(k1), float(k2), tau, r2, iv2_clean) - net_credit
        EarR_pts = (-(float(k3) - float(k1))) + 2.0*bs_call_price_safe(float(k3), float(k2), tau, r2, iv2_clean) - net_credit

    # UEL Infinita: pérdida máxima plana de la oreja derecha al llegar T1
    # Las dos cortas T1 vencen, la larga doble T2 sigue viva
    # Fórmula correcta: valor_limite - net_credit, donde valor_limite = (K1 + K3) - 2·PVK2
    # PVK2 = K2·exp(-r·ΔT) es el valor presente del strike K2 descontado de T2 a T1
    UEL_inf_USD = None
    if tau > 0:
        # Calcular el strike descontado de la larga (K2)
        PVK2 = float(k2) * math.exp(-r2 * tau)
        # Pérdida máxima plana en puntos: valor_limite - net_credit
        # Si net_credit < 0 (recibimos crédito), -net_credit suma a la UEL (peor pérdida)
        # Si net_credit > 0 (pagamos débito), -net_credit resta a la UEL (mejor pérdida)
        UEL_inf_pts = (float(k1) + float(k3)) - 2.0*PVK2 - net_credit
        # Convertir a USD (multiplicador 100 para SPX)
        UEL_inf_USD = round(100.0 * UEL_inf_pts, 2)
    elif tau <= 0:
        # Si no hay diferencia temporal, usar K2 sin descuento
        PVK2 = float(k2)
        UEL_inf_pts = (float(k1) + float(k3)) - 2.0*PVK2 - net_credit
        UEL_inf_USD = round(100.0 * UEL_inf_pts, 2)

    # Ratio UEL / Promedio(EarL, EarR)
    RATIO_UEL_EARS = None
    if UEL_inf_USD is not None and EarL_pts is not None and EarR_pts is not None:
        # Convertir orejas a USD (multiplicador 100 para SPX)
        EarL_USD = EarL_pts * 100.0
        EarR_USD = EarR_pts * 100.0
        avg_ears = (EarL_USD + EarR_USD) / 2.0
        if abs(avg_ears) > 1e-6:  # Evitar división por cero
            RATIO_UEL_EARS = round(UEL_inf_USD / avg_ears, 4)

    iv1_fmt = None if (iv1 is None or (isinstance(iv1,float) and math.isnan(iv1))) else round(float(iv1),6)
    iv2_fmt = None if (iv2 is None or (isinstance(iv2,float) and math.isnan(iv2))) else round(float(iv2),6)
    iv3_fmt = None if (iv3 is None or (isinstance(iv3,float) and math.isnan(iv3))) else round(float(iv3),6)

    # Calcular FF_BAT (Batman-aware Forward Factor)
    # Necesitamos las vegas de K1 y K3 para ponderar
    vega_k1 = np.nan
    vega_k3 = np.nan

    # Calcular vega para K1 (wing izquierdo)
    if iv1 is not None and not (isinstance(iv1, float) and math.isnan(iv1)) and iv1 > 0:
        try:
            vega_k1 = bs_vega_call(spot, float(k1), T1, r1, float(iv1), q1)
        except:
            vega_k1 = np.nan

    # Calcular vega para K3 (wing derecho)
    if iv3 is not None and not (isinstance(iv3, float) and math.isnan(iv3)) and iv3 > 0:
        try:
            vega_k3 = bs_vega_call(spot, float(k3), T1, r1, float(iv3), q1)
        except:
            vega_k3 = np.nan

    # Calcular FF_BAT
    ff_bat_val = compute_FF_BAT(
        IV2_K2=iv2,
        T2_years=T2,
        IV1_K1=iv1,
        IV1_K3=iv3,
        T1_years=T1,
        vega_K1=vega_k1,
        vega_K3=vega_k3,
        row_identifier=row_id
    )

    # Calcular RATIO_BATMAN: (K3-K1) / abs(net_credit)
    # Representa cuántos puntos de spread obtenemos por cada punto de crédito recibido
    # Valores altos indican mejor ratio riesgo/recompensa
    spread_width = float(k3) - float(k1)
    if net_credit != 0 and np.isfinite(net_credit):
        ratio_batman = spread_width / abs(net_credit)
    else:
        ratio_batman = np.nan

    out = {
        "delta_total": round(delta_total,6),
        "theta_total": round(theta_total,6),
        "net_credit": round(net_credit,4),

        "k1": int(k1), "k2": int(k2), "k3": int(k3),

        "iv_k1": iv1_fmt,
        "iv_k2": iv2_fmt,
        "iv_k3": iv3_fmt,

        "FF_BAT": ff_bat_val if np.isfinite(ff_bat_val) else None,
        "RATIO_BATMAN": round(ratio_batman, 4) if np.isfinite(ratio_batman) else None,

        "Death valley": None if (death_valley is None or (isinstance(death_valley,float) and math.isnan(death_valley))) else round(float(death_valley),2),
        "PnLDV": None if (pnl_dv_points is None or (isinstance(pnl_dv_points,float) and math.isnan(pnl_dv_points))) else round(float(pnl_dv_points),2),

        "EarL": None if (EarL_pts is None or (isinstance(EarL_pts,float) and math.isnan(EarL_pts))) else round(float(EarL_pts),2),
        "EarR": None if (EarR_pts is None or (isinstance(EarR_pts,float) and math.isnan(EarR_pts))) else round(float(EarR_pts),2),
        "UEL_inf_USD": UEL_inf_USD,
        "RATIO_UEL_EARS": RATIO_UEL_EARS,

        "DTE1/DTE2": f"{int(round(dte1))}/{int(round(dte2))}",

        "delta_k1": None if math.isnan(d1) else round(d1,6),
        "theta_k1": None if math.isnan(t1) else round(t1,6),
        "delta_k2": None if math.isnan(d2) else round(d2,6),
        "theta_k2": None if math.isnan(t2) else round(t2,6),
        "delta_k3": None if math.isnan(d3) else round(d3,6),
        "theta_k3": None if math.isnan(t3) else round(t3,6),

        "price_bid_short1": None if math.isnan(bid1) else round(bid1,4),
        "price_ask_short1": None if math.isnan(ask1) else round(ask1,4),
        "price_last_short1": None if math.isnan(last1) else round(last1,4),
        "price_mid_short1":  None if math.isnan(p1)   else round(p1,4),

        "price_bid_long2": None if math.isnan(bid2) else round(bid2,4),
        "price_ask_long2": None if math.isnan(ask2) else round(ask2,4),
        "price_last_long2": None if math.isnan(last2) else round(last2,4),
        "price_mid_long2":  None if math.isnan(p2)   else round(p2,4),

        "price_bid_short3": None if math.isnan(bid3) else round(bid3,4),
        "price_ask_short3": None if math.isnan(ask3) else round(ask3,4),
        "price_last_short3": None if math.isnan(last3) else round(last3,4),
        "price_mid_short3":  None if math.isnan(p3)   else round(p3,4),

        "pnl8000_short1": round(pnl_short1,2),
        "pnl8000_long2":  round(pnl_long2,2),
        "pnl8000_short3": round(pnl_short3,2),
        "pnl8000_total":  round(pnl_total,2),
    }
    return out

# ================== MÉTRICAS ALLANTIS (5 LEGS: BWB PUTS + CALL CALENDAR) ==================
def compute_allantis_metrics(spot, r_base, exp1, dte1, k_ul, k_shorts, k_ll, k_call, exp2, dte2, precache):
    """
    Calcula métricas para estructura Allantis de 5 patas:
    - BWB de puts @ DTE1: +4P@UL : -8P@Shorts : +4P@LL
    - Call Calendar: -2C@DTE1 : +2C@DTE2 (mismo strike k_call)

    Args:
        spot: precio actual SPX
        r_base: tasa libre de riesgo
        exp1: expiration DTE1 (RANGE_A, ~160 días)
        dte1: días hasta exp1
        k_ul: strike Upper Line puts (long 4P)
        k_shorts: strike Short puts (short 8P)
        k_ll: strike Lower Line puts (long 4P)
        k_call: strike compartido para calls (short 2C @ DTE1, long 2C @ DTE2)
        exp2: expiration DTE2 (RANGE_B, ~220 días)
        dte2: días hasta exp2
        precache: diccionario con datos de opciones por expiration

    Returns:
        dict con todas las métricas calculadas
    """
    # Obtener datos de puts y calls para ambas expiraciones
    _, puts1, _, puts1_idx = precache[exp1]
    calls1, _, calls1_idx, _ = precache[exp1]
    calls2, _, calls2_idx, _ = precache[exp2]

    T1 = max(dte1, 1) / 365.0
    T2 = max(dte2, 1) / 365.0
    tau = max(T2 - T1, 0.0)
    r1 = r_base
    r2 = r_base
    q1 = q2 = 0.0

    def greeks_price_put(idx: pd.DataFrame, exp, K, T, r, q):
        """Obtiene greeks y precio para una put."""
        key = ("put", exp, float(K), round(T, 6))
        if key in GREEKS_CACHE:
            return GREEKS_CACHE[key]

        row = fetch_row(idx, K)
        if row is None:
            GREEKS_CACHE[key] = (np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan)
            return GREEKS_CACHE[key]

        # Leer precios
        bid = float(row.get("bid", np.nan))
        ask = float(row.get("ask", np.nan))
        last = float(row.get("lastPrice", np.nan))
        if math.isnan(last):
            last = float(row.get("last", np.nan))
        mid = (bid + ask) / 2.0 if (not math.isnan(bid) and not math.isnan(ask) and ask > 0) else (last if not math.isnan(last) else np.nan)

        if math.isnan(mid) or T <= 0:
            GREEKS_CACHE[key] = (np.nan, np.nan, mid, np.nan, bid, ask, last)
            return GREEKS_CACHE[key]

        # Usar greeks del snapshot si están disponibles
        delta_snapshot = float(row.get("delta_BS", np.nan))
        theta_snapshot = float(row.get("theta_BS", np.nan))
        iv_snapshot = float(row.get("IV_BS", np.nan))

        if not math.isnan(delta_snapshot) and not math.isnan(iv_snapshot) and iv_snapshot > 0:
            delta = delta_snapshot
            theta_daily = theta_snapshot * THETA_TO_DAILY if not math.isnan(theta_snapshot) else np.nan
            iv = iv_snapshot
            GREEKS_CACHE[key] = (delta, theta_daily, mid, iv, bid, ask, last)
            return GREEKS_CACHE[key]

        # FALLBACK: calcular manualmente
        iv = implied_vol_put_from_price(spot, K, T, r, q, mid)
        if (iv is None) or (isinstance(iv, float) and (math.isnan(iv) or iv <= 0)):
            delta_exchange = float(row.get("delta", np.nan))
            if not math.isnan(delta_exchange):
                delta = delta_exchange
                theta_daily = theta_snapshot * THETA_TO_DAILY if not math.isnan(theta_snapshot) else np.nan
                GREEKS_CACHE[key] = (delta, theta_daily, mid, iv, bid, ask, last)
                return GREEKS_CACHE[key]
            GREEKS_CACHE[key] = (np.nan, np.nan, mid, iv, bid, ask, last)
            return GREEKS_CACHE[key]

        # Calcular greeks manualmente
        delta = bs_delta_put(spot, K, T, r, iv, q)
        theta_annual_long = bs_theta_put_excel(spot, K, T, r, iv)
        theta_daily = theta_annual_long * THETA_TO_DAILY
        GREEKS_CACHE[key] = (delta, theta_daily, mid, iv, bid, ask, last)
        return GREEKS_CACHE[key]

    def greeks_price_call(idx: pd.DataFrame, exp, K, T, r, q):
        """Obtiene greeks y precio para una call (reutiliza lógica de Batman)."""
        key = ("call", exp, float(K), round(T, 6))
        if key in GREEKS_CACHE:
            return GREEKS_CACHE[key]

        row = fetch_row(idx, K)
        if row is None:
            GREEKS_CACHE[key] = (np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan)
            return GREEKS_CACHE[key]

        bid = float(row.get("bid", np.nan))
        ask = float(row.get("ask", np.nan))
        last = float(row.get("lastPrice", np.nan))
        if math.isnan(last):
            last = float(row.get("last", np.nan))
        mid = (bid + ask) / 2.0 if (not math.isnan(bid) and not math.isnan(ask) and ask > 0) else (last if not math.isnan(last) else np.nan)

        if math.isnan(mid) or T <= 0:
            GREEKS_CACHE[key] = (np.nan, np.nan, mid, np.nan, bid, ask, last)
            return GREEKS_CACHE[key]

        delta_snapshot = float(row.get("delta_BS", np.nan))
        theta_snapshot = float(row.get("theta_BS", np.nan))
        iv_snapshot = float(row.get("IV_BS", np.nan))

        if not math.isnan(delta_snapshot) and not math.isnan(iv_snapshot) and iv_snapshot > 0:
            delta = delta_snapshot
            theta_daily = theta_snapshot * THETA_TO_DAILY if not math.isnan(theta_snapshot) else np.nan
            iv = iv_snapshot
            GREEKS_CACHE[key] = (delta, theta_daily, mid, iv, bid, ask, last)
            return GREEKS_CACHE[key]

        iv = implied_vol_call_from_price(spot, K, T, r, q, mid)
        if (iv is None) or (isinstance(iv, float) and (math.isnan(iv) or iv <= 0)):
            delta_exchange = float(row.get("delta", np.nan))
            if not math.isnan(delta_exchange):
                delta = delta_exchange
                theta_daily = theta_snapshot * THETA_TO_DAILY if not math.isnan(theta_snapshot) else np.nan
                GREEKS_CACHE[key] = (delta, theta_daily, mid, iv, bid, ask, last)
                return GREEKS_CACHE[key]
            GREEKS_CACHE[key] = (np.nan, np.nan, mid, iv, bid, ask, last)
            return GREEKS_CACHE[key]

        delta = bs_delta_call(spot, K, T, r, iv, q)
        theta_annual_long = bs_theta_call_excel(spot, K, T, r, iv)
        theta_daily = theta_annual_long * THETA_TO_DAILY
        GREEKS_CACHE[key] = (delta, theta_daily, mid, iv, bid, ask, last)
        return GREEKS_CACHE[key]

    # Obtener greeks y precios de las 5 patas
    # Puts @ DTE1
    d_ul, t_ul, p_ul, iv_ul, bid_ul, ask_ul, last_ul = greeks_price_put(puts1_idx, exp1, k_ul, T1, r1, q1)  # +4P UL
    d_shorts, t_shorts, p_shorts, iv_shorts, bid_shorts, ask_shorts, last_shorts = greeks_price_put(puts1_idx, exp1, k_shorts, T1, r1, q1)  # -8P Shorts
    d_ll, t_ll, p_ll, iv_ll, bid_ll, ask_ll, last_ll = greeks_price_put(puts1_idx, exp1, k_ll, T1, r1, q1)  # +4P LL

    # Calls
    d_c_short, t_c_short, p_c_short, iv_c_short, bid_c_short, ask_c_short, last_c_short = greeks_price_call(calls1_idx, exp1, k_call, T1, r1, q1)  # -2C @ DTE1
    d_c_long, t_c_long, p_c_long, iv_c_long, bid_c_long, ask_c_long, last_c_long = greeks_price_call(calls2_idx, exp2, k_call, T2, r2, q2)  # +2C @ DTE2

    # Helper para convertir NaN a 0
    dz = lambda x: 0.0 if (x is None or (isinstance(x, float) and math.isnan(x))) else x

    # Calcular delta y theta totales
    # Estructura: +4P@UL -8P@Shorts +4P@LL -2C@DTE1 +2C@DTE2
    delta_total = (
        +4 * dz(d_ul)
        - 8 * dz(d_shorts)
        + 4 * dz(d_ll)
        - 2 * dz(d_c_short)
        + 2 * dz(d_c_long)
    )

    theta_total = (
        +4 * dz(t_ul)
        - 8 * dz(t_shorts)
        + 4 * dz(t_ll)
        - 2 * dz(t_c_short)
        + 2 * dz(t_c_long)
    )

    # Calcular net_credit (puntos SPX)
    # Estructura: +4P@UL -8P@Shorts +4P@LL -2C@DTE1 +2C@DTE2
    p_ul_z = dz(p_ul)
    p_shorts_z = dz(p_shorts)
    p_ll_z = dz(p_ll)
    p_c_short_z = dz(p_c_short)
    p_c_long_z = dz(p_c_long)

    net_credit = (+4 * p_ul_z) + (-8 * p_shorts_z) + (+4 * p_ll_z) + (-2 * p_c_short_z) + (+2 * p_c_long_z)

    # Formatear IVs
    iv_ul_fmt = None if (iv_ul is None or (isinstance(iv_ul, float) and math.isnan(iv_ul))) else round(float(iv_ul), 6)
    iv_shorts_fmt = None if (iv_shorts is None or (isinstance(iv_shorts, float) and math.isnan(iv_shorts))) else round(float(iv_shorts), 6)
    iv_ll_fmt = None if (iv_ll is None or (isinstance(iv_ll, float) and math.isnan(iv_ll))) else round(float(iv_ll), 6)
    iv_c_short_fmt = None if (iv_c_short is None or (isinstance(iv_c_short, float) and math.isnan(iv_c_short))) else round(float(iv_c_short), 6)
    iv_c_long_fmt = None if (iv_c_long is None or (isinstance(iv_c_long, float) and math.isnan(iv_c_long))) else round(float(iv_c_long), 6)

    # ================== CÁLCULO DE PUNTOS GEOMÉTRICOS ALLANTIS @ VENCIMIENTO DTE1 ==================
    # Estructura: BWB de Puts + Call Calendar
    # Puntos: LEL, PuntoLEL, BreakEvenBWB1, PICOBWB, BreakEvenBWB2, VALLE, BreakEvenCCAL1, PICOCCAL, BreakEvenCCAL2, UEL

    # Convertir net_credit a débito para cálculos (convención: débito positivo)
    # net_credit > 0 significa pagamos débito, net_credit < 0 significa recibimos crédito
    net_debit = net_credit  # En puntos SPX

    # --- 1. LEL (Lower Expiration Line) - Asíntota inferior ---
    # Cuando S < k_ll, todas las puts están ITM
    # PnL = 4·(k_ul - S) - 8·(k_shorts - S) + 4·(k_ll - S) - net_debit
    #     = 4k_ul - 8k_shorts + 4k_ll - net_debit (constante)
    LEL_pts = 4*float(k_ul) - 8*float(k_shorts) + 4*float(k_ll) - net_debit
    LEL_USD = LEL_pts * 100.0  # Convertir a USD

    # --- 2. PuntoLEL - Punto de inflexión donde comienza el ascenso ---
    # Ubicado en S = k_ll
    PuntoLEL_strike = float(k_ll)
    PuntoLEL_PnL_pts = LEL_pts  # Mismo valor que LEL
    PuntoLEL_PnL_USD = LEL_USD

    # --- 3. BreakEvenBWB1 - Primer breakeven de la BWB ---
    # En región k_ll < S < k_shorts: PnL = (4k_ul - 8k_shorts - net_debit) + 4S
    # Resolver: (4k_ul - 8k_shorts - net_debit) + 4S = 0
    # S = (8k_shorts - 4k_ul + net_debit) / 4
    BreakEvenBWB1_strike = None
    numerator_be1 = 8*float(k_shorts) - 4*float(k_ul) + net_debit
    if numerator_be1 >= 0:  # Solo válido si S >= 0
        be1_candidate = numerator_be1 / 4.0
        # Verificar que está en rango válido: k_ll < S < k_shorts
        if float(k_ll) < be1_candidate < float(k_shorts):
            BreakEvenBWB1_strike = be1_candidate

    # --- 4. PICOBWB - Máximo de la BWB ---
    # Ubicado en S = k_shorts
    PICOBWB_strike = float(k_shorts)
    # PnL = 4·(k_ul - k_shorts) - net_debit
    PICOBWB_PnL_pts = 4*(float(k_ul) - float(k_shorts)) - net_debit
    PICOBWB_PnL_USD = PICOBWB_PnL_pts * 100.0

    # --- 5. BreakEvenBWB2 - Segundo breakeven de la BWB ---
    # En región k_shorts < S < k_ul: PnL = 4k_ul - 4S - net_debit
    # Resolver: 4k_ul - 4S - net_debit = 0
    # S = k_ul - net_debit/4
    BreakEvenBWB2_strike = None
    be2_candidate = float(k_ul) - net_debit / 4.0
    # Verificar que está en rango válido: k_shorts < S < k_ul
    if float(k_shorts) < be2_candidate < float(k_ul):
        BreakEvenBWB2_strike = be2_candidate

    # --- 6. VALLE (PnLDVAllantis) - Mínimo entre BWB y Calendar Call ---
    # En región S >= k_ul: PnL_BWB = -net_debit (plana)
    # PnL_CCAL = 2·BS_Call(S, k_call, τ, r2, σ_call)
    # Valle ocurre donde la suma es mínima

    # Función auxiliar para calcular PnL total en un precio S
    def pnl_total_at_S(S, k_ul_f, k_shorts_f, k_ll_f, k_call_f, tau_f, r2_f, iv_call_f, net_debit_f):
        """Calcula PnL total de Allantis en precio S al vencimiento DTE1"""
        # PnL BWB de Puts
        if S < k_ll_f:
            pnl_bwb = 4*k_ul_f - 8*k_shorts_f + 4*k_ll_f - net_debit_f
        elif S < k_shorts_f:
            pnl_bwb = (4*k_ul_f - 8*k_shorts_f - net_debit_f) + 4*S
        elif S < k_ul_f:
            pnl_bwb = 4*k_ul_f - 4*S - net_debit_f
        else:
            pnl_bwb = -net_debit_f

        # PnL Call Calendar
        if tau_f > 0 and iv_call_f is not None and not math.isnan(iv_call_f) and iv_call_f > 0:
            # Long call @ DTE2 sigue viva con tiempo restante τ
            long_call_value = bs_call_price(S, k_call_f, tau_f, r2_f, iv_call_f, q=0.0)
            # Short call @ DTE1 ha vencido
            short_call_payoff = max(S - k_call_f, 0.0)
            pnl_ccal = 2*long_call_value - 2*short_call_payoff
        else:
            pnl_ccal = 0.0

        return pnl_bwb + pnl_ccal

    # Buscar valle entre k_ul y k_call usando búsqueda numérica
    VALLE_strike = None
    VALLE_PnL_pts = None
    VALLE_PnL_USD = None

    if tau > 0 and iv_c_long is not None and not math.isnan(iv_c_long) and iv_c_long > 0:
        # Rango de búsqueda del valle: [k_ul, k_call]
        if float(k_ul) < float(k_call):
            # Muestrear 100 puntos en el rango
            search_range = np.linspace(float(k_ul), float(k_call), 100)
            pnl_values = [pnl_total_at_S(s, float(k_ul), float(k_shorts), float(k_ll),
                                         float(k_call), tau, r2, float(iv_c_long), net_debit)
                         for s in search_range]
            min_idx = np.argmin(pnl_values)
            VALLE_strike = search_range[min_idx]
            VALLE_PnL_pts = pnl_values[min_idx]
            VALLE_PnL_USD = VALLE_PnL_pts * 100.0

    # --- 7. BreakEvenCCAL1 - Primer breakeven del Call Calendar ---
    # Buscar raíz de PnL_Total(S) = 0 en región [VALLE, k_call]
    BreakEvenCCAL1_strike = None

    if VALLE_strike is not None and tau > 0 and iv_c_long is not None and not math.isnan(iv_c_long):
        from scipy.optimize import brentq
        try:
            # Definir función objetivo
            def pnl_for_solver(s):
                return pnl_total_at_S(s, float(k_ul), float(k_shorts), float(k_ll),
                                     float(k_call), tau, r2, float(iv_c_long), net_debit)

            # Buscar raíz entre valle y k_call
            # Primero verificar que hay cambio de signo
            pnl_at_valle = pnl_for_solver(VALLE_strike)
            pnl_at_kcall = pnl_for_solver(float(k_call))

            if pnl_at_valle * pnl_at_kcall < 0:  # Cambio de signo
                BreakEvenCCAL1_strike = brentq(pnl_for_solver, VALLE_strike, float(k_call))
        except:
            pass  # Si falla, dejar como None

    # --- 8. PICOCCAL - Máximo del Call Calendar ---
    # Buscar máximo de PnL_Total(S) en región [BreakEvenCCAL1, k_call*1.1]
    PICOCCAL_strike = None
    PICOCCAL_PnL_pts = None
    PICOCCAL_PnL_USD = None

    if tau > 0 and iv_c_long is not None and not math.isnan(iv_c_long):
        # Rango de búsqueda: [k_call*0.95, k_call*1.15]
        search_start = float(k_call) * 0.95
        search_end = float(k_call) * 1.15
        search_range_ccal = np.linspace(search_start, search_end, 100)
        pnl_values_ccal = [pnl_total_at_S(s, float(k_ul), float(k_shorts), float(k_ll),
                                          float(k_call), tau, r2, float(iv_c_long), net_debit)
                          for s in search_range_ccal]
        max_idx = np.argmax(pnl_values_ccal)
        PICOCCAL_strike = search_range_ccal[max_idx]
        PICOCCAL_PnL_pts = pnl_values_ccal[max_idx]
        PICOCCAL_PnL_USD = PICOCCAL_PnL_pts * 100.0

    # --- 9. BreakEvenCCAL2 - Segundo breakeven del Call Calendar ---
    # Buscar raíz de PnL_Total(S) = 0 en región [k_call, k_call*1.5]
    BreakEvenCCAL2_strike = None

    if PICOCCAL_strike is not None and tau > 0 and iv_c_long is not None and not math.isnan(iv_c_long):
        from scipy.optimize import brentq
        try:
            def pnl_for_solver(s):
                return pnl_total_at_S(s, float(k_ul), float(k_shorts), float(k_ll),
                                     float(k_call), tau, r2, float(iv_c_long), net_debit)

            # Buscar raíz entre PICOCCAL y k_call*1.5
            search_end = float(k_call) * 1.5
            pnl_at_pico = pnl_for_solver(PICOCCAL_strike)
            pnl_at_end = pnl_for_solver(search_end)

            if pnl_at_pico * pnl_at_end < 0:  # Cambio de signo
                BreakEvenCCAL2_strike = brentq(pnl_for_solver, PICOCCAL_strike, search_end)
        except:
            pass

    # --- 10. UEL (Upper Expiration Line) - Asíntota superior ---
    # Cuando S -> ∞: PnL = -net_debit + 2·k_call·(1 - e^(-r2·τ))
    UEL_pts = None
    UEL_USD = None
    if tau > 0:
        UEL_pts = -net_debit + 2*float(k_call)*(1 - math.exp(-r2*tau))
        UEL_USD = UEL_pts * 100.0

    # --- 11. MASTER_DISTANCE - Distancia entre breakevens ---
    # Diferencia entre BreakEvenBWB1_strike y BreakEvenCCAL2_strike
    MASTER_DISTANCE = None
    if BreakEvenBWB1_strike is not None and BreakEvenCCAL2_strike is not None:
        MASTER_DISTANCE = BreakEvenBWB1_strike - BreakEvenCCAL2_strike

    # --- 12. AQI (Allantis Quality Index) - Indicador de calidad global ---
    # Similar a BQI_ABS de Batman, pero adaptado a métricas de Allantis
    # Premia el balance entre todas las métricas (no solo tener algunas muy buenas)
    AQI = None

    # Solo calcular si tenemos todas las métricas necesarias
    if (LEL_pts is not None and UEL_pts is not None and
        PICOBWB_PnL_pts is not None and PICOCCAL_PnL_pts is not None and
        VALLE_PnL_pts is not None and theta_total is not None and
        MASTER_DISTANCE is not None):

        # Constantes de normalización y pesos
        # Estos valores se pueden ajustar según la experiencia
        EPS = 1e-6  # Epsilon para evitar división por cero

        # Pesos (theta tiene peso levemente superior: 1.2 vs 1.0 para el resto)
        W_LEL = 1.0
        W_UEL = 1.0
        W_PICO_BWB = 1.0
        W_PICO_CCAL = 1.0
        W_VALLE = 1.0
        W_THETA = 1.2  # Peso levemente superior para theta
        W_MASTER_DIST = 1.0

        # Normalizar cada métrica a escala [0, ∞) donde mayor es mejor
        # LEL: normalizar usando un valor base típico (ej: 0 es malo, 5+ es bueno)
        LEL_norm = max(0, LEL_pts) / (5.0 + EPS)

        # UEL: normalizar usando un valor base típico
        UEL_norm = max(0, UEL_pts) / (10.0 + EPS)

        # PICO BWB: normalizar (valores típicos 0-10)
        PICO_BWB_norm = max(0, PICOBWB_PnL_pts) / (5.0 + EPS)

        # PICO CCAL: normalizar (valores típicos 0-10)
        PICO_CCAL_norm = max(0, PICOCCAL_PnL_pts) / (5.0 + EPS)

        # VALLE: convertir para que menos negativo sea mejor
        # Si VALLE es -10, es peor que si es -2. Si es positivo, es excelente.
        # Usamos: (VALLE + offset) / scale para normalizarlo
        # Asumiendo que valles típicos están entre -20 y +5
        VALLE_norm = (VALLE_PnL_pts + 20.0) / (10.0 + EPS)
        VALLE_norm = max(0, VALLE_norm)  # Asegurar no negativo

        # Theta: normalizar (valores típicos 0-50 en valor absoluto)
        theta_norm = max(0, theta_total) / (20.0 + EPS)

        # MASTER_DISTANCE: normalizar (valores típicos 0-200)
        MASTER_DIST_norm = max(0, MASTER_DISTANCE) / (50.0 + EPS)

        # Calcular media geométrica ponderada
        # Formula: AQI = (∏ metric_i^w_i)^(1/Σw_i)
        # Esto penaliza fuertemente tener alguna métrica muy baja

        sum_weights = W_LEL + W_UEL + W_PICO_BWB + W_PICO_CCAL + W_VALLE + W_THETA + W_MASTER_DIST

        # Para evitar log(0), añadimos un pequeño offset a cada métrica
        offset = 0.1

        try:
            product_term = (
                ((LEL_norm + offset) ** W_LEL) *
                ((UEL_norm + offset) ** W_UEL) *
                ((PICO_BWB_norm + offset) ** W_PICO_BWB) *
                ((PICO_CCAL_norm + offset) ** W_PICO_CCAL) *
                ((VALLE_norm + offset) ** W_VALLE) *
                ((theta_norm + offset) ** W_THETA) *
                ((MASTER_DIST_norm + offset) ** W_MASTER_DIST)
            )

            # Aplicar raíz (1/sum_weights)
            AQI = product_term ** (1.0 / sum_weights)

            # Escalar a rango más interpretable (multiplicar por factor para que valores típicos estén en 1-10)
            AQI = AQI * 2.0

        except (ValueError, ZeroDivisionError, OverflowError):
            AQI = None

    # Construir output
    out = {
        "delta_total": round(delta_total, 6),
        "theta_total": round(theta_total, 6),
        "net_credit": round(net_credit, 4),

        # Strikes
        "k_ul": int(k_ul),
        "k_shorts": int(k_shorts),
        "k_ll": int(k_ll),
        "k_call": int(k_call),

        # IVs
        "iv_ul": iv_ul_fmt,
        "iv_shorts": iv_shorts_fmt,
        "iv_ll": iv_ll_fmt,
        "iv_c_short": iv_c_short_fmt,
        "iv_c_long": iv_c_long_fmt,

        "DTE1/DTE2": f"{int(round(dte1))}/{int(round(dte2))}",

        # Greeks individuales de cada pata
        "delta_ul": None if math.isnan(d_ul) else round(d_ul, 6),
        "theta_ul": None if math.isnan(t_ul) else round(t_ul, 6),
        "delta_shorts": None if math.isnan(d_shorts) else round(d_shorts, 6),
        "theta_shorts": None if math.isnan(t_shorts) else round(t_shorts, 6),
        "delta_ll": None if math.isnan(d_ll) else round(d_ll, 6),
        "theta_ll": None if math.isnan(t_ll) else round(t_ll, 6),
        "delta_c_short": None if math.isnan(d_c_short) else round(d_c_short, 6),
        "theta_c_short": None if math.isnan(t_c_short) else round(t_c_short, 6),
        "delta_c_long": None if math.isnan(d_c_long) else round(d_c_long, 6),
        "theta_c_long": None if math.isnan(t_c_long) else round(t_c_long, 6),

        # Precios de cada pata
        "price_bid_ul": None if math.isnan(bid_ul) else round(bid_ul, 4),
        "price_ask_ul": None if math.isnan(ask_ul) else round(ask_ul, 4),
        "price_last_ul": None if math.isnan(last_ul) else round(last_ul, 4),
        "price_mid_ul": None if math.isnan(p_ul) else round(p_ul, 4),

        "price_bid_shorts": None if math.isnan(bid_shorts) else round(bid_shorts, 4),
        "price_ask_shorts": None if math.isnan(ask_shorts) else round(ask_shorts, 4),
        "price_last_shorts": None if math.isnan(last_shorts) else round(last_shorts, 4),
        "price_mid_shorts": None if math.isnan(p_shorts) else round(p_shorts, 4),

        "price_bid_ll": None if math.isnan(bid_ll) else round(bid_ll, 4),
        "price_ask_ll": None if math.isnan(ask_ll) else round(ask_ll, 4),
        "price_last_ll": None if math.isnan(last_ll) else round(last_ll, 4),
        "price_mid_ll": None if math.isnan(p_ll) else round(p_ll, 4),

        "price_bid_c_short": None if math.isnan(bid_c_short) else round(bid_c_short, 4),
        "price_ask_c_short": None if math.isnan(ask_c_short) else round(ask_c_short, 4),
        "price_last_c_short": None if math.isnan(last_c_short) else round(last_c_short, 4),
        "price_mid_c_short": None if math.isnan(p_c_short) else round(p_c_short, 4),

        "price_bid_c_long": None if math.isnan(bid_c_long) else round(bid_c_long, 4),
        "price_ask_c_long": None if math.isnan(ask_c_long) else round(ask_c_long, 4),
        "price_last_c_long": None if math.isnan(last_c_long) else round(last_c_long, 4),
        "price_mid_c_long": None if math.isnan(p_c_long) else round(p_c_long, 4),

        # ========== PUNTOS GEOMÉTRICOS @ VENCIMIENTO DTE1 ==========
        # Asíntotas
        "LEL_pts": round(LEL_pts, 2) if LEL_pts is not None else None,
        "LEL_USD": round(LEL_USD, 2) if LEL_USD is not None else None,
        "UEL_pts": round(UEL_pts, 2) if UEL_pts is not None else None,
        "UEL_USD": round(UEL_USD, 2) if UEL_USD is not None else None,

        # Punto de inflexión LEL
        "PuntoLEL_strike": round(PuntoLEL_strike, 2) if PuntoLEL_strike is not None else None,
        "PuntoLEL_PnL_pts": round(PuntoLEL_PnL_pts, 2) if PuntoLEL_PnL_pts is not None else None,
        "PuntoLEL_PnL_USD": round(PuntoLEL_PnL_USD, 2) if PuntoLEL_PnL_USD is not None else None,

        # Breakevens BWB
        "BreakEvenBWB1_strike": round(BreakEvenBWB1_strike, 2) if BreakEvenBWB1_strike is not None else None,
        "BreakEvenBWB2_strike": round(BreakEvenBWB2_strike, 2) if BreakEvenBWB2_strike is not None else None,

        # Pico BWB
        "PICOBWB_strike": round(PICOBWB_strike, 2) if PICOBWB_strike is not None else None,
        "PICOBWB_PnL_pts": round(PICOBWB_PnL_pts, 2) if PICOBWB_PnL_pts is not None else None,
        "PICOBWB_PnL_USD": round(PICOBWB_PnL_USD, 2) if PICOBWB_PnL_USD is not None else None,

        # Valle (Death Valley de Allantis)
        "VALLE_strike": round(VALLE_strike, 2) if VALLE_strike is not None else None,
        "VALLE_PnL_pts": round(VALLE_PnL_pts, 2) if VALLE_PnL_pts is not None else None,
        "VALLE_PnL_USD": round(VALLE_PnL_USD, 2) if VALLE_PnL_USD is not None else None,

        # Breakevens Call Calendar
        "BreakEvenCCAL1_strike": round(BreakEvenCCAL1_strike, 2) if BreakEvenCCAL1_strike is not None else None,
        "BreakEvenCCAL2_strike": round(BreakEvenCCAL2_strike, 2) if BreakEvenCCAL2_strike is not None else None,

        # Pico Call Calendar
        "PICOCCAL_strike": round(PICOCCAL_strike, 2) if PICOCCAL_strike is not None else None,
        "PICOCCAL_PnL_pts": round(PICOCCAL_PnL_pts, 2) if PICOCCAL_PnL_pts is not None else None,
        "PICOCCAL_PnL_USD": round(PICOCCAL_PnL_USD, 2) if PICOCCAL_PnL_USD is not None else None,

        # MASTER_DISTANCE - Distancia entre breakevens BWB y CCAL
        "MASTER_DISTANCE": round(MASTER_DISTANCE, 2) if MASTER_DISTANCE is not None else None,

        # AQI - Allantis Quality Index
        "AQI": round(AQI, 4) if AQI is not None else None,
    }

    return out

# ================== PARALELIZACIÓN: WORKER PARA CALCULAR FWD (Mediana Intraday 14×30min) ==================
def _process_one_fwd_batman(args):
    """
    Worker function para calcular FWD de un batman en paralelo.
    Evalúa la estructura en 14 timestamps fijos de mercado US y calcula mediana de PnL.
    Debe ser top-level para ser serializable por ProcessPoolExecutor.

    Args:
        args: tupla (i, row_dict, files_sorted_list, forward_fracs, frac_suffixes,
                     tz_us_str, tz_es_str, nearest_minute_tol, intraday_timestamps)

    Returns:
        dict: resultados FWD para todas las ventanas de este batman (incluye medianas)
    """
    (i, row_dict, files_sorted_list, forward_fracs, frac_suffixes,
     tz_us_str, tz_es_str, nearest_minute_tol, intraday_timestamps) = args

    # Reconstruir ZoneInfo desde strings
    TZ_US_local = ZoneInfo(tz_us_str)
    TZ_ES_local = ZoneInfo(tz_es_str)

    # Función local para buscar archivo forward
    def forward_file_by_offset_local(row_base_idx: int, offset: int):
        idx = row_base_idx + offset
        if 0 <= idx < len(files_sorted_list):
            return files_sorted_list[idx]
        return None

    result = {
        'index': i,
        'success': False,
        'filled': 0,
        'fwd_data': {},
        'errors': []
    }

    try:
        # Parsear DTE1 desde "DTE1/DTE2"
        dte_str = str(row_dict.get("DTE1/DTE2", "0/0"))
        t1_days = int(dte_str.split("/")[0])
    except Exception as e:
        result['errors'].append(('fwd_dte_parse_error', str(e)))
        return result

    if not t1_days or t1_days <= 0:
        result['errors'].append(('fwd_invalid_dte', f'DTE1={t1_days}'))
        return result

    try:
        spot_base = float(row_dict.get("SPX"))
    except Exception:
        spot_base = float('nan')

    # Extraer root values del batman base (una sola vez)
    base_root_exp1_norm = normalize_root_value(row_dict.get("root_exp1"))
    base_root_exp2_norm = normalize_root_value(row_dict.get("root_exp2"))
    base_credit = float(row_dict["net_credit"])

    filled = 0
    for fr, suf in zip(forward_fracs, frac_suffixes):
        forward_steps = max(1, int(round(t1_days * fr)))
        if forward_steps >= t1_days:
            continue

        fwd_path = forward_file_by_offset_local(int(row_dict["__base_idx"]), forward_steps)
        if fwd_path is None or not fwd_path.exists():
            continue

        try:
            m2 = re.search(r"(\d{4}-\d{2}-\d{2})", fwd_path.name)
            if not m2:
                continue
            fwd_date_str_us = m2.group(1)

            # ============================================================
            # OPTIMIZACIÓN: Leer archivo UNA SOLA VEZ para los 14 timestamps
            # ============================================================
            fwd_df = pd.read_csv(fwd_path)
            if fwd_df.empty:
                continue
            fwd_df = td_rescale_strike_only(fwd_df)

            # Acumuladores para mediana sobre 14 timestamps
            pnl_pts_list = []
            pnl_pct_list = []

            # Variables para guardar el primer timestamp válido (columnas originales)
            first_valid_timestamp = None
            first_net_credit = None
            first_pnl_pts = None
            first_pnl_pct = None
            first_spx_chg = None
            first_dia_fwd = None
            first_hora_fwd = None
            first_leg_data = {}

            # ============================================================
            # LOOP: Evaluar estructura en los 14 timestamps fijos
            # ============================================================
            for ts_hhmm in intraday_timestamps:
                fwd_one_min, _ = get_slice_at_target_hhmm(fwd_df, ts_hhmm, nearest_minute_tol)
                if fwd_one_min.empty:
                    continue

                # Revalorizar batman en este timestamp
                bv_details = batman_value_from_df(
                    fwd_one_min,
                    str(row_dict["exp1"]), float(row_dict["k1"]),
                    str(row_dict["exp2"]), float(row_dict["k2"]),
                    float(row_dict["k3"]),
                    root1=base_root_exp1_norm,
                    root2=base_root_exp2_norm
                )

                if not bv_details or not np.isfinite(bv_details.get("net_credit", np.nan)):
                    continue

                net_credit_val = float(bv_details["net_credit"])

                # P&L en puntos
                pnl_pts = (net_credit_val - base_credit)
                pnl_pts_list.append(pnl_pts)

                # P&L en %
                denom_pts = abs(base_credit)
                if denom_pts and np.isfinite(denom_pts) and denom_pts > 0:
                    pct = (pnl_pts / denom_pts) * 100.0
                    pnl_pct_list.append(pct)

                # Guardar datos del primer timestamp válido (para columnas originales sin _mediana)
                if first_valid_timestamp is None:
                    first_valid_timestamp = ts_hhmm
                    first_net_credit = net_credit_val
                    first_pnl_pts = pnl_pts
                    first_pnl_pct = pct if denom_pts and np.isfinite(denom_pts) and denom_pts > 0 else None

                    # Timestamp forward
                    dt_us_fwd = datetime.strptime(f"{fwd_date_str_us} {ts_hhmm}", "%Y-%m-%d %H:%M").replace(tzinfo=TZ_US_local)
                    dt_es_fwd = dt_us_fwd.astimezone(TZ_ES_local)
                    first_dia_fwd = dt_es_fwd.date().isoformat()
                    first_hora_fwd = dt_es_fwd.strftime("%H:%M")

                    # SPX change %
                    spx_fwd = underlying_from_slice(fwd_one_min)
                    if np.isfinite(spx_fwd) and np.isfinite(spot_base) and spot_base > 0:
                        first_spx_chg = round(float((spx_fwd / float(spot_base) - 1.0) * 100.0), 4)

                    # Metadata de las 3 patas
                    for leg_label, leg_info in (("k1", bv_details.get("leg1")),
                                                ("k2", bv_details.get("leg2")),
                                                ("k3", bv_details.get("leg3"))):
                        if not leg_info:
                            continue
                        bid_v, ask_v, mid_v = leg_info.get("bid"), leg_info.get("ask"), leg_info.get("mid")
                        root_v = normalize_root_value(leg_info.get("root"))

                        # Validación de mid
                        if leg_label == "k1":
                            expected_mid = row_dict.get("price_mid_short1")
                        elif leg_label == "k2":
                            expected_mid = row_dict.get("price_mid_long2")
                        else:  # k3
                            expected_mid = row_dict.get("price_mid_short3")
                        mid_status = compare_mid_values(mid_v, expected_mid)

                        # Validación de root
                        if leg_label in ("k1", "k3"):
                            base_root_expected = base_root_exp1_norm
                        else:  # k2
                            base_root_expected = base_root_exp2_norm
                        root_status = "CORRECTO" if (not base_root_expected or root_v==base_root_expected) else "INCORRECTO"

                        first_leg_data[leg_label] = {
                            'file': fwd_path.name,
                            'row': leg_info.get("row_index"),
                            'bid': round(float(bid_v),4) if bid_v is not None else None,
                            'ask': round(float(ask_v),4) if ask_v is not None else None,
                            'mid': round(float(mid_v),4) if mid_v is not None else None,
                            'root': root_v,
                            'check': mid_status,
                            'root_check': root_status
                        }

            # ============================================================
            # CALCULAR MEDIANAS (si tenemos datos)
            # ============================================================
            if pnl_pts_list:
                median_pnl_pts = round(float(np.median(pnl_pts_list)), 4)
                result['fwd_data'][f"PnL_fwd_pts_{suf}_mediana"] = median_pnl_pts

            if pnl_pct_list:
                median_pnl_pct = round(float(np.median(pnl_pct_list)), 2)
                result['fwd_data'][f"PnL_fwd_pct_{suf}_mediana"] = median_pnl_pct

            # ============================================================
            # GUARDAR COLUMNAS ORIGINALES (primer timestamp válido)
            # ============================================================
            if first_valid_timestamp is not None:
                result['fwd_data'][f"net_credit_fwd_{suf}"] = round(first_net_credit, 4)
                result['fwd_data'][f"PnL_fwd_pts_{suf}"] = round(first_pnl_pts, 4)
                result['fwd_data'][f"PnL_fwd_pct_{suf}"] = round(first_pnl_pct, 2) if first_pnl_pct is not None else None
                result['fwd_data'][f"dia_fwd_{suf}"] = first_dia_fwd
                result['fwd_data'][f"hora_fwd_{suf}"] = first_hora_fwd
                if first_spx_chg is not None:
                    result['fwd_data'][f"SPX_chg_pct_{suf}"] = first_spx_chg

                # Metadata de las 3 patas
                for leg_label in ("k1", "k2", "k3"):
                    if leg_label in first_leg_data:
                        ld = first_leg_data[leg_label]
                        result['fwd_data'][f"fwd_file_{leg_label}_{suf}"] = ld['file']
                        result['fwd_data'][f"fwd_row_{leg_label}_{suf}"] = ld['row']
                        result['fwd_data'][f"fwd_bid_{leg_label}_{suf}"] = ld['bid']
                        result['fwd_data'][f"fwd_ask_{leg_label}_{suf}"] = ld['ask']
                        result['fwd_data'][f"fwd_mid_{leg_label}_{suf}"] = ld['mid']
                        result['fwd_data'][f"fwd_root_{leg_label}_{suf}"] = ld['root']
                        result['fwd_data'][f"fwd_check_{leg_label}_{suf}"] = ld['check']
                        result['fwd_data'][f"fwd_root_check_{leg_label}_{suf}"] = ld['root_check']

                filled += 1
                result['errors'].append(('fwd_window_calculated_mediana', f'{suf} (14 timestamps)'))

        except Exception as e:
            result['errors'].append(('fwd_window_error', f'{suf}: {str(e)}'))

    result['filled'] = filled
    result['success'] = filled > 0
    return result

def _process_one_fwd_allantis(args):
    """
    Worker function para calcular FWD de Allantis en paralelo.
    Evalúa la estructura de 5 patas en 14 timestamps fijos de mercado US y calcula mediana de PnL.

    Estructura: +4P@UL : -8P@Shorts : +4P@LL @ DTE1 + -2C@DTE1 : +2C@DTE2

    Args:
        args: tupla (i, row_dict, files_sorted_list, forward_fracs, frac_suffixes,
                     tz_us_str, tz_es_str, nearest_minute_tol, intraday_timestamps)

    Returns:
        dict: resultados FWD para todas las ventanas de este Allantis (incluye medianas)
    """
    (i, row_dict, files_sorted_list, forward_fracs, frac_suffixes,
     tz_us_str, tz_es_str, nearest_minute_tol, intraday_timestamps) = args

    # Reconstruir ZoneInfo desde strings
    TZ_US_local = ZoneInfo(tz_us_str)
    TZ_ES_local = ZoneInfo(tz_es_str)

    # Función local para buscar archivo forward
    def forward_file_by_offset_local(row_base_idx: int, offset: int):
        idx = row_base_idx + offset
        if 0 <= idx < len(files_sorted_list):
            return files_sorted_list[idx]
        return None

    result = {
        'index': i,
        'success': False,
        'filled': 0,
        'fwd_data': {},
        'errors': []
    }

    try:
        # Parsear DTE1 desde "DTE1/DTE2"
        dte_str = str(row_dict.get("DTE1/DTE2", "0/0"))
        t1_days = int(dte_str.split("/")[0])
    except Exception as e:
        result['errors'].append(('fwd_dte_parse_error', str(e)))
        return result

    if not t1_days or t1_days <= 0:
        result['errors'].append(('fwd_invalid_dte', f'DTE1={t1_days}'))
        return result

    try:
        spot_base = float(row_dict.get("SPX"))
    except Exception:
        spot_base = float('nan')

    # Extraer root values del Allantis base
    base_root_exp1_norm = normalize_root_value(row_dict.get("root_exp1"))
    base_root_exp2_norm = normalize_root_value(row_dict.get("root_exp2"))
    base_credit = float(row_dict["net_credit"])

    filled = 0
    for fr, suf in zip(forward_fracs, frac_suffixes):
        forward_steps = max(1, int(round(t1_days * fr)))
        if forward_steps >= t1_days:
            continue

        fwd_path = forward_file_by_offset_local(int(row_dict["__base_idx"]), forward_steps)
        if fwd_path is None or not fwd_path.exists():
            continue

        try:
            m2 = re.search(r"(\d{4}-\d{2}-\d{2})", fwd_path.name)
            if not m2:
                continue
            fwd_date_str_us = m2.group(1)

            # Leer archivo UNA SOLA VEZ para los 14 timestamps
            fwd_df = pd.read_csv(fwd_path)
            if fwd_df.empty:
                continue
            fwd_df = td_rescale_strike_only(fwd_df)

            # Acumuladores para mediana sobre 14 timestamps
            pnl_pts_list = []
            pnl_pct_list = []

            # Variables para el primer timestamp válido
            first_valid_timestamp = None
            first_net_credit = None
            first_pnl_pts = None
            first_pnl_pct = None
            first_spx_chg = None
            first_dia_fwd = None
            first_hora_fwd = None
            first_leg_data = {}

            # LOOP: Evaluar estructura en los 14 timestamps fijos
            for ts_hhmm in intraday_timestamps:
                fwd_one_min, _ = get_slice_at_target_hhmm(fwd_df, ts_hhmm, nearest_minute_tol)
                if fwd_one_min.empty:
                    continue

                # Revalorizar Allantis en este timestamp (5 patas)
                av_details = allantis_value_from_df(
                    fwd_one_min,
                    str(row_dict["exp1"]),
                    float(row_dict["k_ul"]),
                    float(row_dict["k_shorts"]),
                    float(row_dict["k_ll"]),
                    float(row_dict["k_call"]),
                    str(row_dict["exp2"]),
                    root1=base_root_exp1_norm,
                    root2=base_root_exp2_norm
                )

                if not av_details or not np.isfinite(av_details.get("net_credit", np.nan)):
                    continue

                net_credit_val = float(av_details["net_credit"])

                # P&L en puntos
                pnl_pts = (net_credit_val - base_credit)
                pnl_pts_list.append(pnl_pts)

                # P&L en %
                denom_pts = abs(base_credit)
                if denom_pts and np.isfinite(denom_pts) and denom_pts > 0:
                    pct = (pnl_pts / denom_pts) * 100.0
                    pnl_pct_list.append(pct)

                # Guardar datos del primer timestamp válido
                if first_valid_timestamp is None:
                    first_valid_timestamp = ts_hhmm
                    first_net_credit = net_credit_val
                    first_pnl_pts = pnl_pts
                    first_pnl_pct = pct if denom_pts and np.isfinite(denom_pts) and denom_pts > 0 else None

                    # Timestamp forward
                    dt_us_fwd = datetime.strptime(f"{fwd_date_str_us} {ts_hhmm}", "%Y-%m-%d %H:%M").replace(tzinfo=TZ_US_local)
                    dt_es_fwd = dt_us_fwd.astimezone(TZ_ES_local)
                    first_dia_fwd = dt_es_fwd.date().isoformat()
                    first_hora_fwd = dt_es_fwd.strftime("%H:%M")

                    # SPX change %
                    spx_fwd = underlying_from_slice(fwd_one_min)
                    if np.isfinite(spx_fwd) and np.isfinite(spot_base) and spot_base > 0:
                        first_spx_chg = round(float((spx_fwd / float(spot_base) - 1.0) * 100.0), 4)

                    # Metadata de las 5 patas
                    for leg_label, leg_info in (("ul", av_details.get("leg_ul")),
                                                ("shorts", av_details.get("leg_shorts")),
                                                ("ll", av_details.get("leg_ll")),
                                                ("c_short", av_details.get("leg_c_short")),
                                                ("c_long", av_details.get("leg_c_long"))):
                        if not leg_info:
                            continue
                        bid_v, ask_v, mid_v = leg_info.get("bid"), leg_info.get("ask"), leg_info.get("mid")
                        root_v = normalize_root_value(leg_info.get("root"))

                        # Validación de mid
                        if leg_label == "ul":
                            expected_mid = row_dict.get("price_mid_ul")
                        elif leg_label == "shorts":
                            expected_mid = row_dict.get("price_mid_shorts")
                        elif leg_label == "ll":
                            expected_mid = row_dict.get("price_mid_ll")
                        elif leg_label == "c_short":
                            expected_mid = row_dict.get("price_mid_c_short")
                        else:  # c_long
                            expected_mid = row_dict.get("price_mid_c_long")
                        mid_status = compare_mid_values(mid_v, expected_mid)

                        # Validación de root
                        if leg_label in ("ul", "shorts", "ll", "c_short"):
                            base_root_expected = base_root_exp1_norm
                        else:  # c_long
                            base_root_expected = base_root_exp2_norm
                        root_status = "CORRECTO" if (not base_root_expected or root_v==base_root_expected) else "INCORRECTO"

                        first_leg_data[leg_label] = {
                            'file': fwd_path.name,
                            'row': leg_info.get("row_index"),
                            'bid': round(float(bid_v),4) if bid_v is not None else None,
                            'ask': round(float(ask_v),4) if ask_v is not None else None,
                            'mid': round(float(mid_v),4) if mid_v is not None else None,
                            'root': root_v,
                            'check': mid_status,
                            'root_check': root_status
                        }

            # CALCULAR MEDIANAS
            if pnl_pts_list:
                median_pnl_pts = round(float(np.median(pnl_pts_list)), 4)
                result['fwd_data'][f"PnL_fwd_pts_{suf}_mediana"] = median_pnl_pts

            if pnl_pct_list:
                median_pnl_pct = round(float(np.median(pnl_pct_list)), 2)
                result['fwd_data'][f"PnL_fwd_pct_{suf}_mediana"] = median_pnl_pct

            # GUARDAR COLUMNAS ORIGINALES (primer timestamp válido)
            if first_valid_timestamp is not None:
                result['fwd_data'][f"net_credit_fwd_{suf}"] = round(first_net_credit, 4)
                result['fwd_data'][f"PnL_fwd_pts_{suf}"] = round(first_pnl_pts, 4)
                result['fwd_data'][f"PnL_fwd_pct_{suf}"] = round(first_pnl_pct, 2) if first_pnl_pct is not None else None
                result['fwd_data'][f"dia_fwd_{suf}"] = first_dia_fwd
                result['fwd_data'][f"hora_fwd_{suf}"] = first_hora_fwd
                if first_spx_chg is not None:
                    result['fwd_data'][f"SPX_chg_pct_{suf}"] = first_spx_chg

                # Metadata de las 5 patas
                for leg_label in ("ul", "shorts", "ll", "c_short", "c_long"):
                    if leg_label in first_leg_data:
                        ld = first_leg_data[leg_label]
                        result['fwd_data'][f"fwd_file_{leg_label}_{suf}"] = ld['file']
                        result['fwd_data'][f"fwd_row_{leg_label}_{suf}"] = ld['row']
                        result['fwd_data'][f"fwd_bid_{leg_label}_{suf}"] = ld['bid']
                        result['fwd_data'][f"fwd_ask_{leg_label}_{suf}"] = ld['ask']
                        result['fwd_data'][f"fwd_mid_{leg_label}_{suf}"] = ld['mid']
                        result['fwd_data'][f"fwd_root_{leg_label}_{suf}"] = ld['root']
                        result['fwd_data'][f"fwd_check_{leg_label}_{suf}"] = ld['check']
                        result['fwd_data'][f"fwd_root_check_{leg_label}_{suf}"] = ld['root_check']

                filled += 1
                result['errors'].append(('fwd_window_calculated_mediana', f'{suf} (14 timestamps)'))

        except Exception as e:
            result['errors'].append(('fwd_window_error', f'{suf}: {str(e)}'))

    result['filled'] = filled
    result['success'] = filled > 0
    return result

# ================== OPTIMIZACIÓN DE MEMORIA ==================
def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Castea columnas numéricas a tipos más eficientes (float32, int32) para reducir memoria.
    """
    if df.empty:
        return df

    df_opt = df.copy()

    # Float64 → Float32
    float_cols = df_opt.select_dtypes(include=['float64']).columns
    for col in float_cols:
        df_opt[col] = df_opt[col].astype('float32')

    # Int64 → Int32 (si cabe en rango)
    int_cols = df_opt.select_dtypes(include=['int64']).columns
    for col in int_cols:
        col_min = df_opt[col].min() if not df_opt[col].isna().all() else 0
        col_max = df_opt[col].max() if not df_opt[col].isna().all() else 0
        # Rango int32: -2,147,483,648 a 2,147,483,647
        if col_min >= -2147483648 and col_max <= 2147483647:
            df_opt[col] = df_opt[col].astype('Int32')

    return df_opt


def optimize_dtypes_aggressive(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """
    Optimización AGRESIVA de tipos de datos para minimizar uso de memoria.

    Aplica:
    - float64 → float32 (reduce 50% memoria)
    - int64 → int32/int16/int8 (según rango)
    - object → category (para strings repetitivos, reduce 80-95% memoria)
    - Identifica y optimiza fechas/timestamps

    Esta función puede reducir el uso de memoria en 60-80% para DataFrames típicos.
    """
    if df.empty:
        return df

    if verbose:
        mem_before = df.memory_usage(deep=True).sum() / (1024**2)
        print(f"   [OPTIMIZACIÓN] Iniciando optimización de {len(df):,} filas, {len(df.columns)} columnas...")
        print(f"   [OPTIMIZACIÓN] Memoria antes: {mem_before:.1f} MB")

    # Evitar modificar el original
    df = df.copy()

    # 1. Float64 → Float32
    float_cols = df.select_dtypes(include=['float64']).columns
    if verbose and len(float_cols) > 0:
        print(f"   [PASO 1/3] Convirtiendo {len(float_cols)} columnas float64 → float32...", end='', flush=True)

    for col in float_cols:
        df[col] = df[col].astype('float32')

    if verbose and len(float_cols) > 0:
        print(" ✓")

    # 2. Int64 → Int32/Int16/Int8 (según rango)
    int_cols = df.select_dtypes(include=['int64']).columns
    if verbose and len(int_cols) > 0:
        print(f"   [PASO 2/3] Optimizando {len(int_cols)} columnas int64 → int32/int16/int8...", end='', flush=True)

    int8_count = 0
    int16_count = 0
    int32_count = 0

    for col in int_cols:
        if df[col].isna().all():
            continue

        col_min = df[col].min()
        col_max = df[col].max()

        # Intentar int8 (-128 a 127)
        if col_min >= -128 and col_max <= 127:
            df[col] = df[col].astype('int8')
            int8_count += 1
        # Intentar int16 (-32,768 a 32,767)
        elif col_min >= -32768 and col_max <= 32767:
            df[col] = df[col].astype('int16')
            int16_count += 1
        # Intentar int32
        elif col_min >= -2147483648 and col_max <= 2147483647:
            df[col] = df[col].astype('int32')
            int32_count += 1
        # Quedarse en int64

    if verbose and len(int_cols) > 0:
        print(f" ✓ (int8: {int8_count}, int16: {int16_count}, int32: {int32_count})")

    # 3. Object → Category (CRÍTICO para reducir memoria)
    # Esto es especialmente efectivo para columnas con valores repetidos
    obj_cols = df.select_dtypes(include=['object']).columns

    if verbose and len(obj_cols) > 0:
        print(f"   [PASO 3/3] Analizando {len(obj_cols)} columnas object → category...", end='', flush=True)

    category_count = 0
    for col in obj_cols:
        num_unique = df[col].nunique()
        num_total = len(df[col])

        # Si hay menos del 50% de valores únicos, convertir a category
        # Category es muy eficiente cuando hay repetición
        if num_unique < num_total * 0.5:
            df[col] = df[col].astype('category')
            category_count += 1

    if verbose and len(obj_cols) > 0:
        print(f" ✓ ({category_count}/{len(obj_cols)} convertidas a category)")

    # 4. Datetime optimization (si hay columnas de fecha)
    # Pandas datetime64[ns] usa 8 bytes, podemos reducir a datetime64[ms] (4 bytes) si no necesitamos nanosegundos
    datetime_cols = df.select_dtypes(include=['datetime64']).columns
    for col in datetime_cols:
        # Mantener como datetime64[ns] pero asegurarnos que no haya NaT innecesarios
        pass  # Por ahora no optimizar datetime, es complejo y puede romper lógica

    if verbose:
        mem_after = df.memory_usage(deep=True).sum() / (1024**2)
        reduction_pct = 100 * (1 - mem_after / mem_before)
        print(f"   [OPTIMIZACIÓN] Memoria después: {mem_after:.1f} MB ({reduction_pct:.1f}% reducción)")

    return df


# ================== WORKER PARA PARALELIZACIÓN DE CANDIDATOS ==================
def _process_k1_candidate(args):
    """
    Worker para procesar un candidato k1 en paralelo.
    Retorna lista de candidatos Batman generados para este k1.
    """
    (k1, s2_list, s3_list, e1, e2, dte_map, spot, r_base, date_es_str, hhmm_es, _hhmm_us,
     base_idx, calls1_data, calls2_data, FRAC_SUFFIXES, USE_LIQUIDITY_FILTERS,
     MIN_OI_FRONT, MIN_VOL_FRONT, MIN_OI_BACK, MIN_VOL_BACK,
     PREFILTER_CREDIT_MIN, PREFILTER_CREDIT_MAX) = args

    # Reconstruir índices (necesario en proceso separado)
    calls1_idx = calls1_data.set_index("strike", drop=False)
    calls2_idx = calls2_data.set_index("strike", drop=False)

    rows = []

    # Validar liquidez k1
    if USE_LIQUIDITY_FILTERS and not leg_liquidity_ok_idx(calls1_idx, k1, MIN_OI_FRONT, MIN_VOL_FRONT):
        return rows

    mid1 = quick_mid_idx(calls1_idx, k1)
    if math.isnan(mid1):
        return rows

    # Loop k2
    for k2 in s2_list:
        if USE_LIQUIDITY_FILTERS and not leg_liquidity_ok_idx(calls2_idx, k2, MIN_OI_BACK, MIN_VOL_BACK):
            continue
        mid2 = quick_mid_idx(calls2_idx, k2)
        if math.isnan(mid2):
            continue

        # Loop k3
        for k3 in s3_list:
            if USE_LIQUIDITY_FILTERS and not leg_liquidity_ok_idx(calls1_idx, k3, MIN_OI_FRONT, MIN_VOL_FRONT):
                continue
            mid3 = quick_mid_idx(calls1_idx, k3)
            if math.isnan(mid3):
                continue

            # Prefiltro de crédito
            pre_net_credit = (-mid1) + (2*mid2) - mid3
            if not (PREFILTER_CREDIT_MIN <= pre_net_credit <= PREFILTER_CREDIT_MAX):
                continue

            # Validar orden de strikes
            if not (k1 < k2 < k3):
                continue

            # Generar URL
            url = make_url(e1, k1, e2, k2, k3)

            # Crear precache local para compute_strategy_metrics
            # (necesario porque compute_strategy_metrics usa precache global)
            precache_local = {
                e1: (calls1_data, None, calls1_idx, None),
                e2: (calls2_data, None, calls2_idx, None)
            }

            # Computar métricas
            met = compute_strategy_metrics(
                spot, r_base, e1, dte_map[e1], k1, e2, dte_map[e2], k2, k3, precache_local
            )

            # Metadata
            root_exp1 = root_for_exp(e1)
            root_exp2 = root_for_exp(e2)

            # Inicializar columnas FWD vacías (incluye medianas)
            fwd_cols = {}
            for suf in FRAC_SUFFIXES:
                fwd_cols[f"net_credit_fwd_{suf}"] = None
                fwd_cols[f"PnL_fwd_pts_{suf}"] = None
                fwd_cols[f"PnL_fwd_pct_{suf}"] = None
                fwd_cols[f"PnL_fwd_pts_{suf}_mediana"] = None
                fwd_cols[f"PnL_fwd_pct_{suf}_mediana"] = None
                fwd_cols[f"SPX_chg_pct_{suf}"] = None
                fwd_cols[f"dia_fwd_{suf}"] = None
                fwd_cols[f"hora_fwd_{suf}"] = None
                for leg in ("k1", "k2", "k3"):
                    fwd_cols[f"fwd_file_{leg}_{suf}"] = None
                    fwd_cols[f"fwd_row_{leg}_{suf}"] = None
                    fwd_cols[f"fwd_bid_{leg}_{suf}"] = None
                    fwd_cols[f"fwd_ask_{leg}_{suf}"] = None
                    fwd_cols[f"fwd_mid_{leg}_{suf}"] = None
                    fwd_cols[f"fwd_check_{leg}_{suf}"] = None
                    fwd_cols[f"fwd_root_{leg}_{suf}"] = None
                    fwd_cols[f"fwd_root_check_{leg}_{suf}"] = None

            rows.append({
                "url": url,
                "dia": date_es_str,
                "hora": hhmm_es,
                "SPX": spot,
                "r": r_base,
                "__base_idx": base_idx,
                "exp1": e1,
                "exp2": e2,
                "hora_us": _hhmm_us,
                "root_exp1": root_exp1,
                "root_exp2": root_exp2,
                **met,
                **fwd_cols
            })

    return rows

def _process_allantis_candidate(args):
    """
    Worker para procesar un candidato Allantis en paralelo.
    Estructura: +4P@UL : -8P@Shorts : +4P@LL @ DTE1 + -2C@DTE1 : +2C@DTE2
    Retorna lista de candidatos Allantis generados.
    """
    (k_ul, shorts_list, ll_list, call_list, e1, e2, dte_map, spot, r_base, date_es_str, hhmm_es, _hhmm_us,
     base_idx, puts1_data, calls1_data, calls2_data, FRAC_SUFFIXES, USE_LIQUIDITY_FILTERS,
     MIN_OI_FRONT, MIN_VOL_FRONT, MIN_OI_BACK, MIN_VOL_BACK,
     PREFILTER_CREDIT_MIN, PREFILTER_CREDIT_MAX) = args

    # Reconstruir índices (necesario en proceso separado)
    puts1_idx = puts1_data.set_index("strike", drop=False)
    calls1_idx = calls1_data.set_index("strike", drop=False)
    calls2_idx = calls2_data.set_index("strike", drop=False)

    rows = []

    # Validar liquidez k_ul (Upper Line put)
    if USE_LIQUIDITY_FILTERS and not leg_liquidity_ok_idx(puts1_idx, k_ul, MIN_OI_FRONT, MIN_VOL_FRONT):
        return rows

    mid_ul = quick_mid_idx(puts1_idx, k_ul)
    if math.isnan(mid_ul):
        return rows

    # Loop k_shorts
    for k_shorts in shorts_list:
        if USE_LIQUIDITY_FILTERS and not leg_liquidity_ok_idx(puts1_idx, k_shorts, MIN_OI_FRONT, MIN_VOL_FRONT):
            continue
        mid_shorts = quick_mid_idx(puts1_idx, k_shorts)
        if math.isnan(mid_shorts):
            continue

        # Loop k_ll (Lower Line put)
        for k_ll in ll_list:
            if USE_LIQUIDITY_FILTERS and not leg_liquidity_ok_idx(puts1_idx, k_ll, MIN_OI_FRONT, MIN_VOL_FRONT):
                continue
            mid_ll = quick_mid_idx(puts1_idx, k_ll)
            if math.isnan(mid_ll):
                continue

            # Validar orden de strikes de puts: k_ul > k_shorts > k_ll (BWB structure)
            if not (k_ul > k_shorts > k_ll):
                continue

            # Loop k_call (compartido entre DTE1 y DTE2)
            for k_call in call_list:
                # Validar liquidez de short call @ DTE1
                if USE_LIQUIDITY_FILTERS and not leg_liquidity_ok_idx(calls1_idx, k_call, MIN_OI_FRONT, MIN_VOL_FRONT):
                    continue
                mid_c_short = quick_mid_idx(calls1_idx, k_call)
                if math.isnan(mid_c_short):
                    continue

                # Validar liquidez de long call @ DTE2
                if USE_LIQUIDITY_FILTERS and not leg_liquidity_ok_idx(calls2_idx, k_call, MIN_OI_BACK, MIN_VOL_BACK):
                    continue
                mid_c_long = quick_mid_idx(calls2_idx, k_call)
                if math.isnan(mid_c_long):
                    continue

                # Prefiltro de crédito: +4*UL -8*Shorts +4*LL -2*C_short +2*C_long
                pre_net_credit = (+4*mid_ul) + (-8*mid_shorts) + (+4*mid_ll) + (-2*mid_c_short) + (+2*mid_c_long)
                if not (PREFILTER_CREDIT_MIN <= pre_net_credit <= PREFILTER_CREDIT_MAX):
                    continue

                # Generar URL (adaptar make_url para 5 strikes)
                url = make_allantis_url(e1, k_ul, k_shorts, k_ll, k_call, e2)

                # Crear precache local para compute_allantis_metrics
                precache_local = {
                    e1: (calls1_data, puts1_data, calls1_idx, puts1_idx),
                    e2: (calls2_data, None, calls2_idx, None)
                }

                # Computar métricas
                met = compute_allantis_metrics(
                    spot, r_base, e1, dte_map[e1], k_ul, k_shorts, k_ll, k_call, e2, dte_map[e2], precache_local
                )

                # Metadata
                root_exp1 = root_for_exp(e1)
                root_exp2 = root_for_exp(e2)

                # Inicializar columnas FWD vacías (incluye medianas)
                fwd_cols = {}
                for suf in FRAC_SUFFIXES:
                    fwd_cols[f"net_credit_fwd_{suf}"] = None
                    fwd_cols[f"PnL_fwd_pts_{suf}"] = None
                    fwd_cols[f"PnL_fwd_pct_{suf}"] = None
                    fwd_cols[f"PnL_fwd_pts_{suf}_mediana"] = None
                    fwd_cols[f"PnL_fwd_pct_{suf}_mediana"] = None
                    fwd_cols[f"SPX_chg_pct_{suf}"] = None
                    fwd_cols[f"dia_fwd_{suf}"] = None
                    fwd_cols[f"hora_fwd_{suf}"] = None
                    # 5 legs para Allantis
                    for leg in ("ul", "shorts", "ll", "c_short", "c_long"):
                        fwd_cols[f"fwd_file_{leg}_{suf}"] = None
                        fwd_cols[f"fwd_row_{leg}_{suf}"] = None
                        fwd_cols[f"fwd_bid_{leg}_{suf}"] = None
                        fwd_cols[f"fwd_ask_{leg}_{suf}"] = None
                        fwd_cols[f"fwd_mid_{leg}_{suf}"] = None
                        fwd_cols[f"fwd_check_{leg}_{suf}"] = None
                        fwd_cols[f"fwd_root_{leg}_{suf}"] = None
                        fwd_cols[f"fwd_root_check_{leg}_{suf}"] = None

                rows.append({
                    "url": url,
                    "dia": date_es_str,
                    "hora": hhmm_es,
                    "SPX": spot,
                    "r": r_base,
                    "__base_idx": base_idx,
                    "exp1": e1,
                    "exp2": e2,
                    "hora_us": _hhmm_us,
                    "root_exp1": root_exp1,
                    "root_exp2": root_exp2,
                    **met,
                    **fwd_cols
                })

    return rows


# ================== MÓDULO DE ANÁLISIS ESTADÍSTICO ==================
# Funciones para análisis de umbrales y correlaciones
# ====================================================================

# ================== DATACLASSES PARA PARÁMETROS ==================

@dataclass
class AnalysisParams:
    """Parámetros de configuración para el análisis"""
    csv_path: str
    pnl_col: Optional[str] = None
    ratios: Optional[List[str]] = None
    win_threshold: float = 0.0
    winsor: Tuple[float, float] = (1.0, 99.0)
    min_n: int = 200
    quantiles: int = 10
    seed: int = 42
    boots: int = 500
    split: str = "holdout"  # "holdout" o "walk-forward"
    objective: str = "mean_pnl"  # "mean_pnl", "sharpe", "hitrate"
    alpha: float = 0.05
    correlation_suite: List[str] = field(default_factory=lambda: ["pearson", "spearman", "kendall", "pointbiserial"])
    topk: int = 10
    advanced: bool = False
    auto_features: bool = False
    suggestions_k: int = 12
    output_dir: Optional[str] = None


# ================== FUNCIONES DE CARGA Y LIMPIEZA ==================

def load_and_clean(params: AnalysisParams) -> Tuple[pd.DataFrame, str]:
    """
    Carga y limpia el CSV de backtest.

    Returns:
        (DataFrame limpio, nombre de la columna PnL seleccionada)
    """
    logger.info(f"Cargando CSV: {params.csv_path}")

    if not os.path.exists(params.csv_path):
        raise FileNotFoundError(f"El archivo no existe: {params.csv_path}")

    df = pd.read_csv(params.csv_path)
    n_original = len(df)
    logger.info(f"Filas originales: {n_original:,}")

    # Auto-detectar columna PnL si no se especifica
    if params.pnl_col is None:
        pnl_candidates = [
            "PnL_fwd_pct_25_mediana",
            "PnL_fwd_pct_25",
            "PnL_fwd_pct_15_mediana",
            "PnL_fwd_pct_15",
            "PnL_fwd_pct_05_mediana",
            "PnL_fwd_pct_05",
        ]
        for candidate in pnl_candidates:
            if candidate in df.columns:
                pnl_col = candidate
                logger.info(f"Columna PnL auto-detectada: {pnl_col}")
                break
        else:
            # Buscar cualquier columna con "pnl" en minúsculas
            pnl_cols = [c for c in df.columns if 'pnl' in c.lower() and 'pct' in c.lower()]
            if pnl_cols:
                pnl_col = pnl_cols[0]
                logger.warning(f"Usando columna PnL heurística: {pnl_col}")
            else:
                raise ValueError("No se encontró columna PnL. Especifica --pnl-col")
    else:
        pnl_col = params.pnl_col
        if pnl_col not in df.columns:
            raise ValueError(f"Columna PnL '{pnl_col}' no existe en el CSV")

    # Detectar columna de fecha
    date_candidates = ['date', 'datetime', 'timestamp', 'Date', 'DateTime']
    date_col = None
    for candidate in date_candidates:
        if candidate in df.columns:
            date_col = candidate
            break

    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df = df.dropna(subset=[date_col])
        df = df.sort_values(date_col).reset_index(drop=True)
        logger.info(f"Ordenado por fecha: {date_col}")
    else:
        logger.warning("No se detectó columna de fecha. Asumiendo orden cronológico.")
        date_col = 'idx'
        df[date_col] = range(len(df))

    # Auto-detectar ratios si no se especifican
    if params.ratios is None:
        # Excluir columnas forward, net_credit (son resultados, no predictores), fecha, y otras no-métricas
        # IMPORTANTE: PnLDV es una MÉTRICA (Death Valley), no un resultado forward
        exclude_patterns = [
            'pnl_fwd',           # PnL forward (resultado)
            'net_credit',        # net_credit, net_credit_mediana, net_credit_fwd_* (resultados)
            'date', 'time',      # Fechas/horas
            'idx',               # Índice
            'strike',            # Strikes (no son ratios)
            'dte',               # DTE de expiraciones
            'symbol', 'root',    # Símbolos
            'bid', 'ask', 'mid'  # Precios (no son ratios)
        ]
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        ratio_cols = []
        for col in numeric_cols:
            # Excluir si contiene patrones EXCEPTO si es PnLDV (métrica de Death Valley)
            col_lower = col.lower()
            if col == 'PnLDV':  # Incluir explícitamente PnLDV
                ratio_cols.append(col)
            elif not any(pat in col_lower for pat in exclude_patterns):
                ratio_cols.append(col)

        if not ratio_cols:
            raise ValueError("No se detectaron ratios. Especifica --ratios manualmente")

        logger.info(f"Ratios auto-detectados ({len(ratio_cols)}): {', '.join(ratio_cols[:5])}{'...' if len(ratio_cols) > 5 else ''}")
    else:
        ratio_cols = [r.strip() for r in params.ratios]
        missing = [r for r in ratio_cols if r not in df.columns]
        if missing:
            raise ValueError(f"Ratios no encontrados en CSV: {missing}")
        logger.info(f"Ratios especificados ({len(ratio_cols)}): {', '.join(ratio_cols[:5])}{'...' if len(ratio_cols) > 5 else ''}")

    # Limpiar datos: primero eliminar columnas completamente vacías
    df = df.replace([np.inf, -np.inf], np.nan)

    # Identificar ratios con TODOS los valores NaN
    ratios_vacios = [r for r in ratio_cols if df[r].isna().all()]
    if ratios_vacios:
        logger.info(f"Eliminando {len(ratios_vacios)} ratios completamente vacíos: {', '.join(ratios_vacios[:5])}{'...' if len(ratios_vacios) > 5 else ''}")
        ratio_cols = [r for r in ratio_cols if r not in ratios_vacios]

    # Ahora eliminar filas con NaN/Inf solo en PnL o ratios válidos
    cols_to_check = [pnl_col] + ratio_cols
    n_before = len(df)
    df = df.dropna(subset=cols_to_check)
    n_after = len(df)
    logger.info(f"Tras eliminar NaN/Inf: {n_after:,} filas ({n_before - n_after} eliminadas)")

    # Verificar que quedan suficientes filas
    if len(df) < params.min_n:
        logger.warning(f"ADVERTENCIA: Solo {len(df)} filas disponibles (mínimo recomendado: {params.min_n})")
        if len(df) < 10:
            raise ValueError(f"Dataset muy pequeño: {len(df)} filas. Se necesitan al menos 10 filas para análisis.")

    # Winsorización del PnL
    if params.winsor[0] > 0 or params.winsor[1] < 100:
        lower, upper = np.percentile(df[pnl_col], params.winsor)
        df[pnl_col] = df[pnl_col].clip(lower, upper)
        logger.info(f"Winsorización aplicada: p{params.winsor[0]:.1f}={lower:.4f}, p{params.winsor[1]:.1f}={upper:.4f}")

    # Crear columna WIN
    df['WIN'] = (df[pnl_col] > params.win_threshold).astype(int)
    win_rate = df['WIN'].mean()
    logger.info(f"WIN definido como PnL > {params.win_threshold:.4f} → Hit-rate global: {win_rate:.2%}")

    # Crear versiones estandarizadas (z-score)
    scaler = StandardScaler()
    df['z_pnl'] = scaler.fit_transform(df[[pnl_col]])
    for ratio in ratio_cols:
        df[f'z_{ratio}'] = scaler.fit_transform(df[[ratio]])

    # Almacenar metadatos
    df.attrs['pnl_col'] = pnl_col
    df.attrs['date_col'] = date_col
    df.attrs['ratio_cols'] = ratio_cols
    df.attrs['n_original'] = n_original
    df.attrs['n_clean'] = len(df)

    logger.info(f"Dataset final: {len(df):,} filas x {len(df.columns)} columnas")
    return df, pnl_col


# ================== FUNCIONES DE CORRELACIÓN Y RANKING ==================

def bootstrap_correlation(x: np.ndarray, y: np.ndarray, method: str, n_boots: int, seed: int) -> Tuple[float, float, float]:
    """
    Calcula correlación con intervalo de confianza bootstrap.

    Returns:
        (correlación, ci_low, ci_high)
    """
    rng = np.random.RandomState(seed)
    n = len(x)
    boots = []

    for _ in range(n_boots):
        idx = rng.choice(n, size=n, replace=True)
        x_boot, y_boot = x[idx], y[idx]

        if method == 'pearson':
            corr, _ = stats.pearsonr(x_boot, y_boot)
        elif method == 'spearman':
            corr, _ = stats.spearmanr(x_boot, y_boot)
        elif method == 'kendall':
            corr, _ = stats.kendalltau(x_boot, y_boot)
        else:
            corr = 0.0

        boots.append(corr)

    boots = np.array(boots)
    ci_low, ci_high = np.percentile(boots, [2.5, 97.5])
    corr_mean = np.mean(boots)

    return corr_mean, ci_low, ci_high


def compute_correlation_suite(df: pd.DataFrame, ratios: List[str], pnl_col: str, params: AnalysisParams) -> pd.DataFrame:
    """
    Calcula suite completa de correlaciones y ranking de fuerza de relación.

    Returns:
        DataFrame con ranking de ratios por múltiples métricas de correlación
    """
    logger.info(f"Calculando suite de correlaciones para {len(ratios)} ratios...")

    results = []
    pnl = df[pnl_col].values
    win = df['WIN'].values

    for i, ratio in enumerate(ratios, 1):
        if i % 10 == 0:
            logger.info(f"  Procesando ratio {i}/{len(ratios)}: {ratio}")

        r_vals = df[ratio].values

        # Verificar varianza
        if np.std(r_vals) == 0 or np.std(pnl) == 0:
            logger.warning(f"  {ratio}: varianza cero, omitiendo")
            continue

        row = {'ratio': ratio, 'n': len(df)}

        # Pearson
        if 'pearson' in params.correlation_suite:
            try:
                pearson_r, pearson_p = stats.pearsonr(r_vals, pnl)
                p_mean, p_low, p_high = bootstrap_correlation(r_vals, pnl, 'pearson', params.boots, params.seed)
                row['pearson_r'] = pearson_r
                row['pearson_p'] = pearson_p
                row['pearson_ci_low'] = p_low
                row['pearson_ci_high'] = p_high
            except Exception as e:
                logger.warning(f"  {ratio}: error en Pearson - {e}")
                row['pearson_r'] = np.nan

        # Spearman
        if 'spearman' in params.correlation_suite:
            try:
                spearman_rho, spearman_p = stats.spearmanr(r_vals, pnl)
                s_mean, s_low, s_high = bootstrap_correlation(r_vals, pnl, 'spearman', params.boots, params.seed)
                row['spearman_rho'] = spearman_rho
                row['spearman_p'] = spearman_p
                row['spearman_ci_low'] = s_low
                row['spearman_ci_high'] = s_high
            except Exception as e:
                logger.warning(f"  {ratio}: error en Spearman - {e}")
                row['spearman_rho'] = np.nan

        # Kendall
        if 'kendall' in params.correlation_suite:
            try:
                kendall_tau, kendall_p = stats.kendalltau(r_vals, pnl)
                row['kendall_tau'] = kendall_tau
                row['kendall_p'] = kendall_p
            except Exception as e:
                logger.warning(f"  {ratio}: error en Kendall - {e}")
                row['kendall_tau'] = np.nan

        # Point-biserial (ratio vs WIN binario)
        if 'pointbiserial' in params.correlation_suite:
            try:
                pb_r, pb_p = stats.pointbiserialr(win, r_vals)
                row['pointbiserial_r'] = pb_r
                row['pointbiserial_p'] = pb_p
            except Exception as e:
                logger.warning(f"  {ratio}: error en point-biserial - {e}")
                row['pointbiserial_r'] = np.nan

        # Distance Correlation (si disponible)
        if 'dcor' in params.correlation_suite and HAS_DCOR:
            try:
                dcor_val = dcor.distance_correlation(r_vals, pnl)
                row['dcor'] = dcor_val
            except Exception as e:
                logger.warning(f"  {ratio}: error en dcor - {e}")
                row['dcor'] = np.nan

        # MIC (si disponible)
        if 'mic' in params.correlation_suite and HAS_MINEPY:
            try:
                mine = MINE(alpha=0.6, c=15, est="mic_approx")
                mine.compute_score(r_vals, pnl)
                row['mic'] = mine.mic()
            except Exception as e:
                logger.warning(f"  {ratio}: error en MIC - {e}")
                row['mic'] = np.nan

        # Permutation Importance (si --advanced)
        if params.advanced and 'permutation' in params.correlation_suite:
            try:
                X = r_vals.reshape(-1, 1)
                model = LinearRegression()
                score, perm_scores, pvalue = permutation_test_score(
                    model, X, pnl, scoring='r2', cv=5, n_permutations=100, random_state=params.seed
                )
                row['perm_importance'] = score
                row['perm_p'] = pvalue
            except Exception as e:
                logger.warning(f"  {ratio}: error en permutation - {e}")
                row['perm_importance'] = np.nan

        # Effect size por deciles (ΔPnL d10 - d1)
        try:
            deciles = pd.qcut(r_vals, q=10, labels=False, duplicates='drop')
            pnl_d1 = pnl[deciles == 0]
            pnl_d10 = pnl[deciles == 9]

            if len(pnl_d1) > 5 and len(pnl_d10) > 5:
                effect = np.mean(pnl_d10) - np.mean(pnl_d1)

                # Bootstrap IC para effect size
                boots = []
                rng = np.random.RandomState(params.seed)
                for _ in range(params.boots):
                    d1_boot = rng.choice(pnl_d1, size=len(pnl_d1), replace=True)
                    d10_boot = rng.choice(pnl_d10, size=len(pnl_d10), replace=True)
                    boots.append(np.mean(d10_boot) - np.mean(d1_boot))

                effect_ci_low, effect_ci_high = np.percentile(boots, [2.5, 97.5])

                # Cohen's d
                pooled_std = np.sqrt((np.std(pnl_d1)**2 + np.std(pnl_d10)**2) / 2)
                cohens_d = effect / pooled_std if pooled_std > 0 else 0

                row['effect_size_deciles'] = effect
                row['effect_ci_low'] = effect_ci_low
                row['effect_ci_high'] = effect_ci_high
                row['cohens_d'] = cohens_d
            else:
                row['effect_size_deciles'] = np.nan
        except Exception as e:
            logger.warning(f"  {ratio}: error en effect size - {e}")
            row['effect_size_deciles'] = np.nan

        results.append(row)

    corr_df = pd.DataFrame(results)

    # Ordenar por correlación absoluta (Spearman por defecto)
    if 'spearman_rho' in corr_df.columns:
        corr_df['abs_spearman'] = corr_df['spearman_rho'].abs()
        corr_df = corr_df.sort_values('abs_spearman', ascending=False).drop('abs_spearman', axis=1)

    logger.info(f"Suite de correlaciones completada: {len(corr_df)} ratios evaluados")
    return corr_df


# ================== FUNCIONES DE ANÁLISIS DE UMBRALES ==================

def train_find_threshold_for_ratio(df_train: pd.DataFrame, ratio: str, pnl_col: str, params: AnalysisParams) -> Dict[str, Any]:
    """
    Encuentra el umbral óptimo X para un ratio Y en el conjunto de entrenamiento.

    Returns:
        Diccionario con resultados del análisis en Train
    """
    r_vals = df_train[ratio].values
    pnl = df_train[pnl_col].values
    win = df_train['WIN'].values

    results = {
        'ratio': ratio,
        'n_train': len(df_train),
        'bins': [],
        'threshold_X': None,
        'threshold_metrics': {}
    }

    # 1. Binning por cuantiles
    try:
        bins = pd.qcut(r_vals, q=params.quantiles, labels=False, duplicates='drop')
        n_bins = bins.max() + 1

        for bin_idx in range(n_bins):
            mask = (bins == bin_idx)
            bin_pnl = pnl[mask]
            bin_win = win[mask]

            if len(bin_pnl) < 10:
                continue

            bin_stats = {
                'bin': bin_idx,
                'n': len(bin_pnl),
                'hit_rate': np.mean(bin_win),
                'pnl_mean': np.mean(bin_pnl),
                'pnl_median': np.median(bin_pnl),
                'pnl_std': np.std(bin_pnl),
                'sharpe': np.mean(bin_pnl) / np.std(bin_pnl) if np.std(bin_pnl) > 0 else 0,
                'r_min': np.min(r_vals[mask]),
                'r_max': np.max(r_vals[mask]),
                'r_mean': np.mean(r_vals[mask])
            }

            # Cohen's d vs población global
            global_mean = np.mean(pnl)
            global_std = np.std(pnl)
            pooled_std = np.sqrt((bin_stats['pnl_std']**2 + global_std**2) / 2)
            bin_stats['cohens_d'] = (bin_stats['pnl_mean'] - global_mean) / pooled_std if pooled_std > 0 else 0

            results['bins'].append(bin_stats)

    except Exception as e:
        logger.warning(f"  {ratio}: error en binning - {e}")
        return results

    # 2. Monotonicidad
    try:
        spearman_win, p_spearman_win = stats.spearmanr(r_vals, win)
        spearman_pnl, p_spearman_pnl = stats.spearmanr(r_vals, pnl)
        results['monotonicity'] = {
            'spearman_win': spearman_win,
            'p_spearman_win': p_spearman_win,
            'spearman_pnl': spearman_pnl,
            'p_spearman_pnl': p_spearman_pnl
        }
    except Exception as e:
        logger.warning(f"  {ratio}: error en monotonicidad - {e}")

    # 3. Isotonic regression
    try:
        iso_reg = IsotonicRegression(out_of_bounds='clip')
        iso_reg.fit(r_vals, win)
        results['isotonic_fitted'] = True
    except Exception as e:
        logger.warning(f"  {ratio}: error en isotonic - {e}")
        results['isotonic_fitted'] = False

    # 4. Selección de umbral X
    if not results['bins']:
        return results

    # Candidatos X: cortes de cuantiles
    quantile_cuts = [b['r_mean'] for b in results['bins']]

    best_X = None
    best_score = -np.inf
    best_metrics = {}

    for X in quantile_cuts:
        mask_above = (r_vals >= X)
        n_above = np.sum(mask_above)

        if n_above < params.min_n:
            continue

        pnl_above = pnl[mask_above]
        win_above = win[mask_above]

        # Métricas
        mean_pnl = np.mean(pnl_above)
        hit_rate = np.mean(win_above)
        sharpe = mean_pnl / np.std(pnl_above) if np.std(pnl_above) > 0 else 0

        # IC95% bootstrap para PnL medio
        rng = np.random.RandomState(params.seed)
        boots = [np.mean(rng.choice(pnl_above, size=len(pnl_above), replace=True)) for _ in range(params.boots)]
        ci_low, ci_high = np.percentile(boots, [2.5, 97.5])

        # Criterio de selección
        if params.objective == 'mean_pnl':
            score = mean_pnl
        elif params.objective == 'sharpe':
            score = sharpe
        elif params.objective == 'hitrate':
            score = hit_rate
        else:
            score = mean_pnl

        # Requerir que IC95% excluya 0 (opcional, se puede relajar)
        # if ci_low > 0 and score > best_score:
        if score > best_score:
            best_score = score
            best_X = X
            best_metrics = {
                'n': n_above,
                'mean_pnl': mean_pnl,
                'hit_rate': hit_rate,
                'sharpe': sharpe,
                'ci_low': ci_low,
                'ci_high': ci_high,
                'conclusive': ci_low > 0
            }

    results['threshold_X'] = best_X
    results['threshold_metrics'] = best_metrics

    return results


def eval_threshold_oos(df_test: pd.DataFrame, ratio: str, X: float, pnl_col: str, params: AnalysisParams) -> Dict[str, Any]:
    """
    Evalúa el umbral X en el conjunto OOS (Test).

    Returns:
        Diccionario con métricas OOS
    """
    if X is None:
        return {'n_oos': len(df_test), 'valid': False}

    r_vals = df_test[ratio].values
    pnl = df_test[pnl_col].values
    win = df_test['WIN'].values

    # Subset con ratio >= X
    mask_above = (r_vals >= X)
    n_above = np.sum(mask_above)

    if n_above < params.min_n:
        return {'n_oos': len(df_test), 'n_above': n_above, 'valid': False}

    pnl_above = pnl[mask_above]
    win_above = win[mask_above]

    # Métricas OOS
    mean_pnl_oos = np.mean(pnl_above)
    median_pnl_oos = np.median(pnl_above)
    hit_rate_oos = np.mean(win_above)
    sharpe_oos = mean_pnl_oos / np.std(pnl_above) if np.std(pnl_above) > 0 else 0

    # IC95% bootstrap para PnL medio
    rng = np.random.RandomState(params.seed)
    boots = [np.mean(rng.choice(pnl_above, size=len(pnl_above), replace=True)) for _ in range(params.boots)]
    ci_low, ci_high = np.percentile(boots, [2.5, 97.5])

    # Baseline (toda la muestra OOS)
    mean_pnl_baseline = np.mean(pnl)
    hit_rate_baseline = np.mean(win)

    # Lift
    lift_pnl = mean_pnl_oos - mean_pnl_baseline
    lift_hit = hit_rate_oos - hit_rate_baseline

    # t-test vs 0
    t_stat, p_ttest = stats.ttest_1samp(pnl_above, 0)

    # z-test para proporciones (hit-rate vs baseline)
    # Aproximación normal
    p1, p2 = hit_rate_oos, hit_rate_baseline
    n1, n2 = n_above, len(df_test)
    pooled_p = (p1*n1 + p2*n2) / (n1 + n2)
    se = np.sqrt(pooled_p * (1 - pooled_p) * (1/n1 + 1/n2))
    z_stat = (p1 - p2) / se if se > 0 else 0
    p_ztest = 2 * (1 - stats.norm.cdf(abs(z_stat)))

    # Correlación Spearman en OOS
    try:
        spearman_oos, _ = stats.spearmanr(r_vals, pnl)
    except:
        spearman_oos = np.nan

    # Effect size en OOS (deciles)
    try:
        deciles = pd.qcut(r_vals, q=10, labels=False, duplicates='drop')
        pnl_d1 = pnl[deciles == 0]
        pnl_d10 = pnl[deciles == 9]
        effect_oos = np.mean(pnl_d10) - np.mean(pnl_d1) if len(pnl_d1) > 0 and len(pnl_d10) > 0 else np.nan
    except:
        effect_oos = np.nan

    results = {
        'n_oos': len(df_test),
        'n_above': n_above,
        'mean_pnl': mean_pnl_oos,
        'median_pnl': median_pnl_oos,
        'hit_rate': hit_rate_oos,
        'sharpe': sharpe_oos,
        'ci_low': ci_low,
        'ci_high': ci_high,
        'lift_pnl': lift_pnl,
        'lift_hit': lift_hit,
        'baseline_pnl': mean_pnl_baseline,
        'baseline_hit': hit_rate_baseline,
        't_stat': t_stat,
        'p_ttest': p_ttest,
        'z_stat': z_stat,
        'p_ztest': p_ztest,
        'spearman_oos': spearman_oos,
        'effect_oos': effect_oos,
        'valid': True
    }

    return results


# ================== WALK-FORWARD VALIDATION ==================

def walk_forward_validation(df: pd.DataFrame, ratios: List[str], pnl_col: str, date_col: str, params: AnalysisParams) -> Dict[str, Any]:
    """
    Validación walk-forward con folds mensuales deslizantes.

    Returns:
        Diccionario con resultados consolidados por ratio
    """
    logger.info("Ejecutando walk-forward validation...")

    df['month'] = pd.to_datetime(df[date_col]).dt.to_period('M')
    unique_months = df['month'].unique()
    unique_months = sorted(unique_months)

    if len(unique_months) < 3:
        logger.warning("Menos de 3 meses de datos. Usando holdout simple.")
        return {}

    logger.info(f"  {len(unique_months)} meses detectados para walk-forward")

    results_by_ratio = {r: {'folds': [], 'X_history': []} for r in ratios}

    # Para cada fold: entrenar con histórico previo, validar en mes siguiente
    for i in range(2, len(unique_months)):
        train_months = unique_months[:i]
        test_month = unique_months[i]

        df_train = df[df['month'].isin(train_months)].copy()
        df_test = df[df['month'] == test_month].copy()

        if len(df_train) < params.min_n or len(df_test) < 50:
            continue

        logger.info(f"  Fold {i-1}/{len(unique_months)-2}: Train={len(df_train)}, Test={len(df_test)} (mes {test_month})")

        for ratio in ratios:
            # Train: encontrar umbral
            train_result = train_find_threshold_for_ratio(df_train, ratio, pnl_col, params)
            X = train_result['threshold_X']

            if X is None:
                continue

            # Test: evaluar umbral
            oos_result = eval_threshold_oos(df_test, ratio, X, pnl_col, params)

            if not oos_result.get('valid', False):
                continue

            fold_data = {
                'fold': i-1,
                'month': str(test_month),
                'X': X,
                'n_train': len(df_train),
                'n_test': len(df_test),
                **oos_result
            }

            results_by_ratio[ratio]['folds'].append(fold_data)
            results_by_ratio[ratio]['X_history'].append(X)

    # Consolidar resultados por ratio
    consolidated = {}
    for ratio, data in results_by_ratio.items():
        if not data['folds']:
            continue

        folds_df = pd.DataFrame(data['folds'])

        # X recomendado = mediana de X por folds
        X_recommended = np.median(data['X_history'])

        # Métricas agregadas OOS
        mean_pnl_mean = folds_df['mean_pnl'].mean()
        mean_pnl_median = folds_df['mean_pnl'].median()
        hit_rate_mean = folds_df['hit_rate'].mean()
        sharpe_mean = folds_df['sharpe'].mean()

        # % de folds positivos
        pct_positive = (folds_df['mean_pnl'] > 0).mean()

        # Estabilidad de signo (Spearman)
        if 'spearman_oos' in folds_df.columns:
            spearman_values = folds_df['spearman_oos'].dropna()
            if len(spearman_values) > 0:
                sign_stability = (np.sign(spearman_values) == np.sign(spearman_values.iloc[0])).mean()
            else:
                sign_stability = np.nan
        else:
            sign_stability = np.nan

        consolidated[ratio] = {
            'X_recommended': X_recommended,
            'n_folds': len(data['folds']),
            'mean_pnl_mean': mean_pnl_mean,
            'mean_pnl_median': mean_pnl_median,
            'hit_rate_mean': hit_rate_mean,
            'sharpe_mean': sharpe_mean,
            'pct_positive_folds': pct_positive,
            'sign_stability': sign_stability,
            'folds_detail': folds_df
        }

    logger.info(f"Walk-forward completado: {len(consolidated)} ratios con resultados válidos")
    return consolidated


# ================== AUTO FEATURE GENERATION ==================

def generate_auto_features(df: pd.DataFrame, top_ratios: List[str], pnl_col: str, params: AnalysisParams) -> Tuple[pd.DataFrame, List[str]]:
    """
    Genera features candidatos automáticamente (transformaciones e interacciones).

    Returns:
        (DataFrame con nuevas features, lista de nombres de features)
    """
    logger.info(f"Generando features automáticas para top {len(top_ratios)} ratios...")

    df_new = df.copy()
    new_features = []

    # Límite de features
    max_features = 50

    # 1. Transformaciones univariantes
    for ratio in top_ratios[:5]:  # Top 5 para no explotar combinatoria
        r_vals = df[ratio].values

        # log1p
        try:
            feat_name = f"log1p_{ratio}"
            df_new[feat_name] = np.log1p(np.abs(r_vals)) * np.sign(r_vals)
            new_features.append(feat_name)
        except:
            pass

        # sqrt
        try:
            feat_name = f"sqrt_{ratio}"
            df_new[feat_name] = np.sqrt(np.abs(r_vals)) * np.sign(r_vals)
            new_features.append(feat_name)
        except:
            pass

        # squared
        try:
            feat_name = f"sq_{ratio}"
            df_new[feat_name] = r_vals ** 2
            new_features.append(feat_name)
        except:
            pass

        # inverse
        try:
            feat_name = f"inv_{ratio}"
            df_new[feat_name] = 1.0 / (1e-6 + np.abs(r_vals))
            new_features.append(feat_name)
        except:
            pass

        if len(new_features) >= max_features:
            break

    # 2. Interacciones 2×2 (parejas top)
    if len(top_ratios) >= 2 and len(new_features) < max_features:
        for i in range(min(3, len(top_ratios))):
            for j in range(i+1, min(4, len(top_ratios))):
                r1 = df[top_ratios[i]].values
                r2 = df[top_ratios[j]].values

                # Producto
                try:
                    feat_name = f"{top_ratios[i]}_x_{top_ratios[j]}"
                    df_new[feat_name] = r1 * r2
                    new_features.append(feat_name)
                except:
                    pass

                # Razón
                try:
                    feat_name = f"{top_ratios[i]}_div_{top_ratios[j]}"
                    df_new[feat_name] = r1 / (1e-6 + np.abs(r2))
                    new_features.append(feat_name)
                except:
                    pass

                # Diferencia
                try:
                    feat_name = f"{top_ratios[i]}_minus_{top_ratios[j]}"
                    df_new[feat_name] = r1 - r2
                    new_features.append(feat_name)
                except:
                    pass

                if len(new_features) >= max_features:
                    break
            if len(new_features) >= max_features:
                break

    # 3. Score compuesto (suma ponderada top 3)
    if len(top_ratios) >= 3 and len(new_features) < max_features:
        try:
            # Pesos proporcionales a |Spearman|
            z_sum = df[[f'z_{r}' for r in top_ratios[:3] if f'z_{r}' in df.columns]].mean(axis=1)
            feat_name = "composite_score_top3"
            df_new[feat_name] = z_sum
            new_features.append(feat_name)
        except:
            pass

    logger.info(f"  {len(new_features)} nuevas features generadas")

    # Limpiar infinitos y NaNs
    df_new = df_new.replace([np.inf, -np.inf], np.nan)
    for feat in new_features:
        df_new[feat] = df_new[feat].fillna(df_new[feat].median())

    return df_new, new_features


# ================== GENERACIÓN DE SUGERENCIAS ==================

def generate_suggestions(corr_df: pd.DataFrame, top_ratios: List[str], params: AnalysisParams) -> List[str]:
    """
    Genera sugerencias inteligentes de nuevos ratios y combinaciones.

    Returns:
        Lista de strings con sugerencias priorizadas
    """
    suggestions = []

    # Header
    suggestions.append("=" * 80)
    suggestions.append("SUGERENCIAS PRIORIZADAS DE NUEVOS RATIOS Y COMBINACIONES")
    suggestions.append("=" * 80)
    suggestions.append("")

    k = 1

    # 1. Transformaciones univariantes
    if len(top_ratios) > 0:
        suggestions.append(f"{k}. TRANSFORMACIONES UNIVARIANTES sobre ratios top")
        suggestions.append("-" * 80)
        for i, ratio in enumerate(top_ratios[:3], 1):
            suggestions.append(f"   {i}) Ratio: {ratio}")
            suggestions.append(f"      - log1p_{ratio} = log(1 + |{ratio}|) * sign({ratio})")
            suggestions.append(f"      - sqrt_{ratio} = sqrt(|{ratio}|) * sign({ratio})")
            suggestions.append(f"      - sq_{ratio} = {ratio}^2")
            suggestions.append(f"      - inv_{ratio} = 1 / (ε + |{ratio}|)")
            suggestions.append(f"      Motivación: Capturar no linealidades y reducir outliers")
            suggestions.append(f"      Validación: Repetir ranking de correlación (target: |ρ| > actual)")
            suggestions.append("")
        k += 1
        suggestions.append("")

    # 2. Interacciones 2×2
    if len(top_ratios) >= 2:
        suggestions.append(f"{k}. INTERACCIONES 2×2 entre ratios top")
        suggestions.append("-" * 80)
        r1, r2 = top_ratios[0], top_ratios[1]
        suggestions.append(f"   Pareja prioritaria: {r1} × {r2}")
        suggestions.append(f"   - Producto: {r1}_x_{r2} = {r1} * {r2}")
        suggestions.append(f"   - Razón: {r1}_div_{r2} = {r1} / (ε + |{r2}|)")
        suggestions.append(f"   - Diferencia: {r1}_minus_{r2} = {r1} - {r2}")
        suggestions.append(f"   - Spread absoluto: abs_diff = |{r1} - {r2}|")
        suggestions.append(f"   Motivación: Capturar efectos sinérgicos y regímenes condicionados")
        suggestions.append(f"   Validación: Exigir mejora OOS en ΔPnL (d10-d1) > {top_ratios[0]}")
        suggestions.append("")
        k += 1
        suggestions.append("")

    # 3. Score compuesto
    if len(top_ratios) >= 3:
        suggestions.append(f"{k}. SCORE COMPUESTO estable (top 3 ratios)")
        suggestions.append("-" * 80)
        suggestions.append(f"   composite_score = w1*z_{top_ratios[0]} + w2*z_{top_ratios[1]} + w3*z_{top_ratios[2]}")
        suggestions.append(f"   donde w_i = |ρ_i| / Σ|ρ_j| (pesos proporcionales a correlación OOS)")
        suggestions.append(f"   Motivación: Combinar señales complementarias con pesos adaptativos")
        suggestions.append(f"   Validación: Estabilidad en ≥70% de folds + ΔPnL_OOS > ratios individuales")
        suggestions.append("")
        k += 1
        suggestions.append("")

    # 4. Binarización para no linealidades
    if len(top_ratios) > 0:
        suggestions.append(f"{k}. BINARIZACIÓN para relaciones no lineales")
        suggestions.append("-" * 80)
        suggestions.append(f"   Para ratios con flip de signo o no monotónicos:")
        suggestions.append(f"   - bin_{top_ratios[0]} = 1 si {top_ratios[0]} ≥ X* (umbral óptimo), 0 si no")
        suggestions.append(f"   Motivación: Simplificar señal cuando relación continua es ruidosa")
        suggestions.append(f"   Validación: Comparar Sharpe(bin) vs Sharpe(continuo) en OOS")
        suggestions.append("")
        k += 1
        suggestions.append("")

    # 5. Robustez/ruido
    suggestions.append(f"{k}. ROBUSTEZ ante ruido (winsorización y suavizado)")
    suggestions.append("-" * 80)
    suggestions.append(f"   - Winsorización previa del ratio (p1, p99) antes de evaluarlo")
    suggestions.append(f"   - EMA(3-5 días) del ratio si es muy volátil (requiere histórico diario)")
    suggestions.append(f"   - Mediana rolling (ventana = 3-7 obs) en lugar de valor puntual")
    suggestions.append(f"   Motivación: Reducir sensibilidad a outliers y spikes temporales")
    suggestions.append(f"   Validación: Comparar estabilidad por folds (target: >80%)")
    suggestions.append("")
    k += 1
    suggestions.append("")

    # 6. Régimen condicionado (si aplica)
    suggestions.append(f"{k}. FEATURES CONDICIONADAS por régimen (si existen VIX, IV, etc.)")
    suggestions.append("-" * 80)
    suggestions.append(f"   - {top_ratios[0]}_high_vol = {top_ratios[0]} * 1{{VIX > p66}}")
    suggestions.append(f"   - {top_ratios[0]}_low_vol = {top_ratios[0]} * 1{{VIX < p33}}")
    suggestions.append(f"   - piecewise: usar umbrales distintos de {top_ratios[0]} por régimen de volatilidad")
    suggestions.append(f"   Motivación: Adaptar estrategia a diferentes entornos de mercado")
    suggestions.append(f"   Validación: Comparar Sharpe por régimen en OOS")
    suggestions.append("")
    k += 1
    suggestions.append("")

    # 7. Cómo validar cada sugerencia
    suggestions.append(f"{k}. CRITERIOS DE VALIDACIÓN para TODAS las sugerencias")
    suggestions.append("-" * 80)
    suggestions.append(f"   a) Repetir módulo de ranking de correlación (compute_correlation_suite)")
    suggestions.append(f"   b) Repetir módulo de umbrales (train_find_threshold_for_ratio + eval_threshold_oos)")
    suggestions.append(f"   c) Exigir mejora OOS en:")
    suggestions.append(f"      - |ρ_Spearman_OOS| > mejor ratio original")
    suggestions.append(f"      - ΔPnL(d10-d1) con IC95% que excluye 0")
    suggestions.append(f"      - Estabilidad por folds ≥70%")
    suggestions.append(f"   d) Penalizar complejidad: preferir features simples si empatan en OOS")
    suggestions.append("")
    suggestions.append("=" * 80)

    return suggestions


# ================== GENERACIÓN DE REPORTES ==================

def write_reports(
    df: pd.DataFrame,
    pnl_col: str,
    corr_df: pd.DataFrame,
    threshold_results: Dict[str, Any],
    wf_results: Dict[str, Any],
    params: AnalysisParams,
    output_dir: Path
):
    """
    Genera todos los reportes TXT y CSV en la carpeta ANALISIS.
    """
    logger.info("Generando reportes...")

    # Crear carpeta ANALISIS si no existe
    output_dir.mkdir(exist_ok=True, parents=True)

    # ========== CSV: correlation_rank.csv ==========
    corr_path = output_dir / "correlation_rank.csv"
    corr_df.to_csv(corr_path, index=False)
    logger.info(f"  → {corr_path.name}")

    # ========== CSV: resultados_por_bin.csv ==========
    bins_records = []
    for ratio, result in threshold_results.items():
        for bin_data in result.get('bins', []):
            bins_records.append({'ratio': ratio, **bin_data})

    if bins_records:
        bins_df = pd.DataFrame(bins_records)
        bins_path = output_dir / "resultados_por_bin.csv"
        bins_df.to_csv(bins_path, index=False)
        logger.info(f"  → {bins_path.name}")

    # ========== CSV: umbral_oos_por_ratio.csv ==========
    if wf_results:
        wf_records = []
        for ratio, wf_data in wf_results.items():
            folds_df = wf_data.get('folds_detail')
            if folds_df is not None:
                for _, row in folds_df.iterrows():
                    wf_records.append({'ratio': ratio, **row.to_dict()})

        if wf_records:
            wf_path = output_dir / "umbral_oos_por_ratio.csv"
            pd.DataFrame(wf_records).to_csv(wf_path, index=False)
            logger.info(f"  → {wf_path.name}")

    # ========== CSV: resumen_oos.csv ==========
    if wf_results:
        oos_summary = []
        for ratio, wf_data in wf_results.items():
            oos_summary.append({
                'ratio': ratio,
                'X_recommended': wf_data['X_recommended'],
                'n_folds': wf_data['n_folds'],
                'mean_pnl_mean': wf_data['mean_pnl_mean'],
                'mean_pnl_median': wf_data['mean_pnl_median'],
                'hit_rate_mean': wf_data['hit_rate_mean'],
                'sharpe_mean': wf_data['sharpe_mean'],
                'pct_positive_folds': wf_data['pct_positive_folds'],
                'sign_stability': wf_data['sign_stability']
            })

        oos_path = output_dir / "resumen_oos.csv"
        pd.DataFrame(oos_summary).to_csv(oos_path, index=False)
        logger.info(f"  → {oos_path.name}")

    # ========== TXT: reporte_simple.txt ==========
    simple_lines = []
    simple_lines.append("=" * 80)
    simple_lines.append("REPORTE SIMPLE — Análisis de Umbrales y Correlaciones")
    simple_lines.append("=" * 80)
    simple_lines.append("")
    simple_lines.append(f"CSV: {params.csv_path}")
    simple_lines.append(f"PnL: {pnl_col}")
    simple_lines.append(f"Win threshold: {params.win_threshold:.4f}")
    simple_lines.append(f"Split: {params.split}")
    simple_lines.append(f"N total: {len(df):,}")
    simple_lines.append("")

    # Top ratios concluyentes
    simple_lines.append(f"TOP {params.topk} RATIOS POR FUERZA DE RELACIÓN (OOS):")
    simple_lines.append("-" * 80)

    top_corr = corr_df.head(params.topk)
    for i, row in top_corr.iterrows():
        ratio = row['ratio']
        rho = row.get('spearman_rho', np.nan)
        p_rho = row.get('spearman_p', np.nan)
        effect = row.get('effect_size_deciles', np.nan)
        effect_low = row.get('effect_ci_low', np.nan)
        effect_high = row.get('effect_ci_high', np.nan)

        # Buscar umbral si existe
        X_star = None
        if ratio in wf_results:
            X_star = wf_results[ratio].get('X_recommended')
        elif ratio in threshold_results:
            X_star = threshold_results[ratio].get('threshold_X')

        line = f"{i+1}) {ratio}"
        if not np.isnan(rho):
            line += f" | ρ={rho:+.3f} (p={p_rho:.4f})"
        if not np.isnan(effect):
            line += f" | ΔPnL d10-d1={effect:+.4f} [{effect_low:.4f};{effect_high:.4f}]"
        if X_star is not None:
            line += f" | X*={X_star:.3f}"

        simple_lines.append(line)

    simple_lines.append("")
    simple_lines.append("=" * 80)

    simple_path = output_dir / "reporte_simple.txt"
    simple_path.write_text("\n".join(simple_lines), encoding='utf-8')
    logger.info(f"  → {simple_path.name}")

    # ========== TXT: reporte_detallado.txt ==========
    detail_lines = []
    detail_lines.append("=" * 80)
    detail_lines.append("REPORTE DETALLADO — Análisis Estadístico de Umbrales y Correlaciones")
    detail_lines.append("=" * 80)
    detail_lines.append("")
    detail_lines.append("CONFIGURACIÓN:")
    detail_lines.append(f"  CSV: {params.csv_path}")
    detail_lines.append(f"  Columna PnL: {pnl_col}")
    detail_lines.append(f"  Win threshold: {params.win_threshold:.4f}")
    detail_lines.append(f"  Winsorización: p{params.winsor[0]:.1f}-p{params.winsor[1]:.1f}")
    detail_lines.append(f"  Split: {params.split}")
    detail_lines.append(f"  Objective: {params.objective}")
    detail_lines.append(f"  Min-n: {params.min_n}")
    detail_lines.append(f"  Quantiles: {params.quantiles}")
    detail_lines.append(f"  Bootstrap: {params.boots} réplicas, seed={params.seed}")
    detail_lines.append(f"  Alpha: {params.alpha}")
    detail_lines.append(f"  Advanced: {params.advanced}")
    detail_lines.append(f"  Auto-features: {params.auto_features}")
    detail_lines.append(f"  N total: {len(df):,}")
    detail_lines.append("")

    # Sección de correlaciones
    detail_lines.append("=" * 80)
    detail_lines.append("RANKING DE CORRELACIONES (Top " + str(params.topk) + ")")
    detail_lines.append("=" * 80)
    detail_lines.append("")

    for i, row in top_corr.iterrows():
        detail_lines.append(f"{i+1}. {row['ratio']}")
        detail_lines.append(f"   N: {row.get('n', 0):,}")

        if 'spearman_rho' in row and not np.isnan(row['spearman_rho']):
            detail_lines.append(f"   Spearman ρ: {row['spearman_rho']:+.4f} (p={row.get('spearman_p', np.nan):.4f}) [{row.get('spearman_ci_low', np.nan):.4f}; {row.get('spearman_ci_high', np.nan):.4f}]")

        if 'pearson_r' in row and not np.isnan(row['pearson_r']):
            detail_lines.append(f"   Pearson r: {row['pearson_r']:+.4f} (p={row.get('pearson_p', np.nan):.4f}) [{row.get('pearson_ci_low', np.nan):.4f}; {row.get('pearson_ci_high', np.nan):.4f}]")

        if 'kendall_tau' in row and not np.isnan(row['kendall_tau']):
            detail_lines.append(f"   Kendall τ: {row['kendall_tau']:+.4f} (p={row.get('kendall_p', np.nan):.4f})")

        if 'pointbiserial_r' in row and not np.isnan(row['pointbiserial_r']):
            detail_lines.append(f"   Point-biserial r: {row['pointbiserial_r']:+.4f} (p={row.get('pointbiserial_p', np.nan):.4f})")

        if 'effect_size_deciles' in row and not np.isnan(row['effect_size_deciles']):
            detail_lines.append(f"   ΔPnL (d10-d1): {row['effect_size_deciles']:+.4f} [{row.get('effect_ci_low', np.nan):.4f}; {row.get('effect_ci_high', np.nan):.4f}]")
            detail_lines.append(f"   Cohen's d: {row.get('cohens_d', np.nan):.3f}")

        detail_lines.append("")

    detail_lines.append("")

    # Sección de umbrales (si hay resultados)
    if threshold_results or wf_results:
        detail_lines.append("=" * 80)
        detail_lines.append("ANÁLISIS DE UMBRALES")
        detail_lines.append("=" * 80)
        detail_lines.append("")

        for ratio in top_corr['ratio'].head(params.topk):
            detail_lines.append(f"RATIO: {ratio}")
            detail_lines.append("-" * 80)

            # Train
            if ratio in threshold_results:
                result = threshold_results[ratio]
                X = result.get('threshold_X')
                metrics = result.get('threshold_metrics', {})

                if X is not None:
                    detail_lines.append(f"  Umbral X (Train): {X:.4f}")
                    detail_lines.append(f"  N (≥X): {metrics.get('n', 0):,}")
                    detail_lines.append(f"  PnL medio: {metrics.get('mean_pnl', 0):.4f} [{metrics.get('ci_low', 0):.4f}; {metrics.get('ci_high', 0):.4f}]")
                    detail_lines.append(f"  Hit-rate: {metrics.get('hit_rate', 0):.2%}")
                    detail_lines.append(f"  Sharpe: {metrics.get('sharpe', 0):.3f}")
                    detail_lines.append(f"  Concluyente: {'Sí' if metrics.get('conclusive', False) else 'No'}")
                else:
                    detail_lines.append(f"  No se encontró umbral válido en Train")

            # OOS
            if ratio in wf_results:
                wf_data = wf_results[ratio]
                detail_lines.append(f"  OOS (walk-forward, {wf_data['n_folds']} folds):")
                detail_lines.append(f"    X recomendado: {wf_data['X_recommended']:.4f}")
                detail_lines.append(f"    PnL medio: {wf_data['mean_pnl_mean']:.4f} (mediana: {wf_data['mean_pnl_median']:.4f})")
                detail_lines.append(f"    Hit-rate medio: {wf_data['hit_rate_mean']:.2%}")
                detail_lines.append(f"    Sharpe medio: {wf_data['sharpe_mean']:.3f}")
                detail_lines.append(f"    % folds positivos: {wf_data['pct_positive_folds']:.1%}")
                detail_lines.append(f"    Estabilidad signo: {wf_data['sign_stability']:.1%}")

            detail_lines.append("")

    detail_lines.append("=" * 80)
    detail_lines.append("FIN DEL REPORTE DETALLADO")
    detail_lines.append("=" * 80)

    detail_path = output_dir / "reporte_detallado.txt"
    detail_path.write_text("\n".join(detail_lines), encoding='utf-8')
    logger.info(f"  → {detail_path.name}")

    # ========== TXT: sugerencias.txt ==========
    top_ratios = corr_df['ratio'].head(min(5, len(corr_df))).tolist()
    suggestions = generate_suggestions(corr_df, top_ratios, params)

    sugg_path = output_dir / "sugerencias.txt"
    sugg_path.write_text("\n".join(suggestions), encoding='utf-8')
    logger.info(f"  → {sugg_path.name}")

    logger.info("Reportes generados exitosamente")


# ================== ANÁLISIS AVANZADOS ==================

def analyze_feature_importance_rf(df: pd.DataFrame, ratio_cols: List[str], pnl_col: str, params: AnalysisParams) -> Dict[str, Any]:
    """
    Analiza importancia de features usando Random Forest.
    Detecta interacciones no-lineales y ranking de importancia.

    Returns:
        Dict con 'importance_df', 'model', 'train_score', 'test_score'
    """
    logger.info("  [RF] Entrenando Random Forest para feature importance...")

    # Preparar datos
    X = df[ratio_cols].values
    y = df[pnl_col].values

    # Split train/test
    n_train = int(len(df) * 0.7)
    X_train, X_test = X[:n_train], X[n_train:]
    y_train, y_test = y[:n_train], y[n_train:]

    # Entrenar Random Forest
    rf = RandomForestRegressor(
        n_estimators=200,
        max_depth=8,
        min_samples_split=10,
        min_samples_leaf=5,
        random_state=params.seed,
        n_jobs=-1
    )

    rf.fit(X_train, y_train)

    # Feature importance
    importances = rf.feature_importances_
    importance_df = pd.DataFrame({
        'ratio': ratio_cols,
        'importance': importances,
        'importance_pct': importances / importances.sum() * 100
    }).sort_values('importance', ascending=False)

    # Scores
    train_score = rf.score(X_train, y_train)
    test_score = rf.score(X_test, y_test)

    logger.info(f"  [RF] R² Train: {train_score:.4f} | R² Test: {test_score:.4f}")
    logger.info(f"  [RF] Top 3 features: {', '.join(importance_df.head(3)['ratio'].tolist())}")

    return {
        'importance_df': importance_df,
        'model': rf,
        'train_score': train_score,
        'test_score': test_score
    }


def analyze_regimes(df: pd.DataFrame, ratio_cols: List[str], pnl_col: str, params: AnalysisParams) -> Dict[str, Any]:
    """
    Segmenta el dataset por regímenes (BQI_ABS) y analiza correlaciones por régimen.

    Returns:
        Dict con análisis por régimen
    """
    logger.info("  [REGIMES] Analizando regímenes de mercado...")

    regimes = {}

    # Régimen 1: BQI_ABS (si existe)
    if 'BQI_ABS' in df.columns:
        bqi_median = df['BQI_ABS'].median()
        high_bqi = df[df['BQI_ABS'] > bqi_median]
        low_bqi = df[df['BQI_ABS'] <= bqi_median]

        if len(high_bqi) >= params.min_n and len(low_bqi) >= params.min_n:
            # Correlaciones en régimen alto BQI
            high_corr = compute_correlation_suite(high_bqi, ratio_cols, pnl_col, params)
            low_corr = compute_correlation_suite(low_bqi, ratio_cols, pnl_col, params)

            regimes['BQI_ABS'] = {
                'high': {'n': len(high_bqi), 'corr_df': high_corr, 'threshold': bqi_median},
                'low': {'n': len(low_bqi), 'corr_df': low_corr, 'threshold': bqi_median}
            }
            logger.info(f"  [REGIMES] BQI_ABS: High n={len(high_bqi)}, Low n={len(low_bqi)}, threshold={bqi_median:.2f}")

    return regimes


def search_rule_combinations(df: pd.DataFrame, ratio_cols: List[str], pnl_col: str, params: AnalysisParams, top_k: int = 10) -> pd.DataFrame:
    """
    Busca combinaciones de 2 ratios que maximicen PnL.
    Formato: ratio1 > threshold1 AND ratio2 > threshold2

    Returns:
        DataFrame con mejores combinaciones
    """
    logger.info("  [RULES] Buscando combinaciones de reglas óptimas...")

    # Limitar búsqueda a top ratios (por eficiencia)
    candidates = ratio_cols[:min(20, len(ratio_cols))]

    combinations = []

    for i, r1 in enumerate(candidates):
        for r2 in candidates[i+1:]:
            # Probar cuartiles para umbrales
            for q1 in [0.25, 0.5, 0.75]:
                for q2 in [0.25, 0.5, 0.75]:
                    t1 = df[r1].quantile(q1)
                    t2 = df[r2].quantile(q2)

                    # Aplicar regla
                    mask = (df[r1] > t1) & (df[r2] > t2)
                    subset = df[mask]

                    if len(subset) >= params.min_n:
                        mean_pnl = subset[pnl_col].mean()
                        median_pnl = subset[pnl_col].median()
                        std_pnl = subset[pnl_col].std()
                        hit_rate = (subset[pnl_col] > 0).mean()

                        combinations.append({
                            'ratio1': r1,
                            'threshold1': t1,
                            'ratio2': r2,
                            'threshold2': t2,
                            'n': len(subset),
                            'mean_pnl': mean_pnl,
                            'median_pnl': median_pnl,
                            'std_pnl': std_pnl,
                            'hit_rate': hit_rate,
                            'sharpe': mean_pnl / std_pnl if std_pnl > 0 else 0
                        })

    if not combinations:
        logger.warning("  [RULES] No se encontraron combinaciones válidas")
        return pd.DataFrame()

    rules_df = pd.DataFrame(combinations).sort_values('mean_pnl', ascending=False)
    logger.info(f"  [RULES] Evaluadas {len(combinations)} combinaciones, top mean_pnl={rules_df.iloc[0]['mean_pnl']:.4f}")

    return rules_df.head(top_k)


def analyze_pca_clustering(df: pd.DataFrame, ratio_cols: List[str], pnl_col: str, params: AnalysisParams, n_clusters: int = 3) -> Dict[str, Any]:
    """
    Reduce dimensionalidad con PCA y agrupa estructuras con K-Means.
    Identifica "arquetipos" de estructuras rentables.

    Returns:
        Dict con 'pca', 'kmeans', 'cluster_stats'
    """
    logger.info("  [PCA] Ejecutando PCA y clustering...")

    X = df[ratio_cols].values
    y = df[pnl_col].values

    # PCA: reducir a componentes principales
    n_components = min(10, len(ratio_cols))
    pca = PCA(n_components=n_components, random_state=params.seed)
    X_pca = pca.fit_transform(X)

    explained_var = pca.explained_variance_ratio_.sum()
    logger.info(f"  [PCA] {n_components} componentes explican {explained_var:.2%} de varianza")

    # K-Means clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=params.seed, n_init=10)
    labels = kmeans.fit_predict(X_pca)

    # Silhouette score
    if len(set(labels)) > 1:
        sil_score = silhouette_score(X_pca, labels)
        logger.info(f"  [CLUSTER] Silhouette score: {sil_score:.3f}")
    else:
        sil_score = 0.0

    # Estadísticas por cluster
    cluster_stats = []
    for cluster_id in range(n_clusters):
        mask = labels == cluster_id
        cluster_pnl = y[mask]

        if len(cluster_pnl) > 0:
            cluster_stats.append({
                'cluster': cluster_id,
                'n': len(cluster_pnl),
                'mean_pnl': cluster_pnl.mean(),
                'median_pnl': np.median(cluster_pnl),
                'std_pnl': cluster_pnl.std(),
                'hit_rate': (cluster_pnl > 0).mean()
            })

    cluster_stats_df = pd.DataFrame(cluster_stats).sort_values('mean_pnl', ascending=False)
    logger.info(f"  [CLUSTER] Mejor cluster: {cluster_stats_df.iloc[0]['cluster']}, mean_pnl={cluster_stats_df.iloc[0]['mean_pnl']:.4f}")

    return {
        'pca': pca,
        'explained_variance': explained_var,
        'kmeans': kmeans,
        'labels': labels,
        'cluster_stats': cluster_stats_df,
        'silhouette': sil_score
    }


def analyze_temporal_stability(df: pd.DataFrame, ratio_cols: List[str], pnl_col: str, date_col: str, params: AnalysisParams, n_periods: int = 4) -> Dict[str, Any]:
    """
    Divide el dataset en períodos temporales y analiza estabilidad de correlaciones.

    Returns:
        Dict con correlaciones por período y métricas de estabilidad
    """
    logger.info("  [TEMPORAL] Analizando estabilidad temporal de correlaciones...")

    # Dividir en períodos
    df_sorted = df.sort_values(date_col).reset_index(drop=True)
    period_size = len(df_sorted) // n_periods

    if period_size < params.min_n:
        logger.warning(f"  [TEMPORAL] Períodos muy pequeños (n={period_size}), requiere al menos {params.min_n}")
        return {}

    period_corrs = []

    for i in range(n_periods):
        start_idx = i * period_size
        end_idx = start_idx + period_size if i < n_periods - 1 else len(df_sorted)

        period_df = df_sorted.iloc[start_idx:end_idx]

        if len(period_df) >= params.min_n:
            corr_df = compute_correlation_suite(period_df, ratio_cols, pnl_col, params)
            period_corrs.append({
                'period': i + 1,
                'n': len(period_df),
                'corr_df': corr_df
            })

    if not period_corrs:
        return {}

    # Calcular estabilidad (desviación estándar de correlaciones entre períodos)
    stability_data = []

    for ratio in ratio_cols:
        correlations = []
        for p in period_corrs:
            corr_df = p['corr_df']
            if ratio in corr_df['ratio'].values:
                r = corr_df[corr_df['ratio'] == ratio]['pearson_r'].iloc[0]
                correlations.append(r)

        if len(correlations) >= 2:
            mean_corr = np.mean(correlations)
            std_corr = np.std(correlations)
            stability_data.append({
                'ratio': ratio,
                'mean_corr': mean_corr,
                'std_corr': std_corr,
                'stability_score': abs(mean_corr) / (std_corr + 1e-6)  # Alto = estable y fuerte
            })

    stability_df = pd.DataFrame(stability_data).sort_values('stability_score', ascending=False)
    logger.info(f"  [TEMPORAL] Ratio más estable: {stability_df.iloc[0]['ratio']} (score={stability_df.iloc[0]['stability_score']:.2f})")

    return {
        'period_corrs': period_corrs,
        'stability_df': stability_df,
        'n_periods': len(period_corrs)
    }


def write_advanced_reports(
    output_dir: Path,
    pnl_col: str,
    rf_results: Dict[str, Any],
    regime_results: Dict[str, Any],
    rules_df: pd.DataFrame,
    pca_results: Dict[str, Any],
    temporal_results: Dict[str, Any]
):
    """
    Escribe reportes de los análisis avanzados (RF, regímenes, reglas, PCA, temporal).
    """
    logger.info("  [ADVANCED] Escribiendo reportes avanzados...")

    # 1. Random Forest Feature Importance
    if rf_results:
        rf_path = output_dir / "feature_importance_RF.csv"
        rf_results['importance_df'].to_csv(rf_path, index=False)
        logger.info(f"    -> {rf_path.name}")

        # Reporte TXT
        rf_txt = output_dir / "feature_importance_RF.txt"
        with open(rf_txt, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("RANDOM FOREST — Feature Importance (No-lineal)\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"PnL objetivo: {pnl_col}\n")
            f.write(f"R² Train: {rf_results['train_score']:.4f}\n")
            f.write(f"R² Test: {rf_results['test_score']:.4f}\n\n")
            f.write("TOP 20 FEATURES POR IMPORTANCIA:\n")
            f.write("-" * 80 + "\n")
            for i, row in rf_results['importance_df'].head(20).iterrows():
                f.write(f"{i+1:2d}. {row['ratio']:30s} | Importance: {row['importance']:.4f} ({row['importance_pct']:.2f}%)\n")
        logger.info(f"    -> {rf_txt.name}")

    # 2. Análisis de Regímenes
    if regime_results:
        for regime_name, regime_data in regime_results.items():
            regime_dir = output_dir / f"REGIME_{regime_name}"
            regime_dir.mkdir(exist_ok=True, parents=True)

            # High regime
            if 'high' in regime_data:
                high_path = regime_dir / f"{regime_name}_HIGH_correlations.csv"
                regime_data['high']['corr_df'].to_csv(high_path, index=False)

            # Low regime
            if 'low' in regime_data:
                low_path = regime_dir / f"{regime_name}_LOW_correlations.csv"
                regime_data['low']['corr_df'].to_csv(low_path, index=False)

            # Reporte TXT comparativo
            txt_path = regime_dir / f"{regime_name}_comparison.txt"
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write(f"ANÁLISIS DE RÉGIMEN — {regime_name}\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"Threshold: {regime_data['high']['threshold']:.2f}\n")
                f.write(f"  • HIGH {regime_name}: n={regime_data['high']['n']}\n")
                f.write(f"  • LOW {regime_name}: n={regime_data['low']['n']}\n\n")

                f.write("TOP 10 RATIOS EN RÉGIMEN HIGH:\n")
                f.write("-" * 40 + "\n")
                for i, row in regime_data['high']['corr_df'].head(10).iterrows():
                    f.write(f"{i+1:2d}. {row['ratio']:25s} | r={row.get('pearson_r', 0):.4f}\n")

                f.write("\nTOP 10 RATIOS EN RÉGIMEN LOW:\n")
                f.write("-" * 40 + "\n")
                for i, row in regime_data['low']['corr_df'].head(10).iterrows():
                    f.write(f"{i+1:2d}. {row['ratio']:25s} | r={row.get('pearson_r', 0):.4f}\n")

            logger.info(f"    -> {regime_dir.name}/")

    # 3. Combinaciones de Reglas
    if not rules_df.empty:
        rules_path = output_dir / "best_rule_combinations.csv"
        rules_df.to_csv(rules_path, index=False)
        logger.info(f"    -> {rules_path.name}")

        # Reporte TXT
        rules_txt = output_dir / "best_rule_combinations.txt"
        with open(rules_txt, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("MEJORES COMBINACIONES DE REGLAS (2 ratios)\n")
            f.write("=" * 80 + "\n\n")
            f.write("TOP 10 COMBINACIONES POR MEAN PnL:\n")
            f.write("-" * 80 + "\n")
            for i, row in rules_df.head(10).iterrows():
                f.write(f"\n{i+1}. {row['ratio1']} > {row['threshold1']:.4f} AND {row['ratio2']} > {row['threshold2']:.4f}\n")
                f.write(f"   n={row['n']:,} | Mean PnL={row['mean_pnl']:.4f} | Hit Rate={row['hit_rate']:.2%} | Sharpe={row['sharpe']:.3f}\n")
        logger.info(f"    -> {rules_txt.name}")

    # 4. PCA y Clustering
    if pca_results:
        cluster_path = output_dir / "cluster_stats.csv"
        pca_results['cluster_stats'].to_csv(cluster_path, index=False)
        logger.info(f"    -> {cluster_path.name}")

        # Reporte TXT
        pca_txt = output_dir / "pca_clustering.txt"
        with open(pca_txt, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("PCA & CLUSTERING — Arquetipos de Estructuras\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Varianza explicada: {pca_results['explained_variance']:.2%}\n")
            f.write(f"Silhouette score: {pca_results['silhouette']:.3f}\n")
            f.write(f"Número de clusters: {len(pca_results['cluster_stats'])}\n\n")
            f.write("ESTADÍSTICAS POR CLUSTER (ordenados por mean_pnl):\n")
            f.write("-" * 80 + "\n")
            for i, row in pca_results['cluster_stats'].iterrows():
                f.write(f"\nCluster {row['cluster']}:\n")
                f.write(f"  • n={row['n']:,}\n")
                f.write(f"  • Mean PnL={row['mean_pnl']:.4f}\n")
                f.write(f"  • Median PnL={row['median_pnl']:.4f}\n")
                f.write(f"  • Std PnL={row['std_pnl']:.4f}\n")
                f.write(f"  • Hit Rate={row['hit_rate']:.2%}\n")
        logger.info(f"    -> {pca_txt.name}")

    # 5. Estabilidad Temporal
    if temporal_results and 'stability_df' in temporal_results:
        stability_path = output_dir / "temporal_stability.csv"
        temporal_results['stability_df'].to_csv(stability_path, index=False)
        logger.info(f"    -> {stability_path.name}")

        # Reporte TXT
        temporal_txt = output_dir / "temporal_stability.txt"
        with open(temporal_txt, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("ESTABILIDAD TEMPORAL DE CORRELACIONES\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Períodos analizados: {temporal_results['n_periods']}\n\n")
            f.write("TOP 20 RATIOS MÁS ESTABLES:\n")
            f.write("-" * 80 + "\n")
            f.write("(Stability Score = |mean_corr| / std_corr, mayor es mejor)\n\n")
            for i, row in temporal_results['stability_df'].head(20).iterrows():
                f.write(f"{i+1:2d}. {row['ratio']:30s} | Score={row['stability_score']:7.2f} | Mean r={row['mean_corr']:7.4f} | Std={row['std_corr']:.4f}\n")
        logger.info(f"    -> {temporal_txt.name}")

    logger.info("  [ADVANCED] Reportes avanzados completados")


def detect_all_pnl_columns(csv_path: str) -> Dict[str, List[str]]:
    """
    Detecta todas las columnas PnL_fwd_pct y PnL_fwd_pts en el CSV.

    Returns:
        Dict con 'pct' y 'pts', cada uno conteniendo lista de columnas ordenadas
    """
    df_sample = pd.read_csv(csv_path, nrows=0)  # Solo leer headers

    pnl_pct_cols = sorted([col for col in df_sample.columns if col.startswith("PnL_fwd_pct_") and col.endswith("_mediana")])
    pnl_pts_cols = sorted([col for col in df_sample.columns if col.startswith("PnL_fwd_pts_") and col.endswith("_mediana")])

    return {
        'pct': pnl_pct_cols,
        'pts': pnl_pts_cols
    }


def aggregate_correlation_results(all_corr_dfs: List[pd.DataFrame], pnl_group: str) -> pd.DataFrame:
    """
    Agrega resultados de correlación de múltiples ventanas PnL.

    Args:
        all_corr_dfs: Lista de DataFrames de correlación (uno por ventana)
        pnl_group: 'pct' o 'pts'

    Returns:
        DataFrame agregado con promedios de correlaciones
    """
    if not all_corr_dfs:
        return pd.DataFrame()

    # Concatenar todos los resultados
    combined = pd.concat(all_corr_dfs, ignore_index=True)

    # Agrupar por ratio y promediar métricas
    agg_dict = {}
    numeric_cols = combined.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if col in combined.columns and col != 'ratio':
            agg_dict[col] = 'mean'

    if 'ratio' in combined.columns:
        aggregated = combined.groupby('ratio', as_index=False).agg(agg_dict)

        # Ordenar por correlación absoluta promedio
        if 'pearson_r' in aggregated.columns:
            aggregated['abs_r'] = aggregated['pearson_r'].abs()
            aggregated = aggregated.sort_values('abs_r', ascending=False).drop('abs_r', axis=1)

        return aggregated
    else:
        return pd.DataFrame()


# ================== FUNCIÓN PRINCIPAL ==================

def run_statistical_analysis(csv_path: str):
    """
    Ejecuta el análisis estadístico completo sobre TODAS las ventanas PnL forward.

    Analiza individualmente cada ventana PnL_fwd_pct_*_mediana y PnL_fwd_pts_*_mediana,
    luego genera reportes agregados por grupo (pct y pts).

    Args:
        csv_path: Ruta completa al archivo CSV _mediana
    """
    # LISTA FILTRADA DE RATIOS/VARIABLES A ANALIZAR
    ALLOWED_RATIOS = [
        'BQI_ABS',
        'FF_BAT',
        'RATIO_BATMAN',
        'delta_total',
        'theta_total',
        'net_credit',
        'PnLDV'
    ]

    try:
        logger.info("=" * 80)
        logger.info("INICIANDO MÓDULO DE ANÁLISIS ESTADÍSTICO MULTI-VENTANA")
        logger.info("=" * 80)
        logger.info(f"CSV de entrada: {Path(csv_path).name}")
        logger.info(f"Variables a analizar: {', '.join(ALLOWED_RATIOS)}")

        # FASE 0: Detectar todas las columnas PnL disponibles
        logger.info("\nFase 0: Detectando todas las ventanas PnL forward...")
        pnl_columns = detect_all_pnl_columns(csv_path)

        pnl_pct_cols = pnl_columns['pct']
        pnl_pts_cols = pnl_columns['pts']

        logger.info(f"  -> Ventanas PnL_fwd_pct detectadas: {len(pnl_pct_cols)}")
        for col in pnl_pct_cols:
            logger.info(f"     • {col}")

        logger.info(f"  -> Ventanas PnL_fwd_pts detectadas: {len(pnl_pts_cols)}")
        for col in pnl_pts_cols:
            logger.info(f"     • {col}")

        if not pnl_pct_cols and not pnl_pts_cols:
            logger.warning("No se encontraron columnas PnL forward. Análisis abortado.")
            return

        # Combinar todas las ventanas para análisis
        all_pnl_cols = pnl_pct_cols + pnl_pts_cols
        total_ventanas = len(all_pnl_cols)

        logger.info(f"\n  -> TOTAL: {total_ventanas} ventanas PnL a analizar")
        logger.info("=" * 80)

        # Almacenar resultados de todas las ventanas
        results_by_window = {}
        corr_dfs_pct = []
        corr_dfs_pts = []

        # ANÁLISIS INDIVIDUAL POR VENTANA
        for window_idx, pnl_col in enumerate(all_pnl_cols, 1):
            logger.info("\n" + "=" * 80)
            logger.info(f"VENTANA {window_idx}/{total_ventanas}: {pnl_col}")
            logger.info("=" * 80)

            # Determinar grupo (pct o pts)
            pnl_group = 'pct' if 'pct' in pnl_col else 'pts'
            window_name = pnl_col.replace("PnL_fwd_", "").replace("_mediana", "")

            # Configurar parámetros del análisis
            params = AnalysisParams(
                csv_path=str(csv_path),
                pnl_col=pnl_col,  # Especificar la columna PnL actual
                ratios=ALLOWED_RATIOS,  # Ratios filtrados a las 8 variables especificadas
                win_threshold=0.0,  # Umbral para considerar estructura ganadora
                winsor=(1.0, 99.0),  # Winsorización al 1% y 99%
                min_n=10,  # Mínimo de muestras requeridas
                quantiles=5,  # Dividir en quintiles
                seed=42,
                boots=100,  # Réplicas bootstrap
                split="holdout",  # Holdout simple
                objective="mean_pnl",  # Optimizar por PnL promedio
                alpha=0.05,  # Nivel de significancia 5%
                correlation_suite=["pearson", "spearman"],  # Pearson y Spearman
                topk=10,  # Top 10 ratios en reportes
                advanced=False,  # No análisis avanzado por defecto
                auto_features=False,  # Desactivado para rapidez
                suggestions_k=5  # 5 sugerencias
            )

            # 1. Carga y limpieza
            logger.info("Fase 1: Cargando y limpiando datos...")
            df, pnl_col_confirmed = load_and_clean(params)
            date_col = df.attrs.get('date_col', 'date')
            ratio_cols = df.attrs['ratio_cols']
            logger.info(f"  -> PnL columna: {pnl_col_confirmed}")
            logger.info(f"  -> Ratios detectados: {len(ratio_cols)}")
            logger.info(f"  -> Muestras totales: {len(df):,}")

            # 2. Correlaciones
            logger.info("\nFase 2: Calculando suite de correlaciones...")
            corr_df = compute_correlation_suite(df, ratio_cols, pnl_col_confirmed, params)
            logger.info(f"  -> Correlaciones calculadas: {len(corr_df)}")

            # Guardar para agregación posterior
            if pnl_group == 'pct':
                corr_dfs_pct.append(corr_df.copy())
            else:
                corr_dfs_pts.append(corr_df.copy())

            # 3. Validación (walk-forward o holdout)
            logger.info(f"\nFase 3: Validación ({params.split})...")
            threshold_results = {}
            wf_results = {}

            if params.split == 'holdout':
                # Holdout 70/30
                n_train = int(len(df) * 0.7)
                df_train = df.iloc[:n_train].copy()
                df_test = df.iloc[n_train:].copy()
                logger.info(f"  -> Split: Train={len(df_train):,}, Test={len(df_test):,}")

                for i, ratio in enumerate(ratio_cols, 1):
                    if i % 10 == 0:
                        logger.info(f"    Analizando umbrales: {i}/{len(ratio_cols)}")
                    result = train_find_threshold_for_ratio(df_train, ratio, pnl_col_confirmed, params)
                    threshold_results[ratio] = result
                    X = result.get('threshold_X')
                    if X is not None:
                        oos_result = eval_threshold_oos(df_test, ratio, X, pnl_col_confirmed, params)
                        threshold_results[ratio]['oos'] = oos_result

            elif params.split == 'walk-forward':
                wf_results = walk_forward_validation(df, ratio_cols, pnl_col_confirmed, date_col, params)
                logger.info(f"  -> Folds procesados: {wf_results.get('n_folds', 0)}")

            # 4. Generar reportes individuales por ventana
            logger.info("\nFase 4: Generando reportes para esta ventana...")
            output_dir = Path(csv_path).parent / "ANALISIS" / pnl_group / window_name
            write_reports(df, pnl_col_confirmed, corr_df, threshold_results, wf_results, params, output_dir)

            # Almacenar resultados
            results_by_window[pnl_col] = {
                'corr_df': corr_df,
                'threshold_results': threshold_results,
                'wf_results': wf_results,
                'params': params,
                'output_dir': output_dir
            }

            logger.info(f"  -> Resultados individuales guardados en: {output_dir}")

        # FASE FINAL: GENERAR REPORTES AGREGADOS
        logger.info("\n" + "=" * 80)
        logger.info("GENERANDO REPORTES AGREGADOS POR GRUPO")
        logger.info("=" * 80)

        # Agregar resultados PCT
        if corr_dfs_pct:
            logger.info(f"\nAgregando {len(corr_dfs_pct)} ventanas PnL_fwd_pct...")
            agg_corr_pct = aggregate_correlation_results(corr_dfs_pct, 'pct')

            output_dir_pct = Path(csv_path).parent / "ANALISIS" / "AGREGADO_pct"
            output_dir_pct.mkdir(exist_ok=True, parents=True)

            agg_path_pct = output_dir_pct / "correlation_rank_PROMEDIO.csv"
            agg_corr_pct.to_csv(agg_path_pct, index=False)
            logger.info(f"  -> Guardado: {agg_path_pct}")

            # Reporte TXT agregado PCT
            txt_path_pct = output_dir_pct / "reporte_PROMEDIO.txt"
            with open(txt_path_pct, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write("REPORTE AGREGADO — PnL_fwd_pct (PROMEDIO DE TODAS LAS VENTANAS)\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"Ventanas analizadas: {len(corr_dfs_pct)}\n")
                f.write(f"Ventanas: {', '.join([col.replace('PnL_fwd_', '').replace('_mediana', '') for col in pnl_pct_cols])}\n\n")
                f.write("TOP 20 RATIOS POR CORRELACIÓN PROMEDIO:\n")
                f.write("-" * 80 + "\n")
                for i, row in agg_corr_pct.head(20).iterrows():
                    ratio = row.get('ratio', 'N/A')
                    r = row.get('pearson_r', np.nan)
                    rho = row.get('spearman_rho', np.nan)
                    f.write(f"{i+1:2d}. {ratio:30s} | r={r:7.4f} | ρ={rho:7.4f}\n")
            logger.info(f"  -> Guardado: {txt_path_pct}")

        # Agregar resultados PTS
        if corr_dfs_pts:
            logger.info(f"\nAgregando {len(corr_dfs_pts)} ventanas PnL_fwd_pts...")
            agg_corr_pts = aggregate_correlation_results(corr_dfs_pts, 'pts')

            output_dir_pts = Path(csv_path).parent / "ANALISIS" / "AGREGADO_pts"
            output_dir_pts.mkdir(exist_ok=True, parents=True)

            agg_path_pts = output_dir_pts / "correlation_rank_PROMEDIO.csv"
            agg_corr_pts.to_csv(agg_path_pts, index=False)
            logger.info(f"  -> Guardado: {agg_path_pts}")

            # Reporte TXT agregado PTS
            txt_path_pts = output_dir_pts / "reporte_PROMEDIO.txt"
            with open(txt_path_pts, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write("REPORTE AGREGADO — PnL_fwd_pts (PROMEDIO DE TODAS LAS VENTANAS)\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"Ventanas analizadas: {len(corr_dfs_pts)}\n")
                f.write(f"Ventanas: {', '.join([col.replace('PnL_fwd_', '').replace('_mediana', '') for col in pnl_pts_cols])}\n\n")
                f.write("TOP 20 RATIOS POR CORRELACIÓN PROMEDIO:\n")
                f.write("-" * 80 + "\n")
                for i, row in agg_corr_pts.head(20).iterrows():
                    ratio = row.get('ratio', 'N/A')
                    r = row.get('pearson_r', np.nan)
                    rho = row.get('spearman_rho', np.nan)
                    f.write(f"{i+1:2d}. {ratio:30s} | r={r:7.4f} | ρ={rho:7.4f}\n")
            logger.info(f"  -> Guardado: {txt_path_pts}")

        logger.info("\n" + "=" * 80)
        logger.info("ANÁLISIS ESTADÍSTICO MULTI-VENTANA COMPLETADO EXITOSAMENTE")
        logger.info("=" * 80)
        logger.info(f"Total ventanas analizadas: {total_ventanas}")
        logger.info(f"  • PnL_fwd_pct: {len(pnl_pct_cols)} ventanas")
        logger.info(f"  • PnL_fwd_pts: {len(pnl_pts_cols)} ventanas")
        logger.info(f"\nResultados en: {Path(csv_path).parent / 'ANALISIS'}")
        logger.info("  • Reportes individuales: ANALISIS/pct/<ventana>/ y ANALISIS/pts/<ventana>/")
        logger.info("  • Reportes agregados: ANALISIS/AGREGADO_pct/ y ANALISIS/AGREGADO_pts/")
        logger.info("=" * 80)
        logger.info("")

    except Exception as e:
        logger.error(f"ERROR en análisis estadístico: {e}", exc_info=True)
        print(f"\n[ERROR STATS] El análisis estadístico falló: {e}")
        print("[INFO] El backtester continuará sin afectar los resultados principales.")


# ================== MAIN ==================
def main():
    # ============================================================
    # SISTEMA DE RECUPERACIÓN: Detectar directorios huérfanos
    # ============================================================
    print(f"\n{'='*70}")
    print("SISTEMA DE RECUPERACIÓN AUTOMÁTICA")
    print(f"{'='*70}")

    # Buscar directorios temp_batman_* en el escritorio
    orphan_dirs = sorted(DESKTOP.glob("temp_batman_*"), key=lambda p: p.name, reverse=True)

    if orphan_dirs:
        print(f"\n[!] DETECTADOS {len(orphan_dirs)} DIRECTORIO(S) HUÉRFANO(S):")
        for i, orphan_dir in enumerate(orphan_dirs, 1):
            # Verificar archivos importantes
            consolidated_file = orphan_dir / "consolidated_filtered.parquet"
            checkpoint_file = orphan_dir / "checkpoint_optimized.parquet"

            size_mb = sum(f.stat().st_size for f in orphan_dir.glob("*") if f.is_file()) / (1024**2)
            age_hours = (datetime.now().timestamp() - orphan_dir.stat().st_mtime) / 3600

            print(f"\n  [{i}] {orphan_dir.name}")
            print(f"      Tamaño total: {size_mb:.1f} MB")
            print(f"      Antigüedad: {age_hours:.1f} horas")
            print(f"      Consolidated: {'✓' if consolidated_file.exists() else '✗'}")
            print(f"      Checkpoint: {'✓' if checkpoint_file.exists() else '✗'}")

        # Usar el directorio más reciente con archivos válidos
        for orphan_dir in orphan_dirs:
            consolidated_file = orphan_dir / "consolidated_filtered.parquet"
            checkpoint_file = orphan_dir / "checkpoint_optimized.parquet"

            # Si tiene consolidated o checkpoint, puede recuperarse
            if consolidated_file.exists() or checkpoint_file.exists():
                print(f"\n[✓] DIRECTORIO RECUPERABLE ENCONTRADO: {orphan_dir.name}")

                while True:
                    response = input("\n¿Deseas RECUPERAR desde este directorio? (s/n/info): ").strip().lower()

                    if response == "info":
                        print(f"\n[INFO] Contenido del directorio:")
                        for file in sorted(orphan_dir.glob("*")):
                            if file.is_file():
                                file_size_mb = file.stat().st_size / (1024**2)
                                print(f"  - {file.name}: {file_size_mb:.1f} MB")
                        continue

                    elif response == "s":
                        print(f"\n[✓] RECUPERANDO desde directorio huérfano...")

                        # Usar este directorio como temp_dir
                        temp_dir = orphan_dir
                        ts_batch = orphan_dir.name.replace("temp_batman_", "")

                        # Saltar al procesamiento de carga
                        print(f"[INFO] Saltando generación de candidatos (ya existe archivo consolidado)")

                        # Crear parquet_files vacío (no necesitamos eliminarlos)
                        parquet_files = []

                        # Crear batch_out_path
                        batch_out_name = f"ALLANTIS_LIVE_BETA_{ts_batch}.csv"
                        batch_out_path = DESKTOP / safe_filename(batch_out_name)

                        # Saltar al bloque de carga
                        consolidated_path = temp_dir / "consolidated_filtered.parquet"

                        # SALTAR directamente a la sección de carga
                        print(f"\n[✓] Configuración de recuperación completada")
                        print(f"[✓] temp_dir: {temp_dir}")
                        print(f"[✓] consolidated_path: {consolidated_path}")
                        print(f"[✓] batch_out_path: {batch_out_path}")

                        # Marcar que estamos en modo recuperación
                        RECOVERY_MODE = True

                        # Inicializar files_sorted para uso posterior en FWD
                        files_sorted = list_local_files(DATA_DIR)
                        break

                    elif response == "n":
                        print(f"\n[INFO] Continuando con nuevo procesamiento...")
                        RECOVERY_MODE = False
                        break

                    else:
                        print("[!] Respuesta inválida. Por favor ingresa 's', 'n', o 'info'")

                if response in ("s", "n"):
                    break

        # Si no se recupera, limpiar directorios antiguos (opcional)
        if 'RECOVERY_MODE' not in locals() or not RECOVERY_MODE:
            print(f"\n[INFO] No se usará recuperación. ¿Deseas limpiar directorios antiguos?")
            cleanup_response = input("  Esto liberará espacio en disco (s/n): ").strip().lower()

            if cleanup_response == "s":
                for orphan_dir in orphan_dirs:
                    try:
                        import shutil
                        shutil.rmtree(orphan_dir)
                        print(f"  [✓] Eliminado: {orphan_dir.name}")
                    except Exception as e:
                        print(f"  [!] No se pudo eliminar {orphan_dir.name}: {e}")

    else:
        print("[INFO] No se encontraron directorios huérfanos")
        RECOVERY_MODE = False

    # ============================================================
    # MODO NORMAL: Generar nuevos candidatos
    # ============================================================
    if 'RECOVERY_MODE' not in locals() or not RECOVERY_MODE:
        print(f"\n{'='*70}")
        print("INICIANDO PROCESAMIENTO NORMAL")
        print(f"{'='*70}")

        files_sorted = list_local_files(DATA_DIR)
        if not files_sorted:
            print(f"[×] No se encontraron 30MIN*.csv en: {DATA_DIR}")
            return

        k = min(NUM_RANDOM_FILES, len(files_sorted))
        chosen_files = random.sample(files_sorted, k=k)

        ts_batch = datetime.now(TZ_ES).strftime("%Y%m%d_%H%M%S")
        batch_out_name = f"ALLANTIS_V1_LIVE_BETA{ts_batch}.csv"
        batch_out_path = DESKTOP / safe_filename(batch_out_name)

        # Directorio temporal para Parquet incrementales
        temp_dir = DESKTOP / f"temp_batman_{ts_batch}"
        temp_dir.mkdir(exist_ok=True)
        parquet_files = []

        # ============================================================
        # GENERACIÓN DE CANDIDATOS (solo en modo normal)
        # ============================================================
        for pick_idx, chosen_file in enumerate(chosen_files, start=1):
            base_idx = files_sorted.index(chosen_file)
            print(f"[i] Archivo elegido al azar {pick_idx}/{k}: {chosen_file.name} (índice {base_idx} de {len(files_sorted)})")

            m = re.search(r"(\d{4}-\d{2}-\d{2})", chosen_file.name)
            if not m:
                print(f"[!] {chosen_file.name}: no pude extraer fecha, salto.")
                continue
            date_str_us = m.group(1)
            print(f"===== Archivo: {chosen_file.name} | Fecha US: {date_str_us} =====")

            df = pd.read_csv(chosen_file)
            if df.empty:
                print("[×] Archivo de opciones vacío."); continue
            df = td_rescale_strike_only(df)

            # Cadena canónica (día completo, sin descartar por spread en selección)
            df_canon = build_canonical_chain(df)
            if df_canon.empty:
                print("     [!] Cadena canónica vacía tras limpieza."); continue

            # Slicing por minuto
            for t_idx, target_hhmm_us in enumerate(TARGET_HHMMS_US if not IGNORE_TARGET_MINUTE else ["ALL"], start=1):
                if IGNORE_TARGET_MINUTE:
                    one_min, used_hhmm_us = df.copy(), "ALL"
                else:
                    one_min, used_hhmm_us = get_slice_at_target_hhmm(df, target_hhmm_us, NEAREST_MINUTE_TOLERANCE)
                status = "OK" if not one_min.empty else "NO-DATA"
                print(f"  - [{t_idx:02d}/{len(TARGET_HHMMS_US if not IGNORE_TARGET_MINUTE else ['ALL'])}] HH:MM US objetivo {target_hhmm_us} -> usado {used_hhmm_us} -> {status}")
                if one_min.empty:
                    continue

                # Cache de greeks por snapshot
                global GREEKS_CACHE
                GREEKS_CACHE = {}

                # Hora ES + SPOT + r
                _hhmm_us = resolve_used_hhmm(used_hhmm_us, one_min)
                try:
                    dt_us = datetime.strptime(f"{date_str_us} {_hhmm_us}", "%Y-%m-%d %H:%M").replace(tzinfo=TZ_US)
                except Exception:
                    # fallback robusto
                    dt_us = datetime.strptime(f"{date_str_us} 12:00", "%Y-%m-%d %H:%M").replace(tzinfo=TZ_US)
                dt_es = dt_us.astimezone(TZ_ES)
                hhmm_es = dt_es.strftime("%H:%M")
                date_es_str = dt_es.date().isoformat()

                spot = underlying_from_slice(one_min)
                r_base = fetch_risk_free_historical_fred(target_date=date_str_us, fallback=RISK_FREE_R)
                print(f"     SPOT @ {hhmm_es} ES: {spot:.2f} | r(DGS1MO @ {date_str_us})={r_base:.6f}")

                expirations, precache = build_chains(one_min)
                if not expirations:
                    print("     [!] Sin expiraciones en este minuto."); continue
                dte_map = compute_dte_map(expirations, date_str_us)

                # Usar TODAS las expiraciones en RANGE_A y RANGE_B, ordenadas por DTE ascendente
                exp_A_all=[e for e in expirations if RANGE_A[0]<=dte_map[e]<=RANGE_A[1]]
                exp_B_all=[e for e in expirations if RANGE_B[0]<=dte_map[e]<=RANGE_B[1]]
                exp_A = sorted(exp_A_all, key=lambda e: dte_map[e])
                exp_B = sorted(exp_B_all, key=lambda e: dte_map[e])

                print(f"     Expiraciones únicas cargadas: {len(set(exp_A)|set(exp_B))}")
                print(f"     Combos brutos aprox.: {len(exp_A)}*{len(exp_B)}*|K1|*|K2|*|K3| (cómputo local)")

                rows=[]

                # ========== SELECCIÓN DE CANDIDATOS ALLANTIS ==========
                print(f"     [INFO] Generando combinaciones de estructuras Allantis...")

                # MODO INCREMENTAL vs BATCH
                if INCREMENTAL_MODE:
                    # ========== MODO INCREMENTAL: Procesa 1 a 1, escribe inmediatamente ==========
                    print(f"     [INCREMENTAL] Modo incremental activado (workers={INCREMENTAL_WORKERS})")
                    print(f"     [INCREMENTAL] Procesamiento combinación por combinación con escritura inmediata...")

                    import csv
                    import tempfile as tmpfile_module

                    # Crear archivo temporal CSV para escritura incremental
                    temp_csv_fd, temp_csv_path = tmpfile_module.mkstemp(suffix=".csv", prefix="allantis_incr_", dir=temp_dir)
                    os.close(temp_csv_fd)  # Cerrar descriptor, usaremos open() normal
                    print(f"     [INCREMENTAL] Archivo temporal: {temp_csv_path}")

                    csv_writer = None
                    csv_file_handle = None
                    total_candidates_written = 0
                    total_combinations = 0

                    # Calcular número total de combinaciones para progreso
                    for e1 in exp_A:
                        calls1, puts1, _, _ = precache[e1]
                        strikes_puts1 = sorted(set(puts1["strike"].tolist()))
                        strikes_calls1 = sorted(set(calls1["strike"].tolist()))
                        puts1_idx = puts1.set_index("strike")
                        calls1_idx = calls1.set_index("strike")

                        ul_list = gen_ul_put_candidates_by_delta(puts1_idx, strikes_puts1, spot, dte_map[e1], r_base, UL_PUT_DELTA_MIN, UL_PUT_DELTA_MAX)
                        if not ul_list:
                            continue

                        for e2 in exp_B:
                            total_combinations += len(ul_list)

                    print(f"     [INCREMENTAL] Total de combinaciones a procesar: {total_combinations}")

                    # Procesar combinaciones incrementalmente
                    comb_idx = 0
                    for e1_idx, e1 in enumerate(exp_A, start=1):
                        calls1, puts1, _, _ = precache[e1]
                        strikes_puts1 = sorted(set(puts1["strike"].tolist()))
                        strikes_calls1 = sorted(set(calls1["strike"].tolist()))

                        puts1_idx = puts1.set_index("strike")
                        calls1_idx = calls1.set_index("strike")

                        print(f"     [INCREMENTAL] Exp1 {e1_idx}/{len(exp_A)}: {e1} (DTE={dte_map[e1]})")

                        # Generar candidatos
                        ul_list = gen_ul_put_candidates_by_delta(puts1_idx, strikes_puts1, spot, dte_map[e1], r_base, UL_PUT_DELTA_MIN, UL_PUT_DELTA_MAX)
                        shorts_list = gen_short_put_candidates_by_delta(puts1_idx, strikes_puts1, spot, dte_map[e1], r_base, SHORT_PUT_DELTA_MIN, SHORT_PUT_DELTA_MAX)
                        ll_list = gen_ll_put_candidates_by_delta(puts1_idx, strikes_puts1, spot, dte_map[e1], r_base, LL_PUT_DELTA_MIN, LL_PUT_DELTA_MAX)
                        call_list = gen_short_call_candidates_by_delta(calls1_idx, strikes_calls1, spot, dte_map[e1], r_base, SHORT_CALL_DELTA_MIN, SHORT_CALL_DELTA_MAX)

                        if not ul_list or not shorts_list or not ll_list or not call_list:
                            print(f"       → Sin candidatos válidos para exp1={e1}")
                            continue

                        print(f"       → UL:{len(ul_list)} Shorts:{len(shorts_list)} LL:{len(ll_list)} Calls:{len(call_list)}")

                        for e2_idx, e2 in enumerate(exp_B, start=1):
                            calls2, _, _, _ = precache[e2]
                            print(f"       [INCREMENTAL] Exp2 {e2_idx}/{len(exp_B)}: {e2} (DTE={dte_map[e2]})")

                            # Procesar cada k_ul individualmente
                            for ul_idx, k_ul in enumerate(ul_list, start=1):
                                comb_idx += 1

                                # Log detallado cada combinación
                                if comb_idx % 10 == 0 or comb_idx == total_combinations:
                                    progress_pct = 100 * comb_idx / max(1, total_combinations)
                                    print(f"       [INCR {comb_idx}/{total_combinations} ({progress_pct:.1f}%)] E1={e1} E2={e2} UL={k_ul:.0f} → Procesando...")

                                # Preparar tarea
                                task = (
                                    k_ul, shorts_list, ll_list, call_list, e1, e2, dte_map, spot, r_base,
                                    date_es_str, hhmm_es, _hhmm_us, base_idx,
                                    puts1.reset_index(drop=True),
                                    calls1.reset_index(drop=True),
                                    calls2.reset_index(drop=True),
                                    FRAC_SUFFIXES, USE_LIQUIDITY_FILTERS,
                                    MIN_OI_FRONT, MIN_VOL_FRONT, MIN_OI_BACK, MIN_VOL_BACK,
                                    PREFILTER_CREDIT_MIN, PREFILTER_CREDIT_MAX
                                )

                                # Procesar inmediatamente (sin acumular)
                                if INCREMENTAL_WORKERS == 1:
                                    # Modo secuencial (mínima memoria)
                                    result_rows = _process_allantis_candidate(task)
                                else:
                                    # Modo semi-paralelo (procesar con pool pequeño)
                                    with ProcessPoolExecutor(max_workers=INCREMENTAL_WORKERS) as micro_executor:
                                        result_rows = list(micro_executor.map(_process_allantis_candidate, [task]))[0]

                                # Escribir inmediatamente si hay resultados
                                if result_rows:
                                    if csv_writer is None:
                                        # Primera escritura: crear header
                                        csv_file_handle = open(temp_csv_path, 'w', newline='', encoding='utf-8')
                                        csv_writer = csv.DictWriter(csv_file_handle, fieldnames=result_rows[0].keys())
                                        csv_writer.writeheader()

                                    csv_writer.writerows(result_rows)
                                    csv_file_handle.flush()  # Forzar escritura a disco
                                    total_candidates_written += len(result_rows)

                                    if comb_idx % 10 == 0 or comb_idx == total_combinations:
                                        print(f"         ✓ Escritos {len(result_rows)} candidatos → Total acum: {total_candidates_written}")

                                # Liberar memoria inmediatamente
                                del result_rows

                    # Cerrar archivo CSV
                    if csv_file_handle:
                        csv_file_handle.close()

                    print(f"     [INCREMENTAL] Completado. {total_candidates_written} candidatos escritos en {temp_csv_path}")

                    # Leer CSV y convertir a rows para compatibilidad con código posterior
                    if total_candidates_written > 0:
                        df_temp = pd.read_csv(temp_csv_path)
                        rows = df_temp.to_dict('records')
                        print(f"     [INCREMENTAL] Cargados {len(rows)} candidatos desde archivo temporal")
                        del df_temp
                    else:
                        rows = []

                else:
                    # ========== MODO BATCH: Acumula tareas y procesa en lotes ==========
                    # Allantis: 5 patas (UL, Shorts, LL puts @ DTE1 + Short/Long calls)
                    # Preparar todas las tareas de k_ul para procesamiento paralelo
                    tasks = []
                    for e1 in exp_A:
                        calls1, puts1, _, _ = precache[e1]
                        strikes_puts1 = sorted(set(puts1["strike"].tolist()))
                        strikes_calls1 = sorted(set(calls1["strike"].tolist()))

                        # Indexar por strike para búsqueda rápida
                        puts1_idx = puts1.set_index("strike")
                        calls1_idx = calls1.set_index("strike")

                        # Generar candidatos UL (Upper Line puts) basados en delta
                        ul_list = gen_ul_put_candidates_by_delta(
                            puts1_idx, strikes_puts1, spot, dte_map[e1], r_base,
                            delta_min=UL_PUT_DELTA_MIN,
                            delta_max=UL_PUT_DELTA_MAX
                        )

                        # Generar candidatos Shorts (Short puts) basados en delta
                        shorts_list = gen_short_put_candidates_by_delta(
                            puts1_idx, strikes_puts1, spot, dte_map[e1], r_base,
                            delta_min=SHORT_PUT_DELTA_MIN,
                            delta_max=SHORT_PUT_DELTA_MAX
                        )

                        # Generar candidatos LL (Lower Line puts) basados en delta
                        ll_list = gen_ll_put_candidates_by_delta(
                            puts1_idx, strikes_puts1, spot, dte_map[e1], r_base,
                            delta_min=LL_PUT_DELTA_MIN,
                            delta_max=LL_PUT_DELTA_MAX
                        )

                        # Generar candidatos Short Calls @ DTE1 basados en delta
                        call_list = gen_short_call_candidates_by_delta(
                            calls1_idx, strikes_calls1, spot, dte_map[e1], r_base,
                            delta_min=SHORT_CALL_DELTA_MIN,
                            delta_max=SHORT_CALL_DELTA_MAX
                        )

                        if not ul_list or not shorts_list or not ll_list or not call_list:
                            continue

                        for e2 in exp_B:
                            calls2, _, _, _ = precache[e2]

                            # Crear tareas para cada k_ul
                            for k_ul in ul_list:
                                task = (
                                    k_ul, shorts_list, ll_list, call_list, e1, e2, dte_map, spot, r_base,
                                    date_es_str, hhmm_es, _hhmm_us, base_idx,
                                    puts1.reset_index(drop=True),   # DataFrame serializable
                                    calls1.reset_index(drop=True),  # DataFrame serializable
                                    calls2.reset_index(drop=True),  # DataFrame serializable
                                    FRAC_SUFFIXES, USE_LIQUIDITY_FILTERS,
                                    MIN_OI_FRONT, MIN_VOL_FRONT, MIN_OI_BACK, MIN_VOL_BACK,
                                    PREFILTER_CREDIT_MIN, PREFILTER_CREDIT_MAX
                                )
                                tasks.append(task)

                    # Ejecutar en paralelo si hay tareas - PROCESAMIENTO POR LOTES (evita MemoryError)
                    if tasks:
                        import os as os_module
                        num_workers = os_module.cpu_count() or 1
                        total_tasks = len(tasks)
                        print(f"     [PARALLEL] Procesando {total_tasks} tareas Allantis con {num_workers} workers...")

                        # Dividir tareas en lotes (chunks) para reducir presión de memoria
                        batch_size = PARALLEL_BATCH_SIZE
                        num_batches = (total_tasks + batch_size - 1) // batch_size  # Redondeo hacia arriba
                        print(f"     [PARALLEL] Dividiendo en {num_batches} lotes de ~{batch_size} tareas cada uno...")

                        tasks_completed = 0
                        for batch_num in range(num_batches):
                            # Extraer el lote actual
                            start_idx = batch_num * batch_size
                            end_idx = min(start_idx + batch_size, total_tasks)
                            batch_tasks = tasks[start_idx:end_idx]
                            batch_len = len(batch_tasks)

                            print(f"     [PARALLEL] Procesando lote {batch_num + 1}/{num_batches} ({batch_len} tareas)...")

                            # Procesar lote con ProcessPoolExecutor
                            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                                # Iteración lazy sobre resultados del lote actual
                                for task_idx, result in enumerate(executor.map(_process_allantis_candidate, batch_tasks), start=1):
                                    rows.extend(result)
                                    tasks_completed += 1

                                    # Mostrar progreso global
                                    if tasks_completed % max(1, total_tasks // 10) == 0 or tasks_completed == total_tasks:
                                        progress_pct = 100 * tasks_completed / total_tasks
                                        print(f"     [PARALLEL] Progreso candidatos: {tasks_completed}/{total_tasks} ({progress_pct:.1f}%)")

                            print(f"     [PARALLEL] Lote {batch_num + 1}/{num_batches} completado. Total acumulado: {len(rows)} candidatos.")

                        print(f"     [PARALLEL] Completado. {len(rows)} candidatos Allantis generados.")

                print(f"     Candidatos generados: {len(rows)}")

            # ============================================================
            # VOLCADO INCREMENTAL POR DÍA (stream to disk)
            # ============================================================
            if rows:
                df_day = pd.DataFrame(rows)

                # Optimizar dtypes antes de volcar
                df_day = optimize_dtypes(df_day)

                # Guardar Parquet incremental
                print(f"     [INFO] Guardando {len(rows)} estructuras en archivo parquet...")
                parquet_path = temp_dir / f"day_{pick_idx:03d}_{date_str_us.replace('-', '')}.parquet"
                df_day.to_parquet(parquet_path, index=False, engine='pyarrow', compression='snappy')
                parquet_files.append(parquet_path)
                print(f"     [✓] Guardado: {parquet_path.name}")

                mem_mb = df_day.memory_usage(deep=True).sum() / 1024**2
                print(f"     [STREAM] Volcado incremental: {parquet_path.name} ({len(df_day)} filas, {mem_mb:.1f} MB)")

            # Liberar memoria
            del df_day
            del rows
        else:
            print(f"     [STREAM] Sin candidatos para este día, no se genera Parquet.")

        # Progreso global por archivo procesado en modo normal
        overall_progress = 100 * pick_idx / k
        print(f"[PROGRESO] Archivos procesados: {pick_idx}/{k} ({overall_progress:.1f}%)")

    # ============================================================
    # CONSOLIDACIÓN INCREMENTAL: Filtrar y escribir sin acumular en RAM
    # ============================================================

    # Imports necesarios tanto para modo normal como recuperación
    import pyarrow as pa
    import pyarrow.parquet as pq

    # En modo recuperación, consolidated_path ya existe
    if 'consolidated_path' not in locals():
        consolidated_path = temp_dir / "consolidated_filtered.parquet"

    # En modo recuperación, no hay parquet_files pero sí consolidated_path
    if 'RECOVERY_MODE' in locals() and RECOVERY_MODE:
        print(f"\n{'='*70}")
        print(f"MODO RECUPERACIÓN: Saltando consolidación (ya existe)")
        print(f"{'='*70}")

        if not consolidated_path.exists():
            print(f"[×] ERROR: Archivo consolidado no encontrado en modo recuperación")
            print(f"    Esperado: {consolidated_path}")
            return

        print(f"[✓] Archivo consolidado encontrado: {consolidated_path}")
        print(f"[INFO] Procediendo directamente a la carga...")

    else:
        # Modo normal: consolidar parquet_files
        if not parquet_files:
            print("[×] No se generaron candidatos en ningún día.")
            return

        print(f"\n{'='*70}")
        print(f"CONSOLIDANDO {len(parquet_files)} archivos Parquet...")
        print(f"APLICANDO PREFILTROS Y ESCRIBIENDO INCREMENTAL (sin acumular en RAM)...")
        print(f"{'='*70}")
        writer = None
        schema = None
        total_filas_leidas = 0
        total_filas_filtradas = 0

        for pq_file in parquet_files:
            df_chunk = pd.read_parquet(pq_file)
            filas_antes = len(df_chunk)
            total_filas_leidas += filas_antes

            # ========== FASE 1: Prefiltros básicos (ya existentes) ==========
            mask = pd.Series(True, index=df_chunk.index)

            # Prefiltro NET_CREDIT
            if "net_credit" in df_chunk.columns:
                mask &= (df_chunk["net_credit"] <= PREFILTER_CREDIT_MAX) & (df_chunk["net_credit"] >= PREFILTER_CREDIT_MIN)

            # Prefiltro DELTA_TOTAL
            if "delta_total" in df_chunk.columns:
                mask &= (df_chunk["delta_total"] >= DELTA_MIN) & (df_chunk["delta_total"] <= DELTA_MAX)

            # Prefiltro THETA_TOTAL
            if "theta_total" in df_chunk.columns:
                mask &= (df_chunk["theta_total"] >= THETA_MIN) & (df_chunk["theta_total"] <= THETA_MAX)

            # Prefiltro UEL_inf_USD
            if "UEL_inf_USD" in df_chunk.columns:
                mask &= (df_chunk["UEL_inf_USD"] >= UEL_INF_MIN) & (df_chunk["UEL_inf_USD"] <= UEL_INF_MAX)

            # RATIO_UEL_EARS eliminado - no aplica para Allantis

            # Aplicar máscara de prefiltros básicos
            df_chunk = df_chunk[mask]
            del mask

            # ========== FASE 2: AQI (Allantis Quality Index) ==========
            # AQI ya se calcula en process_one_allantis() y está disponible en el CSV
            # Balancea LEL, UEL, picos BWB/CCAL, valle, theta, MASTER_DISTANCE
            # No es necesario calcular nada aquí - AQI viene en los datos

            # ========== FASE 3: Filtros avanzados (PnLDV, AQI) ==========
            mask_advanced = pd.Series(True, index=df_chunk.index)

            # Filtro PnLDV (si está habilitado) - DESHABILITADO para Allantis
            if FILTER_PNLDV_ENABLED and "PnLDV" in df_chunk.columns:
                mask_advanced &= df_chunk["PnLDV"].notna() & (df_chunk["PnLDV"] >= PNLDV_MIN) & (df_chunk["PnLDV"] <= PNLDV_MAX)

            # Filtro AQI (Allantis Quality Index)
            if FILTER_AQI_ENABLED and "AQI" in df_chunk.columns:
                mask_advanced &= df_chunk["AQI"].notna() & (df_chunk["AQI"] >= AQI_MIN) & (df_chunk["AQI"] <= AQI_MAX)

            # Filtro LEL (Lower Expected Loss - Asíntota inferior)
            if FILTER_LEL_ENABLED and "LEL_pts" in df_chunk.columns:
                mask_advanced &= df_chunk["LEL_pts"].notna() & (df_chunk["LEL_pts"] >= LEL_MIN) & (df_chunk["LEL_pts"] <= LEL_MAX)

            # Filtro UEL (Upper Expected Loss - Asíntota superior)
            if FILTER_UEL_ENABLED and "UEL_pts" in df_chunk.columns:
                mask_advanced &= df_chunk["UEL_pts"].notna() & (df_chunk["UEL_pts"] >= UEL_MIN) & (df_chunk["UEL_pts"] <= UEL_MAX)

            # Aplicar filtros avanzados
            df_chunk = df_chunk[mask_advanced]
            del mask_advanced

            filas_despues = len(df_chunk)
            total_filas_filtradas += filas_despues

            # Escribir chunk filtrado al archivo consolidado (INCREMENTAL)
            if not df_chunk.empty:
                table = pa.Table.from_pandas(df_chunk)
                if writer is None:
                    # Primera escritura: crear schema y writer
                    schema = table.schema
                    writer = pq.ParquetWriter(consolidated_path, schema)
                writer.write_table(table)
                del table

            del df_chunk
            print(f"  - Procesado: {pq_file.name} ({filas_antes} → {filas_despues} filas tras prefiltros+BQI_ABS+filtros)")

        # Cerrar writer
        if writer is not None:
            writer.close()

        if total_filas_filtradas == 0:
            print("\n[!] ADVERTENCIA: No quedan filas después de aplicar prefiltros")
            return

        print(f"\n[✓] Consolidación incremental completada: {total_filas_leidas} filas → {total_filas_filtradas} filas tras prefiltros")
        print(f"[✓] Reducción: {100*(1-total_filas_filtradas/total_filas_leidas):.1f}% eliminadas")
        print(f"[✓] Archivo consolidado: {consolidated_path}")

    # ============================================================
    # CARGA SEGURA CON GESTIÓN DE MEMORIA
    # ============================================================
    print(f"\n[INFO] Cargando DataFrame consolidado filtrado desde disco...")

    import psutil
    import gc

    # ============================================================
    # DETECCIÓN DE CHECKPOINT EXISTENTE (recuperación automática)
    # ============================================================
    checkpoint_path = temp_dir / "checkpoint_optimized.parquet"

    if checkpoint_path.exists():
        checkpoint_size_mb = checkpoint_path.stat().st_size / (1024**2)
        consolidated_size_mb = consolidated_path.stat().st_size / (1024**2)

        print(f"\n[!] CHECKPOINT DETECTADO:")
        print(f"    - Checkpoint optimizado: {checkpoint_size_mb:.1f} MB")
        print(f"    - Archivo consolidado: {consolidated_size_mb:.1f} MB")
        print(f"[INFO] Usando checkpoint (ya optimizado, carga más rápida)")

        # Cargar metadatos de tipos si existen
        metadata_path = temp_dir / "checkpoint_dtypes.json"
        dtypes_dict = None
        if metadata_path.exists():
            import json
            with open(metadata_path, 'r') as f:
                dtypes_dict = json.load(f)
            print(f"[INFO] Metadatos de tipos encontrados - restaurando optimizaciones")

        # Cargar desde checkpoint
        df = pd.read_parquet(checkpoint_path)

        # Restaurar tipos optimizados (Parquet pierde info de category)
        if dtypes_dict:
            print(f"[INFO] Aplicando tipos optimizados...", end='', flush=True)
            for col, dtype_str in dtypes_dict.items():
                if col in df.columns:
                    try:
                        if dtype_str == 'category':
                            df[col] = df[col].astype('category')
                        elif 'int' in dtype_str or 'float' in dtype_str:
                            df[col] = df[col].astype(dtype_str)
                    except:
                        pass  # Si falla conversión, mantener tipo actual
            print(f" ✓")

        mem_final_mb = df.memory_usage(deep=True).sum() / (1024**2)

        print(f"[✓] DataFrame cargado desde checkpoint: {len(df):,} filas")
        print(f"[✓] Memoria usada: {mem_final_mb:.1f} MB ({mem_final_mb/1024:.2f} GB)")

    else:
        # No hay checkpoint, procesar archivo consolidado
        print(f"[INFO] No se encontró checkpoint previo, cargando desde archivo consolidado...")

        # Obtener memoria disponible
        mem_available_gb = psutil.virtual_memory().available / (1024**3)
        file_size_gb = consolidated_path.stat().st_size / (1024**3)

        print(f"[INFO] Memoria disponible: {mem_available_gb:.2f} GB")
        print(f"[INFO] Tamaño archivo: {file_size_gb:.2f} GB")

        # Estrategia 1: Intentar carga optimizada (conversión de tipos automática)
        try:
            # Leer metadatos primero
            pq_file = pq.ParquetFile(consolidated_path)
            total_rows = pq_file.metadata.num_rows

            print(f"[INFO] Archivo contiene {total_rows:,} filas")

            # Si el archivo es > 50% de la memoria disponible, usar chunks
            if file_size_gb > 0.5 * mem_available_gb:
                print(f"[!] Archivo grande detectado - usando carga por chunks")

                # Calcular tamaño de chunk seguro (usar ~20% de memoria disponible)
                chunk_memory_target_gb = 0.2 * mem_available_gb
                estimated_row_size_bytes = (file_size_gb * 1024**3) / total_rows
                chunk_size = int((chunk_memory_target_gb * 1024**3) / estimated_row_size_bytes)
                chunk_size = max(10_000, min(chunk_size, 1_000_000))  # Entre 10K y 1M

                print(f"[INFO] Tamaño de chunk: {chunk_size:,} filas")

                # Leer por chunks y consolidar
                chunks = []
                total_memory_mb = 0
                num_batches = (total_rows + chunk_size - 1) // chunk_size  # Calcular total de chunks

                print(f"[INFO] Procesando {num_batches} chunks estimados...")

                for i, batch in enumerate(pq_file.iter_batches(batch_size=chunk_size)):
                    print(f"\n  ┌─ Chunk {i+1}/{num_batches} ─────────────────────")
                    print(f"  │ Convirtiendo PyArrow → Pandas...", end='', flush=True)
                    df_chunk = batch.to_pandas()
                    print(f" ✓ ({len(df_chunk):,} filas)")

                    # Optimizar tipos ANTES de acumular
                    print(f"  │ Optimizando tipos de datos...")
                    df_chunk = optimize_dtypes_aggressive(df_chunk, verbose=True)

                    chunk_mem_mb = df_chunk.memory_usage(deep=True).sum() / (1024**2)
                    total_memory_mb += chunk_mem_mb

                    chunks.append(df_chunk)

                    print(f"  │ Chunk optimizado: {chunk_mem_mb:.1f} MB")
                    print(f"  └─ Acumulado en memoria: {total_memory_mb:.1f} MB ({len(chunks)} chunks)")

                    # Si acumulamos demasiado, forzar consolidación parcial
                    if total_memory_mb > 1024 * 3:  # 3 GB
                        print(f"\n  [!] Límite de memoria alcanzado ({total_memory_mb:.1f} MB > 3,000 MB)")
                        print(f"  [INFO] Consolidando {len(chunks)} chunks intermedios...", end='', flush=True)
                        df_partial = pd.concat(chunks, ignore_index=True)
                        print(f" ✓")
                        print(f"  [INFO] Re-optimizando DataFrame consolidado...")
                        df_partial = optimize_dtypes_aggressive(df_partial, verbose=True)
                        chunks = [df_partial]
                        gc.collect()
                        total_memory_mb = df_partial.memory_usage(deep=True).sum() / (1024**2)
                        print(f"  [✓] Consolidación intermedia completada: {total_memory_mb:.1f} MB")

                # Consolidar todos los chunks
                print(f"\n[INFO] Consolidación final de {len(chunks)} chunks...", end='', flush=True)
                df = pd.concat(chunks, ignore_index=True)
                print(" ✓")
                del chunks
                gc.collect()

            else:
                # Archivo pequeño - intentar carga directa pero optimizada
                print(f"[INFO] Carga directa (archivo < 50% memoria disponible)")
                print(f"[INFO] Intentando leer archivo completo...", end='', flush=True)

                try:
                    df = pd.read_parquet(consolidated_path)
                    print(f" ✓ ({len(df):,} filas cargadas)")

                    # Optimizar tipos inmediatamente
                    print(f"\n[INFO] Optimizando tipos de datos del DataFrame completo...")
                    mem_before_mb = df.memory_usage(deep=True).sum() / (1024**2)
                    print(f"[INFO] Memoria ANTES de optimizar: {mem_before_mb:.1f} MB")

                    df = optimize_dtypes_aggressive(df, verbose=True)

                    mem_after_mb = df.memory_usage(deep=True).sum() / (1024**2)
                    reduction_pct = 100 * (1 - mem_after_mb / mem_before_mb)
                    print(f"\n[✓] Optimización completada: {mem_before_mb:.1f} MB → {mem_after_mb:.1f} MB ({reduction_pct:.1f}% reducción)")

                except (MemoryError, Exception) as e:
                    # Carga directa falló - forzar carga por chunks
                    print(f" ✗")
                    print(f"\n[!] Carga directa falló: {type(e).__name__}")
                    print(f"[INFO] Cambiando automáticamente a CARGA POR CHUNKS...")

                    # Calcular tamaño de chunk conservador (10% de memoria disponible)
                    chunk_memory_target_gb = 0.1 * mem_available_gb
                    estimated_row_size_bytes = (file_size_gb * 1024**3) / total_rows
                    chunk_size = int((chunk_memory_target_gb * 1024**3) / estimated_row_size_bytes)
                    chunk_size = max(10_000, min(chunk_size, 500_000))  # Entre 10K y 500K

                    print(f"[INFO] Tamaño de chunk conservador: {chunk_size:,} filas")

                    # Leer por chunks y consolidar
                    chunks = []
                    total_memory_mb = 0
                    num_batches = (total_rows + chunk_size - 1) // chunk_size

                    print(f"[INFO] Procesando {num_batches} chunks estimados...")

                    for i, batch in enumerate(pq_file.iter_batches(batch_size=chunk_size)):
                        print(f"\n  ┌─ Chunk {i+1}/{num_batches} ─────────────────────")
                        print(f"  │ Convirtiendo PyArrow → Pandas...", end='', flush=True)
                        df_chunk = batch.to_pandas()
                        print(f" ✓ ({len(df_chunk):,} filas)")

                        # Optimizar tipos ANTES de acumular
                        print(f"  │ Optimizando tipos de datos...")
                        df_chunk = optimize_dtypes_aggressive(df_chunk, verbose=True)

                        chunk_mem_mb = df_chunk.memory_usage(deep=True).sum() / (1024**2)
                        total_memory_mb += chunk_mem_mb

                        chunks.append(df_chunk)

                        print(f"  │ Chunk optimizado: {chunk_mem_mb:.1f} MB")
                        print(f"  └─ Acumulado en memoria: {total_memory_mb:.1f} MB ({len(chunks)} chunks)")

                        # Si acumulamos demasiado, forzar consolidación parcial
                        if total_memory_mb > 1024 * 2:  # 2 GB (más conservador)
                            print(f"\n  [!] Límite de memoria alcanzado ({total_memory_mb:.1f} MB > 2,000 MB)")
                            print(f"  [INFO] Consolidando {len(chunks)} chunks intermedios...", end='', flush=True)
                            df_partial = pd.concat(chunks, ignore_index=True)
                            print(f" ✓")
                            print(f"  [INFO] Re-optimizando DataFrame consolidado...")
                            df_partial = optimize_dtypes_aggressive(df_partial, verbose=True)
                            chunks = [df_partial]
                            gc.collect()
                            total_memory_mb = df_partial.memory_usage(deep=True).sum() / (1024**2)
                            print(f"  [✓] Consolidación intermedia completada: {total_memory_mb:.1f} MB")

                    # Consolidar todos los chunks
                    print(f"\n[INFO] Consolidación final de {len(chunks)} chunks...", end='', flush=True)
                    df = pd.concat(chunks, ignore_index=True)
                    print(" ✓")
                    del chunks
                    gc.collect()

            print(f"\n[✓] DataFrame cargado exitosamente: {len(df):,} filas")
            mem_final_mb = df.memory_usage(deep=True).sum() / (1024**2)
            mem_final_gb = mem_final_mb / 1024
            print(f"[✓] Memoria usada: {mem_final_mb:.1f} MB ({mem_final_gb:.2f} GB)")

            # Mostrar estadísticas de memoria disponible
            import psutil
            mem_available_after = psutil.virtual_memory().available / (1024**3)
            mem_percent_used = psutil.virtual_memory().percent
            print(f"[INFO] RAM disponible ahora: {mem_available_after:.2f} GB ({100-mem_percent_used:.1f}% libre)")

            # ============================================================
            # CHECKPOINT: Guardar DataFrame optimizado para evitar reprocesar
            # ============================================================
            print(f"\n{'─'*70}")
            print(f"GUARDANDO CHECKPOINT")
            print(f"{'─'*70}")
            print(f"[INFO] Comprimiendo DataFrame a disco (formato Parquet Snappy)...", end='', flush=True)

            import time
            start_time = time.time()
            df.to_parquet(checkpoint_path, index=False, engine='pyarrow', compression='snappy')
            elapsed_time = time.time() - start_time

            checkpoint_size_mb = checkpoint_path.stat().st_size / (1024**2)
            compression_ratio = (mem_final_mb / checkpoint_size_mb) if checkpoint_size_mb > 0 else 1

            print(f" ✓ ({elapsed_time:.1f} segundos)")
            print(f"[✓] Checkpoint guardado: {checkpoint_path.name}")
            print(f"[✓] Tamaño en disco: {checkpoint_size_mb:.1f} MB (ratio compresión: {compression_ratio:.1f}x)")

            # Guardar metadatos de tipos optimizados para restaurar al cargar
            import json
            dtypes_dict = {col: str(dtype) for col, dtype in df.dtypes.items()}
            metadata_path = temp_dir / "checkpoint_dtypes.json"
            with open(metadata_path, 'w') as f:
                json.dump(dtypes_dict, f, indent=2)
            print(f"[✓] Metadatos de tipos guardados: {metadata_path.name}")
            print(f"[INFO] Si el proceso falla después de aquí, recupera desde este checkpoint")

        except MemoryError as e:
            print(f"\n[×] ERROR DE MEMORIA al cargar archivo consolidado:")
            print(f"    {str(e)}")
            print(f"\n[!] SOLUCIÓN: El archivo es demasiado grande para la memoria disponible.")
            print(f"    Opciones:")
            print(f"    1. Aumentar filtros para reducir más filas")
            print(f"    2. Procesar en múltiples lotes por fecha")
            print(f"    3. Usar una máquina con más RAM")
            print(f"    4. El archivo consolidado está en: {consolidated_path}")
            print(f"       Puedes intentar procesarlo manualmente en chunks")
            raise
        except Exception as e:
            print(f"\n[×] ERROR inesperado al cargar archivo consolidado:")
            print(f"    {str(e)}")
            print(f"[INFO] Archivo consolidado disponible en: {consolidated_path}")
            raise

    # Eliminar archivos parquet individuales para liberar espacio en disco
    import gc
    for pq_file in parquet_files:
        try:
            pq_file.unlink()
        except Exception as e:
            print(f"[!] No se pudo eliminar {pq_file.name}: {e}")

    # Forzar recolección de basura para liberar memoria
    gc.collect()
    print(f"[✓] Archivos temporales eliminados y memoria liberada")

    # ============================================================
    # VERIFICACIÓN: Todos los filtros ya fueron aplicados durante consolidación
    # ============================================================
    print(f"\n{'='*70}")
    print(f"VERIFICANDO FILTROS (ya aplicados durante consolidación)")
    print(f"{'='*70}")

    if not df.empty:
        # Verificar NET_CREDIT (ya filtrado)
        stats = get_filter_stats(df, "net_credit", PREFILTER_CREDIT_MIN, PREFILTER_CREDIT_MAX)
        print(f"[✓] NET_CREDIT ya prefiltrado: {format_filter_log('NET_CREDIT', PREFILTER_CREDIT_MIN, PREFILTER_CREDIT_MAX, len(df), len(df), stats)}")

        # Verificar DELTA_TOTAL (ya filtrado)
        stats = get_filter_stats(df, "delta_total", DELTA_MIN, DELTA_MAX)
        print(f"[✓] DELTA_TOTAL ya prefiltrado: {format_filter_log('DELTA_TOTAL', DELTA_MIN, DELTA_MAX, len(df), len(df), stats)}")

        # Verificar THETA_TOTAL (ya filtrado)
        stats = get_filter_stats(df, "theta_total", THETA_MIN, THETA_MAX)
        print(f"[✓] THETA_TOTAL ya prefiltrado: {format_filter_log('THETA_TOTAL', THETA_MIN, THETA_MAX, len(df), len(df), stats)}")

        # Verificar UEL_inf_USD (ya filtrado)
        stats = get_filter_stats(df, "UEL_inf_USD", UEL_INF_MIN, UEL_INF_MAX)
        print(f"[✓] UEL_inf_USD ya prefiltrado: {format_filter_log('UEL_inf_USD', UEL_INF_MIN, UEL_INF_MAX, len(df), len(df), stats)}")

        # RATIO_UEL_EARS eliminado - no aplica para Allantis

        # Verificar PnLDV (ya filtrado si estaba habilitado)
        if "PnLDV" in df.columns:
            if FILTER_PNLDV_ENABLED:
                stats = get_filter_stats(df, "PnLDV", PNLDV_MIN, PNLDV_MAX)
                print(f"[✓] PnLDV ya prefiltrado: {format_filter_log('PnLDV', PNLDV_MIN, PNLDV_MAX, len(df), len(df), stats)}")
            else:
                stats = get_filter_stats(df, "PnLDV", PNLDV_MIN, PNLDV_MAX)
                print(f"[✓] PnLDV [DESACTIVADO]: {format_filter_log('PnLDV', PNLDV_MIN, PNLDV_MAX, len(df), len(df), stats)}")

        # Verificar BQI_ABS (ya filtrado si estaba habilitado)
        if "BQI_ABS" in df.columns:
            if FILTER_BQI_ABS_ENABLED:
                stats = get_filter_stats(df, "BQI_ABS", BQI_ABS_MIN, BQI_ABS_MAX)
                print(f"[✓] BQI_ABS ya prefiltrado: {format_filter_log('BQI_ABS', BQI_ABS_MIN, BQI_ABS_MAX, len(df), len(df), stats)}")
            else:
                stats = get_filter_stats(df, "BQI_ABS", BQI_ABS_MIN, BQI_ABS_MAX)
                print(f"[✓] BQI_ABS [DESACTIVADO]: {format_filter_log('BQI_ABS', BQI_ABS_MIN, BQI_ABS_MAX, len(df), len(df), stats)}")
        else:
            print(f"[!] BQI_ABS no encontrado (no se calculó durante consolidación)")

    # Liberar memoria
    import gc
    gc.collect()
    print(f"\n[✓] Todos los filtros fueron aplicados durante consolidación incremental")
    print(f"[✓] No se requiere filtrado adicional - DataFrame listo para ordenar")

    # ============================================================
    # VALIDACIÓN DE MEMORIA Y SELECCIÓN DE ESTRATEGIA
    # ============================================================
    print(f"\n{'='*70}")
    print(f"VALIDANDO MEMORIA Y SELECCIONANDO ESTRATEGIA DE ORDENAMIENTO")
    print(f"{'='*70}")

    mem_available_gb = psutil.virtual_memory().available / (1024**3)
    df_memory_gb = df.memory_usage(deep=True).sum() / (1024**3)

    print(f"[INFO] Memoria disponible: {mem_available_gb:.2f} GB")
    print(f"[INFO] DataFrame en memoria: {df_memory_gb:.2f} GB")
    print(f"[INFO] Filas totales: {len(df):,}")

    # Determinar estrategia de ordenamiento según memoria disponible
    # Con tipos optimizados (category, int32), pandas sort_values necesita ~1.5x
    required_memory_optimal = df_memory_gb * 1.5  # Ordenamiento en memoria completo
    required_memory_minimum = df_memory_gb * 0.3  # External sort (solo columnas de orden)

    # Decidir estrategia
    if mem_available_gb >= required_memory_optimal:
        sort_strategy = "in_memory"
        print(f"[✓] Memoria suficiente - usando ordenamiento EN MEMORIA")
    elif mem_available_gb >= required_memory_minimum:
        sort_strategy = "external_sort"
        print(f"[!] Memoria limitada - usando EXTERNAL SORT (out-of-core)")
        print(f"[INFO] Esta estrategia puede procesar datasets de cualquier tamaño")
    else:
        # Intentar liberar memoria
        print(f"\n[!] ADVERTENCIA: Memoria muy limitada ({mem_available_gb:.2f} GB)")
        print(f"[INFO] Forzando recolección de basura agresiva...")

        gc.collect()
        gc.collect()

        mem_available_gb = psutil.virtual_memory().available / (1024**3)
        print(f"[INFO] Memoria disponible tras gc: {mem_available_gb:.2f} GB")

        if mem_available_gb >= required_memory_minimum:
            sort_strategy = "external_sort"
            print(f"[✓] Usando EXTERNAL SORT (requiere ~{required_memory_minimum:.2f} GB)")
        else:
            sort_strategy = "external_sort_minimal"
            print(f"[!] Memoria crítica - usando EXTERNAL SORT MINIMALISTA")
            print(f"[INFO] Procesará por mini-chunks (más lento pero sin límite de tamaño)")

    # Orden interactivo con tie-breakers
    if df.empty:
        print("\nNo hay filas tras filtros; no se aplica orden. CSV vacío.")
    else:
        # Usar RANKING_MODE para determinar el criterio de ordenamiento
        choice = RANKING_MODE.lower() if RANKING_MODE else "theta"  # Usar RANKING_MODE configurado

        print(f"\n{'─'*70}")
        print(f"ORDENAMIENTO DE CANDIDATOS ALLANTIS")
        print(f"{'─'*70}")
        print(f"[INFO] Criterio seleccionado: {choice} (RANKING_MODE)")
        print(f"[INFO] Estrategia: {sort_strategy}")

        try:
            import time
            start_time = time.time()

            # Determinar columnas de ordenamiento según criterio (adaptado para Allantis)
            if choice in ("theta", "t", "θ", "theta_total"):
                sort_cols = ["theta_total", "delta_total"]
                sort_asc = [False, False]
                used = "theta_total → delta_total"
            elif choice in ("delta", "d", "delta_total"):
                sort_cols = ["delta_total", "theta_total"]
                sort_asc = [False, False]
                used = "delta_total → theta_total"
            elif choice in ("credit", "net_credit", "nc"):
                sort_cols = ["net_credit", "theta_total"]
                sort_asc = [True, False]  # Menor net_credit primero (más crédito recibido)
                used = "net_credit → theta_total"
            elif choice in ("aqi", "AQI", "allantis_quality"):
                sort_cols = ["AQI", "theta_total"]
                sort_asc = [False, False]  # Mayor AQI primero (mejor calidad)
                used = "AQI → theta_total"
            else:
                # Default: theta_total
                sort_cols = ["theta_total", "delta_total"]
                sort_asc = [False, False]
                used = "theta_total → delta_total"

            # Ejecutar ordenamiento según estrategia
            if sort_strategy == "in_memory":
                # ============================================================
                # ESTRATEGIA 1: ORDENAMIENTO EN MEMORIA (rápido)
                # ============================================================
                print(f"[INFO] Ordenando {len(df):,} filas en memoria...", end='', flush=True)
                df = df.sort_values(by=sort_cols, ascending=sort_asc, kind='mergesort')
                print(f" ✓")

            elif sort_strategy == "external_sort":
                # ============================================================
                # ESTRATEGIA 2: EXTERNAL SORT (eficiente en memoria)
                # ============================================================
                print(f"[INFO] Iniciando External Sort para {len(df):,} filas")
                print(f"[INFO] Fase 1/3: Extrayendo columnas de ordenamiento...", end='', flush=True)

                # Extraer solo las columnas necesarias para ordenar
                df_sort = df[sort_cols].copy()
                sort_memory_mb = df_sort.memory_usage(deep=True).sum() / (1024**2)
                print(f" ✓ ({sort_memory_mb:.1f} MB)")

                print(f"[INFO] Fase 2/3: Ordenando columnas clave...", end='', flush=True)
                sorted_indices = df_sort.sort_values(by=sort_cols, ascending=sort_asc).index.to_numpy()
                del df_sort
                gc.collect()
                print(f" ✓")

                # Guardar índices ordenados
                print(f"[INFO] Guardando índices ordenados a disco...", end='', flush=True)
                sort_index_path = temp_dir / "sort_indices.npy"
                np.save(sort_index_path, sorted_indices)
                print(f" ✓")

                print(f"[INFO] Fase 3/3: Reordenando DataFrame por chunks...")
                # Guardar DataFrame actual a disco temporal
                temp_unsorted_path = temp_dir / "temp_for_reorder.parquet"
                df.to_parquet(temp_unsorted_path, index=True)
                del df
                gc.collect()

                # Reordenar por chunks usando archivo en disco
                chunk_size = 500000
                n_chunks = (len(sorted_indices) + chunk_size - 1) // chunk_size

                # Cargar DataFrame completo solo si cabe en memoria
                try:
                    print(f"  Intentando carga completa para reordenamiento rápido...", end='', flush=True)
                    df_temp = pd.read_parquet(temp_unsorted_path)

                    # Restaurar tipos
                    if dtypes_dict:
                        for col, dtype_str in dtypes_dict.items():
                            if col in df_temp.columns:
                                try:
                                    if dtype_str == 'category':
                                        df_temp[col] = df_temp[col].astype('category')
                                    elif 'int' in dtype_str or 'float' in dtype_str:
                                        df_temp[col] = df_temp[col].astype(dtype_str)
                                except:
                                    pass

                    # Reordenar
                    df = df_temp.iloc[sorted_indices].reset_index(drop=True)
                    del df_temp
                    gc.collect()
                    print(f" ✓ (reordenamiento rápido)")

                except MemoryError:
                    # Si no cabe, procesar por chunks
                    print(f" ✗ (sin memoria)")
                    print(f"  Procesando por chunks (más lento)...")

                    # Usar PyArrow para lectura eficiente por row groups
                    pq_file = pq.ParquetFile(temp_unsorted_path)

                    reordered_chunks = []
                    for i in range(n_chunks):
                        start_idx = i * chunk_size
                        end_idx = min((i + 1) * chunk_size, len(sorted_indices))
                        chunk_indices = sorted_indices[start_idx:end_idx]

                        print(f"  Chunk {i+1}/{n_chunks} ({len(chunk_indices):,} filas)...", end='', flush=True)

                        # Leer DataFrame completo (única opción eficiente con Parquet)
                        df_chunk_full = pq_file.read().to_pandas()

                        # Seleccionar solo filas necesarias
                        df_chunk = df_chunk_full.iloc[chunk_indices].reset_index(drop=True)
                        del df_chunk_full
                        gc.collect()

                        # Restaurar tipos
                        if dtypes_dict:
                            for col, dtype_str in dtypes_dict.items():
                                if col in df_chunk.columns:
                                    try:
                                        if dtype_str == 'category':
                                            df_chunk[col] = df_chunk[col].astype('category')
                                        elif 'int' in dtype_str or 'float' in dtype_str:
                                            df_chunk[col] = df_chunk[col].astype(dtype_str)
                                    except:
                                        pass

                        reordered_chunks.append(df_chunk)
                        print(f" ✓")

                        # Consolidar si acumulamos mucho
                        if len(reordered_chunks) >= 5:
                            print(f"  Consolidando intermedios...", end='', flush=True)
                            df_partial = pd.concat(reordered_chunks, ignore_index=True)
                            reordered_chunks = [df_partial]
                            gc.collect()
                            print(f" ✓")

                    # Consolidación final
                    print(f"  Consolidación final...", end='', flush=True)
                    df = pd.concat(reordered_chunks, ignore_index=True)
                    del reordered_chunks
                    gc.collect()
                    print(f" ✓")

                # Limpiar archivo temporal
                temp_unsorted_path.unlink()

            elif sort_strategy == "external_sort_minimal":
                # ============================================================
                # ESTRATEGIA 3: EXTERNAL SORT MINIMALISTA (memoria crítica)
                # ============================================================
                print(f"[INFO] Iniciando External Sort Minimalista para {len(df):,} filas")
                print(f"[!] ADVERTENCIA: Este modo es MUY LENTO pero puede procesar cualquier tamaño")

                # Guardar DataFrame a disco primero
                temp_unsorted_path = temp_dir / "temp_unsorted.parquet"
                print(f"[INFO] Guardando DataFrame a disco...", end='', flush=True)
                df.to_parquet(temp_unsorted_path, index=True)  # Guardar con índice original
                del df
                gc.collect()
                print(f" ✓")

                # Cargar solo columnas de ordenamiento
                print(f"[INFO] Cargando solo columnas de ordenamiento...", end='', flush=True)
                df_sort = pd.read_parquet(temp_unsorted_path, columns=sort_cols)
                print(f" ✓")

                # Ordenar y obtener índices
                print(f"[INFO] Calculando orden...", end='', flush=True)
                sorted_indices = df_sort.sort_values(by=sort_cols, ascending=sort_asc).index.to_numpy()
                del df_sort
                gc.collect()
                print(f" ✓")

                # Reordenar por micro-chunks
                print(f"[INFO] Reordenando por micro-chunks (100k filas)...")
                chunk_size = 100000
                n_chunks = (len(sorted_indices) + chunk_size - 1) // chunk_size

                reordered_chunks = []
                for i in range(n_chunks):
                    start_idx = i * chunk_size
                    end_idx = min((i + 1) * chunk_size, len(sorted_indices))
                    chunk_indices = sorted_indices[start_idx:end_idx]

                    print(f"  Micro-chunk {i+1}/{n_chunks}...", end='', flush=True)
                    df_chunk = pd.read_parquet(temp_unsorted_path)
                    df_chunk = df_chunk.iloc[chunk_indices].reset_index(drop=True)

                    # Restaurar tipos
                    if dtypes_dict:
                        for col, dtype_str in dtypes_dict.items():
                            if col in df_chunk.columns:
                                try:
                                    if dtype_str == 'category':
                                        df_chunk[col] = df_chunk[col].astype('category')
                                except:
                                    pass

                    reordered_chunks.append(df_chunk)
                    print(f" ✓")

                    if len(reordered_chunks) >= 3:
                        df_partial = pd.concat(reordered_chunks, ignore_index=True)
                        reordered_chunks = [df_partial]
                        gc.collect()

                df = pd.concat(reordered_chunks, ignore_index=True)
                del reordered_chunks
                gc.collect()

                # Limpiar archivo temporal
                temp_unsorted_path.unlink()

            elapsed_sort = time.time() - start_time
            print(f"\n[✓] Ordenamiento completado: {used}")
            print(f"[✓] Tiempo total: {elapsed_sort:.1f} segundos")

            # Liberar memoria tras ordenar
            gc.collect()
            mem_after_sort = psutil.virtual_memory().available / (1024**3)
            print(f"[✓] Memoria disponible: {mem_after_sort:.2f} GB")

        except Exception as e:
            print(f"\n[×] ERROR durante ordenamiento:")
            print(f"    {str(e)}")
            print(f"[INFO] Checkpoint disponible en: {checkpoint_path}")
            raise

    # ============================================================
    # SISTEMA FWD (FORWARD TESTING) — POST-RANKING
    # ============================================================
    fwd_indices_to_process = []

    if not df.empty and (FWD_ON_WINNERS or FWD_ON_LOSERS):
        print(f"\n{'='*70}")
        print("INICIANDO CÁLCULO FWD (Forward Testing)")
        print(f"{'='*70}")

        # Convertir columnas Categorical a tipos regulares para evitar errores en FWD
        print(f"\n[FWD] Preparando DataFrame: convirtiendo columnas Categorical a tipos regulares...")
        categorical_cols = df.select_dtypes(include=['category']).columns
        if len(categorical_cols) > 0:
            for col in categorical_cols:
                # Determinar el tipo apropiado basado en el contenido
                try:
                    # Intentar convertir a float si parece numérico
                    df[col] = df[col].astype('float32')
                except (ValueError, TypeError):
                    # Si falla, convertir a object/string
                    df[col] = df[col].astype('object')
            print(f"[FWD] Convertidas {len(categorical_cols)} columnas Categorical: {', '.join(list(categorical_cols)[:5])}{'...' if len(categorical_cols) > 5 else ''}")
        else:
            print(f"[FWD] No hay columnas Categorical para convertir")

        # Inicializar listas de índices
        winner_indices = []
        loser_indices = []

        # Parsear FWD_TOP_PCT
        pct_value = parse_percentage(FWD_TOP_PCT)
        if pct_value is None:
            print(f"\n[FWD] ⚠️ FWD_TOP_PCT no está definido. Abortando FWD.")
        else:
            print(f"\n[FWD] Modo de selección por PORCENTAJE activado: {pct_value*100:.1f}%")
            print(f"[FWD] Métrica de ranking: {RANKING_MODE} (descendente)")

            # Determinar qué batmans procesar
            if FWD_ON_WINNERS and FWD_ON_LOSERS:
                # Ambas activas: hacer FWD sobre winners Y losers
                df_valid = df[df[RANKING_MODE].notna()].copy()
                total_valid = len(df_valid)

                # Calcular N = max(1, floor(p × total_filas_validas))
                N = max(1, int(np.floor(pct_value * total_valid)))

                if N == 0 or total_valid == 0:
                    print(f"\n[FWD] ⚠️ No hay filas válidas o N=0. Abortando FWD.")
                else:
                    # Orden descendente: mejores son los primeros, peores son los últimos
                    # Winners: primeras N filas
                    # Losers: últimas N filas
                    df_winners_selected = df_valid.iloc[:N].copy()
                    df_losers_selected = df_valid.iloc[-N:].copy()

                    # Deduplicar por dia + DTE1/DTE2 (keep="first" preserva orden)
                    num_winners_before = len(df_winners_selected)
                    df_winners_unique = df_winners_selected.drop_duplicates(subset=["dia", "DTE1/DTE2"], keep="first")
                    winner_indices = list(df_winners_unique.index)

                    num_losers_before = len(df_losers_selected)
                    df_losers_unique = df_losers_selected.drop_duplicates(subset=["dia", "DTE1/DTE2"], keep="first")
                    loser_indices = list(df_losers_unique.index)

                    print(f"\n[FWD] WINNERS - Selección por porcentaje posicional {pct_value*100:.1f}%:")
                    print(f"  - Métrica: {RANKING_MODE} (desc)")
                    print(f"  - Total filas válidas: {total_valid}")
                    print(f"  - N calculado: {N} (floor({{pct_value}} × {{total_valid}}))")
                    print(f"  - Batmans seleccionados (primeras {N} filas): {num_winners_before}")
                    print(f"  - Batmans tras deduplicación dia+DTE1/DTE2: {len(winner_indices)}")

                    print(f"\n[FWD] LOSERS - Selección por porcentaje posicional {pct_value*100:.1f}%:")
                    print(f"  - Métrica: {RANKING_MODE} (desc)")
                    print(f"  - Total filas válidas: {total_valid}")
                    print(f"  - N calculado: {N} (floor({{pct_value}} × {{total_valid}}))")
                    print(f"  - Batmans seleccionados (últimas {N} filas): {num_losers_before}")
                    print(f"  - Batmans tras deduplicación dia+DTE1/DTE2: {len(loser_indices)}")

                fwd_indices_to_process = winner_indices + loser_indices
                print(f"[FWD] Total batmans para FWD: {len(fwd_indices_to_process)} ({len(winner_indices)} winners + {len(loser_indices)} losers)")

                # ============================================================
                # ETIQUETAR WL_PRE y RANK_PRE (persistir para análisis post-FWD)
                # ============================================================
                # Inicializar columnas si no existen
                if "WL_PRE" not in df.columns:
                    df["WL_PRE"] = None
                if "RANK_PRE" not in df.columns:
                    df["RANK_PRE"] = None

                # Etiquetar winners con ranking posicional
                for rank_pos, idx in enumerate(winner_indices, start=1):
                    df.at[idx, "WL_PRE"] = "WIN"
                    df.at[idx, "RANK_PRE"] = rank_pos

                # Etiquetar losers con ranking posicional (inverso para mantener orden)
                for rank_pos, idx in enumerate(loser_indices, start=1):
                    df.at[idx, "WL_PRE"] = "LOS"
                    df.at[idx, "RANK_PRE"] = rank_pos

                print(f"[FWD] Etiquetas WL_PRE persistidas: {len(winner_indices)} WIN + {len(loser_indices)} LOS")

            elif FWD_ON_WINNERS:
                # Solo winners
                df_valid = df[df[RANKING_MODE].notna()].copy()
                total_valid = len(df_valid)
                N = max(1, int(np.floor(pct_value * total_valid)))

                if N == 0 or total_valid == 0:
                    print(f"\n[FWD] ⚠️ No hay filas válidas o N=0. Abortando FWD.")
                else:
                    # Winners: primeras N filas (mejores valores altos)
                    df_winners_selected = df_valid.iloc[:N].copy()

                    # Deduplicar por dia + DTE1/DTE2 (keep="first" preserva orden)
                    num_before = len(df_winners_selected)
                    df_winners_unique = df_winners_selected.drop_duplicates(subset=["dia", "DTE1/DTE2"], keep="first")
                    winner_indices = list(df_winners_unique.index)

                    print(f"\n[FWD] WINNERS - Selección por porcentaje posicional {pct_value*100:.1f}%:")
                    print(f"  - Métrica: {RANKING_MODE} (desc)")
                    print(f"  - Total filas válidas: {total_valid}")
                    print(f"  - N calculado: {N} (floor({{pct_value}} × {{total_valid}}))")
                    print(f"  - Batmans seleccionados (primeras {N} filas): {num_before}")
                    print(f"  - Batmans tras deduplicación dia+DTE1/DTE2: {len(winner_indices)}")

                fwd_indices_to_process = winner_indices
                print(f"[FWD] Calculando forward para {len(fwd_indices_to_process)} batmans...")

                # Etiquetar WL_PRE y RANK_PRE (solo winners)
                if "WL_PRE" not in df.columns:
                    df["WL_PRE"] = None
                if "RANK_PRE" not in df.columns:
                    df["RANK_PRE"] = None

                for rank_pos, idx in enumerate(winner_indices, start=1):
                    df.at[idx, "WL_PRE"] = "WIN"
                    df.at[idx, "RANK_PRE"] = rank_pos

                print(f"[FWD] Etiquetas WL_PRE persistidas: {len(winner_indices)} WIN")

            elif FWD_ON_LOSERS:
                # Solo losers
                df_valid = df[df[RANKING_MODE].notna()].copy()
                total_valid = len(df_valid)
                N = max(1, int(np.floor(pct_value * total_valid)))

                if N == 0 or total_valid == 0:
                    print(f"\n[FWD] ⚠️ No hay filas válidas o N=0. Abortando FWD.")
                else:
                    # Losers: últimas N filas (peores valores bajos)
                    df_losers_selected = df_valid.iloc[-N:].copy()

                    # Deduplicar por dia + DTE1/DTE2 (keep="first" preserva orden)
                    num_before = len(df_losers_selected)
                    df_losers_unique = df_losers_selected.drop_duplicates(subset=["dia", "DTE1/DTE2"], keep="first")
                    loser_indices = list(df_losers_unique.index)

                    print(f"\n[FWD] LOSERS - Selección por porcentaje posicional {pct_value*100:.1f}%:")
                    print(f"  - Métrica: {RANKING_MODE} (desc)")
                    print(f"  - Total filas válidas: {total_valid}")
                    print(f"  - N calculado: {N} (floor({{pct_value}} × {{total_valid}}))")
                    print(f"  - Batmans seleccionados (últimas {N} filas): {num_before}")
                    print(f"  - Batmans tras deduplicación dia+DTE1/DTE2: {len(loser_indices)}")

                fwd_indices_to_process = loser_indices
                print(f"[FWD] Calculando forward para {len(fwd_indices_to_process)} batmans...")

                # Etiquetar WL_PRE y RANK_PRE (solo losers)
                if "WL_PRE" not in df.columns:
                    df["WL_PRE"] = None
                if "RANK_PRE" not in df.columns:
                    df["RANK_PRE"] = None

                for rank_pos, idx in enumerate(loser_indices, start=1):
                    df.at[idx, "WL_PRE"] = "LOS"
                    df.at[idx, "RANK_PRE"] = rank_pos

                print(f"[FWD] Etiquetas WL_PRE persistidas: {len(loser_indices)} LOS")

        # Eliminar duplicados manteniendo orden
        fwd_indices_to_process = list(dict.fromkeys(fwd_indices_to_process))

        print(f"Total a procesar: {len(fwd_indices_to_process)} batmans")
        print(f"Ventanas FWD: {FRAC_SUFFIXES} (% de DTE1)")
        print(f"{'='*70}\n")

        if fwd_indices_to_process:
            # Preparar argumentos para workers
            worker_args = []
            for idx in fwd_indices_to_process:
                row = df.loc[idx]
                row_dict = row.to_dict()

                args_tuple = (
                    idx,
                    row_dict,
                    files_sorted,
                    FORWARD_FRACS,
                    FRAC_SUFFIXES,
                    str(TZ_US),
                    str(TZ_ES),
                    NEAREST_MINUTE_TOLERANCE,
                    FWD_INTRADAY_TIMESTAMPS
                )
                worker_args.append(args_tuple)

            # Paralelización con ProcessPoolExecutor
            print(f"[FWD INTRADAY MEDIANA 14x30min] Lanzando {len(worker_args)} tareas en paralelo...")
            print(f"[FWD] Evaluando estructura en {len(FWD_INTRADAY_TIMESTAMPS)} timestamps por ventana: {FWD_INTRADAY_TIMESTAMPS}")
            completed_count = 0
            failed_count = 0
            total_fwd_tasks = len(worker_args)

            with ProcessPoolExecutor() as executor:
                futures = {executor.submit(_process_one_fwd_allantis, arg): arg[0] for arg in worker_args}

                for processed_idx, future in enumerate(as_completed(futures), start=1):
                    idx = futures[future]
                    try:
                        result = future.result(timeout=60)

                        if result['success']:
                            # Actualizar DataFrame con resultados FWD
                            for col_name, col_value in result['fwd_data'].items():
                                df.at[idx, col_name] = col_value
                            completed_count += 1
                        else:
                            failed_count += 1

                    except Exception as e:
                        print(f"[FWD] Error procesando índice {idx}: {e}")
                        failed_count += 1

                    if processed_idx % max(1, total_fwd_tasks // 10) == 0 or processed_idx == total_fwd_tasks:
                        progress_pct = 100 * processed_idx / total_fwd_tasks
                        print(f"[FWD] Progreso: {processed_idx}/{total_fwd_tasks} ({progress_pct:.1f}%)")

            print(f"\n[FWD INTRADAY MEDIANA 14x30min] Cálculo completado:")
            print(f"  ✓ Exitosos: {completed_count}")
            print(f"  ✗ Fallidos: {failed_count}")
            print(f"  Columnas generadas: PnL_fwd_pts_*_mediana, PnL_fwd_pct_*_mediana")
            print(f"{'='*70}\n")

    # Reordenación de columnas (adaptado para Allantis)
    if not df.empty:
        preferred_order = [
            # Columnas principales en orden específico
            "dia",
            "url",
            "net_credit",
            "DTE1/DTE2",
            "k_ul",                   # Strikes de Allantis (5 patas)
            "k_shorts",
            "k_ll",
            "k_call",
            "delta_total",
            "theta_total",
            # IVs de las 5 patas
            "iv_ul",
            "iv_shorts",
            "iv_ll",
            "iv_c_short",
            "iv_c_long",
            # Métricas Batman eliminadas (no aplican a BWB)
        ]

        # Separar columnas: prioritarias, fwd/root/pnl8000 (al final), y resto (medio)
        priority_cols = [c for c in preferred_order if c in df.columns]
        all_remaining = [c for c in df.columns if c not in set(preferred_order)]

        # Identificar columnas que deben ir al final (fwd, root_exp, pnl8000)
        last_cols = [c for c in all_remaining if any(x in c.lower() for x in ['fwd', 'root_exp', 'pnl8000'])]

        # Resto de columnas van en el medio
        middle_cols = [c for c in all_remaining if c not in last_cols]

        # Orden final: prioritarias + medio + últimas
        df = df[priority_cols + middle_cols + last_cols]

    print(f"\nTotal enlaces tras filtro: {len(df)}")
    print(df.head(SHOW_MAX).to_string(index=False))

    # Exportación (numérico limpio para ordenar bien en CSV)
    # RATIO_UEL_EARS eliminado - no aplica para Allantis
    num_cols = ["BQI_ABS","BQR_1000","PnLDV","EarL","EarR","UEL_inf_USD","Death valley","delta_total","theta_total","net_credit"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df.to_csv(batch_out_path, index=False, encoding="utf-8-sig", na_rep="")
    print(f"\nCSV guardado en: {batch_out_path} | Filas: {len(df)}")

    # ============================================================
    # PROCESO ADICIONAL: CSV COPIA CON net_credit_mediana (T+0) y net_credit_diff
    # ============================================================
    # PROPÓSITO: Calcular la mediana del net_credit en T+0 usando múltiples timestamps intradiarios
    #            para comparar con el net_credit original y detectar anomalías
    #
    # METODOLOGÍA:
    #   1. Para cada estructura Batman, evalúa net_credit en N timestamps del mismo día (T+0)
    #   2. Calcula la MEDIANA de estos net_credits (más robusta que la media ante outliers)
    #   3. Compara net_credit original vs net_credit_mediana → genera net_credit_diff (%)
    #   4. Filtra estructuras con net_credit_diff fuera de rango aceptable
    #
    # OPTIMIZACIÓN (VECTORIZADA):
    #   - Agrupa filas por día para cargar cada CSV UNA SOLA VEZ
    #   - Pre-carga todos los slices de timestamps necesarios en memoria
    #   - Reduce I/O de disco significativamente (factor 10x-100x más rápido)
    #   - Usa operaciones vectorizadas de NumPy/Pandas donde es posible
    # ============================================================
    print("\n" + "="*80)
    print("INICIANDO PROCESO ADICIONAL: Cálculo de net_credit_mediana (T+0) y net_credit_diff")
    print("="*80)

    # Crear copia del DataFrame
    df_copy = df.copy()

    # Filtrar solo filas con PnL_fwd_pct_01 no nulo
    col_filter = "PnL_fwd_pct_25_mediana" if "PnL_fwd_pct_25_mediana" in df_copy.columns else "PnL_fwd_pct_25"
    if col_filter not in df_copy.columns:
        print(f"[WARNING] Columna {col_filter} no encontrada. No se puede procesar CSV adicional.")
    else:
        df_copy = df_copy[df_copy[col_filter].notna()].copy()
        print(f"Filas tras filtro por {col_filter} no nulo: {len(df_copy)}")

        if len(df_copy) > 0:
            # Caché para DataFrames del día base
            _base_day_cache = {}

            def get_base_day_df_cached(dia_str):
                """
                Obtiene el DataFrame del día base desde caché o lo carga.
                """
                if not dia_str or dia_str in _base_day_cache:
                    return _base_day_cache.get(dia_str)

                try:
                    # Buscar archivo correspondiente a dia_str
                    matching_files = [f for f in files_sorted if dia_str in f.name]
                    if not matching_files:
                        print(f"  [WARNING] No se encontró archivo para fecha {dia_str}")
                        _base_day_cache[dia_str] = None
                        return None

                    base_path = matching_files[0]
                    base_df = pd.read_csv(base_path)
                    base_df = td_rescale_strike_only(base_df)
                    _base_day_cache[dia_str] = base_df
                    return base_df

                except Exception as e:
                    print(f"  [ERROR] Cargando archivo para {dia_str}: {e}")
                    _base_day_cache[dia_str] = None
                    return None

            # ============================================================
            # VERSIÓN VECTORIZADA: Cálculo de net_credit_mediana (T+0)
            # ============================================================
            # Agrupa filas por día para aprovechar caché de DataFrames y reducir I/O
            print(f"\nCalculando net_credit_mediana (T+0) para {len(df_copy)} filas... [VECTORIZADO]")

            # Agrupar por día para procesar en batch
            df_copy_with_idx = df_copy.reset_index(drop=False)
            grouped_by_day = df_copy_with_idx.groupby('dia', sort=False)

            # Inicializar arrays para resultados
            medianas = np.full(len(df_copy_with_idx), np.nan, dtype=float)
            n_valids = np.zeros(len(df_copy_with_idx), dtype=int)

            total_days = len(grouped_by_day)
            processed_rows = 0

            # Procesar cada día (todas sus filas juntas)
            for day_idx, (dia_str, day_group) in enumerate(grouped_by_day, 1):
                # Cargar DataFrame del día UNA SOLA VEZ
                base_df = get_base_day_df_cached(dia_str)

                if base_df is None or base_df.empty:
                    # Dejar NaN para todas las filas de este día
                    processed_rows += len(day_group)
                    print(f"  [{day_idx}/{total_days}] Día {dia_str}: {len(day_group)} filas - SIN DATOS")
                    continue

                # Pre-cargar TODOS los slices de timestamps necesarios para este día
                slice_cache = {}
                for ts_hhmm in FWD_INTRADAY_TIMESTAMPS:
                    slice_df, _ = get_slice_at_target_hhmm(base_df, ts_hhmm, NEAREST_MINUTE_TOLERANCE)
                    slice_cache[ts_hhmm] = slice_df

                # Procesar cada fila del día
                for row_idx, row in day_group.iterrows():
                    try:
                        # Extraer parámetros del Batman
                        exp1 = str(row.get("exp1", ""))
                        exp2 = str(row.get("exp2", ""))
                        k1 = float(row.get("k1", 0))
                        k2 = float(row.get("k2", 0))
                        k3 = float(row.get("k3", 0))
                        root_exp1_norm = normalize_root_value(row.get("root_exp1"))
                        root_exp2_norm = normalize_root_value(row.get("root_exp2"))

                        # Lista para acumular net_credit válidos
                        net_credit_list = []

                        # Evaluar en los timestamps intradía (usando caché)
                        for ts_hhmm in FWD_INTRADAY_TIMESTAMPS:
                            slice_df = slice_cache.get(ts_hhmm)
                            if slice_df is None or slice_df.empty:
                                continue

                            # Revalorizar Batman en este timestamp
                            bv_details = batman_value_from_df(
                                slice_df,
                                exp1, k1,
                                exp2, k2,
                                k3,
                                root1=root_exp1_norm,
                                root2=root_exp2_norm
                            )

                            if bv_details and np.isfinite(bv_details.get("net_credit", np.nan)):
                                net_credit_list.append(float(bv_details["net_credit"]))

                        # Calcular mediana si hay suficientes observaciones
                        n_valid = len(net_credit_list)
                        if n_valid >= 4:
                            mediana = float(np.median(net_credit_list))
                            medianas[row_idx] = round(mediana, 4)
                            n_valids[row_idx] = n_valid
                        else:
                            n_valids[row_idx] = n_valid

                    except Exception as e:
                        print(f"  [ERROR] Fila índice {row_idx}: {e}")
                        continue

                processed_rows += len(day_group)
                print(f"  [{day_idx}/{total_days}] Día {dia_str}: {len(day_group)} filas procesadas | Total: {processed_rows}/{len(df_copy_with_idx)}")

            # Insertar columnas net_credit_mediana y net_credit_mediana_n
            df_copy["net_credit_mediana"] = medianas
            df_copy["net_credit_mediana_n"] = n_valids

            # Calcular net_credit_diff (VECTORIZADO - sin apply)
            # Fórmula: 100 * (net_credit - net_credit_mediana) / abs(net_credit_mediana)
            nc = pd.to_numeric(df_copy["net_credit"], errors="coerce")
            nc_med = pd.to_numeric(df_copy["net_credit_mediana"], errors="coerce")

            # Calcular diferencia porcentual solo donde nc_med es válido y no cero
            mask_valid = nc_med.notna() & (nc_med != 0)
            df_copy["net_credit_diff"] = np.where(
                mask_valid,
                np.round(100.0 * (nc - nc_med) / np.abs(nc_med), 2),
                np.nan
            )

            # Asegurar tipos numéricos
            df_copy["net_credit"] = pd.to_numeric(df_copy["net_credit"], errors="coerce")
            df_copy["net_credit_mediana"] = pd.to_numeric(df_copy["net_credit_mediana"], errors="coerce")
            df_copy["net_credit_diff"] = pd.to_numeric(df_copy["net_credit_diff"], errors="coerce")
            df_copy["net_credit_mediana_n"] = pd.to_numeric(df_copy["net_credit_mediana_n"], errors="coerce")

            # Aplicar filtro de net_credit_diff si está habilitado
            if FILTER_NET_CREDIT_DIFF_ENABLED:
                filas_antes = len(df_copy)
                stats = get_filter_stats(df_copy, "net_credit_diff", NET_CREDIT_DIFF_MIN, NET_CREDIT_DIFF_MAX)
                df_copy = df_copy[
                    (df_copy["net_credit_diff"] >= NET_CREDIT_DIFF_MIN) &
                    (df_copy["net_credit_diff"] <= NET_CREDIT_DIFF_MAX)
                ].copy()
                filas_despues = len(df_copy)
                print(format_filter_log("NET_CREDIT_DIFF", NET_CREDIT_DIFF_MIN, NET_CREDIT_DIFF_MAX,
                                        filas_antes, filas_despues, stats, indent="                "))
            else:
                # Filtro desactivado, pero mostramos las estadísticas
                stats = get_filter_stats(df_copy, "net_credit_diff", NET_CREDIT_DIFF_MIN, NET_CREDIT_DIFF_MAX)
                print(format_filter_log("NET_CREDIT_DIFF [DESACTIVADO]", NET_CREDIT_DIFF_MIN, NET_CREDIT_DIFF_MAX,
                                        len(df_copy), len(df_copy), stats, indent="                "))

            # ============================================================
            # ANÁLISIS WINNERS/LOSERS (CSV COPIA FILTRADO) - ESTRATIFICADO POR WL_PRE
            # ============================================================
            winner_indices_copy = []
            loser_indices_copy = []

            if len(df_copy) > 0 and (FWD_ON_WINNERS or FWD_ON_LOSERS):
                print(f"\n{'='*80}")
                print("ANÁLISIS WINNERS/LOSERS - ESTRATIFICADO POR WL_PRE (CSV Copia Filtrado)")
                print(f"{'='*80}")

                # Parsear WL_TOP_PCT (nuevo parámetro separado del corte pre-FWD)
                wl_pct_value = parse_percentage(WL_TOP_PCT)
                if wl_pct_value is None:
                    print(f"[WINNERS/LOSERS] ⚠️ WL_TOP_PCT no está definido. Saltando análisis.")
                else:
                    print(f"[WINNERS/LOSERS] WL_TOP_PCT configurado: {wl_pct_value*100:.1f}%")
                    print(f"[WINNERS/LOSERS] Método: ESTRATIFICACIÓN por grupo WL_PRE (no re-ranking global)")
                    print(f"[WINNERS/LOSERS] Las etiquetas WIN/LOS provienen del corte pre-FWD original")

                    # Verificar que la columna WL_PRE existe en df_copy
                    if "WL_PRE" not in df_copy.columns:
                        print(f"[WINNERS/LOSERS] ⚠️ Columna WL_PRE no encontrada en df_copy. Saltando análisis.")
                        print(f"[WINNERS/LOSERS] Esto puede ocurrir si el dataset filtrado no contiene las etiquetas originales.")
                    else:
                        # Separar supervivientes por grupo (WIN/LOS)
                        df_winners_survivors = df_copy[df_copy["WL_PRE"] == "WIN"].copy()
                        df_losers_survivors = df_copy[df_copy["WL_PRE"] == "LOS"].copy()

                        n_win = len(df_winners_survivors)
                        n_los = len(df_losers_survivors)

                        print(f"\n[SUPERVIVIENTES POST-FILTROS]")
                        print(f"  - WINNERS (WL_PRE='WIN'): {n_win}")
                        print(f"  - LOSERS (WL_PRE='LOS'): {n_los}")
                        print(f"  - Total supervivientes: {n_win + n_los}")

                        # Calcular k_win y k_los (cuántos tomar de cada grupo)
                        k_win = max(0, int(np.floor(wl_pct_value * n_win))) if n_win > 0 else 0
                        k_los = max(0, int(np.floor(wl_pct_value * n_los))) if n_los > 0 else 0

                        print(f"\n[ESTRATIFICACIÓN {wl_pct_value*100:.1f}%]")
                        print(f"  - k_win = floor({wl_pct_value} × {n_win}) = {k_win}")
                        print(f"  - k_los = floor({wl_pct_value} × {n_los}) = {k_los}")

                        # Seleccionar dentro de cada estrato usando RANK_PRE (orden original)
                        if FWD_ON_WINNERS and k_win > 0:
                            # Ordenar por RANK_PRE (los mejores tienen RANK_PRE más bajo)
                            df_winners_sorted = df_winners_survivors.sort_values(by="RANK_PRE", ascending=True)
                            # Tomar los k_win mejores
                            df_winners_selected = df_winners_sorted.iloc[:k_win].copy()
                            winner_indices_copy = list(df_winners_selected.index)

                            print(f"\n[WINNERS] Selección estratificada:")
                            print(f"  - Supervivientes WIN: {n_win}")
                            print(f"  - Tomados (top {wl_pct_value*100:.1f}% por RANK_PRE): {len(winner_indices_copy)}")
                            print(f"  - Criterio: RANK_PRE ascendente (mejores rankings primero)")
                        elif FWD_ON_WINNERS and k_win == 0:
                            print(f"\n[WINNERS] ⚠️ k_win = 0 (no hay suficientes supervivientes o WL_TOP_PCT muy bajo)")

                        if FWD_ON_LOSERS and k_los > 0:
                            # Ordenar por RANK_PRE (los peores dentro de LOS tienen RANK_PRE más alto)
                            df_losers_sorted = df_losers_survivors.sort_values(by="RANK_PRE", ascending=False)
                            # Tomar los k_los peores (mayor RANK_PRE en el estrato LOS)
                            df_losers_selected = df_losers_sorted.iloc[:k_los].copy()
                            loser_indices_copy = list(df_losers_selected.index)

                            print(f"\n[LOSERS] Selección estratificada:")
                            print(f"  - Supervivientes LOS: {n_los}")
                            print(f"  - Tomados (top {wl_pct_value*100:.1f}% por RANK_PRE inverso): {len(loser_indices_copy)}")
                            print(f"  - Criterio: RANK_PRE descendente (peores del grupo LOS)")
                        elif FWD_ON_LOSERS and k_los == 0:
                            print(f"\n[LOSERS] ⚠️ k_los = 0 (no hay suficientes supervivientes o WL_TOP_PCT muy bajo)")

                        print(f"\n[TOTAL PARA ANÁLISIS W/L] Winners: {len(winner_indices_copy)} | Losers: {len(loser_indices_copy)}")
                        print(f"[NOTA] Estos grupos mantienen sus etiquetas originales WIN/LOS del corte pre-FWD")

                # Calcular estadísticas de promedios Winners vs Losers
                pnl_cols_mediana_sorted = sorted([col for col in df_copy.columns if col.startswith("PnL_fwd_pct_") and col.endswith("_mediana")])
                if len(pnl_cols_mediana_sorted) > 0 and (len(winner_indices_copy) > 0 or len(loser_indices_copy) > 0):
                    print("\n" + "-"*80)
                    print("ESTADÍSTICAS WINNERS vs LOSERS (columnas _mediana)")
                    print("-"*80)

                    for col in pnl_cols_mediana_sorted:
                        sufijo = col.replace("PnL_fwd_pct_", "").replace("_mediana", "")
                        pnl_data = pd.to_numeric(df_copy[col], errors='coerce')

                        if FWD_ON_WINNERS and len(winner_indices_copy) > 0:
                            winners_data = pnl_data[df_copy.index.isin(winner_indices_copy)]
                            winners_data_valid = winners_data.dropna()
                            if len(winners_data_valid) > 0:
                                print(f"\nVentana W{sufijo} - WINNERS (n={len(winners_data_valid)}):")
                                print(f"  • Promedio: {winners_data_valid.mean():.2f}%")
                                print(f"  • Mediana:  {winners_data_valid.median():.2f}%")
                                print(f"  • Std Dev:  {winners_data_valid.std():.2f}%")
                                print(f"  • Min/Max:  {winners_data_valid.min():.2f}% / {winners_data_valid.max():.2f}%")

                        if FWD_ON_LOSERS and len(loser_indices_copy) > 0:
                            losers_data = pnl_data[df_copy.index.isin(loser_indices_copy)]
                            losers_data_valid = losers_data.dropna()
                            if len(losers_data_valid) > 0:
                                print(f"\nVentana W{sufijo} - LOSERS (n={len(losers_data_valid)}):")
                                print(f"  • Promedio: {losers_data_valid.mean():.2f}%")
                                print(f"  • Mediana:  {losers_data_valid.median():.2f}%")
                                print(f"  • Std Dev:  {losers_data_valid.std():.2f}%")
                                print(f"  • Min/Max:  {losers_data_valid.min():.2f}% / {losers_data_valid.max():.2f}%")

                        if FWD_ON_WINNERS and FWD_ON_LOSERS and len(winner_indices_copy) > 0 and len(loser_indices_copy) > 0:
                            winners_data_valid = pnl_data[df_copy.index.isin(winner_indices_copy)].dropna()
                            losers_data_valid = pnl_data[df_copy.index.isin(loser_indices_copy)].dropna()
                            if len(winners_data_valid) > 0 and len(losers_data_valid) > 0:
                                diff_promedio = winners_data_valid.mean() - losers_data_valid.mean()
                                print(f"\n  → DIFERENCIA (Winners - Losers) Promedio: {diff_promedio:+.2f}%")

                print(f"{'='*80}\n")

            # ============================================================
            # ANÁLISIS DE CORRELACIONES PEARSON - SOLO WINNERS (CSV COPIA FILTRADO)
            # ============================================================
            # Las correlaciones se calculan ÚNICAMENTE sobre los WINNERS seleccionados
            # para ver qué métricas predicen mejor el rendimiento FWD de las mejores estructuras
            if len(df_copy) > 0 and FWD_ON_WINNERS and len(winner_indices_copy) > 0:
                print(f"\n{'='*80}")
                print("ANÁLISIS DE CORRELACIONES PEARSON - SOLO WINNERS")
                print(f"{'='*80}")

                # Filtrar solo los winners seleccionados
                df_winners_only = df_copy[df_copy.index.isin(winner_indices_copy)].copy()
                print(f"Filas WINNERS para correlación: {len(df_winners_only)}")

                try:
                    # Métricas a correlacionar (FILTRADAS - Solo las 8 variables especificadas)
                    metrics_to_correlate = [
                        'BQI_ABS',
                        'FF_ATM',
                        'FF_BAT',
                        'RATIO_BATMAN',
                        'delta_total',
                        'theta_total',
                        'net_credit',
                        'PnLDV'
                    ]

                    # Verificar qué métricas están disponibles
                    available_metrics = [m for m in metrics_to_correlate if m in df_winners_only.columns]

                    # Ventanas de PnL_fwd_pct_*_mediana (W01, W05, W15, W25, W50)
                    pnl_mediana_cols = [
                        'PnL_fwd_pct_01_mediana',
                        'PnL_fwd_pct_05_mediana',
                        'PnL_fwd_pct_15_mediana',
                        'PnL_fwd_pct_25_mediana',
                        'PnL_fwd_pct_50_mediana'
                    ]

                    # Verificar qué columnas de PnL existen
                    available_pnl_cols = [col for col in pnl_mediana_cols if col in df_winners_only.columns]

                    if len(available_pnl_cols) == 0:
                        print("[WARNING] No se encontraron columnas PnL_fwd_pct_*_mediana")
                    elif len(available_metrics) == 0:
                        print("[WARNING] No se encontraron métricas para correlacionar")
                    else:
                        print(f"\nColumnas PnL disponibles: {len(available_pnl_cols)}")
                        print(f"Métricas disponibles: {len(available_metrics)}")
                        print("\nCorrelaciones Pearson entre PnL_fwd_pct_*_mediana y métricas (SOLO WINNERS):\n")

                        for pnl_col in available_pnl_cols:
                            # Extraer sufijo (01, 05, 15, 25, 50)
                            sufijo = pnl_col.replace('PnL_fwd_pct_', '').replace('_mediana', '')

                            # Convertir columna PnL a numérica
                            pnl_data = pd.to_numeric(df_winners_only[pnl_col], errors='coerce')
                            valid_pnl_mask = pnl_data.notna()

                            if valid_pnl_mask.sum() < 2:
                                print(f"Ventana W{sufijo}: Datos insuficientes (n={valid_pnl_mask.sum()})")
                                continue

                            print(f"Ventana W{sufijo} (n={valid_pnl_mask.sum()}):")

                            for metric in available_metrics:
                                # Convertir métrica a numérica
                                metric_data = pd.to_numeric(df_winners_only[metric], errors='coerce')

                                # Filtrar solo filas donde ambos valores son válidos
                                both_valid = valid_pnl_mask & metric_data.notna()

                                if both_valid.sum() >= 2:
                                    corr = pnl_data[both_valid].corr(metric_data[both_valid])
                                    print(f"  • {metric:22s}: {corr:+.4f}")

                            print()

                except Exception as e:
                    print(f"✗ Error calculando correlaciones: {e}")
                    import traceback
                    traceback.print_exc()

                print(f"{'='*80}\n")
            elif len(df_copy) > 0 and (not FWD_ON_WINNERS or len(winner_indices_copy) == 0):
                print(f"\n{'='*80}")
                print("ANÁLISIS DE CORRELACIONES PEARSON - OMITIDO")
                print(f"{'='*80}")
                print("[INFO] No hay WINNERS seleccionados para calcular correlaciones.")
                print("[INFO] Las correlaciones solo se calculan sobre WINNERS.")
                print(f"{'='*80}\n")

            # ============================================================
            # REORDENAMIENTO DE COLUMNAS según especificación
            # ============================================================
            # Definir orden deseado (adaptado para Allantis)
            columnas_ordenadas = [
                # Columnas principales en orden específico
                "dia",
                "url",
                "net_credit",
                "DTE1/DTE2",
                "k_ul",                   # Strikes de Allantis (5 patas)
                "k_shorts",
                "k_ll",
                "k_call",
                "delta_total",
                "theta_total",
                # Métricas de Batman/BQI eliminadas - no aplican a Allantis
                # "Death valley", "PnLDV", "EarL", "EarR", "UEL_inf_USD" eliminados
            ]

            # Agregar columnas que existen pero no están en la lista (el resto...)
            columnas_existentes = set(df_copy.columns)
            columnas_ya_ordenadas = set(columnas_ordenadas)
            columnas_restantes = sorted(columnas_existentes - columnas_ya_ordenadas)

            # Separar columnas restantes: fwd/root/pnl8000 (al final) vs resto (medio)
            columnas_final = [c for c in columnas_restantes if any(x in c.lower() for x in ['fwd', 'root_exp', 'pnl8000'])]
            columnas_medio = [c for c in columnas_restantes if c not in columnas_final]

            # Construir lista final: ordenadas + medio + últimas
            columnas_finales = [c for c in columnas_ordenadas if c in columnas_existentes] + columnas_medio + columnas_final

            # Aplicar reordenamiento
            df_copy = df_copy[columnas_finales]

            # Generar nombre para CSV copia
            batch_out_name_copy = batch_out_name.replace(".csv", "_T0_mediana.csv")
            batch_out_path_copy = DESKTOP / safe_filename(batch_out_name_copy)

            # Guardar CSV copia
            df_copy.to_csv(batch_out_path_copy, index=False, encoding="utf-8-sig", na_rep="")
            print(f"\n[CSV COPIA] Guardado en: {batch_out_path_copy}")
            print(f"[CSV COPIA] Filas: {len(df_copy)}")
            print(f"[CSV COPIA] Columnas nuevas: net_credit_mediana, net_credit_mediana_n, net_credit_diff")

            # Mostrar estadísticas
            valid_medianas = df_copy["net_credit_mediana"].notna().sum()
            print(f"[STATS] Filas con net_credit_mediana válida: {valid_medianas}/{len(df_copy)}")
            if valid_medianas > 0:
                avg_n = df_copy[df_copy["net_credit_mediana"].notna()]["net_credit_mediana_n"].mean()
                print(f"[STATS] Promedio de timestamps válidos por fila: {avg_n:.1f}")

            # ============================================================
            # GRÁFICO: Promedio de PnL_fwd_pct_*_mediana por periodo W
            # ============================================================
            print("\n" + "="*80)
            print("GENERANDO GRÁFICO: Promedio PnL_fwd_pct por periodo W")
            print("="*80)

            # Identificar columnas PnL_fwd_pct_*_mediana (5 periodos W)
            pnl_cols = [col for col in df_copy.columns if col.startswith("PnL_fwd_pct_") and col.endswith("_mediana")]
            pnl_cols_sorted = sorted(pnl_cols)  # Ordenar para mantener secuencia W1, W2, W3, W4, W5

            if len(pnl_cols_sorted) >= 1 and len(df_copy) > 0:
                # Calcular promedio para cada periodo W
                promedios = []
                labels_w = []

                for col in pnl_cols_sorted:
                    # Extraer sufijo (ej: "01", "05", "15", "25", "50")
                    sufijo = col.replace("PnL_fwd_pct_", "").replace("_mediana", "")
                    # Calcular promedio (ignorando NaN)
                    promedio = df_copy[col].mean()

                    if np.isfinite(promedio):
                        promedios.append(promedio)
                        labels_w.append(f"W{sufijo}")
                    else:
                        print(f"  [WARNING] Columna {col} no tiene datos válidos para promediar")

                # Generar gráfico si hay datos válidos
                if len(promedios) >= 1:
                    fig, ax = plt.subplots(figsize=(10, 6))

                    # Gráfico lineal
                    ax.plot(range(len(promedios)), promedios, marker='o', linewidth=2, markersize=8, color='steelblue')

                    # Configuración del gráfico
                    ax.set_xlabel("Periodo W (Fracción del DTE1)", fontsize=12, fontweight='bold')
                    ax.set_ylabel("Promedio PnL_fwd_pct (%)", fontsize=12, fontweight='bold')
                    ax.set_title("Promedio de PnL_fwd_pct_mediana por Periodo W", fontsize=14, fontweight='bold')
                    ax.set_xticks(range(len(labels_w)))
                    ax.set_xticklabels(labels_w, fontsize=10)
                    ax.grid(True, alpha=0.3, linestyle='--')
                    ax.axhline(y=0, color='red', linestyle='--', linewidth=1, alpha=0.7)

                    # Ajustar layout
                    plt.tight_layout()

                    # Guardar gráfico
                    plot_name = batch_out_name.replace(".csv", "_PnL_pct_W_promedio.png")
                    plot_path = DESKTOP / safe_filename(plot_name)
                    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
                    plt.close()

                    print(f"[GRÁFICO] Guardado en: {plot_path}")
                    print(f"[GRÁFICO] Periodos graficados: {len(promedios)}")
                    print(f"[GRÁFICO] Valores: {', '.join([f'{labels_w[i]}={promedios[i]:.2f}%' for i in range(len(promedios))])}")
                else:
                    print("[INFO] No hay suficientes datos válidos para generar el gráfico")
            else:
                print("[INFO] No se encontraron columnas PnL_fwd_pct_*_mediana para graficar")

            print("="*80 + "\n")

            # ============================================================
            # GRÁFICO: Promedio de PnL_fwd_pts_*_mediana por periodo W
            # ============================================================
            print("\n" + "="*80)
            print("GENERANDO GRÁFICO: Promedio PnL_fwd_pts por periodo W")
            print("="*80)

            # Identificar columnas PnL_fwd_pts_*_mediana (5 periodos W)
            pnl_pts_cols = [col for col in df_copy.columns if col.startswith("PnL_fwd_pts_") and col.endswith("_mediana")]
            pnl_pts_cols_sorted = sorted(pnl_pts_cols)  # Ordenar para mantener secuencia W1, W2, W3, W4, W5

            if len(pnl_pts_cols_sorted) >= 1 and len(df_copy) > 0:
                # Calcular promedio para cada periodo W
                promedios_pts = []
                labels_w_pts = []

                for col in pnl_pts_cols_sorted:
                    # Extraer sufijo (ej: "01", "05", "15", "25", "50")
                    sufijo = col.replace("PnL_fwd_pts_", "").replace("_mediana", "")
                    # Calcular promedio (ignorando NaN)
                    promedio = df_copy[col].mean()

                    if np.isfinite(promedio):
                        promedios_pts.append(promedio)
                        labels_w_pts.append(f"W{sufijo}")
                    else:
                        print(f"  [WARNING] Columna {col} no tiene datos válidos para promediar")

                # Generar gráfico si hay datos válidos
                if len(promedios_pts) >= 1:
                    fig, ax = plt.subplots(figsize=(10, 6))

                    # Gráfico lineal
                    ax.plot(range(len(promedios_pts)), promedios_pts, marker='o', linewidth=2, markersize=8, color='darkorange')

                    # Configuración del gráfico
                    ax.set_xlabel("Periodo W (Fracción del DTE1)", fontsize=12, fontweight='bold')
                    ax.set_ylabel("Promedio PnL_fwd_pts (puntos)", fontsize=12, fontweight='bold')
                    ax.set_title("Promedio de PnL_fwd_pts_mediana por Periodo W", fontsize=14, fontweight='bold')
                    ax.set_xticks(range(len(labels_w_pts)))
                    ax.set_xticklabels(labels_w_pts, fontsize=10)
                    ax.grid(True, alpha=0.3, linestyle='--')
                    ax.axhline(y=0, color='red', linestyle='--', linewidth=1, alpha=0.7)

                    # Ajustar layout
                    plt.tight_layout()

                    # Guardar gráfico
                    plot_name_pts = batch_out_name.replace(".csv", "_PnL_pts_W_promedio.png")
                    plot_path_pts = DESKTOP / safe_filename(plot_name_pts)
                    plt.savefig(plot_path_pts, dpi=150, bbox_inches='tight')
                    plt.close()

                    print(f"[GRÁFICO] Guardado en: {plot_path_pts}")
                    print(f"[GRÁFICO] Periodos graficados: {len(promedios_pts)}")
                    print(f"[GRÁFICO] Valores: {', '.join([f'{labels_w_pts[i]}={promedios_pts[i]:.2f} pts' for i in range(len(promedios_pts))])}")
                else:
                    print("[INFO] No hay suficientes datos válidos para generar el gráfico")
            else:
                print("[INFO] No se encontraron columnas PnL_fwd_pts_*_mediana para graficar")

            print("="*80 + "\n")

            # ============================================================
            # PREPARAR COLUMNAS PARA GRÁFICOS FILTRADOS
            # ============================================================
            # Identificar columnas PnL para usar en gráficos filtrados
            pnl_pct_cols = [col for col in df_copy.columns if col.startswith("PnL_fwd_pct_") and col.endswith("_mediana")]
            pnl_pts_cols = [col for col in df_copy.columns if col.startswith("PnL_fwd_pts_") and col.endswith("_mediana")]

            # ============================================================
            # GRÁFICOS 4A/4B: FILTRADOS POR BQI_ABS > FILTER_BQI_ABS_THRESHOLD
            # ============================================================
            if "BQI_ABS" in df_copy.columns:
                df_bqi_filtered = df_copy[pd.to_numeric(df_copy["BQI_ABS"], errors='coerce') > FILTER_BQI_ABS_THRESHOLD]
                n_bqi = len(df_bqi_filtered)

                if n_bqi > 0:
                    print(f"\n[GRÁFICOS 4A/4B] Filtrado por BQI_ABS > {FILTER_BQI_ABS_THRESHOLD} (n={n_bqi}/{len(df_copy)})")

                    # GRÁFICO 4A: PnL_fwd_pct
                    if len(pnl_pct_cols) > 0:
                        promedios_bqi_pct = []
                        labels_bqi_pct = []

                        for col in pnl_pct_cols:
                            sufijo = col.replace("PnL_fwd_pct_", "").replace("_mediana", "")
                            valid_data = pd.to_numeric(df_bqi_filtered[col], errors="coerce").dropna()
                            if len(valid_data) > 0:
                                promedio = valid_data.mean()
                                promedios_bqi_pct.append(promedio)
                                labels_bqi_pct.append(f"W{sufijo}")

                        if len(promedios_bqi_pct) > 0:
                            fig, ax = plt.subplots(figsize=(10, 6))
                            ax.plot(labels_bqi_pct, promedios_bqi_pct, 'o-', linewidth=2.5, markersize=10, color='#e67e22', label='Promedio PnL %')
                            ax.axhline(y=0, color='red', linestyle='--', linewidth=1.5, alpha=0.7)
                            ax.set_xlabel('Ventana Forward (% de DTE)', fontsize=12, fontweight='bold')
                            ax.set_ylabel('Promedio PnL (%)', fontsize=12, fontweight='bold')
                            ax.set_title(f'Promedio PnL_fwd_pct_mediana por Periodo W\n(Filtrado: BQI_ABS > {FILTER_BQI_ABS_THRESHOLD}, n={n_bqi})', fontsize=13, fontweight='bold')
                            ax.grid(True, alpha=0.3)
                            ax.legend(fontsize=10)
                            plt.tight_layout()

                            plot_name_bqi_pct = batch_out_name.replace(".csv", f"_T0_BQI_ABS_gt{FILTER_BQI_ABS_THRESHOLD}_pct.png")
                            plot_path_bqi_pct = DESKTOP / plot_name_bqi_pct
                            plt.savefig(plot_path_bqi_pct, dpi=150, bbox_inches='tight')
                            plt.close()

                            print(f"  ✓ [4A] Guardado: {plot_path_bqi_pct}")
                            print(f"  Valores: {', '.join([f'{labels_bqi_pct[i]}={promedios_bqi_pct[i]:.2f}%' for i in range(len(promedios_bqi_pct))])}")

                    # GRÁFICO 4B: PnL_fwd_pts
                    if len(pnl_pts_cols) > 0:
                        promedios_bqi_pts = []
                        labels_bqi_pts = []

                        for col in pnl_pts_cols:
                            sufijo = col.replace("PnL_fwd_pts_", "").replace("_mediana", "")
                            valid_data = pd.to_numeric(df_bqi_filtered[col], errors="coerce").dropna()
                            if len(valid_data) > 0:
                                promedio = valid_data.mean()
                                promedios_bqi_pts.append(promedio)
                                labels_bqi_pts.append(f"W{sufijo}")

                        if len(promedios_bqi_pts) > 0:
                            fig, ax = plt.subplots(figsize=(10, 6))
                            ax.plot(labels_bqi_pts, promedios_bqi_pts, 's-', linewidth=2.5, markersize=10, color='#e67e22', label='Promedio PnL pts')
                            ax.axhline(y=0, color='red', linestyle='--', linewidth=1.5, alpha=0.7)
                            ax.set_xlabel('Ventana Forward (% de DTE)', fontsize=12, fontweight='bold')
                            ax.set_ylabel('Promedio PnL (puntos)', fontsize=12, fontweight='bold')
                            ax.set_title(f'Promedio PnL_fwd_pts_mediana por Periodo W\n(Filtrado: BQI_ABS > {FILTER_BQI_ABS_THRESHOLD}, n={n_bqi})', fontsize=13, fontweight='bold')
                            ax.grid(True, alpha=0.3)
                            ax.legend(fontsize=10)
                            plt.tight_layout()

                            plot_name_bqi_pts = batch_out_name.replace(".csv", f"_T0_BQI_ABS_gt{FILTER_BQI_ABS_THRESHOLD}_pts.png")
                            plot_path_bqi_pts = DESKTOP / plot_name_bqi_pts
                            plt.savefig(plot_path_bqi_pts, dpi=150, bbox_inches='tight')
                            plt.close()

                            print(f"  ✓ [4B] Guardado: {plot_path_bqi_pts}")
                            print(f"  Valores: {', '.join([f'{labels_bqi_pts[i]}={promedios_bqi_pts[i]:.2f}pts' for i in range(len(promedios_bqi_pts))])}")
                else:
                    print(f"\n[GRÁFICOS 4A/4B] ⚠️ No hay datos con BQI_ABS > {FILTER_BQI_ABS_THRESHOLD}")
            else:
                print(f"\n[GRÁFICOS 4A/4B] ⚠️ Columna 'BQI_ABS' no encontrada")

            # ============================================================
            # GRÁFICOS 6A/6B: FILTRADOS POR FF_BAT > FILTER_FF_BAT_THRESHOLD
            # ============================================================
            if "FF_BAT" in df_copy.columns:
                df_ff_bat_filtered = df_copy[pd.to_numeric(df_copy["FF_BAT"], errors='coerce') > FILTER_FF_BAT_THRESHOLD]
                n_ff_bat = len(df_ff_bat_filtered)

                if n_ff_bat > 0:
                    print(f"\n[GRÁFICOS 6A/6B] Filtrado por FF_BAT > {FILTER_FF_BAT_THRESHOLD} (n={n_ff_bat}/{len(df_copy)})")

                    # GRÁFICO 6A: PnL_fwd_pct
                    if len(pnl_pct_cols) > 0:
                        promedios_ff_bat_pct = []
                        labels_ff_bat_pct = []

                        for col in pnl_pct_cols:
                            sufijo = col.replace("PnL_fwd_pct_", "").replace("_mediana", "")
                            valid_data = pd.to_numeric(df_ff_bat_filtered[col], errors="coerce").dropna()
                            if len(valid_data) > 0:
                                promedio = valid_data.mean()
                                promedios_ff_bat_pct.append(promedio)
                                labels_ff_bat_pct.append(f"W{sufijo}")

                        if len(promedios_ff_bat_pct) > 0:
                            fig, ax = plt.subplots(figsize=(10, 6))
                            ax.plot(labels_ff_bat_pct, promedios_ff_bat_pct, 'o-', linewidth=2.5, markersize=10, color='#e74c3c', label='Promedio PnL %')
                            ax.axhline(y=0, color='red', linestyle='--', linewidth=1.5, alpha=0.7)
                            ax.set_xlabel('Ventana Forward (% de DTE)', fontsize=12, fontweight='bold')
                            ax.set_ylabel('Promedio PnL (%)', fontsize=12, fontweight='bold')
                            ax.set_title(f'Promedio PnL_fwd_pct_mediana por Periodo W\n(Filtrado: FF_BAT > {FILTER_FF_BAT_THRESHOLD}, n={n_ff_bat})', fontsize=13, fontweight='bold')
                            ax.grid(True, alpha=0.3)
                            ax.legend(fontsize=10)
                            plt.tight_layout()

                            plot_name_ff_bat_pct = batch_out_name.replace(".csv", f"_T0_FF_BAT_gt{FILTER_FF_BAT_THRESHOLD}_pct.png")
                            plot_path_ff_bat_pct = DESKTOP / plot_name_ff_bat_pct
                            plt.savefig(plot_path_ff_bat_pct, dpi=150, bbox_inches='tight')
                            plt.close()

                            print(f"  ✓ [6A] Guardado: {plot_path_ff_bat_pct}")
                            print(f"  Valores: {', '.join([f'{labels_ff_bat_pct[i]}={promedios_ff_bat_pct[i]:.2f}%' for i in range(len(promedios_ff_bat_pct))])}")

                    # GRÁFICO 6B: PnL_fwd_pts
                    if len(pnl_pts_cols) > 0:
                        promedios_ff_bat_pts = []
                        labels_ff_bat_pts = []

                        for col in pnl_pts_cols:
                            sufijo = col.replace("PnL_fwd_pts_", "").replace("_mediana", "")
                            valid_data = pd.to_numeric(df_ff_bat_filtered[col], errors="coerce").dropna()
                            if len(valid_data) > 0:
                                promedio = valid_data.mean()
                                promedios_ff_bat_pts.append(promedio)
                                labels_ff_bat_pts.append(f"W{sufijo}")

                        if len(promedios_ff_bat_pts) > 0:
                            fig, ax = plt.subplots(figsize=(10, 6))
                            ax.plot(labels_ff_bat_pts, promedios_ff_bat_pts, 's-', linewidth=2.5, markersize=10, color='#e74c3c', label='Promedio PnL pts')
                            ax.axhline(y=0, color='red', linestyle='--', linewidth=1.5, alpha=0.7)
                            ax.set_xlabel('Ventana Forward (% de DTE)', fontsize=12, fontweight='bold')
                            ax.set_ylabel('Promedio PnL (puntos)', fontsize=12, fontweight='bold')
                            ax.set_title(f'Promedio PnL_fwd_pts_mediana por Periodo W\n(Filtrado: FF_BAT > {FILTER_FF_BAT_THRESHOLD}, n={n_ff_bat})', fontsize=13, fontweight='bold')
                            ax.grid(True, alpha=0.3)
                            ax.legend(fontsize=10)
                            plt.tight_layout()

                            plot_name_ff_bat_pts = batch_out_name.replace(".csv", f"_T0_FF_BAT_gt{FILTER_FF_BAT_THRESHOLD}_pts.png")
                            plot_path_ff_bat_pts = DESKTOP / plot_name_ff_bat_pts
                            plt.savefig(plot_path_ff_bat_pts, dpi=150, bbox_inches='tight')
                            plt.close()

                            print(f"  ✓ [6B] Guardado: {plot_path_ff_bat_pts}")
                            print(f"  Valores: {', '.join([f'{labels_ff_bat_pts[i]}={promedios_ff_bat_pts[i]:.2f}pts' for i in range(len(promedios_ff_bat_pts))])}")
                else:
                    print(f"\n[GRÁFICOS 6A/6B] ⚠️ No hay datos con FF_BAT > {FILTER_FF_BAT_THRESHOLD}")
            else:
                print(f"\n[GRÁFICOS 6A/6B] ⚠️ Columna 'FF_BAT' no encontrada")

            # ============================================================
            # GRÁFICOS COMPARATIVOS: WINNERS vs LOSERS (usando columnas _mediana)
            # ============================================================
            if (FWD_ON_WINNERS or FWD_ON_LOSERS) and (len(winner_indices_copy) > 0 or len(loser_indices_copy) > 0):
                print("\n" + "="*80)
                print("GENERANDO GRÁFICOS COMPARATIVOS: WINNERS vs LOSERS")
                print("="*80)

                # Identificar columnas PnL_fwd_pct_*_mediana
                pnl_cols_mediana = [col for col in df_copy.columns if col.startswith("PnL_fwd_pct_") and col.endswith("_mediana")]
                pnl_cols_mediana_sorted = sorted(pnl_cols_mediana)

                if len(pnl_cols_mediana_sorted) >= 1:
                    # Crear máscaras para winners y losers
                    winners_mask = df_copy.index.isin(winner_indices_copy) if FWD_ON_WINNERS else pd.Series([False]*len(df_copy), index=df_copy.index)
                    losers_mask = df_copy.index.isin(loser_indices_copy) if FWD_ON_LOSERS else pd.Series([False]*len(df_copy), index=df_copy.index)

                    # Calcular promedios para cada ventana
                    ventanas_x = []
                    winners_means = []
                    losers_means = []

                    for col in pnl_cols_mediana_sorted:
                        # Extraer sufijo (01, 05, 15, 25, 50)
                        sufijo = col.replace("PnL_fwd_pct_", "").replace("_mediana", "")
                        ventanas_x.append(f"W{sufijo}")

                        # Convertir a numérico
                        pnl_data = pd.to_numeric(df_copy[col], errors='coerce')
                        valid_mask = pnl_data.notna()

                        # Calcular promedio para winners
                        if FWD_ON_WINNERS:
                            winners_pnl = pnl_data[winners_mask & valid_mask]
                            winners_means.append(winners_pnl.mean() if not winners_pnl.empty else None)
                        else:
                            winners_means.append(None)

                        # Calcular promedio para losers
                        if FWD_ON_LOSERS:
                            losers_pnl = pnl_data[losers_mask & valid_mask]
                            losers_means.append(losers_pnl.mean() if not losers_pnl.empty else None)
                        else:
                            losers_means.append(None)

                    # Generar gráfico comparativo
                    fig, ax = plt.subplots(figsize=(12, 7))

                    # Plot winners (verde)
                    if FWD_ON_WINNERS and any(m is not None for m in winners_means):
                        x_winners = [i for i, m in enumerate(winners_means) if m is not None]
                        y_winners = [m for m in winners_means if m is not None]
                        if x_winners:
                            ax.plot(x_winners, y_winners, marker='o', linewidth=2.5, markersize=8,
                                   color='#2ecc71', label=f'Winners (n={len(winner_indices_copy)})', alpha=0.9)

                    # Plot losers (rojo)
                    if FWD_ON_LOSERS and any(m is not None for m in losers_means):
                        x_losers = [i for i, m in enumerate(losers_means) if m is not None]
                        y_losers = [m for m in losers_means if m is not None]
                        if x_losers:
                            ax.plot(x_losers, y_losers, marker='s', linewidth=2.5, markersize=8,
                                   color='#e74c3c', label=f'Losers (n={len(loser_indices_copy)})', alpha=0.9)

                    # Configuración del gráfico
                    ax.set_xlabel("Periodo W (Fracción del DTE1)", fontsize=12, fontweight='bold')
                    ax.set_ylabel("Promedio PnL_fwd_pct_mediana (%)", fontsize=12, fontweight='bold')
                    ax.set_title(f"Comparativa Winners vs Losers - PnL_fwd_pct_mediana\n(Estratificado por WL_PRE, WL_TOP_PCT={wl_pct_value*100:.1f}%)",
                                fontsize=14, fontweight='bold')
                    ax.set_xticks(range(len(ventanas_x)))
                    ax.set_xticklabels(ventanas_x, fontsize=10)
                    ax.grid(True, alpha=0.3, linestyle='--')
                    ax.axhline(y=0, color='gray', linestyle='--', linewidth=1, alpha=0.7)
                    ax.legend(fontsize=11, loc='best')

                    # Ajustar layout
                    plt.tight_layout()

                    # Guardar gráfico
                    plot_name_wl = batch_out_name.replace(".csv", "_Winners_vs_Losers_pct.png")
                    plot_path_wl = DESKTOP / safe_filename(plot_name_wl)
                    plt.savefig(plot_path_wl, dpi=150, bbox_inches='tight')
                    plt.close()

                    print(f"[GRÁFICO W/L] Guardado en: {plot_path_wl}")
                    print(f"[GRÁFICO W/L] Periodos: {len(ventanas_x)}")
                    if FWD_ON_WINNERS:
                        print(f"[GRÁFICO W/L] Winners (n={len(winner_indices_copy)}): {', '.join([f'{ventanas_x[i]}={winners_means[i]:.2f}%' if winners_means[i] is not None else f'{ventanas_x[i]}=N/A' for i in range(len(ventanas_x))])}")
                    if FWD_ON_LOSERS:
                        print(f"[GRÁFICO W/L] Losers (n={len(loser_indices_copy)}): {', '.join([f'{ventanas_x[i]}={losers_means[i]:.2f}%' if losers_means[i] is not None else f'{ventanas_x[i]}=N/A' for i in range(len(ventanas_x))])}")

                    # ============================================================
                    # GRÁFICO COMPARATIVO: WINNERS vs LOSERS (PnL_fwd_pts_mediana)
                    # ============================================================
                    print("\n" + "-"*80)

                    # Identificar columnas PnL_fwd_pts_*_mediana
                    pnl_pts_cols_mediana = [col for col in df_copy.columns if col.startswith("PnL_fwd_pts_") and col.endswith("_mediana")]
                    pnl_pts_cols_mediana_sorted = sorted(pnl_pts_cols_mediana)

                    if len(pnl_pts_cols_mediana_sorted) >= 1:
                        # Calcular promedios para cada ventana
                        ventanas_pts_x = []
                        winners_pts_means = []
                        losers_pts_means = []

                        for col in pnl_pts_cols_mediana_sorted:
                            # Extraer sufijo (01, 05, 15, 25, 50)
                            sufijo = col.replace("PnL_fwd_pts_", "").replace("_mediana", "")
                            ventanas_pts_x.append(f"W{sufijo}")

                            # Convertir a numérico
                            pnl_pts_data = pd.to_numeric(df_copy[col], errors='coerce')
                            valid_pts_mask = pnl_pts_data.notna()

                            # Calcular promedio para winners
                            if FWD_ON_WINNERS:
                                winners_pts_pnl = pnl_pts_data[winners_mask & valid_pts_mask]
                                winners_pts_means.append(winners_pts_pnl.mean() if not winners_pts_pnl.empty else None)
                            else:
                                winners_pts_means.append(None)

                            # Calcular promedio para losers
                            if FWD_ON_LOSERS:
                                losers_pts_pnl = pnl_pts_data[losers_mask & valid_pts_mask]
                                losers_pts_means.append(losers_pts_pnl.mean() if not losers_pts_pnl.empty else None)
                            else:
                                losers_pts_means.append(None)

                        # Generar gráfico comparativo
                        fig, ax = plt.subplots(figsize=(12, 7))

                        # Plot winners (verde)
                        if FWD_ON_WINNERS and any(m is not None for m in winners_pts_means):
                            x_winners_pts = [i for i, m in enumerate(winners_pts_means) if m is not None]
                            y_winners_pts = [m for m in winners_pts_means if m is not None]
                            if x_winners_pts:
                                ax.plot(x_winners_pts, y_winners_pts, marker='o', linewidth=2.5, markersize=8,
                                       color='#2ecc71', label=f'Winners (n={len(winner_indices_copy)})', alpha=0.9)

                        # Plot losers (rojo)
                        if FWD_ON_LOSERS and any(m is not None for m in losers_pts_means):
                            x_losers_pts = [i for i, m in enumerate(losers_pts_means) if m is not None]
                            y_losers_pts = [m for m in losers_pts_means if m is not None]
                            if x_losers_pts:
                                ax.plot(x_losers_pts, y_losers_pts, marker='s', linewidth=2.5, markersize=8,
                                       color='#e74c3c', label=f'Losers (n={len(loser_indices_copy)})', alpha=0.9)

                        # Configuración del gráfico
                        ax.set_xlabel("Periodo W (Fracción del DTE1)", fontsize=12, fontweight='bold')
                        ax.set_ylabel("Promedio PnL_fwd_pts_mediana (puntos)", fontsize=12, fontweight='bold')
                        ax.set_title(f"Comparativa Winners vs Losers - PnL_fwd_pts_mediana\n(Estratificado por WL_PRE, WL_TOP_PCT={wl_pct_value*100:.1f}%)",
                                    fontsize=14, fontweight='bold')
                        ax.set_xticks(range(len(ventanas_pts_x)))
                        ax.set_xticklabels(ventanas_pts_x, fontsize=10)
                        ax.grid(True, alpha=0.3, linestyle='--')
                        ax.axhline(y=0, color='gray', linestyle='--', linewidth=1, alpha=0.7)
                        ax.legend(fontsize=11, loc='best')

                        # Ajustar layout
                        plt.tight_layout()

                        # Guardar gráfico
                        plot_name_wl_pts = batch_out_name.replace(".csv", "_Winners_vs_Losers_pts.png")
                        plot_path_wl_pts = DESKTOP / safe_filename(plot_name_wl_pts)
                        plt.savefig(plot_path_wl_pts, dpi=150, bbox_inches='tight')
                        plt.close()

                        print(f"[GRÁFICO W/L] Guardado en: {plot_path_wl_pts}")
                        print(f"[GRÁFICO W/L] Periodos: {len(ventanas_pts_x)}")
                        if FWD_ON_WINNERS:
                            print(f"[GRÁFICO W/L] Winners (n={len(winner_indices_copy)}): {', '.join([f'{ventanas_pts_x[i]}={winners_pts_means[i]:.2f}pts' if winners_pts_means[i] is not None else f'{ventanas_pts_x[i]}=N/A' for i in range(len(ventanas_pts_x))])}")
                        if FWD_ON_LOSERS:
                            print(f"[GRÁFICO W/L] Losers (n={len(loser_indices_copy)}): {', '.join([f'{ventanas_pts_x[i]}={losers_pts_means[i]:.2f}pts' if losers_pts_means[i] is not None else f'{ventanas_pts_x[i]}=N/A' for i in range(len(ventanas_pts_x))])}")

                    print("="*80 + "\n")
                else:
                    print("[INFO] No se encontraron columnas PnL_fwd_pct_*_mediana para gráficos W/L")
                    print("="*80 + "\n")

            print("="*80 + "\n")
        else:
            print("[INFO] No hay filas válidas tras el filtro. No se genera CSV copia.")

    print("="*80)
    print("FIN PROCESO ADICIONAL")
    print("="*80 + "\n")

    # ============================================================
    # MÓDULO DE ANÁLISIS ESTADÍSTICO (sobre CSV _mediana)
    # ============================================================
    if STATISTICAL_ANALYSIS:
        print("\n" + "="*80)
        print("INICIANDO ANÁLISIS ESTADÍSTICO AUTOMÁTICO")
        print("="*80)

        # Buscar el CSV _mediana generado
        csv_mediana_path = None
        if 'batch_out_path_copy' in locals():
            csv_mediana_path = batch_out_path_copy
            if csv_mediana_path and csv_mediana_path.exists():
                print(f"[STATS] CSV _mediana detectado: {csv_mediana_path.name}")
                print(f"[STATS] Ejecutando análisis estadístico de umbrales y correlaciones...")
                print("")

                # Ejecutar análisis estadístico
                run_statistical_analysis(str(csv_mediana_path))

                print("\n" + "="*80)
                print("ANÁLISIS ESTADÍSTICO FINALIZADO")
                print("="*80 + "\n")
            else:
                print(f"[INFO] No se encontró CSV _mediana. Análisis estadístico omitido.")
        else:
            print(f"[INFO] No se generó CSV _mediana. Análisis estadístico omitido.")
    else:
        print("\n" + "="*80)
        print("[INFO] ANÁLISIS ESTADÍSTICO DESACTIVADO (STATISTICAL_ANALYSIS = False)")
        print("="*80 + "\n")

    # Limpieza de archivos temporales Parquet
    # Eliminamos el directorio temporal completo con todo su contenido
    print(f"\n[CLEANUP] Limpiando archivos temporales...")

    # Eliminar directorio temporal completo (con todos los archivos dentro)
    try:
        import shutil
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
            print(f"[CLEANUP] Directorio temporal eliminado completamente: {temp_dir.name}")
    except Exception as e:
        print(f"[CLEANUP] No se pudo eliminar directorio temporal: {e}")
        # Listar contenido restante para diagnóstico
        try:
            if temp_dir.exists():
                remaining = list(temp_dir.iterdir())
                if remaining:
                    print(f"[CLEANUP] Archivos restantes en directorio temporal:")
                    for f in remaining:
                        file_size_mb = f.stat().st_size / (1024**2) if f.is_file() else 0
                        print(f"  - {f.name} ({file_size_mb:.1f} MB)")
        except:
            pass

if __name__=="__main__":
    main()
