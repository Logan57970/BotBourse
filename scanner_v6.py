bash

cat > /home/claude/scanner_v6.py << 'PYEOF'
"""
=====================================================
   SCANNER BOURSE V6 — RAILWAY OPTIMISE
   Sources : Finnhub + Alpha Vantage + Google News
             Reddit + CNN Fear & Greed
=====================================================

INSTALLATION :
  pip install requests discord.py flask schedule numpy

CLES API GRATUITES NECESSAIRES :
  Finnhub      : finnhub.io -> Get free API key
  Alpha Vantage: alphavantage.co -> Get Free API Key

CONFIGURATION :
  Remplis les variables ci-dessous OU
  sur Railway : Settings -> Variables

COMMANDES DISCORD :
  !scan              -> Scan complet
  !momentum          -> Actions +10% + score J+1/J+3
  !analyse NVDA      -> Analyse complete + score
  !stats NVDA        -> Taux de reussite reel
  !stats             -> Stats globales
  !marche            -> Fear & Greed + contexte
  !news NVDA         -> Actualites recentes
  !alerte NVDA 150   -> Alerte si NVDA < 150$
  !alerte NVDA +5%   -> Alerte si NVDA +5%
  !alertes           -> Voir alertes actives
  !supprimer NVDA    -> Supprimer alerte
  !watch NVDA        -> Ajouter watchlist
  !unwatch NVDA      -> Retirer watchlist
  !watchlist         -> Voir watchlist
  !backtest NVDA     -> Backtest 6 mois
  !buy NVDA 10 500   -> Enregistrer achat
  !sell NVDA 5       -> Enregistrer vente
  !portfolio         -> Portefeuille + P&L
  !semaine           -> Rapport hebdomadaire
  !prix NVDA         -> Prix actuel
  !help              -> Toutes les commandes

DASHBOARD : http://localhost:5000
"""

import requests
import feedparser
import schedule
import time
import json
import sqlite3
import threading
import io
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, render_template_string
import os

try:
    import discord
    from discord.ext import commands, tasks
    DISCORD_PY = True
except ImportError:
    DISCORD_PY = False
    print("discord.py non installe — pip install discord.py")

# =====================================================
#   CONFIGURATION
#   Sur Railway : Settings -> Variables
# =====================================================
DISCORD_TOKEN   = os.environ.get("DISCORD_TOKEN",   "TON_TOKEN_DISCORD")
CHANNEL_ID      = int(os.environ.get("CHANNEL_ID",  "123456789"))
FINNHUB_KEY     = os.environ.get("FINNHUB_KEY",     "TON_FINNHUB_KEY")
ALPHAVANTAGE_KEY= os.environ.get("ALPHAVANTAGE_KEY","TON_ALPHAVANTAGE_KEY")
PORT            = int(os.environ.get("PORT",         5000))

SEUIL_MOMENTUM  = 10.0
SEUIL_VEILLE    = 3.0
MAX_WORKERS     = 8    # Raisonnable pour eviter les rate limits

# Horaires (bot demarre a 08h00 via Railway cron, s'arrete a 22h45)
HORAIRES = {
    "pre_marche":   "08:15",
    "ouverture_eu": "09:00",
    "ouverture_us": "15:30",
    "cloture_eu":   "17:30",
    "verification": "22:30",
    "arret":        "22:45",
}

# =====================================================
#   ACTIONS SURVEILLEES
# =====================================================
ACTIONS_US = [
    "NVDA","MSFT","AAPL","GOOGL","META","AMZN","TSLA","AMD","INTC","CRM",
    "ADBE","ORCL","QCOM","AVGO","NOW","PLTR","UBER","SNOW",
    "JPM","BAC","GS","MS","WFC","BLK","AXP","V","MA","PYPL",
    "JNJ","UNH","PFE","MRK","ABBV","LLY","BMY","AMGN","GILD",
    "XOM","CVX","COP","SLB","EOG","OXY","HAL",
    "WMT","COST","TGT","HD","MCD","SBUX","NKE","PG","KO","PEP","NFLX","DIS",
    "CAT","DE","HON","GE","RTX","LMT","BA","UPS","FDX",
    "AMT","PLD","EQIX","SPG",
]

ACTIONS_FR = [
    "MC.PA","AIR.PA","TTE.PA","SAN.PA","BNP.PA","OR.PA","SU.PA",
    "SAF.PA","DSY.PA","CAP.PA","KER.PA","RMS.PA","EL.PA",
    "BN.PA","ACA.PA","GLE.PA","STM.PA","WRL.PA","RNO.PA","VIV.PA",
]

ACTIONS_EU = [
    "ASML.AS","SAP.DE","SIE.DE","ADYEN.AS","ALV.DE","BMW.DE","BAYN.DE",
]

SECTEURS = {
    "Tech US":       ["NVDA","MSFT","AAPL","GOOGL","META","AMZN","TSLA","AMD","INTC","CRM","ADBE","ORCL","QCOM","AVGO","NOW"],
    "Finance US":    ["JPM","BAC","GS","MS","WFC","BLK","AXP","V","MA","PYPL"],
    "Sante US":      ["JNJ","UNH","PFE","MRK","ABBV","LLY","BMY","AMGN","GILD"],
    "Energie US":    ["XOM","CVX","COP","SLB","EOG","OXY","HAL"],
    "Conso US":      ["WMT","COST","TGT","HD","MCD","SBUX","NKE","PG","KO","PEP"],
    "Industrie US":  ["CAT","DE","HON","GE","RTX","LMT","BA","UPS","FDX"],
    "CAC 40":        ["MC.PA","AIR.PA","TTE.PA","SAN.PA","BNP.PA","OR.PA","SAF.PA","DSY.PA","CAP.PA","KER.PA","RMS.PA"],
    "Tech EU":       ["ASML.AS","SAP.DE","SIE.DE","ADYEN.AS","STM.PA"],
    "Immo US":       ["AMT","PLD","EQIX","SPG"],
}

TOUTES_ACTIONS    = list(set(ACTIONS_US + ACTIONS_FR + ACTIONS_EU))
TOUTES_SECTEURS   = [t for lst in SECTEURS.values() for t in lst]

NOMS = {
    "ASML.AS":"ASML","SAP.DE":"SAP","SIE.DE":"Siemens","ADYEN.AS":"Adyen",
    "ALV.DE":"Allianz","BMW.DE":"BMW","BAYN.DE":"Bayer",
    "MC.PA":"LVMH","AIR.PA":"Airbus","TTE.PA":"TotalEnergies",
    "SAN.PA":"Sanofi","BNP.PA":"BNP Paribas","OR.PA":"L'Oreal",
    "SAF.PA":"Safran","DSY.PA":"Dassault Sys","CAP.PA":"Capgemini",
    "KER.PA":"Kering","RMS.PA":"Hermes","EL.PA":"EssilorLuxottica",
    "BN.PA":"Danone","ACA.PA":"Credit Agricole","GLE.PA":"Societe Generale",
    "STM.PA":"STMicro","WRL.PA":"Worldline","RNO.PA":"Renault","VIV.PA":"Vivendi",
}

def nom(ticker):
    return NOMS.get(ticker, ticker.replace(".PA","").replace(".DE","").replace(".AS",""))

# =====================================================
#   BASE DE DONNEES SQLITE
# =====================================================
DB_PATH = os.environ.get("DB_PATH", "scanner_v6.db")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS scans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT, ticker TEXT, prix REAL,
        var_1j REAL, var_5j REAL, rsi REAL,
        score INTEGER, secteur TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS signaux_momentum (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date_signal TEXT, ticker TEXT,
        prix_signal REAL, variation_signal REAL,
        score_j1 REAL, score_j3 REAL, causes TEXT,
        prix_j1 REAL, var_reelle_j1 REAL,
        verifie_j1 INTEGER DEFAULT 0, date_verif_j1 TEXT,
        prix_j3 REAL, var_reelle_j3 REAL,
        verifie_j3 INTEGER DEFAULT 0, date_verif_j3 TEXT,
        succes_j1 INTEGER, succes_j3 INTEGER)""")
    c.execute("""CREATE TABLE IF NOT EXISTS alertes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT, type TEXT, valeur REAL,
        active INTEGER DEFAULT 1,
        date_creation TEXT, date_declenchement TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS watchlist (
        ticker TEXT PRIMARY KEY, date_ajout TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS portefeuille (
        ticker TEXT PRIMARY KEY,
        quantite REAL, prix_achat REAL, date_achat TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT, ticker TEXT, quantite REAL,
        prix REAL, pnl REAL, date TEXT)""")
    conn.commit()
    conn.close()
    print("Base de donnees V6 initialisee")

def db():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

# =====================================================
#   FINNHUB — PRIX + INDICATEURS (60 req/min gratuit)
# =====================================================
FINNHUB_BASE = "https://finnhub.io/api/v1"
_finnhub_cache = {}
_finnhub_cache_time = {}
CACHE_DUREE = 300  # 5 minutes

def finnhub_get(endpoint, params):
    """Appel Finnhub avec cache pour eviter les rate limits."""
    cle = endpoint + json.dumps(params, sort_keys=True)
    now = time.time()
    if cle in _finnhub_cache and now - _finnhub_cache_time.get(cle, 0) < CACHE_DUREE:
        return _finnhub_cache[cle]
    try:
        params["token"] = FINNHUB_KEY
        r = requests.get(
            f"{FINNHUB_BASE}/{endpoint}",
            params=params, timeout=8
        )
        if r.status_code == 429:
            print(f"Finnhub rate limit — attente 60s")
            time.sleep(60)
            r = requests.get(f"{FINNHUB_BASE}/{endpoint}", params=params, timeout=8)
        data = r.json()
        _finnhub_cache[cle] = data
        _finnhub_cache_time[cle] = now
        return data
    except Exception as e:
        print(f"Finnhub {endpoint} : {e}")
        return {}

def get_prix_finnhub(ticker):
    """Prix actuel + variation depuis Finnhub."""
    # Convertir ticker EU pour Finnhub
    ticker_fh = convertir_ticker_finnhub(ticker)
    data = finnhub_get("quote", {"symbol": ticker_fh})
    if not data or data.get("c", 0) == 0:
        return None
    return {
        "prix":     round(data.get("c", 0), 4),
        "var_1j":   round(data.get("dp", 0), 2),
        "haut":     round(data.get("h", 0), 4),
        "bas":      round(data.get("l", 0), 4),
        "ouverture":round(data.get("o", 0), 4),
        "prev":     round(data.get("pc", 0), 4),
    }

def get_candles_finnhub(ticker, resolution="D", jours=90):
    """
    Bougies historiques depuis Finnhub.
    resolution : 1, 5, 15, 30, 60, D, W, M
    """
    ticker_fh = convertir_ticker_finnhub(ticker)
    now  = int(time.time())
    from_ts = now - jours * 86400
    data = finnhub_get("stock/candle", {
        "symbol":     ticker_fh,
        "resolution": resolution,
        "from":       from_ts,
        "to":         now,
    })
    if not data or data.get("s") != "ok":
        return None
    closes  = data.get("c", [])
    highs   = data.get("h", [])
    lows    = data.get("l", [])
    volumes = data.get("v", [])
    times   = data.get("t", [])
    if len(closes) < 5:
        return None
    return {
        "closes":  closes,
        "highs":   highs,
        "lows":    lows,
        "volumes": volumes,
        "times":   times,
    }

def convertir_ticker_finnhub(ticker):
    """Convertit les tickers EU au format Finnhub."""
    conversions = {
        "MC.PA":    "MC.PA",    "AIR.PA":  "AIR.PA",
        "TTE.PA":   "TTE.PA",   "SAN.PA":  "SAN.PA",
        "BNP.PA":   "BNP.PA",   "OR.PA":   "OR.PA",
        "SAF.PA":   "SAF.PA",   "DSY.PA":  "DSY.PA",
        "CAP.PA":   "CAP.PA",   "KER.PA":  "KER.PA",
        "RMS.PA":   "RMS.PA",   "EL.PA":   "EL.PA",
        "BN.PA":    "BN.PA",    "ACA.PA":  "ACA.PA",
        "GLE.PA":   "GLE.PA",   "STM.PA":  "STM.PA",
        "WRL.PA":   "WRL.PA",   "RNO.PA":  "RNO.PA",
        "VIV.PA":   "VIV.PA",   "SU.PA":   "SU.PA",
        "ASML.AS":  "ASML.AS",  "SAP.DE":  "SAP.DE",
        "SIE.DE":   "SIE.DE",   "ADYEN.AS":"ADYEN.AS",
        "ALV.DE":   "ALV.DE",   "BMW.DE":  "BMW.DE",
        "BAYN.DE":  "BAYN.DE",
    }
    return conversions.get(ticker, ticker)

def get_earnings_finnhub(ticker):
    """Prochains earnings depuis Finnhub."""
    ticker_fh = convertir_ticker_finnhub(ticker)
    data = finnhub_get("calendar/earnings", {
        "symbol": ticker_fh,
        "from":   datetime.now().strftime("%Y-%m-%d"),
        "to":     (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d"),
    })
    earnings = data.get("earningsCalendar", [])
    return earnings[:1] if earnings else []

def get_sentiment_finnhub(ticker):
    """Sentiment des news depuis Finnhub."""
    ticker_fh = convertir_ticker_finnhub(ticker)
    data = finnhub_get("news-sentiment", {"symbol": ticker_fh})
    if not data or "sentiment" not in data:
        return None
    s = data["sentiment"]
    return {
        "score":     round(s.get("companyNewsScore", 0.5), 3),
        "buzz":      round(data.get("buzz", {}).get("buzz", 0), 3),
        "articles":  data.get("buzz", {}).get("articlesInLastWeek", 0),
        "positif":   round(s.get("bullishPercent", 0) * 100, 1),
        "negatif":   round(s.get("bearishPercent", 0) * 100, 1),
    }

# =====================================================
#   ALPHA VANTAGE — RSI + MACD + HISTORIQUE
#   25 req/jour gratuit -> on cache tout
# =====================================================
AV_BASE   = "https://www.alphavantage.co/query"
_av_cache = {}

def av_get(function, symbol, extra_params=None):
    """Appel Alpha Vantage avec cache persistant (SQLite)."""
    cle = f"av_{function}_{symbol}"
    if cle in _av_cache:
        return _av_cache[cle]
    try:
        params = {
            "function":  function,
            "symbol":    symbol,
            "apikey":    ALPHAVANTAGE_KEY,
        }
        if extra_params:
            params.update(extra_params)
        r    = requests.get(AV_BASE, params=params, timeout=10)
        data = r.json()
        if "Note" in data or "Information" in data:
            print(f"Alpha Vantage limite atteinte pour {symbol}")
            return None
        _av_cache[cle] = data
        return data
    except Exception as e:
        print(f"Alpha Vantage {function} {symbol} : {e}")
        return None

def get_historique_av(ticker):
    """Prix historique journalier depuis Alpha Vantage."""
    data = av_get("TIME_SERIES_DAILY", ticker, {"outputsize": "compact"})
    if not data or "Time Series (Daily)" not in data:
        return None
    ts     = data["Time Series (Daily)"]
    dates  = sorted(ts.keys(), reverse=True)[:90]
    closes = [float(ts[d]["4. close"]) for d in dates]
    vols   = [float(ts[d]["5. volume"]) for d in dates]
    highs  = [float(ts[d]["2. high"])   for d in dates]
    lows   = [float(ts[d]["3. low"])    for d in dates]
    return {
        "closes":  list(reversed(closes)),
        "volumes": list(reversed(vols)),
        "highs":   list(reversed(highs)),
        "lows":    list(reversed(lows)),
        "dates":   list(reversed(dates)),
    }

def get_rsi_av(ticker):
    """RSI depuis Alpha Vantage (deja calcule)."""
    data = av_get("RSI", ticker, {
        "interval":    "daily",
        "time_period": 14,
        "series_type": "close",
    })
    if not data or "Technical Analysis: RSI" not in data:
        return None
    vals = data["Technical Analysis: RSI"]
    date = sorted(vals.keys(), reverse=True)[0]
    return round(float(vals[date]["RSI"]), 1)

def get_macd_av(ticker):
    """MACD depuis Alpha Vantage (deja calcule)."""
    data = av_get("MACD", ticker, {
        "interval":          "daily",
        "series_type":       "close",
        "fastperiod":        12,
        "slowperiod":        26,
        "signalperiod":      9,
    })
    if not data or "Technical Analysis: MACD" not in data:
        return None
    vals = data["Technical Analysis: MACD"]
    date = sorted(vals.keys(), reverse=True)[0]
    macd_val   = float(vals[date]["MACD"])
    signal_val = float(vals[date]["MACD_Signal"])
    histo      = float(vals[date]["MACD_Hist"])
    return {
        "signal":      "haussier" if macd_val > signal_val else "baissier",
        "valeur":      round(macd_val, 4),
        "histogramme": round(histo, 4),
    }

# =====================================================
#   CALCULS TECHNIQUES LOCAUX (sur donnees Finnhub)
# =====================================================
def calculer_rsi_local(closes, periode=14):
    if len(closes) < periode+1: return 50.0
    gains  = [max(closes[i]-closes[i-1], 0) for i in range(1,len(closes))]
    pertes = [max(closes[i-1]-closes[i], 0) for i in range(1,len(closes))]
    ag = sum(gains[-periode:])  / periode
    ap = sum(pertes[-periode:]) / periode
    if ap == 0: return 100.0
    return round(100 - (100/(1+ag/ap)), 1)

def calculer_macd_local(closes):
    if len(closes) < 26: return {"signal":"neutre","histogramme":0}
    def ema(data, span):
        k=2/(span+1); v=data[0]
        for d in data[1:]: v=d*k+v*(1-k)
        return v
    macd_val   = ema(closes[-12:],12) - ema(closes[-26:],26)
    signal_val = ema(closes[-9:], 9)
    histo      = macd_val - signal_val
    return {
        "signal":      "haussier" if macd_val > signal_val else "baissier",
        "histogramme": round(histo, 4),
    }

def calculer_bollinger_local(closes, periode=20):
    if len(closes) < periode:
        return {"signal":"neutre","haut":0,"bas":0,"mm":0}
    recents = closes[-periode:]
    mm   = sum(recents) / periode
    std  = (sum((x-mm)**2 for x in recents)/periode)**0.5
    haut = mm + 2*std
    bas  = mm - 2*std
    actuel = closes[-1]
    if actuel >= haut:   signal = "surchete"
    elif actuel <= bas:  signal = "survendu"
    else:                signal = "neutre"
    return {
        "signal": signal,
        "haut":   round(haut, 4),
        "bas":    round(bas, 4),
        "mm":     round(mm, 4),
    }

def calculer_mm_local(closes):
    actuel = closes[-1]; res = {}
    for p, n in [(50,"mm50"),(200,"mm200")]:
        if len(closes) >= p:
            mm = sum(closes[-p:]) / p
            res[n] = round(mm, 4)
            res[f"{n}_signal"] = "haussier" if actuel > mm else "baissier"
        else:
            res[n] = None; res[f"{n}_signal"] = "neutre"
    if res.get("mm50") and res.get("mm200"):
        res["golden_cross"] = res["mm50"] > res["mm200"]
    return res

def calculer_volume_ratio(volumes):
    if len(volumes) < 10: return 1.0
    moy = sum(volumes[-10:-1]) / 9
    return round(volumes[-1] / moy, 2) if moy > 0 else 1.0

def calculer_variation(closes, n):
    if len(closes) < n+1: return 0.0
    return round(((closes[-1]-closes[-n-1])/closes[-n-1])*100, 2)

# =====================================================
#   GOOGLE NEWS RSS — ACTUALITES
# =====================================================
def google_news(query, langue="fr"):
    """News depuis Google News RSS — illimite et gratuit."""
    try:
        q   = requests.utils.quote(query)
        url = f"https://news.google.com/rss/search?q={q}&hl={langue}&gl=FR&ceid=FR:{langue.upper()}"
        flux= feedparser.parse(url)
        return [
            {
                "titre":  e.get("title","")[:120],
                "url":    e.get("link",""),
                "date":   e.get("published",""),
                "source": e.get("source",{}).get("title","Google News"),
            }
            for e in flux.entries[:6]
        ]
    except Exception as e:
        print(f"Google News : {e}")
        return []

def get_news_action(ticker):
    """Recupere les news d'une action depuis Google News (FR + EN)."""
    n = nom(ticker)
    news_fr = google_news(f"{n} bourse action resultat")
    news_en = google_news(f"{ticker} stock earnings news", "en")
    # Finnhub news en complement
    news_fh = get_news_finnhub(ticker)
    seen, unique = set(), []
    for article in news_fr + news_en + news_fh:
        t = article.get("titre","")
        if t and t not in seen:
            seen.add(t)
            unique.append(article)
    return unique[:6]

def get_news_finnhub(ticker):
    """News de l'entreprise depuis Finnhub."""
    ticker_fh = convertir_ticker_finnhub(ticker)
    date_from = (datetime.now()-timedelta(days=7)).strftime("%Y-%m-%d")
    date_to   = datetime.now().strftime("%Y-%m-%d")
    data = finnhub_get("company-news", {
        "symbol": ticker_fh,
        "from":   date_from,
        "to":     date_to,
    })
    if not isinstance(data, list):
        return []
    return [
        {"titre": a.get("headline","")[:120], "url": a.get("url",""), "source": a.get("source","")}
        for a in data[:4]
        if a.get("headline")
    ]

def identifier_causes(titres):
    causes_map = {
        "Resultats trimestriels":  ["earnings","resultats","quarterly","benefice","revenue","profit","EPS","chiffre"],
        "Guidance relevee":        ["guidance","outlook","forecast","prevision","objectif"],
        "Acquisition Fusion":      ["acquisition","merger","fusion","buyout","deal","rachat"],
        "Nouveau contrat produit": ["launch","lancement","contrat","contract","partenariat","accord"],
        "Recommandation analyste": ["upgrade","downgrade","target","analyst","price target"],
        "Contexte macro":          ["fed","inflation","taux","interest rate","recession"],
        "Rachat actions":          ["buyback","repurchase","dividende","dividend"],
        "Risque probleme":         ["recall","lawsuit","amende","fine","scandal","fraude"],
    }
    txt = " ".join(titres).lower()
    res = [c for c,kws in causes_map.items() if any(k.lower() in txt for k in kws)]
    return res or ["Mouvement technique / sentiment general"]

# =====================================================
#   REDDIT — SENTIMENT COMMUNAUTE
# =====================================================
def get_reddit(ticker):
    res = []
    for sub in ["investing","wallstreetbets","stocks"]:
        try:
            r = requests.get(
                f"https://www.reddit.com/r/{sub}/search.json",
                params={"q":ticker,"sort":"hot","limit":4,"t":"day"},
                headers={"User-Agent":"scanner_bourse/1.0"},
                timeout=8
            )
            for p in r.json().get("data",{}).get("children",[]):
                d = p["data"]
                res.append({
                    "titre": d.get("title","")[:100],
                    "score": d.get("score",0),
                    "sub":   sub,
                })
            time.sleep(0.5)
        except: pass
    return sorted(res, key=lambda x: x["score"], reverse=True)[:4]

# =====================================================
#   FEAR & GREED INDEX (CNN)
# =====================================================
def get_fear_greed():
    try:
        r    = requests.get(
            "https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
            headers={"User-Agent":"Mozilla/5.0"},
            timeout=10
        )
        data  = r.json()
        score = round(float(data["fear_and_greed"]["score"]), 1)
        rating= data["fear_and_greed"]["rating"]
        trad  = {
            "Extreme Fear":  "Peur extreme",
            "Fear":          "Peur",
            "Neutral":       "Neutre",
            "Greed":         "Avidite",
            "Extreme Greed": "Avidite extreme",
        }
        if score <= 25:   emoji = "🔴"
        elif score <= 45: emoji = "🟠"
        elif score <= 55: emoji = "🟡"
        elif score <= 75: emoji = "🟢"
        else:             emoji = "🔥"
        return {"score":score,"rating":trad.get(rating,rating),"emoji":emoji}
    except:
        return {"score":50,"rating":"Indisponible","emoji":"⚪"}

def get_macro():
    """Donnees macro via Finnhub (indices + obligations)."""
    macro = {}
    indices = {
        "S&P 500":  "^GSPC",
        "NASDAQ":   "^IXIC",
        "CAC 40":   "^FCHI",
        "DAX":      "^GDAXI",
        "VIX":      "^VIX",
    }
    for nom_idx, ticker in indices.items():
        data = finnhub_get("quote", {"symbol": ticker})
        if data and data.get("c", 0) != 0:
            macro[nom_idx] = {
                "nom":       nom_idx,
                "valeur":    round(data.get("c",0), 2),
                "variation": round(data.get("dp",0), 2),
            }
        time.sleep(0.3)
    return macro

# =====================================================
#   SCORE D'OPPORTUNITE 0 -> 100
# =====================================================
def calculer_score(var_1j, var_5j, rsi, vol_ratio, macd,
                   boll=None, mm=None, ticker=None, sentiment_fh=None):
    s = 50

    # Momentum
    if var_1j > 4:    s += 20
    elif var_1j > 2:  s += 12
    elif var_1j > 0:  s += 5
    elif var_1j < -4: s -= 20
    elif var_1j < -2: s -= 10

    if var_5j > 8:    s += 15
    elif var_5j > 3:  s += 8
    elif var_5j < -8: s -= 15
    elif var_5j < -3: s -= 8

    # RSI
    if 50 <= rsi <= 70:   s += 15
    elif 30 <= rsi < 50:  s += 5
    elif rsi < 30:        s += 10
    elif rsi > 80:        s -= 15

    # Volume
    if vol_ratio > 2:     s += 15
    elif vol_ratio > 1.5: s += 8

    # MACD
    macd_signal = macd.get("signal","neutre") if isinstance(macd,dict) else macd
    if macd_signal == "haussier":
        s += 10
        if isinstance(macd,dict) and macd.get("histogramme",0) > 0:
            s += 3
    elif macd_signal == "baissier":
        s -= 10

    # Bollinger
    if boll:
        if boll.get("signal") == "survendu":   s += 8
        elif boll.get("signal") == "surchete": s -= 8

    # Golden Cross
    if mm and mm.get("golden_cross"):     s += 5
    elif mm and mm.get("golden_cross") == False: s -= 5

    # Sentiment Finnhub (NOUVEAUTE — utilise les news)
    if sentiment_fh:
        sc_fh = sentiment_fh.get("score", 0.5)
        buzz  = sentiment_fh.get("buzz", 0)
        if sc_fh > 0.7 and buzz > 1:   s += 8
        elif sc_fh > 0.6:               s += 4
        elif sc_fh < 0.3:               s -= 8
        elif sc_fh < 0.4:               s -= 4

    # Historique reel depuis SQLite
    if ticker:
        taux = get_taux_reussite(ticker)
        if taux is not None:
            if taux >= 75:   s += 8
            elif taux >= 60: s += 4
            elif taux <= 30: s -= 8
            elif taux <= 40: s -= 4

    return max(0, min(s, 100))

# =====================================================
#   SUIVI DES PREDICTIONS (coeur de la V6)
# =====================================================
def sauvegarder_signal(ticker, prix, variation, score_j1, score_j3, causes):
    conn = db(); c = conn.cursor()
    c.execute(
        "INSERT INTO signaux_momentum "
        "(date_signal,ticker,prix_signal,variation_signal,score_j1,score_j3,causes) "
        "VALUES (?,?,?,?,?,?,?)",
        (datetime.now().isoformat(),ticker,prix,variation,score_j1,score_j3,json.dumps(causes))
    )
    conn.commit(); conn.close()

def verifier_predictions():
    conn = db(); c = conn.cursor()
    now  = datetime.now()
    verif = 0; ok_j1 = 0; ok_j3 = 0

    # J+1
    hier      = (now - timedelta(days=1)).isoformat()
    avant_hier= (now - timedelta(days=2)).isoformat()
    rows_j1   = c.execute(
        "SELECT id,ticker,prix_signal FROM signaux_momentum "
        "WHERE verifie_j1=0 AND date_signal BETWEEN ? AND ?",
        (avant_hier, hier)
    ).fetchall()

    for sid, ticker, prix_signal in rows_j1:
        try:
            quote = get_prix_finnhub(ticker)
            if not quote: continue
            prix_actuel = quote["prix"]
            var_reelle  = round(((prix_actuel-prix_signal)/prix_signal)*100, 2)
            succes      = 1 if var_reelle > 0 else 0
            c.execute(
                "UPDATE signaux_momentum SET prix_j1=?,var_reelle_j1=?,"
                "verifie_j1=1,date_verif_j1=?,succes_j1=? WHERE id=?",
                (prix_actuel,var_reelle,now.isoformat(),succes,sid)
            )
            verif += 1
            if succes: ok_j1 += 1
        except: pass

    # J+3
    il_y_a_3j = (now - timedelta(days=3)).isoformat()
    il_y_a_4j = (now - timedelta(days=4)).isoformat()
    rows_j3   = c.execute(
        "SELECT id,ticker,prix_signal FROM signaux_momentum "
        "WHERE verifie_j3=0 AND date_signal BETWEEN ? AND ?",
        (il_y_a_4j, il_y_a_3j)
    ).fetchall()

    for sid, ticker, prix_signal in rows_j3:
        try:
            quote = get_prix_finnhub(ticker)
            if not quote: continue
            prix_actuel = quote["prix"]
            var_reelle  = round(((prix_actuel-prix_signal)/prix_signal)*100, 2)
            succes      = 1 if var_reelle > 0 else 0
            c.execute(
                "UPDATE signaux_momentum SET prix_j3=?,var_reelle_j3=?,"
                "verifie_j3=1,date_verif_j3=?,succes_j3=? WHERE id=?",
                (prix_actuel,var_reelle,now.isoformat(),succes,sid)
            )
            if succes: ok_j3 += 1
        except: pass

    conn.commit(); conn.close()
    return {"verifications":verif,"ok_j1":ok_j1,"ok_j3":ok_j3}

def get_taux_reussite(ticker):
    conn = db(); c = conn.cursor()
    rows = c.execute(
        "SELECT succes_j1 FROM signaux_momentum "
        "WHERE ticker=? AND verifie_j1=1 ORDER BY date_signal DESC LIMIT 20",
        (ticker,)
    ).fetchall()
    conn.close()
    if not rows or len(rows) < 3: return None
    return round(sum(r[0] for r in rows)/len(rows)*100, 1)

def get_stats_ticker(ticker):
    conn = db(); c = conn.cursor()
    rows_j1 = c.execute(
        "SELECT var_reelle_j1,succes_j1,score_j1 FROM signaux_momentum "
        "WHERE ticker=? AND verifie_j1=1 ORDER BY date_signal DESC LIMIT 30",
        (ticker,)
    ).fetchall()
    rows_j3 = c.execute(
        "SELECT var_reelle_j3,succes_j3 FROM signaux_momentum "
        "WHERE ticker=? AND verifie_j3=1 ORDER BY date_signal DESC LIMIT 30",
        (ticker,)
    ).fetchall()
    conn.close()

    def stats(rows):
        if not rows or len(rows) < 3: return None
        reussites = sum(1 for r in rows if r[1]==1)
        taux      = round(reussites/len(rows)*100, 1)
        gains     = [r[0] for r in rows if r[0] is not None]
        gain_moy  = round(sum(gains)/len(gains), 2) if gains else 0
        gain_ok   = round(sum(g for g in gains if g>0)/max(1,sum(1 for g in gains if g>0)), 2)
        perte_ko  = round(sum(g for g in gains if g<=0)/max(1,sum(1 for g in gains if g<=0)), 2)
        return {
            "total":    len(rows),
            "reussites":reussites,
            "taux":     taux,
            "gain_moy": gain_moy,
            "gain_ok":  gain_ok,
            "perte_ko": perte_ko,
            "meilleur": round(max(gains), 2) if gains else 0,
            "pire":     round(min(gains), 2) if gains else 0,
        }

    return {"j1": stats(rows_j1), "j3": stats(rows_j3)}

def get_stats_globales():
    conn = db(); c = conn.cursor()
    rows = c.execute(
        "SELECT ticker,COUNT(*),SUM(succes_j1),AVG(var_reelle_j1) "
        "FROM signaux_momentum WHERE verifie_j1=1 "
        "GROUP BY ticker HAVING COUNT(*)>=3 "
        "ORDER BY AVG(var_reelle_j1) DESC"
    ).fetchall()
    total_verif    = c.execute("SELECT COUNT(*) FROM signaux_momentum WHERE verifie_j1=1").fetchone()[0]
    total_reussites= c.execute("SELECT SUM(succes_j1) FROM signaux_momentum WHERE verifie_j1=1").fetchone()[0] or 0
    conn.close()
    taux_global = round(total_reussites/total_verif*100,1) if total_verif>0 else 0
    return {
        "par_ticker":    rows[:10],
        "total_verif":   total_verif,
        "total_reussites":total_reussites,
        "taux_global":   taux_global,
    }

# =====================================================
#   SCAN PRINCIPAL — FINNHUB + AV + NEWS
# =====================================================
def analyser_ticker(ticker, secteur=""):
    """
    Analyse complete d'un ticker :
    1. Prix depuis Finnhub (temps reel)
    2. Indicateurs depuis Alpha Vantage (calcules) ou locaux
    3. Sentiment Finnhub (score des news)
    4. Score final
    """
    try:
        # 1. Prix Finnhub
        quote = get_prix_finnhub(ticker)
        if not quote or quote["prix"] == 0:
            return None

        prix   = quote["prix"]
        var_1j = quote["var_1j"]

        # 2. Candles pour indicateurs locaux
        candles = get_candles_finnhub(ticker, "D", 90)

        if candles and len(candles["closes"]) >= 20:
            closes  = candles["closes"]
            volumes = candles["volumes"]
            highs   = candles["highs"]
            lows    = candles["lows"]
            rsi     = calculer_rsi_local(closes)
            macd    = calculer_macd_local(closes)
            boll    = calculer_bollinger_local(closes)
            mm      = calculer_mm_local(closes)
            vol_r   = calculer_volume_ratio(volumes)
            var_5j  = calculer_variation(closes, 5)
            sr      = {
                "support":    round(min(lows[-20:]),  4),
                "resistance": round(max(highs[-20:]), 4),
            }
        else:
            # Fallback Alpha Vantage si Finnhub ne donne pas de candles
            hist = get_historique_av(ticker)
            if not hist or len(hist["closes"]) < 10:
                return None
            closes  = hist["closes"]
            volumes = hist["volumes"]
            highs   = hist["highs"]
            lows    = hist["lows"]
            rsi     = get_rsi_av(ticker) or calculer_rsi_local(closes)
            macd_av = get_macd_av(ticker)
            macd    = macd_av if macd_av else calculer_macd_local(closes)
            boll    = calculer_bollinger_local(closes)
            mm      = calculer_mm_local(closes)
            vol_r   = calculer_volume_ratio(volumes)
            var_5j  = calculer_variation(closes, 5)
            sr      = {
                "support":    round(min(lows[-20:]),  4),
                "resistance": round(max(highs[-20:]), 4),
            }

        # 3. Sentiment Finnhub (news scoring)
        sentiment = get_sentiment_finnhub(ticker)

        # 4. Score
        score = calculer_score(
            var_1j, var_5j, rsi, vol_r, macd,
            boll, mm, ticker, sentiment
        )

        return {
            "ticker":    ticker,
            "nom":       nom(ticker),
            "secteur":   secteur,
            "prix":      prix,
            "var_1j":    var_1j,
            "var_5j":    var_5j,
            "rsi":       rsi,
            "vol_ratio": vol_r,
            "macd":      macd,
            "boll":      boll,
            "mm":        mm,
            "sr":        sr,
            "sentiment": sentiment,
            "score":     score,
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        print(f"  {ticker} : {e}")
        return None

def analyser_actions(tickers=None):
    """Scan parallele de toutes les actions."""
    tickers     = tickers or TOUTES_SECTEURS
    resultats   = []
    par_secteur = {}
    ticker_to_sec = {t:s for s,lst in SECTEURS.items() for t in lst}

    print(f"Scan {len(tickers)} actions ({MAX_WORKERS} workers)...")
    debut = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(analyser_ticker, t, ticker_to_sec.get(t,"Autre")): t
            for t in tickers
        }
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                resultats.append(res)

    resultats = sorted(resultats, key=lambda x: x["score"], reverse=True)
    print(f"OK — {len(resultats)} actions en {round(time.time()-debut,1)}s")

    for secteur, lst in SECTEURS.items():
        scores = [r["score"] for r in resultats if r["ticker"] in lst]
        if scores:
            par_secteur[secteur] = round(sum(scores)/len(scores), 1)

    # Sauvegarde
    conn = db(); c = conn.cursor(); now = datetime.now().isoformat()
    for r in resultats:
        c.execute(
            "INSERT INTO scans (date,ticker,prix,var_1j,var_5j,rsi,score,secteur) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (now,r["ticker"],r["prix"],r["var_1j"],r["var_5j"],r["rsi"],r["score"],r["secteur"])
        )
    conn.commit(); conn.close()
    return resultats, par_secteur

# =====================================================
#   SCORE CONTINUATION 0 -> 10
# =====================================================
CAUSES_FOND = ["Resultats trimestriels","Guidance relevee","Acquisition Fusion","Nouveau contrat produit","Rachat actions"]
CAUSES_PONC = ["Mouvement technique / sentiment general","Risque probleme"]

def score_continuation(variation, rsi, vol_ratio, macd, causes,
                        reddit_sc, hist_cont, boll=None, mm=None,
                        ticker=None, sentiment_fh=None):
    j1, j3 = 0.0, 0.0
    details = []

    # Causes
    nb_fond = sum(1 for c in causes if c in CAUSES_FOND)
    nb_ponc = sum(1 for c in causes if c in CAUSES_PONC)
    if nb_fond >= 2:   j1+=3.0; j3+=3.0; details.append(("Causes fondamentales multiples","+3.0"))
    elif nb_fond == 1: j1+=2.5; j3+=2.5; details.append(("Cause fondamentale","+2.5"))
    elif nb_ponc > 0:  j1-=1.5; j3-=2.0; details.append(("Cause ponctuelle","-1.5/-2.0"))

    # RSI
    if rsi < 60:        j1+=2.0; j3+=2.0; details.append(("RSI<60 room to run","+2.0"))
    elif rsi < 75:      j1+=1.0; j3+=0.5; details.append(("RSI modere 60-75","+1.0/+0.5"))
    elif rsi < 85:      j1-=0.5; j3-=1.0; details.append(("RSI eleve 75-85","-0.5/-1.0"))
    else:               j1-=2.0; j3-=2.5; details.append(("RSI>85 surchete","-2.0/-2.5"))

    # Volume
    if vol_ratio >= 4:    j1+=2.0; j3+=1.5; details.append((f"Volume exceptionnel x{vol_ratio:.1f}","+2.0/+1.5"))
    elif vol_ratio >= 2.5:j1+=1.5; j3+=1.0; details.append((f"Volume fort x{vol_ratio:.1f}","+1.5/+1.0"))
    elif vol_ratio >= 1.5:j1+=0.5; j3+=0.5; details.append((f"Volume ok x{vol_ratio:.1f}","+0.5"))
    else:                 j1-=1.0; j3-=1.0; details.append((f"Volume faible x{vol_ratio:.1f}","-1.0"))

    # MACD
    macd_signal = macd.get("signal","neutre") if isinstance(macd,dict) else macd
    if macd_signal == "haussier": j1+=1.0; j3+=1.5; details.append(("MACD haussier","+1.0/+1.5"))
    else:                          j1-=0.5; j3-=1.0; details.append(("MACD baissier","-0.5/-1.0"))

    # Reddit
    if reddit_sc > 500:  j1+=1.0; j3+=0.5; details.append((f"Buzz Reddit fort ({reddit_sc}pts)","+1.0/+0.5"))
    elif reddit_sc > 100:j1+=0.5; j3+=0.5; details.append((f"Buzz Reddit modere","+0.5"))

    # Sentiment Finnhub (news score)
    if sentiment_fh:
        sc_fh = sentiment_fh.get("score", 0.5)
        buzz  = sentiment_fh.get("buzz", 0)
        pos   = sentiment_fh.get("positif", 50)
        if sc_fh > 0.7 and buzz > 1:
            j1+=1.5; j3+=1.0; details.append((f"Sentiment news tres positif ({pos}% bull)","+1.5/+1.0"))
        elif sc_fh > 0.6:
            j1+=0.5; j3+=0.5; details.append((f"Sentiment news positif","+0.5"))
        elif sc_fh < 0.3:
            j1-=1.5; j3-=1.5; details.append((f"Sentiment news tres negatif","-1.5"))
        elif sc_fh < 0.4:
            j1-=0.5; j3-=1.0; details.append((f"Sentiment news negatif","-0.5/-1.0"))

    # Historique
    if hist_cont > 3:   j1+=1.0; j3+=1.5; details.append((f"Historique J+3 +{hist_cont:.1f}%","+1.0/+1.5"))
    elif hist_cont > 0: j1+=0.5; j3+=0.5; details.append((f"Historique leger +{hist_cont:.1f}%","+0.5"))
    elif hist_cont <-2: j1-=1.0; j3-=1.5; details.append((f"Historique negatif {hist_cont:.1f}%","-1.0/-1.5"))

    # Bollinger
    if boll:
        if boll.get("signal") == "survendu":   j1+=1.0; j3+=1.0; details.append(("Bollinger survendu","+1.0"))
        elif boll.get("signal") == "surchete": j1-=1.0; j3-=1.0; details.append(("Bollinger surchete","-1.0"))

    # Golden Cross
    if mm and mm.get("golden_cross"):     j1+=0.5; j3+=1.0; details.append(("Golden Cross MM50>MM200","+0.5/+1.0"))

    # Amplitude
    if variation > 25:   j1-=1.5; j3-=2.0; details.append((f"Mouvement extreme {variation:.1f}%","-1.5/-2.0"))
    elif variation > 15: j1-=0.5; j3-=1.0; details.append((f"Mouvement fort {variation:.1f}%","-0.5/-1.0"))

    # Historique reel SQLite
    if ticker:
        taux = get_taux_reussite(ticker)
        if taux is not None:
            if taux >= 75:
                j1+=1.5; j3+=1.5; details.append((f"Historique reel {taux}% reussite","+1.5"))
            elif taux >= 60:
                j1+=0.5; j3+=0.5; details.append((f"Historique reel {taux}%","+0.5"))
            elif taux <= 30:
                j1-=1.5; j3-=1.5; details.append((f"Historique reel faible {taux}%","-1.5"))

    j1 = round(max(0, min(10, j1)), 1)
    j3 = round(max(0, min(10, j3)), 1)

    def verdict(s):
        if s >= 8:   return "Tres probable — tous les feux verts"
        elif s >= 7: return "Probable — signaux solides"
        elif s >= 6: return "Possible — surveiller la consolidation"
        elif s >= 5: return "Incertain"
        elif s >= 4: return "Peu probable"
        elif s >= 3: return "Risque pull-back"
        else:        return "Tres risque — retournement probable"

    return {"j1":j1,"j3":j3,"details":details,"verdict_j1":verdict(j1),"verdict_j3":verdict(j3)}

def barre(score):
    p=int(score); d=1 if (score-p)>=0.5 else 0; v=10-p-d
    return f"`{'#'*p}{'+' if d else ''}{'-'*v}` **{score}/10**"

def historique_continuation_local(ticker, seuil=10.0):
    """Calcule la continuation historique depuis les candles Finnhub."""
    try:
        candles = get_candles_finnhub(ticker, "D", 365)
        if not candles or len(candles["closes"]) < 20:
            return 0.0
        closes = candles["closes"]
        suites = []
        for i in range(1, len(closes)-4):
            vj = (closes[i]-closes[i-1])/closes[i-1]*100
            if vj >= seuil:
                vj3 = (closes[i+3]-closes[i])/closes[i]*100
                suites.append(vj3)
        return round(sum(suites)/len(suites), 2) if suites else 0.0
    except: return 0.0

# =====================================================
#   MOMENTUM +10%
# =====================================================
def detecter_momentum(seuil=None, tickers=None):
    seuil   = seuil or SEUIL_MOMENTUM
    tickers = tickers or TOUTES_ACTIONS
    res     = []
    print(f"Scan momentum +{seuil}% — {len(tickers)} actions...")

    def check(ticker):
        try:
            quote = get_prix_finnhub(ticker)
            if not quote: return None
            var = quote["var_1j"]
            if var < seuil: return None
            candles = get_candles_finnhub(ticker, "D", 30)
            if candles and len(candles["closes"]) >= 10:
                closes  = candles["closes"]
                volumes = candles["volumes"]
                rsi     = calculer_rsi_local(closes)
                macd    = calculer_macd_local(closes)
                boll    = calculer_bollinger_local(closes)
                mm      = calculer_mm_local(closes)
                vr      = calculer_volume_ratio(volumes)
            else:
                rsi=50; macd={"signal":"neutre","histogramme":0}
                boll={"signal":"neutre"}; mm={}; vr=1.0
            sentiment = get_sentiment_finnhub(ticker)
            return {
                "ticker":    ticker,
                "nom":       nom(ticker),
                "prix":      quote["prix"],
                "variation": var,
                "vol_ratio": vr,
                "rsi":       rsi,
                "macd":      macd,
                "boll":      boll,
                "mm":        mm,
                "sentiment": sentiment,
                "marche":    "France" if ".PA" in ticker else "EU" if any(s in ticker for s in [".DE",".AS"]) else "US",
            }
        except: return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(check, t): t for t in tickers}
        for fut in as_completed(futures):
            r = fut.result()
            if r:
                res.append(r)
                print(f"  MOMENTUM : {r['ticker']} +{r['variation']:.2f}%")

    return sorted(res, key=lambda x: x["variation"], reverse=True)

def analyser_action_momentum(action):
    t   = action["ticker"]
    var = action["variation"]

    # News multi-sources
    news    = get_news_action(t)
    reddit  = get_reddit(t)
    causes  = identifier_causes([n["titre"] for n in news])
    rs      = sum(r.get("score",0) for r in reddit)
    hc      = historique_continuation_local(t, SEUIL_MOMENTUM)
    sc      = score_continuation(
        var, action["rsi"], action["vol_ratio"], action["macd"],
        causes, rs, hc, action.get("boll"), action.get("mm"),
        ticker=t, sentiment_fh=action.get("sentiment")
    )
    sauvegarder_signal(t, action["prix"], var, sc["j1"], sc["j3"], causes)
    return {
        "action":  action,
        "causes":  causes,
        "news":    news[:5],
        "reddit":  reddit[:3],
        "hist_cont": hc,
        "score":   sc,
    }

# =====================================================
#   GRAPHIQUE (prix Finnhub + indicateurs locaux)
# =====================================================
def generer_graphique(ticker, jours=90):
    try:
        candles = get_candles_finnhub(ticker, "D", jours)
        if not candles or len(candles["closes"]) < 10:
            return None

        closes  = candles["closes"]
        volumes = candles["volumes"]
        times   = [datetime.fromtimestamp(t) for t in candles["times"]]

        fig, (ax1,ax2) = plt.subplots(2,1,figsize=(12,7),gridspec_kw={"height_ratios":[3,1]})
        fig.patch.set_facecolor("#0f0f10")
        for ax in [ax1,ax2]:
            ax.set_facecolor("#1a1a1c")
            ax.tick_params(colors="#9c9a92",labelsize=9)
            for s in ax.spines.values(): s.set_color("#333")

        import numpy as np_local
        prix_arr = np_local.array(closes)

        ax1.plot(times, prix_arr, color="#E2E0D8", linewidth=1.5, label="Prix", zorder=3)

        if len(closes) >= 20:
            mm20 = np_local.convolve(prix_arr, np_local.ones(20)/20, mode="valid")
            std20= np_local.array([np_local.std(closes[i:i+20]) for i in range(len(closes)-19)])
            t20  = times[19:]
            ax1.fill_between(t20, mm20+2*std20, mm20-2*std20, alpha=0.15, color="#5865F2", label="Bollinger")
            ax1.plot(t20, mm20, color="#5865F2", linewidth=0.8, linestyle="--", alpha=0.7)

        if len(closes) >= 50:
            mm50 = np_local.convolve(prix_arr, np_local.ones(50)/50, mode="valid")
            ax1.plot(times[49:], mm50, color="#F0997B", linewidth=1, label="MM50")

        if len(closes) >= 200:
            mm200= np_local.convolve(prix_arr, np_local.ones(200)/200, mode="valid")
            ax1.plot(times[199:], mm200, color="#1D9E75", linewidth=1, label="MM200")

        sr_bas  = round(min(candles["lows"][-20:]),  4)
        sr_haut = round(max(candles["highs"][-20:]), 4)
        ax1.axhline(sr_bas,  color="#E24B4A", linewidth=0.8, linestyle=":", alpha=0.8, label=f"Support {sr_bas}")
        ax1.axhline(sr_haut, color="#1D9E75", linewidth=0.8, linestyle=":", alpha=0.8, label=f"Resistance {sr_haut}")

        ax1.set_title(f"{ticker} ({nom(ticker)}) — {jours}j", color="#E2E0D8", fontsize=13)
        ax1.legend(loc="upper left", fontsize=8, facecolor="#1a1a1c", labelcolor="#E2E0D8", framealpha=0.8)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
        ax1.grid(color="#333", linewidth=0.4, alpha=0.5)

        colors_vol = ["#1D9E75" if i==0 or closes[i]>=closes[i-1] else "#E24B4A" for i in range(len(closes))]
        ax2.bar(times, volumes, color=colors_vol, alpha=0.7, width=0.8)
        ax2.set_ylabel("Volume", color="#9c9a92", fontsize=8)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
        ax2.grid(color="#333", linewidth=0.4, alpha=0.5)

        plt.tight_layout(pad=1.5)
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=130, bbox_inches="tight", facecolor="#0f0f10")
        plt.close(fig)
        buf.seek(0)
        return buf.read()

    except Exception as e:
        print(f"Graphique {ticker} : {e}")
        return None

# =====================================================
#   WATCHLIST + PORTEFEUILLE + ALERTES
# =====================================================
def charger_watchlist():
    conn=db();c=conn.cursor()
    rows=c.execute("SELECT ticker FROM watchlist").fetchall()
    conn.close(); return [r[0] for r in rows]

def ajouter_watchlist(ticker):
    conn=db();c=conn.cursor()
    c.execute("INSERT OR IGNORE INTO watchlist (ticker,date_ajout) VALUES (?,?)",
              (ticker.upper(),datetime.now().isoformat()))
    n=c.rowcount; conn.commit(); conn.close(); return n>0

def retirer_watchlist(ticker):
    conn=db();c=conn.cursor()
    c.execute("DELETE FROM watchlist WHERE ticker=?",(ticker.upper(),))
    n=c.rowcount; conn.commit(); conn.close(); return n>0

def charger_portefeuille():
    conn=db();c=conn.cursor()
    rows=c.execute("SELECT ticker,quantite,prix_achat,date_achat FROM portefeuille").fetchall()
    conn.close()
    return [{"ticker":r[0],"quantite":r[1],"prix_achat":r[2],"date_achat":r[3]} for r in rows]

def acheter(ticker, quantite, prix_achat):
    conn=db();c=conn.cursor();t=ticker.upper()
    ex=c.execute("SELECT quantite,prix_achat FROM portefeuille WHERE ticker=?",(t,)).fetchone()
    if ex:
        tq=ex[0]+quantite; tc=ex[0]*ex[1]+quantite*prix_achat
        c.execute("UPDATE portefeuille SET quantite=?,prix_achat=? WHERE ticker=?",(tq,round(tc/tq,2),t))
    else:
        c.execute("INSERT INTO portefeuille (ticker,quantite,prix_achat,date_achat) VALUES (?,?,?,?)",
                  (t,quantite,prix_achat,datetime.now().strftime("%d/%m/%Y")))
    c.execute("INSERT INTO transactions (type,ticker,quantite,prix,date) VALUES (?,?,?,?,?)",
              ("achat",t,quantite,prix_achat,datetime.now().isoformat()))
    conn.commit(); conn.close()

def vendre(ticker, quantite):
    conn=db();c=conn.cursor();t=ticker.upper()
    pos=c.execute("SELECT quantite,prix_achat FROM portefeuille WHERE ticker=?",(t,)).fetchone()
    if not pos: conn.close(); return None,"Aucune position"
    if quantite > pos[0]: conn.close(); return None,f"Tu n as que {pos[0]} actions"
    quote = get_prix_finnhub(t)
    pa    = quote["prix"] if quote else pos[1]
    pnl   = round((pa-pos[1])*quantite, 2)
    new_qte = pos[0]-quantite
    if new_qte == 0: c.execute("DELETE FROM portefeuille WHERE ticker=?",(t,))
    else: c.execute("UPDATE portefeuille SET quantite=? WHERE ticker=?",(new_qte,t))
    c.execute("INSERT INTO transactions (type,ticker,quantite,prix,pnl,date) VALUES (?,?,?,?,?,?)",
              ("vente",t,quantite,pa,pnl,datetime.now().isoformat()))
    conn.commit(); conn.close()
    return pnl, pa

def get_alertes():
    conn=db();c=conn.cursor()
    rows=c.execute("SELECT id,ticker,type,valeur FROM alertes WHERE active=1").fetchall()
    conn.close(); return rows

def ajouter_alerte_db(ticker, type_alerte, valeur):
    conn=db();c=conn.cursor()
    c.execute("INSERT INTO alertes (ticker,type,valeur,date_creation) VALUES (?,?,?,?)",
              (ticker.upper(),type_alerte,valeur,datetime.now().isoformat()))
    conn.commit(); conn.close()

def supprimer_alerte_db(ticker):
    conn=db();c=conn.cursor()
    c.execute("UPDATE alertes SET active=0 WHERE ticker=? AND active=1",(ticker.upper(),))
    n=c.rowcount; conn.commit(); conn.close(); return n

def verifier_alertes():
    alertes = get_alertes()
    if not alertes: return
    conn=db();c=conn.cursor()
    for aid,ticker,type_alerte,valeur in alertes:
        quote = get_prix_finnhub(ticker)
        if not quote: continue
        prix = quote["prix"]; var = quote["var_1j"]
        declenche=False; msg=""
        if type_alerte=="sous" and prix<=valeur:
            declenche=True; msg=f"**{ticker}** est passe sous **{valeur}** — Prix : **{prix}**"
        elif type_alerte=="dessus" and prix>=valeur:
            declenche=True; msg=f"**{ticker}** a depasse **{valeur}** — Prix : **{prix}**"
        elif type_alerte=="hausse_pct" and var>=valeur:
            declenche=True; msg=f"**{ticker}** a progresse de **+{var:.2f}%** (seuil : +{valeur}%)"
        elif type_alerte=="baisse_pct" and var<=-valeur:
            declenche=True; msg=f"**{ticker}** a chute de **{var:.2f}%** (seuil : -{valeur}%)"
        if declenche:
            c.execute("UPDATE alertes SET active=0,date_declenchement=? WHERE id=?",
                      (datetime.now().isoformat(),aid))
            envoyer_embed_http(f"ALERTE — {ticker}",msg,0xFF6B00,
                               [{"name":"Action","value":f"Tape `!analyse {ticker}` pour l analyse complete","inline":False}])
    conn.commit(); conn.close()

# =====================================================
#   STATS AUTOMATIQUES (rapport du matin)
# =====================================================
def rapport_stats_automatique():
    conn = db(); c = conn.cursor()
    row = c.execute(
        "SELECT COUNT(*), SUM(succes_j1), AVG(var_reelle_j1) "
        "FROM signaux_momentum WHERE verifie_j1=1"
    ).fetchone()
    if not row or not row[0] or row[0] < 3:
        conn.close(); return

    total, reussites, gain_moy = row
    taux_global = round((reussites or 0) / total * 100, 1)

    top = c.execute(
        "SELECT ticker, COUNT(*) as nb, SUM(succes_j1) as ok, AVG(var_reelle_j1) as gain "
        "FROM signaux_momentum WHERE verifie_j1=1 "
        "GROUP BY ticker HAVING COUNT(*)>=3 "
        "ORDER BY AVG(var_reelle_j1) DESC LIMIT 5"
    ).fetchall()

    hier = (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")
    hier_rows = c.execute(
        "SELECT ticker, var_reelle_j1, succes_j1, variation_signal "
        "FROM signaux_momentum "
        "WHERE date_verif_j1 LIKE ? AND verifie_j1=1 "
        "ORDER BY var_reelle_j1 DESC LIMIT 5",
        (hier + "%",)
    ).fetchall()
    conn.close()

    if not top: return

    champs = []

    # Taux global
    emoji_g = "OK" if taux_global>=60 else "MOY" if taux_global>=40 else "KO"
    champs.append({
        "name":  "Taux de reussite global",
        "value": (emoji_g + " " + str(taux_global) + "% sur "
                  + str(int(total)) + " signaux | Gain moy : "
                  + str(round(gain_moy or 0, 2)) + "%"),
        "inline": False,
    })

    # Top tickers
    lines = []
    for ticker, nb, ok, gain in top:
        taux  = round((ok or 0) / nb * 100, 1)
        emoji = "OK" if taux>=60 else "MOY" if taux>=40 else "KO"
        lines.append(
            emoji + " **" + str(ticker) + "** : "
            + str(taux) + "% (" + str(int(nb)) + " signaux) | "
            + str(round(gain or 0, 2)) + "%"
        )
    champs.append({"name":"Top tickers fiables","value":"\n".join(lines),"inline":False})

    # Verifications d hier
    if hier_rows:
        hier_lines = []
        for ticker, var_reelle, succes, var_signal in hier_rows:
            emoji = "OK" if succes else "KO"
            hier_lines.append(
                emoji + " **" + str(ticker) + "**"
                + " : signal +" + str(round(var_signal or 0,1)) + "%"
                + " -> reel " + str(round(var_reelle or 0,2)) + "%"
            )
        champs.append({"name":"Verifications d hier","value":"\n".join(hier_lines),"inline":False})

    envoyer_embed_http(
        "Stats Predictions — " + datetime.now().strftime("%d/%m/%Y"),
        "Le bot avait-il raison ? Bilan automatique.",
        0x5865F2, champs
    )

# =====================================================
#   RAPPORT PRE-MARCHE (08h15)
# =====================================================
def rapport_pre_marche():
    print("Rapport pre-marche...")
    rapport_stats_automatique()
    time.sleep(1)

    fg      = get_fear_greed()
    macro   = get_macro()
    wl      = charger_watchlist()

    champs  = [
        {"name":"Fear & Greed","value":fg["emoji"]+" **"+str(fg["score"])+"/100 — "+fg["rating"]+"**","inline":False},
    ]

    for n_idx, m in macro.items():
        sg = "+" if m["variation"] >= 0 else ""
        champs.append({
            "name":  m["nom"],
            "value": "**" + str(m["valeur"]) + "** (" + sg + str(m["variation"]) + "%)",
            "inline": True,
        })

    if wl:
        champs.append({"name":"Ta watchlist","value":"  ".join("**"+t+"**" for t in wl),"inline":False})

    if fg["score"] <= 25:   conseil = "Peur extreme -> chercher les fondamentaux solides en zone support"
    elif fg["score"] <= 45: conseil = "Marche craintif -> etre selectif, attendre confirmation"
    elif fg["score"] <= 55: conseil = "Marche neutre -> suivre les signaux techniques"
    elif fg["score"] <= 75: conseil = "Marche optimiste -> momentum favorable, surveiller surextensions"
    else:                   conseil = "Avidite extreme -> risque correction, proteger les gains"

    champs.append({"name":"Strategie du jour","value":conseil,"inline":False})
    champs.append({"name":"Planning","value":
                   "09:00 -> Ouverture EU\n15:30 -> Ouverture US\n17:30 -> Rapport cloture EU\n22:30 -> Verification predictions + arret",
                   "inline":False})

    envoyer_embed_http(
        "Briefing Pre-Marche — " + datetime.now().strftime("%d/%m/%Y %H:%M"),
        "Resume du contexte avant ouverture",
        0x5865F2, champs
    )

def rapport_final():
    """Rapport final + verification predictions + arret."""
    print("Rapport final...")
    res = verifier_predictions()

    if res["verifications"] > 0:
        taux = round(res["ok_j1"]/res["verifications"]*100, 1) if res["verifications"]>0 else 0
        envoyer_embed_http(
            "Verification Predictions — " + datetime.now().strftime("%d/%m/%Y"),
            "Le bot avait-il raison aujourd hui ?",
            0x5865F2,
            [{"name":"Signaux verifies J+1","value":str(res["verifications"]),"inline":True},
             {"name":"Taux de reussite","value":str(taux)+"%","inline":True},
             {"name":"Conseil","value":"Tape `!stats` pour le detail complet par action","inline":False}]
        )

    envoyer_embed_http(
        "Bot en veille — Bonne nuit !",
        "Reprise demain a 08h00 automatiquement via Railway.",
        0x888780
    )
    time.sleep(30)
    os._exit(0)

# =====================================================
#   DISCORD — ENVOI HTTP
# =====================================================
def envoyer_embed_http(titre, description, couleur, champs=None):
    url  = f"https://discord.com/api/v10/channels/{CHANNEL_ID}/messages"
    hdrs = {"Authorization":f"Bot {DISCORD_TOKEN}","Content-Type":"application/json"}
    embed = {
        "title":       titre[:256],
        "description": description[:4096],
        "color":       couleur,
        "fields":      (champs or [])[:25],
        "footer":      {"text":"Scanner V6 — Finnhub + AV + Google News — Analyse educative"},
        "timestamp":   datetime.now(datetime.UTC if hasattr(datetime,'UTC') else __import__('datetime').timezone.utc).isoformat(),
    }
    try:
        requests.post(url, headers=hdrs, json={"embeds":[embed]}, timeout=10)
        time.sleep(0.5)
    except Exception as e:
        print(f"Discord : {e}")

# =====================================================
#   BOT DISCORD
# =====================================================
if DISCORD_PY:
    intents = discord.Intents.default(); intents.message_content = True
    bot     = commands.Bot(command_prefix="!", intents=intents, help_command=None)

    def envoyer_embed(titre, description, couleur, champs=None, channel=None):
        async def _s():
            ch = bot.get_channel(channel or CHANNEL_ID)
            if not ch: return
            em = discord.Embed(title=titre[:256], description=description[:4096], color=couleur)
            for f in (champs or [])[:25]:
                em.add_field(name=f["name"][:256], value=f["value"][:1024], inline=f.get("inline",False))
            em.set_footer(text="Scanner V6 — Finnhub + AV + Google News")
            em.timestamp = discord.utils.utcnow()
            await ch.send(embed=em)
        if bot.loop and bot.loop.is_running():
            bot.loop.call_soon_threadsafe(lambda: bot.loop.create_task(_s()))

    def envoyer_message(contenu, channel=None):
        async def _s():
            ch = bot.get_channel(channel or CHANNEL_ID)
            if ch: await ch.send(contenu[:2000])
        if bot.loop and bot.loop.is_running():
            bot.loop.call_soon_threadsafe(lambda: bot.loop.create_task(_s()))

    _cache = {"resultats":[],"secteurs":{},"momentum":[]}

    def lancer_scan_bg():
        def _run():
            try:
                resultats, par_secteur = analyser_actions()
                _cache["resultats"]    = resultats
                _cache["secteurs"]     = par_secteur
                fg = get_fear_greed()
                top= resultats[:5]

                secteur_fort   = max(par_secteur, key=par_secteur.get) if par_secteur else "N/A"
                secteur_faible = min(par_secteur, key=par_secteur.get) if par_secteur else "N/A"

                envoyer_embed(
                    "Scanner V6 — " + datetime.now().strftime("%d/%m/%Y %H:%M"),
                    ("**" + str(len(resultats)) + " actions** analysees\n\n"
                     "Fear & Greed : " + fg["emoji"] + " **" + str(fg["score"]) + "/100 — " + fg["rating"] + "**\n"
                     "Secteur fort : **" + secteur_fort + "**\n"
                     "Secteur faible : **" + secteur_faible + "**"),
                    0x5865F2
                )
                time.sleep(0.8)

                champs_top = []
                for i, a in enumerate(top, 1):
                    taux     = get_taux_reussite(a["ticker"])
                    taux_txt = " | Histo : " + str(taux) + "%" if taux else ""
                    sent     = a.get("sentiment")
                    sent_txt = " | News : " + str(sent["positif"]) + "% bull" if sent else ""
                    champs_top.append({
                        "name":   "#" + str(i) + " " + a["ticker"] + " — " + str(a["score"]) + "/100",
                        "value":  (str(a["prix"]) + " | " + str(a["var_1j"]) + "% 1j | RSI " + str(a["rsi"])
                                   + taux_txt + sent_txt + "\n" + a["secteur"]),
                        "inline": False,
                    })
                envoyer_embed("Top 5 Opportunites","",0x1D9E75,champs_top)
                time.sleep(0.8)

                if par_secteur:
                    sec = sorted(par_secteur.items(), key=lambda x: x[1], reverse=True)
                    envoyer_embed("Force par Secteur","",0x378ADD,
                                  [{"name":n,"value":str(s)+"/100","inline":True} for n,s in sec])

                verifier_alertes()

                # Momentum
                actions_m = detecter_momentum()
                _cache["momentum"] = actions_m
                if actions_m:
                    envoyer_embed(
                        "ALERTE MOMENTUM +" + str(SEUIL_MOMENTUM) + "%",
                        "**" + str(len(actions_m)) + " action(s)** ! Tape `!momentum` pour l analyse.",
                        0xFF0000,
                        [{"name":a["ticker"],"value":"+"+str(a["variation"])+"% | RSI "+str(a["rsi"]),"inline":True}
                         for a in actions_m[:8]]
                    )
                print("Scan V6 termine !")
            except Exception as e:
                envoyer_message("Erreur scan : " + str(e))
        threading.Thread(target=_run, daemon=True).start()

    @bot.event
    async def on_ready():
        print(f"Bot V6 connecte : {bot.user}")
        horaires_auto.start()
        verif_alertes_loop.start()
        envoyer_embed(
            "Scanner Bourse V6 — En ligne !",
            ("Sources : **Finnhub** (prix temps reel) + **Alpha Vantage** (indicateurs)"
             " + **Google News** (actualites) + **Reddit** (sentiment)\n\n"
             "Horaires : **08h00** demarrage -> **22h45** arret auto\n"
             "Dashboard : http://localhost:" + str(PORT) + "\n"
             "Tape `!help` pour les commandes"),
            0x1D9E75,
            [{"name":"Actions","value":str(len(TOUTES_ACTIONS)),"inline":True},
             {"name":"Secteurs","value":str(len(SECTEURS)),"inline":True},
             {"name":"Sources","value":"Finnhub + AV + Google News + Reddit","inline":True}]
        )
        import asyncio
        await asyncio.sleep(10)
        lancer_scan_bg()

    @tasks.loop(minutes=1)
    async def horaires_auto():
        h = datetime.now().strftime("%H:%M")
        if h == HORAIRES["pre_marche"]:
            threading.Thread(target=rapport_pre_marche, daemon=True).start()
        elif h == HORAIRES["ouverture_eu"]:
            envoyer_message("Ouverture EU — Scan en cours...")
            lancer_scan_bg()
        elif h == HORAIRES["ouverture_us"]:
            envoyer_message("Ouverture US — Scan en cours...")
            lancer_scan_bg()
        elif h == HORAIRES["cloture_eu"]:
            lancer_scan_bg()
        elif h == HORAIRES["verification"]:
            threading.Thread(target=rapport_final, daemon=True).start()

    @tasks.loop(hours=1)
    async def verif_alertes_loop():
        verifier_alertes()

    # ── COMMANDES ──

    @bot.command(name="scan")
    async def cmd_scan(ctx):
        await ctx.send("Scan en cours (environ 1 minute)...")
        lancer_scan_bg()

    @bot.command(name="momentum")
    async def cmd_momentum(ctx, seuil: float=None):
        seuil = seuil or SEUIL_MOMENTUM
        await ctx.send("Scan momentum +"+str(seuil)+"% sur "+str(len(TOUTES_ACTIONS))+" actions...")
        def _run():
            fg      = get_fear_greed()
            actions = detecter_momentum(seuil)
            if not actions:
                envoyer_message("Aucune action ne fait +"+str(seuil)+"% actuellement."); return
            envoyer_embed(
                "Momentum +" + str(seuil) + "%",
                ("**" + str(len(actions)) + " action(s)**\n"
                 "Fear & Greed : " + fg["emoji"] + " " + str(fg["score"]) + "/100"),
                0xFF6B00,
                [{"name":a["ticker"]+" ("+a["marche"]+")","value":"**+"+str(a["variation"])+"% ** | RSI "+str(a["rsi"])+" | Vol x"+str(a["vol_ratio"]),"inline":True}
                 for a in actions[:10]]
            )
            time.sleep(1)
            for action in actions[:3]:
                analyse = analyser_action_momentum(action)
                a = analyse["action"]; sc = analyse["score"]; t = a["ticker"]; v = a["variation"]
                taux     = get_taux_reussite(t)
                taux_txt = "\nHistorique reel : **" + str(taux) + "%** de reussite" if taux else ""
                sent     = a.get("sentiment")
                sent_txt = ""
                if sent:
                    sent_txt = "\nSentiment news : **" + str(sent["positif"]) + "% positif** | Buzz : " + str(sent["buzz"])

                news_txt = "\n".join("- "+n["titre"][:80] for n in analyse["news"][:3]) or "Aucune"
                champs   = [
                    {"name":"Donnees",     "value":str(a["prix"])+" | +"+str(v)+"% | RSI "+str(a["rsi"])+" | Vol x"+str(a["vol_ratio"]),"inline":False},
                    {"name":"Causes",      "value":" | ".join(analyse["causes"]),"inline":False},
                    {"name":"Google News", "value":news_txt,"inline":False},
                    {"name":"Historique J+3","value":"**"+str(analyse["hist_cont"]) + "%** en moy apres +" + str(SEUIL_MOMENTUM) + "%" + taux_txt + sent_txt,"inline":False},
                ]
                if analyse["reddit"]:
                    champs.append({"name":"Reddit","value":"\n".join("- "+r["titre"][:70] for r in analyse["reddit"][:2]),"inline":False})
                envoyer_embed(a["nom"]+" ("+t+") — +"+str(v)+"% "+a["marche"],"",0xFF6B00,champs)
                time.sleep(0.8)
                detail = "\n".join(d[0]+" -> "+d[1] for d in sc["details"][:7])
                envoyer_embed(
                    "Score continuation — " + t,
                    "J+1 : **"+str(sc["j1"])+"/10** | J+3 : **"+str(sc["j3"])+"/10**",
                    0x1D9E75 if sc["j1"]>=7 else 0xBA7517 if sc["j1"]>=5 else 0xE24B4A,
                    [{"name":"Demain (J+1)","value":barre(sc["j1"])+"\n"+sc["verdict_j1"],"inline":False},
                     {"name":"J+3",         "value":barre(sc["j3"])+"\n"+sc["verdict_j3"],"inline":False},
                     {"name":"Detail",      "value":detail,"inline":False}]
                )
                time.sleep(0.8)
        threading.Thread(target=_run, daemon=True).start()

    @bot.command(name="analyse")
    async def cmd_analyse(ctx, ticker: str):
        t = ticker.upper()
        await ctx.send("Analyse de **"+t+"**...")
        async with ctx.typing():
            data = await bot.loop.run_in_executor(None, generer_graphique, t)
            if data:
                f  = discord.File(io.BytesIO(data), filename=t+".png")
                em = discord.Embed(title=t+" — Graphique 90 jours", color=0x5865F2)
                em.set_image(url="attachment://"+t+".png")
                em.set_footer(text="Source : Finnhub | MM50 orange | MM200 vert | Bollinger bleu")
                await ctx.send(file=f, embed=em)
        def _run():
            try:
                quote = get_prix_finnhub(t)
                if not quote: envoyer_message("Prix introuvable pour "+t+"."); return
                candles = get_candles_finnhub(t, "D", 90)
                if candles and len(candles["closes"]) >= 10:
                    closes  = candles["closes"]
                    volumes = candles["volumes"]
                    rsi     = calculer_rsi_local(closes)
                    macd_d  = calculer_macd_local(closes)
                    boll_d  = calculer_bollinger_local(closes)
                    mm_d    = calculer_mm_local(closes)
                    vr      = calculer_volume_ratio(volumes)
                    var_5j  = calculer_variation(closes, 5)
                    sr      = {"support":round(min(candles["lows"][-20:]),4),"resistance":round(max(candles["highs"][-20:]),4)}
                else:
                    rsi=50; macd_d={"signal":"neutre","histogramme":0}
                    boll_d={"signal":"neutre","haut":0,"bas":0,"mm":0}
                    mm_d={}; vr=1.0; var_5j=0.0; sr={"support":0,"resistance":0}

                sentiment = get_sentiment_finnhub(t)
                news      = get_news_action(t)
                reddit    = get_reddit(t)
                causes    = identifier_causes([n["titre"] for n in news])
                taux      = get_taux_reussite(t)
                hc        = historique_continuation_local(t)
                sc        = score_continuation(
                    quote["var_1j"], rsi, vr, macd_d, causes,
                    sum(r.get("score",0) for r in reddit), hc,
                    boll_d, mm_d, ticker=t, sentiment_fh=sentiment
                )

                taux_txt = ""
                if taux:
                    taux_txt = "\n\nHistorique reel : **" + str(taux) + "% de reussite** sur ce ticker"

                sent_txt = ""
                if sentiment:
                    sent_txt = (str(sentiment["positif"]) + "% bull / "
                                + str(sentiment["negatif"]) + "% bear | "
                                + str(sentiment["articles"]) + " articles")

                champs = [
                    {"name":"Prix","value":str(quote["prix"])+" | "+str(quote["var_1j"])+"% (1j) | "+str(var_5j)+"% (5j)","inline":False},
                    {"name":"RSI","value":str(rsi),"inline":True},
                    {"name":"MACD","value":macd_d["signal"]+" (histo : "+str(macd_d["histogramme"])+")","inline":True},
                    {"name":"Bollinger","value":boll_d["signal"]+" | Haut : "+str(boll_d["haut"])+" | Bas : "+str(boll_d["bas"]),"inline":True},
                    {"name":"Volume","value":"x"+str(vr),"inline":True},
                    {"name":"MM50","value":str(mm_d.get("mm50","?"))+" ("+mm_d.get("mm50_signal","?")+")" if mm_d.get("mm50") else "N/A","inline":True},
                    {"name":"Support/Resistance","value":"Support : "+str(sr["support"])+" | Resistance : "+str(sr["resistance"]),"inline":True},
                    {"name":"Sentiment Finnhub","value":sent_txt or "N/A","inline":False},
                    {"name":"Causes (Google News + Finnhub)","value":" | ".join(causes),"inline":False},
                    {"name":"Google News","value":"\n".join("- "+n["titre"][:80] for n in news[:3]) or "Aucune","inline":False},
                ]
                if reddit:
                    champs.append({"name":"Reddit","value":"\n".join("- "+r["titre"][:70] for r in reddit[:2]),"inline":False})

                envoyer_embed("Analyse — "+t, taux_txt, 0x5865F2, champs)
                time.sleep(0.8)
                detail = "\n".join(d[0]+" -> "+d[1] for d in sc["details"][:7])
                envoyer_embed(
                    "Score continuation — " + t,
                    "J+1 : **"+str(sc["j1"])+"/10** | J+3 : **"+str(sc["j3"])+"/10**",
                    0x1D9E75 if sc["j1"]>=7 else 0xBA7517 if sc["j1"]>=5 else 0xE24B4A,
                    [{"name":"Demain (J+1)","value":barre(sc["j1"])+"\n"+sc["verdict_j1"],"inline":False},
                     {"name":"J+3",         "value":barre(sc["j3"])+"\n"+sc["verdict_j3"],"inline":False},
                     {"name":"Detail",      "value":detail,"inline":False}]
                )
            except Exception as e:
                envoyer_message("Erreur analyse "+t+" : "+str(e))
        threading.Thread(target=_run, daemon=True).start()

    @bot.command(name="news")
    async def cmd_news(ctx, ticker: str):
        t = ticker.upper()
        await ctx.send("Actualites de **"+t+"**...")
        def _run():
            news      = get_news_action(t)
            sentiment = get_sentiment_finnhub(t)
            reddit    = get_reddit(t)
            champs    = []
            if sentiment:
                champs.append({
                    "name":  "Sentiment Finnhub (scoring IA des news)",
                    "value": ("Score : **" + str(sentiment["score"]) + "** | "
                              + str(sentiment["positif"]) + "% bull / " + str(sentiment["negatif"]) + "% bear\n"
                              + str(sentiment["articles"]) + " articles la semaine derniere"),
                    "inline": False,
                })
            if news:
                champs.append({
                    "name":  "Google News + Finnhub",
                    "value": "\n".join("- **["+n["titre"][:70]+"]**("+n.get("url","")+")" for n in news[:4]),
                    "inline": False,
                })
            if reddit:
                champs.append({
                    "name":  "Reddit",
                    "value": "\n".join("- "+r["titre"][:80]+" (up:"+str(r["score"])+")" for r in reddit[:3]),
                    "inline": False,
                })
            couleur = 0x1D9E75 if sentiment and sentiment["score"]>0.6 else 0xE24B4A if sentiment and sentiment["score"]<0.4 else 0x5865F2
            envoyer_embed("Actualites — "+t, "", couleur, champs)
        threading.Thread(target=_run, daemon=True).start()

    @bot.command(name="stats")
    async def cmd_stats(ctx, ticker: str=None):
        if ticker:
            t = ticker.upper()
            await ctx.send("Stats reelles pour **"+t+"**...")
            stats = get_stats_ticker(t)
            champs = []
            for horizon, label in [("j1","J+1 (lendemain)"),("j3","J+3 (3 jours)")]:
                s = stats[horizon]
                if not s:
                    champs.append({"name":label,"value":"Pas assez de donnees (min 3 signaux)","inline":False}); continue
                emoji = "OK" if s["taux"]>=60 else "MOY" if s["taux"]>=40 else "KO"
                champs.append({
                    "name":  label,
                    "value": (emoji + " Taux : **" + str(s["taux"]) + "%** (" + str(s["reussites"]) + "/" + str(s["total"]) + " signaux)\n"
                              + "Gain moyen : **" + str(s["gain_moy"]) + "%**\n"
                              + "Gain quand OK : **" + str(s["gain_ok"]) + "%** | Perte quand KO : **" + str(s["perte_ko"]) + "%**\n"
                              + "Meilleur : **" + str(s["meilleur"]) + "%** | Pire : **" + str(s["pire"]) + "%**"),
                    "inline": False,
                })
            envoyer_embed("Stats Reelles — "+t,"Basees sur les predictions verifiees automatiquement",0x5865F2,champs)
        else:
            await ctx.send("Stats globales...")
            def _run():
                stats = get_stats_globales()
                if stats["total_verif"] < 3:
                    envoyer_message("Pas encore assez de donnees. Lance `!momentum` pour commencer a enregistrer des signaux."); return
                champs = [{"name":"Global","value":"**"+str(stats["taux_global"])+"%** de reussite sur "+str(stats["total_verif"])+" signaux","inline":False}]
                for ticker,nb,reussites,gain_moy in stats["par_ticker"][:8]:
                    taux  = round((reussites or 0)/nb*100, 1) if nb>0 else 0
                    emoji = "OK" if taux>=60 else "MOY" if taux>=40 else "KO"
                    champs.append({
                        "name":  emoji+" "+str(ticker),
                        "value": "Taux : **"+str(taux)+"%** | Gain moy : **"+str(round(gain_moy or 0,2))+"%** | "+str(int(nb))+" signaux",
                        "inline": True,
                    })
                envoyer_embed("Stats Globales — Taux de Reussite","Top tickers selon les predictions verifiees",0x5865F2,champs)
            threading.Thread(target=_run, daemon=True).start()

    @bot.command(name="marche")
    async def cmd_marche(ctx):
        def _run():
            fg    = get_fear_greed()
            macro = get_macro()
            champs= [{"name":"Fear & Greed","value":fg["emoji"]+" **"+str(fg["score"])+"/100 — "+fg["rating"]+"**","inline":False}]
            for n_idx, m in macro.items():
                sg = "+" if m["variation"]>=0 else ""
                champs.append({"name":m["nom"],"value":"**"+str(m["valeur"])+"** ("+sg+str(m["variation"])+"%)","inline":True})
            if fg["score"]<=25:   conseil="Peur extreme -> opportunites sur fondamentaux solides"
            elif fg["score"]<=45: conseil="Marche craintif -> etre selectif"
            elif fg["score"]<=55: conseil="Marche neutre -> signaux techniques"
            elif fg["score"]<=75: conseil="Marche optimiste -> momentum favorable"
            else:                 conseil="Avidite extreme -> risque correction"
            champs.append({"name":"Analyse","value":conseil,"inline":False})
            envoyer_embed("Contexte de Marche — "+datetime.now().strftime("%d/%m/%Y %H:%M"),"",0x5865F2,champs)
        threading.Thread(target=_run, daemon=True).start()

    @bot.command(name="prix")
    async def cmd_prix(ctx, ticker: str):
        t = ticker.upper()
        quote = get_prix_finnhub(t)
        if not quote: await ctx.send("Ticker introuvable : "+ticker); return
        sg = "+" if quote["var_1j"]>=0 else ""
        em = discord.Embed(
            title=t+" ("+nom(t)+") — Prix temps reel",
            description="**"+str(quote["prix"])+"** | "+sg+str(quote["var_1j"])+"% (1j)",
            color=0x1D9E75 if quote["var_1j"]>=0 else 0xE24B4A
        )
        em.add_field(name="Haut",    value=str(quote["haut"]),     inline=True)
        em.add_field(name="Bas",     value=str(quote["bas"]),      inline=True)
        em.add_field(name="Ouverture",value=str(quote["ouverture"]),inline=True)
        em.set_footer(text="Source : Finnhub — temps reel")
        em.timestamp = discord.utils.utcnow()
        await ctx.send(embed=em)

    @bot.command(name="graphique")
    async def cmd_graphique(ctx, ticker: str):
        t = ticker.upper()
        async with ctx.typing():
            data = await bot.loop.run_in_executor(None, generer_graphique, t)
            if data:
                f  = discord.File(io.BytesIO(data), filename=t+".png")
                em = discord.Embed(title=t+" — Graphique 90 jours", color=0x5865F2)
                em.set_image(url="attachment://"+t+".png")
                em.set_footer(text="Finnhub | MM50 orange | MM200 vert | Bollinger bleu")
                await ctx.send(file=f, embed=em)
            else: await ctx.send("Graphique indisponible pour "+t+".")

    @bot.command(name="alerte")
    async def cmd_alerte(ctx, ticker: str, valeur: str):
        t = ticker.upper()
        try:
            if "%" in valeur:
                v  = float(valeur.replace("%","").replace("+","").replace("-",""))
                ta = "hausse_pct" if "+" in valeur else "baisse_pct"
                desc = "hausse de +"+str(v)+"%" if ta=="hausse_pct" else "baisse de -"+str(v)+"%"
            else:
                v  = float(valeur.replace("+",""))
                ta = "dessus" if "+" in valeur or v>0 else "sous"
                desc = "au-dessus de "+str(v) if ta=="dessus" else "sous "+str(v)
            ajouter_alerte_db(t, ta, v)
            await ctx.send("Alerte creee : **"+t+"** -> si "+desc)
        except Exception as e:
            await ctx.send("Usage : `!alerte NVDA 150` ou `!alerte NVDA +5%`\nErreur : "+str(e))

    @bot.command(name="alertes")
    async def cmd_alertes(ctx):
        al = get_alertes()
        if not al: await ctx.send("Aucune alerte. `!alerte TICKER VALEUR`"); return
        em = discord.Embed(title="Alertes actives ("+str(len(al))+")", color=0xF0997B)
        for a in al: em.add_field(name=a[1], value=str(a[2])+" "+a[3], inline=True)
        await ctx.send(embed=em)

    @bot.command(name="supprimer")
    async def cmd_supprimer(ctx, ticker: str):
        n = supprimer_alerte_db(ticker)
        await ctx.send(("OK" if n>0 else "Aucune alerte")+" pour **"+ticker.upper()+"** ("+str(n)+" supprimee(s)).")

    @bot.command(name="watch")
    async def cmd_watch(ctx, ticker: str):
        ok = ajouter_watchlist(ticker)
        await ctx.send(("Ajoute" if ok else "Deja dans")+" ta watchlist : **"+ticker.upper()+"**.")

    @bot.command(name="unwatch")
    async def cmd_unwatch(ctx, ticker: str):
        ok = retirer_watchlist(ticker)
        await ctx.send(("Retire" if ok else "Non trouve")+" : **"+ticker.upper()+"**.")

    @bot.command(name="watchlist")
    async def cmd_watchlist(ctx):
        wl = charger_watchlist()
        if not wl: await ctx.send("Watchlist vide. `!watch TICKER`"); return
        em = discord.Embed(title="Watchlist ("+str(len(wl))+" actions)",
                           description="  ".join("**"+t+"**" for t in wl), color=0x5865F2)
        await ctx.send(embed=em)

    @bot.command(name="backtest")
    async def cmd_backtest(ctx, ticker: str):
        t = ticker.upper()
        await ctx.send("Backtest **"+t+"** sur 1 an (Finnhub)...")
        def _run():
            hc = historique_continuation_local(t, 5.0)
            candles = get_candles_finnhub(t, "D", 365)
            if not candles or len(candles["closes"]) < 30:
                envoyer_message("Pas assez de donnees pour "+t+"."); return
            closes  = candles["closes"]
            signaux = []
            for i in range(14, len(closes)-5):
                if calculer_rsi_local(closes[:i]) < 35:
                    pe = closes[i]; ps = closes[i+5]
                    g  = round(((ps-pe)/pe)*100, 2)
                    signaux.append(g)
            if not signaux:
                envoyer_message("Aucun signal RSI<35 sur 1 an pour "+t+"."); return
            nb_g  = sum(1 for g in signaux if g > 0)
            taux  = round(nb_g/len(signaux)*100, 1)
            envoyer_embed(
                "Backtest "+t+" — 1 an (RSI<35 -> J+5)",
                str(len(signaux))+" signaux detectes.",
                0x1D9E75 if sum(signaux)>0 else 0xE24B4A,
                [{"name":"Taux reussite","value":str(taux)+"%","inline":True},
                 {"name":"Gain moyen",   "value":str(round(sum(signaux)/len(signaux),2))+"%","inline":True},
                 {"name":"Gain total",   "value":str(round(sum(signaux),2))+"%","inline":True},
                 {"name":"Meilleur",     "value":str(round(max(signaux),2))+"%","inline":True},
                 {"name":"Pire",         "value":str(round(min(signaux),2))+"%","inline":True},
                 {"name":"Continuation historique",
                  "value":"En moy apres +5% : **"+str(hc)+"%** a J+3","inline":False}]
            )
        threading.Thread(target=_run, daemon=True).start()

    @bot.command(name="buy")
    async def cmd_buy(ctx, ticker: str, quantite: float, prix: float):
        acheter(ticker, quantite, prix)
        await ctx.send("Achat : **"+ticker.upper()+"** — "+str(quantite)+" x "+str(prix)+"$ = "+str(round(quantite*prix,2))+"$")

    @bot.command(name="sell")
    async def cmd_sell(ctx, ticker: str, quantite: float):
        pnl, pa = vendre(ticker, quantite)
        if pnl is None: await ctx.send("Erreur : "+str(pa)); return
        await ctx.send(("Gain" if pnl>=0 else "Perte")+" — Vente **"+ticker.upper()+"** — "+str(quantite)+" x "+str(pa)+"$ | P&L : **"+str(pnl)+"$**")

    @bot.command(name="portfolio")
    async def cmd_portfolio(ctx):
        positions = charger_portefeuille()
        if not positions: await ctx.send("Portefeuille vide. `!buy TICKER QTE PRIX`"); return
        champs=[]; pnl_tot=0; val_tot=0
        for pos in positions:
            quote = get_prix_finnhub(pos["ticker"])
            pa    = quote["prix"] if quote else pos["prix_achat"]
            pnl   = round((pa-pos["prix_achat"])*pos["quantite"], 2)
            pct   = round((pa-pos["prix_achat"])/pos["prix_achat"]*100, 2)
            val   = round(pa*pos["quantite"], 2)
            pnl_tot += pnl; val_tot += val
            champs.append({
                "name":  ("OK" if pnl>=0 else "KO")+" "+pos["ticker"]+" x"+str(pos["quantite"]),
                "value": "PRU "+str(pos["prix_achat"])+"$ -> "+str(pa)+"$ | "+str(val)+"$ | **"+str(pnl)+"$ ("+str(pct)+"%)**",
                "inline": False,
            })
        envoyer_embed("Portefeuille — "+str(round(val_tot,2))+"$","P&L total : **"+str(pnl_tot)+"$**",
                      0x1D9E75 if pnl_tot>=0 else 0xE24B4A, champs)

    @bot.command(name="semaine")
    async def cmd_semaine(ctx):
        await ctx.send("Rapport hebdomadaire...")
        def _run():
            conn=db();c=conn.cursor()
            date_min=(datetime.now()-timedelta(days=7)).isoformat()
            rows=c.execute(
                "SELECT ticker,AVG(score),MAX(var_1j),COUNT(*) FROM scans "
                "WHERE date>? GROUP BY ticker ORDER BY AVG(score) DESC LIMIT 8",
                (date_min,)
            ).fetchall()
            conn.close()
            if not rows: envoyer_message("Pas assez de donnees cette semaine."); return
            champs=[{"name":"#"+str(i+1)+" "+r[0],"value":"Score moy : **"+str(round(r[1],1))+"/100** | Max : +"+str(round(r[2],2))+"%","inline":True}
                    for i,r in enumerate(rows)]
            envoyer_embed("Rapport Hebdo — "+datetime.now().strftime("%d/%m/%Y"),"",0x5865F2,champs)
        threading.Thread(target=_run, daemon=True).start()

    @bot.command(name="help")
    async def cmd_help(ctx):
        em = discord.Embed(
            title="Scanner V6 — Commandes",
            description="Sources : Finnhub (temps reel) + Alpha Vantage + Google News + Reddit",
            color=0x5865F2
        )
        cmds = [
            ("!scan",            "Scan general complet"),
            ("!momentum [seuil]","Actions +10% + score J+1/J+3"),
            ("!analyse TICKER",  "Analyse complete + graphique"),
            ("!news TICKER",     "Actualites + sentiment Finnhub"),
            ("!stats TICKER",    "Taux de reussite reel des signaux"),
            ("!stats",           "Stats globales"),
            ("!marche",          "Fear & Greed + indices"),
            ("!prix TICKER",     "Prix temps reel Finnhub"),
            ("!graphique TICKER","Graphique MM50/200 + Bollinger"),
            ("!alerte NVDA 150", "Alerte si NVDA < 150$"),
            ("!alerte NVDA +5%", "Alerte si NVDA +5%"),
            ("!alertes",         "Voir alertes actives"),
            ("!supprimer TICKER","Supprimer alerte"),
            ("!watch TICKER",    "Ajouter watchlist"),
            ("!unwatch TICKER",  "Retirer watchlist"),
            ("!watchlist",       "Voir watchlist"),
            ("!backtest TICKER", "Backtest RSI 1 an"),
            ("!buy T QTE PRIX",  "Enregistrer achat"),
            ("!sell T QTE",      "Enregistrer vente"),
            ("!portfolio",       "Portefeuille + P&L"),
            ("!semaine",         "Rapport hebdomadaire"),
        ]
        for c,d in cmds: em.add_field(name=c, value=d, inline=True)
        em.set_footer(text="Dashboard : http://localhost:"+str(PORT))
        await ctx.send(embed=em)

# =====================================================
#   DASHBOARD FLASK
# =====================================================
HTML = """<!DOCTYPE html><html lang="fr"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Scanner V6</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,sans-serif;background:#0f0f10;color:#e2e0d8;padding:2rem}
h1{font-size:1.4rem;font-weight:500;margin-bottom:1.5rem}
h2{font-size:.85rem;font-weight:500;color:#9c9a92;text-transform:uppercase;letter-spacing:.06em;margin-bottom:1rem}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:2rem}
.card{background:#1a1a1c;border-radius:12px;padding:1rem;border:.5px solid #333}
.card .label{font-size:12px;color:#9c9a92;margin-bottom:6px}.card .value{font-size:22px;font-weight:500}
table{width:100%;border-collapse:collapse;font-size:13px;margin-bottom:2rem}
th{text-align:left;padding:8px 12px;color:#9c9a92;font-weight:400;border-bottom:.5px solid #333}
td{padding:10px 12px;border-bottom:.5px solid #1f1f20}tr:hover td{background:#1a1a1c}
.up{color:#1D9E75}.down{color:#E24B4A}
.sh{color:#1D9E75;font-weight:500}.sm{color:#BA7517;font-weight:500}.sl{color:#E24B4A;font-weight:500}
.section{margin-bottom:2.5rem}.pp{color:#1D9E75}.pn{color:#E24B4A}
.refresh{float:right;font-size:12px;color:#666}
.source-badge{background:#1a1a1c;border:.5px solid #333;border-radius:100px;padding:2px 8px;font-size:11px;margin-right:4px}
</style></head><body>
<h1>Scanner Bourse V6 <span class="refresh">{{ now }} — <a href="/refresh" style="color:#5865F2">Actualiser</a></span></h1>
<div style="margin-bottom:1.5rem;font-size:13px;color:#9c9a92">
  Sources :
  <span class="source-badge">Finnhub (temps reel)</span>
  <span class="source-badge">Alpha Vantage (indicateurs)</span>
  <span class="source-badge">Google News (actualites)</span>
  <span class="source-badge">Reddit (sentiment)</span>
</div>
<div class="section"><h2>Resume</h2>
<div class="grid">
  <div class="card"><div class="label">Actions analysees</div><div class="value">{{ s.total }}</div></div>
  <div class="card"><div class="label">Score moyen</div><div class="value">{{ s.score_moy }}<span style="font-size:14px;color:#666">/100</span></div></div>
  <div class="card"><div class="label">RSI&lt;32</div><div class="value">{{ s.survente }}</div></div>
  <div class="card"><div class="label">Predictions verifiees</div><div class="value" style="color:#5865F2">{{ s.predictions }}</div></div>
  <div class="card"><div class="label">Taux reussite global</div>
    <div class="value {{ 'up' if s.taux_global>=60 else 'down' if s.taux_global<40 else '' }}">
      {{ s.taux_global }}%
    </div>
  </div>
</div></div>
<div class="section"><h2>Top 20 Opportunites</h2>
<table>
<tr><th>#</th><th>Ticker</th><th>Secteur</th><th>Prix</th><th>1j</th><th>5j</th><th>RSI</th><th>MACD</th><th>Bollinger</th><th>Sentiment</th><th>Score</th><th>Taux Reel</th></tr>
{% for a in top20 %}<tr>
  <td>{{ loop.index }}</td>
  <td><strong>{{ a.ticker }}</strong><br><span style="font-size:11px;color:#666">{{ a.nom }}</span></td>
  <td style="font-size:11px;color:#666">{{ a.secteur }}</td>
  <td>{{ a.prix }}</td>
  <td class="{{ 'up' if a.var_1j>=0 else 'down' }}">{{ '+' if a.var_1j>=0 else '' }}{{ a.var_1j }}%</td>
  <td class="{{ 'up' if a.var_5j>=0 else 'down' }}">{{ '+' if a.var_5j>=0 else '' }}{{ a.var_5j }}%</td>
  <td>{{ a.rsi }}</td>
  <td class="{{ 'up' if a.macd and a.macd.signal=='haussier' else 'down' }}" style="font-size:11px">{{ a.macd.signal if a.macd else '-' }}</td>
  <td style="font-size:11px">{{ a.boll.signal if a.boll else '-' }}</td>
  <td style="font-size:11px">{% if a.sentiment %}{{ a.sentiment.positif }}% bull{% else %}-{% endif %}</td>
  <td class="{{ 'sh' if a.score>=75 else 'sm' if a.score>=55 else 'sl' }}">{{ a.score }}/100</td>
  <td style="font-size:11px">
    {% if a.taux_reel is not none %}<span style="color:{{ '#1D9E75' if a.taux_reel>=60 else '#E24B4A' if a.taux_reel<40 else '#BA7517' }}">{{ a.taux_reel }}%</span>
    {% else %}<span style="color:#666">-</span>{% endif %}
  </td>
</tr>{% endfor %}</table></div>
{% if portfolio %}<div class="section"><h2>Portefeuille</h2>
<table><tr><th>Ticker</th><th>Qte</th><th>PRU</th><th>Actuel</th><th>Valeur</th><th>P&L</th></tr>
{% for p in portfolio %}<tr>
  <td><strong>{{ p.ticker }}</strong></td><td>{{ p.quantite }}</td>
  <td>{{ p.prix_achat }}$</td><td>{{ p.prix_actuel }}$</td><td>{{ p.valeur }}$</td>
  <td class="{{ 'pp' if p.pnl>=0 else 'pn' }}">{{ '+' if p.pnl>=0 else '' }}{{ p.pnl }}$</td>
</tr>{% endfor %}</table></div>{% endif %}
<script>setTimeout(()=>location.reload(),300000);</script>
</body></html>"""

app    = Flask(__name__)
_cache = {"resultats":[], "secteurs":{}}

@app.route("/")
def dashboard():
    global _cache
    if not _cache["resultats"]:
        _cache["resultats"], _cache["secteurs"] = analyser_actions()

    r   = _cache["resultats"]
    pf  = charger_portefeuille()
    port= []
    for pos in pf:
        quote = get_prix_finnhub(pos["ticker"])
        pa    = quote["prix"] if quote else pos["prix_achat"]
        port.append({**pos,"prix_actuel":pa,"valeur":round(pa*pos["quantite"],2),"pnl":round((pa-pos["prix_achat"])*pos["quantite"],2)})

    conn = db(); c = conn.cursor()
    vt   = c.execute("SELECT COUNT(*) FROM signaux_momentum WHERE verifie_j1=1").fetchone()[0]
    vr   = c.execute("SELECT SUM(succes_j1) FROM signaux_momentum WHERE verifie_j1=1").fetchone()[0] or 0
    conn.close()
    taux_global = round(vr/vt*100,1) if vt>0 else 0

    top20 = []
    for a in r[:20]:
        taux = get_taux_reussite(a["ticker"])
        top20.append({**a,"taux_reel":taux})

    s = {
        "total":      len(r),
        "score_moy":  round(sum(x["score"] for x in r)/len(r),1) if r else 0,
        "survente":   sum(1 for x in r if x["rsi"]<32),
        "predictions":vt,
        "taux_global":taux_global,
    }
    return render_template_string(HTML, now=datetime.now().strftime("%d/%m/%Y %H:%M"),
                                  top20=top20, portfolio=port, s=s)

@app.route("/refresh")
def refresh():
    global _cache
    _cache["resultats"], _cache["secteurs"] = analyser_actions()
    return {"ok":True,"actions":len(_cache["resultats"])}

@app.route("/health")
def health():
    return {"status":"ok","time":datetime.now().isoformat()}

# =====================================================
#   LANCEMENT
# =====================================================
if __name__ == "__main__":
    print("="*55)
    print("  SCANNER BOURSE V6 — FINNHUB + AV + GOOGLE NEWS")
    print("="*55)
    print(f"  Actions scanner  : {len(TOUTES_SECTEURS)}")
    print(f"  Actions momentum : {len(TOUTES_ACTIONS)}")
    print(f"  Sources          : Finnhub + Alpha Vantage + Google News + Reddit")
    print(f"  Port             : {PORT}")
    print("="*55)

    if FINNHUB_KEY == "TON_FINNHUB_KEY":
        print("ATTENTION : Configure FINNHUB_KEY !")
        print("  -> finnhub.io -> Get free API key")
    if ALPHAVANTAGE_KEY == "TON_ALPHAVANTAGE_KEY":
        print("ATTENTION : Configure ALPHAVANTAGE_KEY !")
        print("  -> alphavantage.co -> Get Free API Key")

    init_db()

    threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False),
        daemon=True
    ).start()
    print(f"Dashboard demarre sur le port {PORT}")

    if DISCORD_PY:
        bot.run(DISCORD_TOKEN)
    else:
        print("Installe discord.py : pip install discord.py")
        while True: time.sleep(60)
PYEOF
echo "V6 final : $(wc -l < /home/claude/scanner_v6.py) lignes"
python3 -c "import ast; ast.parse(open('/home/claude/scanner_v6.py').read()); print('Syntaxe OK')"
Sortie

V6 final : 2174 lignes
Syntaxe OK
