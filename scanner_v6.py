"""
=====================================================
   SCANNER BOURSE V6 — RAILWAY 24/7 OPTIMISE
=====================================================

NOUVEAUTES V6 :
  - Horaires intelligents (demarre 30min avant marche,
    s'arrete apres cloture US)
  - Verification automatique des predictions J+1 et J+3
  - Taux de reussite reel par action et par secteur
  - Score ameliore par l'historique reel
  - Compatible Railway (Procfile + requirements.txt inclus)
  - Arret automatique propre en fin de journee

COMMANDES DISCORD :
  !scan              -> Scan general
  !momentum          -> Actions +10% + score continuation
  !analyse NVDA      -> Analyse complete + score
  !stats NVDA        -> Taux de reussite reel des signaux
  !stats             -> Stats globales de tous les signaux
  !verifie           -> Verifie les predictions en attente
  !marche            -> Fear & Greed + contexte macro
  !macro             -> Taux, VIX, or, petrole
  !earnings          -> Resultats entreprises semaine
  !alerte NVDA 150   -> Alerte si NVDA < 150$
  !alerte NVDA +5%   -> Alerte si NVDA +5%
  !alertes           -> Voir alertes actives
  !supprimer NVDA    -> Supprimer alerte
  !graphique NVDA    -> Graphique avec indicateurs
  !watch NVDA        -> Ajouter watchlist
  !unwatch NVDA      -> Retirer watchlist
  !watchlist         -> Voir watchlist
  !backtest NVDA     -> Backtest RSI 6 mois
  !buy NVDA 10 500   -> Enregistrer achat
  !sell NVDA 5       -> Enregistrer vente
  !portfolio         -> Portefeuille + P&L
  !semaine           -> Rapport hebdomadaire
  !prix NVDA         -> Prix actuel
  !help              -> Toutes les commandes

DASHBOARD :
  http://localhost:5000 (local)
  https://ton-app.railway.app (Railway)
"""

import yfinance as yf
import feedparser
import requests
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
from bs4 import BeautifulSoup
from flask import Flask, render_template_string
import os
import sys

# Import module stats automatiques
try:
    from stats_auto import rapport_stats_automatique as _stats_auto
    HAS_STATS_AUTO = True
except ImportError:
    HAS_STATS_AUTO = False

try:
    import discord
    from discord.ext import commands, tasks
    DISCORD_PY = True
except ImportError:
    DISCORD_PY = False
    print("discord.py non installe")

# =====================================================
#   CONFIGURATION
#   Sur Railway : met ces valeurs dans les variables
#   d'environnement (Settings -> Variables)
# =====================================================
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "TON_TOKEN_BOT_DISCORD")
CHANNEL_ID    = int(os.environ.get("CHANNEL_ID", "123456789012345678"))
PORT          = int(os.environ.get("PORT", 5000))

MAX_WORKERS   = 20
SEUIL_MOMENTUM = 10.0
SEUIL_VEILLE   = 3.0

# =====================================================
#   HORAIRES MARCHES
# =====================================================
HORAIRES = {
    "demarrage":      "08:00",   # Bot demarre (via Railway cron)
    "pre_marche":     "08:15",   # Analyse pre-ouverture + briefing
    "ouverture_eu":   "09:00",   # Ouverture EU
    "ouverture_us":   "15:30",   # Ouverture US (14h30 UTC+1)
    "cloture_eu":     "17:30",   # Cloture EU + rapport
    "cloture_us":     "22:15",   # Cloture US + rapport final
    "verification":   "22:30",   # Verification predictions J+1
    "arret":          "22:45",   # Arret automatique du bot
}

def heure_actuelle():
    return datetime.now().strftime("%H:%M")

def est_heures_marche():
    h = datetime.now().hour
    return 8 <= h < 23

def minutes_avant_arret():
    """Retourne les minutes avant l'arret automatique."""
    now = datetime.now()
    arret = now.replace(hour=22, minute=45, second=0)
    if now > arret:
        return 0
    return int((arret - now).total_seconds() / 60)

# =====================================================
#   ACTIONS
# =====================================================
ACTIONS_US_GRANDES = [
    "NVDA","MSFT","AAPL","GOOGL","META","AMZN","TSLA","AMD","INTC","CRM",
    "ADBE","ORCL","QCOM","AVGO","NOW","SNOW","PLTR","UBER","ABNB",
    "JPM","BAC","GS","MS","WFC","BLK","AXP","V","MA","PYPL","SQ",
    "JNJ","UNH","PFE","MRK","ABBV","LLY","BMY","AMGN","GILD","CVS","MRNA",
    "XOM","CVX","COP","SLB","EOG","PSX","VLO","MPC","OXY","HAL",
    "WMT","COST","TGT","HD","MCD","SBUX","NKE","PG","KO","PEP","DIS","NFLX",
    "CAT","DE","HON","GE","MMM","RTX","LMT","BA","UPS","FDX",
    "AMT","PLD","EQIX","SPG","O",
]
ACTIONS_US_PME = [
    "SMCI","MARA","RIOT","CLSK","IREN","CELH","HIMS","RXRX",
    "AEHR","AMBA","CRDO","FORM","POWI","DKNG","PENN",
    "ASTS","RKLB","LUNR","ACHR","JOBY","IONQ","RGTI","QUBT",
    "SOFI","AFRM","UPST","LC","PLUG","FCEL","BLDP",
    "WOLF","LSCC","MCHP","SWKS","LASR","COHU","ONTO",
]
ACTIONS_FR = [
    "MC.PA","AIR.PA","TTE.PA","SAN.PA","BNP.PA","OR.PA","SU.PA","AI.PA",
    "SAF.PA","RI.PA","DSY.PA","CAP.PA","KER.PA","RMS.PA","EL.PA",
    "DG.PA","BN.PA","ACA.PA","GLE.PA","CS.PA","LR.PA","STM.PA","WRL.PA",
    "OVH.PA","RNO.PA","VIV.PA","PUB.PA","SGO.PA",
    "ALO.PA","ATO.PA","CNP.PA","FDJ.PA","GTT.PA","HO.PA","TFI.PA",
]

SECTEURS = {
    "Tech US":       ["NVDA","MSFT","AAPL","GOOGL","META","AMZN","TSLA","AMD","INTC","CRM","ADBE","ORCL","QCOM","AVGO","NOW"],
    "Finance US":    ["JPM","BAC","GS","MS","WFC","BLK","AXP","V","MA","PYPL"],
    "Sante US":      ["JNJ","UNH","PFE","MRK","ABBV","LLY","BMY","AMGN","GILD","CVS"],
    "Energie US":    ["XOM","CVX","COP","SLB","EOG","PSX","VLO","MPC","OXY","HAL"],
    "Conso US":      ["WMT","COST","TGT","HD","MCD","SBUX","NKE","PG","KO","PEP"],
    "Industrie US":  ["CAT","DE","HON","GE","MMM","RTX","LMT","BA","UPS","FDX"],
    "Tech EU":       ["ASML.AS","SAP.DE","CAP.PA","DSY.PA","STM.PA","IFX.DE","AIR.PA","SAF.PA"],
    "Finance EU":    ["BNP.PA","SAN.PA","MC.PA","KER.PA","RMS.PA","DBK.DE","ADYEN.AS","WRL.PA"],
    "Immo US":       ["AMT","PLD","EQIX","SPG","O"],
    "Crypto":        ["BTC-USD","ETH-USD","SOL-USD","BNB-USD"],
}

TOUTES_ACTIONS_SCANNER  = [t for lst in SECTEURS.values() for t in lst]
TOUTES_ACTIONS_MOMENTUM = list(set(ACTIONS_US_GRANDES+ACTIONS_US_PME+ACTIONS_FR))

NOMS = {
    "ASML.AS":"ASML","STM.PA":"STMicro","IFX.DE":"Infineon",
    "SAP.DE":"SAP","CAP.PA":"Capgemini","DSY.PA":"Dassault Sys",
    "MC.PA":"LVMH","KER.PA":"Kering","RMS.PA":"Hermes",
    "BNP.PA":"BNP Paribas","SAN.PA":"Sanofi","TTE.PA":"TotalEnergies",
    "AIR.PA":"Airbus","SAF.PA":"Safran","RNO.PA":"Renault",
    "WRL.PA":"Worldline","ADYEN.AS":"Adyen","DBK.DE":"Deutsche Bank",
}

CORRELATIONS_US_EU = {
    "NVDA":["ASML.AS","STM.PA","IFX.DE"],
    "MSFT":["SAP.DE","CAP.PA","DSY.PA"],
    "AMZN":["MC.PA"],
    "TSLA":["RNO.PA"],
    "JPM": ["BNP.PA","SAN.PA","DBK.DE"],
    "V":   ["WRL.PA","ADYEN.AS"],
    "BA":  ["AIR.PA","SAF.PA"],
}

# =====================================================
#   BASE DE DONNEES SQLITE
# =====================================================
DB_PATH = os.environ.get("DB_PATH", "scanner_v6.db")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()

    # Scans
    c.execute("""CREATE TABLE IF NOT EXISTS scans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT, ticker TEXT, prix REAL,
        var_1j REAL, var_5j REAL, rsi REAL,
        score INTEGER, secteur TEXT)""")

    # Signaux momentum avec suivi des resultats reels
    c.execute("""CREATE TABLE IF NOT EXISTS signaux_momentum (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date_signal TEXT,
        ticker TEXT,
        prix_signal REAL,
        variation_signal REAL,
        score_j1 REAL,
        score_j3 REAL,
        causes TEXT,
        -- Resultat reel J+1
        prix_j1 REAL,
        var_reelle_j1 REAL,
        verifie_j1 INTEGER DEFAULT 0,
        date_verif_j1 TEXT,
        -- Resultat reel J+3
        prix_j3 REAL,
        var_reelle_j3 REAL,
        verifie_j3 INTEGER DEFAULT 0,
        date_verif_j3 TEXT,
        -- Succes (1 = signal bon, 0 = signal mauvais)
        succes_j1 INTEGER,
        succes_j3 INTEGER)""")

    # Alertes
    c.execute("""CREATE TABLE IF NOT EXISTS alertes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT, type TEXT, valeur REAL,
        active INTEGER DEFAULT 1,
        date_creation TEXT, date_declenchement TEXT)""")

    # Watchlist
    c.execute("""CREATE TABLE IF NOT EXISTS watchlist (
        ticker TEXT PRIMARY KEY, date_ajout TEXT)""")

    # Portefeuille
    c.execute("""CREATE TABLE IF NOT EXISTS portefeuille (
        ticker TEXT PRIMARY KEY,
        quantite REAL, prix_achat REAL, date_achat TEXT)""")

    # Transactions
    c.execute("""CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT, ticker TEXT, quantite REAL,
        prix REAL, pnl REAL, date TEXT)""")

    # Cache macro
    c.execute("""CREATE TABLE IF NOT EXISTS macro_cache (
        cle TEXT PRIMARY KEY, valeur TEXT, date_maj TEXT)""")

    conn.commit(); conn.close()
    print("Base de donnees V6 initialisee")

def db():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

# =====================================================
#   SUIVI DES PREDICTIONS — COEUR DE LA V6
# =====================================================
def sauvegarder_signal(ticker, prix, variation, score_j1, score_j3, causes):
    """Enregistre un signal momentum pour verification future."""
    conn = db(); c = conn.cursor()
    c.execute("""INSERT INTO signaux_momentum
                 (date_signal,ticker,prix_signal,variation_signal,score_j1,score_j3,causes)
                 VALUES (?,?,?,?,?,?,?)""",
              (datetime.now().isoformat(), ticker, prix, variation,
               score_j1, score_j3, json.dumps(causes)))
    conn.commit(); conn.close()

def verifier_predictions():
    """
    Verifie les predictions passees et calcule si elles etaient bonnes.
    Appelee automatiquement chaque soir a 22h30.
    """
    conn = db(); c = conn.cursor()
    now  = datetime.now()
    verifications = 0
    reussites_j1  = 0
    reussites_j3  = 0

    # Verification J+1 (signaux d'hier non verifies)
    hier = (now - timedelta(days=1)).isoformat()
    avant_hier = (now - timedelta(days=2)).isoformat()
    signaux_j1 = c.execute("""SELECT id,ticker,prix_signal,score_j1
                               FROM signaux_momentum
                               WHERE verifie_j1=0 AND date_signal BETWEEN ? AND ?""",
                            (avant_hier, hier)).fetchall()

    for sid, ticker, prix_signal, score_j1 in signaux_j1:
        try:
            hist = yf.Ticker(ticker).history(period="2d")
            if hist.empty or len(hist) < 1: continue
            prix_actuel = round(hist["Close"].iloc[-1], 2)
            var_reelle  = round(((prix_actuel - prix_signal) / prix_signal) * 100, 2)
            succes      = 1 if var_reelle > 0 else 0
            c.execute("""UPDATE signaux_momentum SET
                         prix_j1=?, var_reelle_j1=?, verifie_j1=1,
                         date_verif_j1=?, succes_j1=?
                         WHERE id=?""",
                      (prix_actuel, var_reelle, now.isoformat(), succes, sid))
            verifications += 1
            if succes: reussites_j1 += 1
        except: pass

    # Verification J+3 (signaux d'il y a 3 jours)
    il_y_a_3j = (now - timedelta(days=3)).isoformat()
    il_y_a_4j = (now - timedelta(days=4)).isoformat()
    signaux_j3 = c.execute("""SELECT id,ticker,prix_signal,score_j3
                               FROM signaux_momentum
                               WHERE verifie_j3=0 AND date_signal BETWEEN ? AND ?""",
                            (il_y_a_4j, il_y_a_3j)).fetchall()

    for sid, ticker, prix_signal, score_j3 in signaux_j3:
        try:
            hist = yf.Ticker(ticker).history(period="5d")
            if hist.empty or len(hist) < 1: continue
            prix_actuel = round(hist["Close"].iloc[-1], 2)
            var_reelle  = round(((prix_actuel - prix_signal) / prix_signal) * 100, 2)
            succes      = 1 if var_reelle > 0 else 0
            c.execute("""UPDATE signaux_momentum SET
                         prix_j3=?, var_reelle_j3=?, verifie_j3=1,
                         date_verif_j3=?, succes_j3=?
                         WHERE id=?""",
                      (prix_actuel, var_reelle, now.isoformat(), succes, sid))
            if succes: reussites_j3 += 1
        except: pass

    conn.commit(); conn.close()

    return {
        "verifications": verifications,
        "reussites_j1":  reussites_j1,
        "reussites_j3":  reussites_j3,
    }

def get_stats_ticker(ticker):
    """Retourne les stats reelles d'un ticker : taux de reussite, gain moyen."""
    conn = db(); c = conn.cursor()
    rows_j1 = c.execute("""SELECT var_reelle_j1, succes_j1, score_j1, date_signal
                            FROM signaux_momentum
                            WHERE ticker=? AND verifie_j1=1
                            ORDER BY date_signal DESC LIMIT 30""",
                         (ticker,)).fetchall()
    rows_j3 = c.execute("""SELECT var_reelle_j3, succes_j3, score_j3
                            FROM signaux_momentum
                            WHERE ticker=? AND verifie_j3=1
                            ORDER BY date_signal DESC LIMIT 30""",
                         (ticker,)).fetchall()
    conn.close()

    def calc_stats(rows):
        if not rows: return None
        reussites  = sum(1 for r in rows if r[1] == 1)
        taux       = round(reussites / len(rows) * 100, 1)
        gains      = [r[0] for r in rows if r[0] is not None]
        gain_moy   = round(sum(gains) / len(gains), 2) if gains else 0
        gain_quand_reussi  = round(sum(g for g in gains if g > 0) / max(1, sum(1 for g in gains if g > 0)), 2)
        perte_quand_rate   = round(sum(g for g in gains if g <= 0) / max(1, sum(1 for g in gains if g <= 0)), 2)
        meilleur   = max(gains) if gains else 0
        pire       = min(gains) if gains else 0
        return {
            "total":          len(rows),
            "reussites":      reussites,
            "taux":           taux,
            "gain_moyen":     gain_moy,
            "gain_si_ok":     gain_quand_reussi,
            "perte_si_ko":    perte_quand_rate,
            "meilleur":       round(meilleur, 2),
            "pire":           round(pire, 2),
        }

    return {"j1": calc_stats(rows_j1), "j3": calc_stats(rows_j3)}

def get_stats_globales():
    """Stats globales de tous les signaux enregistres."""
    conn = db(); c = conn.cursor()

    # Stats globales J+1
    rows_j1 = c.execute("""SELECT ticker, COUNT(*), SUM(succes_j1), AVG(var_reelle_j1)
                            FROM signaux_momentum WHERE verifie_j1=1
                            GROUP BY ticker HAVING COUNT(*)>=3
                            ORDER BY AVG(var_reelle_j1) DESC""").fetchall()

    # Stats par secteur
    rows_sec = c.execute("""SELECT s.secteur, COUNT(*), SUM(sm.succes_j1), AVG(sm.var_reelle_j1)
                             FROM signaux_momentum sm
                             JOIN scans s ON sm.ticker=s.ticker
                             WHERE sm.verifie_j1=1
                             GROUP BY s.secteur""").fetchall()

    # Temps moyen pour +10%
    rows_temps = c.execute("""SELECT ticker, date_signal, var_reelle_j1, var_reelle_j3
                               FROM signaux_momentum
                               WHERE succes_j1=1 AND var_reelle_j1>=10
                               ORDER BY date_signal DESC LIMIT 20""").fetchall()

    conn.close()
    return {
        "par_ticker": rows_j1[:10],
        "par_secteur": rows_sec,
        "succes_rapides": rows_temps,
    }

def get_taux_reussite_ticker(ticker):
    """Retourne le taux de reussite pour le calcul du score."""
    conn = db(); c = conn.cursor()
    rows = c.execute("""SELECT succes_j1 FROM signaux_momentum
                        WHERE ticker=? AND verifie_j1=1
                        ORDER BY date_signal DESC LIMIT 20""", (ticker,)).fetchall()
    conn.close()
    if not rows or len(rows) < 3: return None
    return round(sum(r[0] for r in rows) / len(rows) * 100, 1)

# =====================================================
#   INDICATEURS TECHNIQUES
# =====================================================
def calculer_rsi(prix, periode=14):
    if len(prix) < periode+1: return 50.0
    d = [prix.iloc[i]-prix.iloc[i-1] for i in range(1,len(prix))]
    g = [x for x in d if x > 0]; p = [-x for x in d if x < 0]
    if not g: return 0.0
    if not p: return 100.0
    return round(100-(100/(1+sum(g[-periode:])/periode/(sum(p[-periode:])/periode))),1)

def calculer_macd(prix):
    if len(prix) < 26: return "neutre"
    return "haussier" if prix.ewm(span=12,adjust=False).mean().iloc[-1] > prix.ewm(span=26,adjust=False).mean().iloc[-1] else "baissier"

def calculer_bollinger(prix, periode=20):
    if len(prix) < periode: return {"signal":"neutre","haut":0,"bas":0,"mm":0}
    mm=prix.rolling(periode).mean().iloc[-1]; std=prix.rolling(periode).std().iloc[-1]
    haut=mm+2*std; bas=mm-2*std; actuel=prix.iloc[-1]
    return {"signal":"survendu" if actuel<=bas else "surchete" if actuel>=haut else "neutre",
            "haut":round(haut,2),"bas":round(bas,2),"mm":round(mm,2)}

def calculer_mm(prix):
    actuel=prix.iloc[-1]; res={}
    for p,n in [(50,"mm50"),(200,"mm200")]:
        if len(prix)>=p:
            mm=prix.rolling(p).mean().iloc[-1]
            res[n]=round(mm,2); res[f"{n}_signal"]="haussier" if actuel>mm else "baissier"
        else: res[n]=None; res[f"{n}_signal"]="neutre"
    if res.get("mm50") and res.get("mm200"):
        res["golden_cross"]=res["mm50"]>res["mm200"]
    return res

def calculer_score(var_1j, var_5j, rsi, vol_ratio, macd, boll=None, mm=None, ticker=None):
    s = 50
    if var_1j > 4:    s += 20
    elif var_1j > 2:  s += 12
    elif var_1j > 0:  s += 5
    elif var_1j < -4: s -= 20
    elif var_1j < -2: s -= 10
    if var_5j > 8:    s += 15
    elif var_5j > 3:  s += 8
    elif var_5j < -8: s -= 15
    elif var_5j < -3: s -= 8
    if 50<=rsi<=70:   s += 15
    elif 30<=rsi<50:  s += 5
    elif rsi < 30:    s += 10
    elif rsi > 80:    s -= 15
    if vol_ratio > 2: s += 15
    elif vol_ratio > 1.5: s += 8
    if macd == "haussier": s += 10
    elif macd == "baissier": s -= 10
    if boll:
        if boll["signal"] == "survendu":  s += 8
        elif boll["signal"] == "surchete": s -= 8
    if mm and mm.get("golden_cross"): s += 5
    # Bonus historique reel
    if ticker:
        taux = get_taux_reussite_ticker(ticker)
        if taux is not None:
            if taux >= 70:   s += 8
            elif taux >= 60: s += 4
            elif taux <= 30: s -= 8
            elif taux <= 40: s -= 4
    return max(0, min(s, 100))

# =====================================================
#   SCORE CONTINUATION AMELIORE
# =====================================================
CAUSES_FOND = ["Resultats trimestriels","Guidance relevee","Acquisition Fusion","Nouveau contrat","Rachat actions"]
CAUSES_PONC = ["Mouvement technique / sentiment general","Risque"]

def score_continuation(variation, rsi, vol_ratio, macd, causes,
                        reddit_sc, tweets_eng, hist_cont,
                        boll=None, mm=None, ticker=None):
    j1, j3 = 0.0, 0.0; details = []

    nb_fond = sum(1 for c in causes if c in CAUSES_FOND)
    nb_ponc = sum(1 for c in causes if c in CAUSES_PONC)
    if nb_fond >= 2:   j1+=3.0; j3+=3.0; details.append(("Causes fondamentales multiples","+3.0"))
    elif nb_fond == 1: j1+=2.5; j3+=2.5; details.append(("Cause fondamentale solide","+2.5"))
    elif nb_ponc > 0:  j1-=1.5; j3-=2.0; details.append(("Cause ponctuelle","-1.5/-2.0"))

    if rsi < 60:        j1+=2.0; j3+=2.0; details.append(("RSI<60 room to run","+2.0"))
    elif rsi < 75:      j1+=1.0; j3+=0.5; details.append(("RSI modere 60-75","+1.0/+0.5"))
    elif rsi < 85:      j1-=0.5; j3-=1.0; details.append(("RSI eleve 75-85","-0.5/-1.0"))
    else:               j1-=2.0; j3-=2.5; details.append(("RSI>85 surchete","-2.0/-2.5"))

    if vol_ratio >= 4:    j1+=2.0; j3+=1.5; details.append((f"Volume exceptionnel x{vol_ratio:.1f}","+2.0/+1.5"))
    elif vol_ratio >= 2.5: j1+=1.5; j3+=1.0; details.append((f"Volume fort x{vol_ratio:.1f}","+1.5/+1.0"))
    elif vol_ratio >= 1.5: j1+=0.5; j3+=0.5; details.append((f"Volume ok x{vol_ratio:.1f}","+0.5"))
    else:                  j1-=1.0; j3-=1.0; details.append((f"Volume faible x{vol_ratio:.1f}","-1.0"))

    if macd == "haussier": j1+=1.0; j3+=1.5; details.append(("MACD haussier","+1.0/+1.5"))
    else:                  j1-=0.5; j3-=1.0; details.append(("MACD baissier","-0.5/-1.0"))

    sent = reddit_sc + tweets_eng
    if sent > 500:  j1+=1.0; j3+=0.5; details.append((f"Buzz fort ({sent}pts)","+1.0/+0.5"))
    elif sent > 100: j1+=0.5; j3+=0.5; details.append((f"Buzz modere ({sent}pts)","+0.5"))

    if hist_cont > 3:   j1+=1.0; j3+=1.5; details.append((f"Historique J+3 +{hist_cont:.1f}%","+1.0/+1.5"))
    elif hist_cont > 0: j1+=0.5; j3+=0.5; details.append((f"Historique leger +{hist_cont:.1f}%","+0.5"))
    elif hist_cont <-2: j1-=1.0; j3-=1.5; details.append((f"Historique negatif {hist_cont:.1f}%","-1.0/-1.5"))

    if boll:
        if boll["signal"] == "survendu":  j1+=1.0; j3+=1.0; details.append(("Bollinger survendu","+1.0"))
        elif boll["signal"] == "surchete":j1-=1.0; j3-=1.0; details.append(("Bollinger surchete","-1.0"))
    if mm and mm.get("golden_cross"): j1+=0.5; j3+=1.0; details.append(("Golden Cross MM50>MM200","+0.5/+1.0"))

    if variation > 25:   j1-=1.5; j3-=2.0; details.append((f"Mouvement extreme {variation:.1f}%","-1.5/-2.0"))
    elif variation > 15: j1-=0.5; j3-=1.0; details.append((f"Mouvement fort {variation:.1f}%","-0.5/-1.0"))

    # BONUS HISTORIQUE REEL depuis SQLite
    if ticker:
        taux = get_taux_reussite_ticker(ticker)
        if taux is not None:
            if taux >= 75:
                j1+=1.5; j3+=1.5
                details.append((f"Historique reel : {taux}% de reussite sur ce ticker","+1.5"))
            elif taux >= 60:
                j1+=0.5; j3+=0.5
                details.append((f"Historique reel : {taux}% de reussite","+0.5"))
            elif taux <= 30:
                j1-=1.5; j3-=1.5
                details.append((f"Historique reel : seulement {taux}% de reussite","-1.5"))
            elif taux <= 40:
                j1-=0.5; j3-=0.5
                details.append((f"Historique reel : {taux}% de reussite (faible)","-0.5"))

    j1 = round(max(0, min(10, j1)), 1)
    j3 = round(max(0, min(10, j3)), 1)

    def v(s):
        if s >= 8:   return "Tres probable — tous les feux verts"
        elif s >= 7: return "Probable — cause solide + volume confirme"
        elif s >= 6: return "Possible — surveiller la consolidation"
        elif s >= 5: return "Incertain"
        elif s >= 4: return "Peu probable"
        elif s >= 3: return "Risque pull-back"
        else:        return "Tres risque — retournement probable"

    return {"j1":j1,"j3":j3,"details":details,"verdict_j1":v(j1),"verdict_j3":v(j3)}

def barre(score):
    p=int(score); d=1 if (score-p)>=0.5 else 0; v=10-p-d
    return f"`{'#'*p}{'+' if d else ''}{'-'*v}` **{score}/10**"

# =====================================================
#   SOURCES : NEWS + REDDIT + X
# =====================================================
HEADERS_WEB = {"User-Agent":"Mozilla/5.0 Chrome/120.0.0.0","Accept-Language":"fr-FR,fr;q=0.9"}

def google_news(query, langue="fr"):
    try:
        q   = requests.utils.quote(query)
        url = f"https://news.google.com/rss/search?q={q}&hl={langue}&gl=FR&ceid=FR:{langue.upper()}"
        return [{"titre":e.get("title","")[:120],"url":e.get("link","")}
                for e in feedparser.parse(url).entries[:6]]
    except: return []

def scraper_zonebourse(ticker):
    slugs = {
        "NVDA":"nvidia-corporation","MSFT":"microsoft","AAPL":"apple",
        "AMZN":"amazon-com","GOOGL":"alphabet","META":"meta-platforms",
        "TSLA":"tesla","AMD":"advanced-micro-devices",
        "AIR.PA":"airbus","MC.PA":"lvmh","SAP.DE":"sap",
    }
    slug = slugs.get(ticker.upper(), ticker.lower().replace(".","-"))
    news = []
    try:
        r    = requests.get(f"https://www.zonebourse.com/cours/action/{slug}/",headers=HEADERS_WEB,timeout=10)
        soup = BeautifulSoup(r.text,"html.parser")
        for a in soup.find_all("a",href=True)[:40]:
            t = a.get_text(strip=True)
            if len(t)>40 and any(kw in t.lower() for kw in ["benefice","resultat","hausse","baisse","acquisition","contrat","profit"]):
                news.append(t[:120])
        time.sleep(1)
    except: pass
    return {"news":news[:4]}

def get_reddit(ticker):
    res = []
    for sub in ["investing","wallstreetbets","stocks"]:
        try:
            r = requests.get(f"https://www.reddit.com/r/{sub}/search.json",
                             params={"q":ticker,"sort":"hot","limit":4,"t":"day"},
                             headers={"User-Agent":"scanner/1.0"},timeout=8)
            for p in r.json().get("data",{}).get("children",[]):
                d = p["data"]
                res.append({"titre":d.get("title","")[:100],"score":d.get("score",0)})
            time.sleep(1)
        except: pass
    return sorted(res,key=lambda x:x["score"],reverse=True)[:4]

def identifier_causes(titres):
    causes_map = {
        "Resultats trimestriels": ["earnings","resultats","quarterly","benefice","revenue","profit","EPS"],
        "Guidance relevee":       ["guidance","outlook","forecast","prevision"],
        "Acquisition Fusion":     ["acquisition","merger","fusion","buyout","deal","rachat"],
        "Nouveau contrat":        ["launch","lancement","contrat","contract","partenariat"],
        "Analyste":               ["upgrade","downgrade","target","analyst","price target"],
        "Macro":                  ["fed","inflation","taux","interest rate"],
        "Rachat actions":         ["buyback","repurchase","dividende"],
        "Risque":                 ["recall","lawsuit","amende","fine","scandal"],
    }
    txt = " ".join(titres).lower()
    res = [c for c,kws in causes_map.items() if any(k.lower() in txt for k in kws)]
    return res or ["Mouvement technique / sentiment general"]

def historique_continuation(ticker, seuil=10.0):
    try:
        hist = yf.Ticker(ticker).history(period="1y")
        if hist.empty or len(hist)<20: return 0.0
        suites = []
        for i in range(1,len(hist)-4):
            vj=(hist["Close"].iloc[i]-hist["Close"].iloc[i-1])/hist["Close"].iloc[i-1]*100
            if vj>=seuil:
                vj3=(hist["Close"].iloc[i+3]-hist["Close"].iloc[i])/hist["Close"].iloc[i]*100
                suites.append(vj3)
        return round(sum(suites)/len(suites),2) if suites else 0.0
    except: return 0.0

# =====================================================
#   SCAN PARALLELE
# =====================================================
def analyser_ticker(ticker, secteur=""):
    try:
        hist = yf.Ticker(ticker).history(period="3mo")
        if hist.empty or len(hist)<20: return None
        pa=round(hist["Close"].iloc[-1],2); ph=round(hist["Close"].iloc[-2],2)
        p5j=round(hist["Close"].iloc[-6],2) if len(hist)>=6 else ph
        v1j=round(((pa-ph)/ph)*100,2); v5j=round(((pa-p5j)/p5j)*100,2)
        vr=round(hist["Volume"].tail(5).mean()/hist["Volume"].mean(),2) if hist["Volume"].mean()>0 else 1.0
        rsi=calculer_rsi(hist["Close"]); macd=calculer_macd(hist["Close"])
        boll=calculer_bollinger(hist["Close"]); mm=calculer_mm(hist["Close"])
        sr={"support":round(float(hist["Low"].tail(20).min()),2),
            "resistance":round(float(hist["High"].tail(20).max()),2)}
        sc=calculer_score(v1j,v5j,rsi,vr,macd,boll,mm,ticker)
        return {"ticker":ticker,"secteur":secteur,"prix":pa,"var_1j":v1j,"var_5j":v5j,
                "rsi":rsi,"vol_ratio":vr,"macd":macd,"boll":boll,"mm":mm,"sr":sr,"score":sc,
                "timestamp":datetime.now().isoformat()}
    except Exception as e:
        print(f"  {ticker} : {e}"); return None

def analyser_actions(tickers=None):
    tickers     = tickers or TOUTES_ACTIONS_SCANNER
    resultats   = []; par_secteur = {}
    ticker_to_secteur = {t:s for s,lst in SECTEURS.items() for t in lst}
    print(f"Scan parallele {len(tickers)} actions ({MAX_WORKERS} threads)...")
    debut = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(analyser_ticker,t,ticker_to_secteur.get(t,"Autre")):t for t in tickers}
        for fut in as_completed(futures):
            res = fut.result()
            if res: resultats.append(res)
    resultats = sorted(resultats,key=lambda x:x["score"],reverse=True)
    print(f"OK — {len(resultats)} actions en {round(time.time()-debut,1)}s")
    for secteur,lst in SECTEURS.items():
        scores=[r["score"] for r in resultats if r["ticker"] in lst]
        if scores: par_secteur[secteur]=round(sum(scores)/len(scores),1)
    # Sauvegarde
    conn=db(); c=conn.cursor(); now=datetime.now().isoformat()
    for r in resultats:
        c.execute("INSERT INTO scans (date,ticker,prix,var_1j,var_5j,rsi,score,secteur) VALUES (?,?,?,?,?,?,?,?)",
                  (now,r["ticker"],r["prix"],r["var_1j"],r["var_5j"],r["rsi"],r["score"],r["secteur"]))
    conn.commit(); conn.close()
    return resultats, par_secteur

# =====================================================
#   MOMENTUM +10%
# =====================================================
def analyser_ticker_momentum(ticker):
    try:
        hist = yf.Ticker(ticker).history(period="5d")
        if hist.empty or len(hist)<2: return None
        pa=hist["Close"].iloc[-1]; ph=hist["Close"].iloc[-2]
        var=round(((pa-ph)/ph)*100,2)
        if var < SEUIL_MOMENTUM: return None
        vr=round(hist["Volume"].tail(3).mean()/hist["Volume"].mean(),2) if hist["Volume"].mean()>0 else 1.0
        return {"ticker":ticker,"prix":round(pa,2),"variation":var,"vol_ratio":vr,
                "rsi":calculer_rsi(hist["Close"]),"macd":calculer_macd(hist["Close"]),
                "boll":calculer_bollinger(hist["Close"]),"mm":calculer_mm(hist["Close"]),
                "marche":"France" if ".PA" in ticker else "US",
                "nom":NOMS.get(ticker,ticker.replace(".PA","").replace(".DE",""))}
    except: return None

def detecter_momentum(seuil=None, tickers=None):
    seuil   = seuil or SEUIL_MOMENTUM
    tickers = tickers or TOUTES_ACTIONS_MOMENTUM
    res     = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(analyser_ticker_momentum,t):t for t in tickers}
        for fut in as_completed(futures):
            r = fut.result()
            if r: res.append(r); print(f"  MOMENTUM : {r['ticker']} +{r['variation']:.2f}%")
    return sorted(res,key=lambda x:x["variation"],reverse=True)

def analyser_action_momentum(action):
    t,var = action["ticker"],action["variation"]
    nom   = action["nom"]
    news_fr = google_news(f"{nom} bourse hausse aujourd'hui")
    news_en = google_news(f"{t} stock surge today","en")
    zb      = scraper_zonebourse(t)
    reddit  = get_reddit(t)
    causes  = identifier_causes([n["titre"] for n in news_fr+news_en]+zb["news"])
    rs      = sum(r.get("score",0) for r in reddit)
    hc      = historique_continuation(t, SEUIL_MOMENTUM)
    sc      = score_continuation(var,action["rsi"],action["vol_ratio"],action["macd"],
                                 causes,rs,0,hc,action.get("boll"),action.get("mm"),ticker=t)
    # Sauvegarde pour verification future
    sauvegarder_signal(t,action["prix"],var,sc["j1"],sc["j3"],causes)
    return {"action":action,"causes":causes,"news":(news_fr+news_en)[:4],
            "zb_news":zb["news"][:3],"reddit":reddit[:3],
            "hist_cont":hc,"score":sc}

# =====================================================
#   MACRO + FEAR & GREED
# =====================================================
def get_fear_greed():
    try:
        r = requests.get("https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
                         headers={"User-Agent":"Mozilla/5.0"},timeout=10)
        data  = r.json()
        score = round(float(data["fear_and_greed"]["score"]),1)
        rating= data["fear_and_greed"]["rating"]
        trad  = {"Extreme Fear":"Peur extreme","Fear":"Peur","Neutral":"Neutre",
                 "Greed":"Avidite","Extreme Greed":"Avidite extreme"}
        emoji = "🔴" if score<=25 else "🟠" if score<=45 else "🟡" if score<=55 else "🟢" if score<=75 else "🔥"
        return {"score":score,"rating":trad.get(rating,rating),"emoji":emoji}
    except:
        return {"score":50,"rating":"Indisponible","emoji":"⚪"}

def get_macro():
    macro = {}
    for ticker,nom,cle in [("^TNX","Taux 10ans US","taux_10ans"),
                             ("^VIX","VIX Volatilite","vix"),
                             ("DX-Y.NYB","Dollar Index","dxy"),
                             ("GC=F","Or ($/oz)","or"),
                             ("CL=F","Petrole WTI","petrole")]:
        try:
            hist = yf.Ticker(ticker).history(period="5d")
            if not hist.empty and len(hist)>=2:
                v=round(hist["Close"].iloc[-1],2); vh=round(hist["Close"].iloc[-2],2)
                macro[cle]={"nom":nom,"valeur":v,"variation":round(v-vh,2)}
        except: pass
    return macro

def get_earnings_semaine():
    earnings = []
    for ticker in ["NVDA","MSFT","AAPL","GOOGL","META","AMZN","TSLA","AMD","JPM","BAC","GS","JNJ","PFE","XOM","V","MA","NFLX"]:
        try:
            info = yf.Ticker(ticker).calendar
            if info is None or info.empty: continue
            if "Earnings Date" in info.index:
                d = info.loc["Earnings Date"].iloc[0]
                if hasattr(d,'date'):
                    d = d.date()
                    auj = datetime.now().date()
                    if auj <= d <= auj+timedelta(days=7):
                        earnings.append({"ticker":ticker,"date":d.strftime("%d/%m/%Y"),"jours":(d-auj).days})
        except: pass
    return sorted(earnings,key=lambda x:x["jours"])

# =====================================================
#   GRAPHIQUE
# =====================================================
def generer_graphique(ticker, periode="3mo"):
    try:
        hist = yf.Ticker(ticker).history(period=periode)
        if hist.empty or len(hist)<10: return None
        prix=hist["Close"]; dates=hist.index
        fig,(ax1,ax2)=plt.subplots(2,1,figsize=(12,7),gridspec_kw={"height_ratios":[3,1]})
        fig.patch.set_facecolor("#0f0f10")
        for ax in [ax1,ax2]:
            ax.set_facecolor("#1a1a1c")
            ax.tick_params(colors="#9c9a92",labelsize=9)
            for s in ax.spines.values(): s.set_color("#333")
        ax1.plot(dates,prix,color="#E2E0D8",linewidth=1.5,label="Prix",zorder=3)
        if len(prix)>=20:
            mm20=prix.rolling(20).mean(); std=prix.rolling(20).std()
            ax1.fill_between(dates,mm20+2*std,mm20-2*std,alpha=0.15,color="#5865F2",label="Bollinger")
        if len(prix)>=50:
            ax1.plot(dates,prix.rolling(50).mean(),color="#F0997B",linewidth=1,label="MM50")
        if len(prix)>=200:
            ax1.plot(dates,prix.rolling(200).mean(),color="#1D9E75",linewidth=1,label="MM200")
        sr_bas=round(float(hist["Low"].tail(20).min()),2)
        sr_haut=round(float(hist["High"].tail(20).max()),2)
        ax1.axhline(sr_bas,color="#E24B4A",linewidth=0.8,linestyle=":",alpha=0.8,label=f"Support {sr_bas}")
        ax1.axhline(sr_haut,color="#1D9E75",linewidth=0.8,linestyle=":",alpha=0.8,label=f"Resistance {sr_haut}")
        ax1.set_title(f"{ticker} — {periode}",color="#E2E0D8",fontsize=13)
        ax1.legend(loc="upper left",fontsize=8,facecolor="#1a1a1c",labelcolor="#E2E0D8",framealpha=0.8)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
        ax1.grid(color="#333",linewidth=0.4,alpha=0.5)
        colors_vol=["#1D9E75" if hist["Close"].iloc[i]>=hist["Open"].iloc[i] else "#E24B4A" for i in range(len(hist))]
        ax2.bar(dates,hist["Volume"],color=colors_vol,alpha=0.7,width=0.8)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
        ax2.grid(color="#333",linewidth=0.4,alpha=0.5)
        plt.tight_layout(pad=1.5)
        buf=io.BytesIO(); plt.savefig(buf,format="png",dpi=130,bbox_inches="tight",facecolor="#0f0f10")
        plt.close(fig); buf.seek(0); return buf.read()
    except Exception as e:
        print(f"Graphique {ticker} : {e}"); return None

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

def acheter(ticker,quantite,prix_achat):
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

def vendre(ticker,quantite):
    conn=db();c=conn.cursor();t=ticker.upper()
    pos=c.execute("SELECT quantite,prix_achat FROM portefeuille WHERE ticker=?",(t,)).fetchone()
    if not pos: conn.close(); return None,"Aucune position"
    if quantite>pos[0]: conn.close(); return None,f"Tu n'as que {pos[0]} actions"
    try: pa=round(yf.Ticker(t).history(period="1d")["Close"].iloc[-1],2)
    except: pa=pos[1]
    pnl=round((pa-pos[1])*quantite,2)
    new_qte=pos[0]-quantite
    if new_qte==0: c.execute("DELETE FROM portefeuille WHERE ticker=?",(t,))
    else: c.execute("UPDATE portefeuille SET quantite=? WHERE ticker=?",(new_qte,t))
    c.execute("INSERT INTO transactions (type,ticker,quantite,prix,pnl,date) VALUES (?,?,?,?,?,?)",
              ("vente",t,quantite,pa,pnl,datetime.now().isoformat()))
    conn.commit(); conn.close()
    return pnl,pa

def get_alertes():
    conn=db();c=conn.cursor()
    rows=c.execute("SELECT id,ticker,type,valeur FROM alertes WHERE active=1").fetchall()
    conn.close(); return rows

def ajouter_alerte_db(ticker,type_alerte,valeur):
    conn=db();c=conn.cursor()
    c.execute("INSERT INTO alertes (ticker,type,valeur,date_creation) VALUES (?,?,?,?)",
              (ticker.upper(),type_alerte,valeur,datetime.now().isoformat()))
    conn.commit(); conn.close()

def supprimer_alerte_db(ticker):
    conn=db();c=conn.cursor()
    c.execute("UPDATE alertes SET active=0 WHERE ticker=? AND active=1",(ticker.upper(),))
    n=c.rowcount; conn.commit(); conn.close(); return n

def verifier_alertes_prix():
    alertes=get_alertes()
    if not alertes: return
    conn=db();c=conn.cursor()
    for aid,ticker,type_alerte,valeur in alertes:
        try:
            hist=yf.Ticker(ticker).history(period="2d")
            if hist.empty or len(hist)<2: continue
            pa=hist["Close"].iloc[-1]; ph=hist["Close"].iloc[-2]
            prix=round(pa,2); var=round(((pa-ph)/ph)*100,2)
            declenche=False; msg=""
            if type_alerte=="sous" and prix<=valeur:
                declenche=True; msg=f"**{ticker}** est passe sous **{valeur}$** -> Prix : **{prix}$**"
            elif type_alerte=="dessus" and prix>=valeur:
                declenche=True; msg=f"**{ticker}** a depasse **{valeur}$** -> Prix : **{prix}$**"
            elif type_alerte=="hausse_pct" and var>=valeur:
                declenche=True; msg=f"**{ticker}** a progresse de **+{var:.2f}%** (seuil : +{valeur}%)"
            elif type_alerte=="baisse_pct" and var<=-valeur:
                declenche=True; msg=f"**{ticker}** a chute de **{var:.2f}%** (seuil : -{valeur}%)"
            if declenche:
                c.execute("UPDATE alertes SET active=0,date_declenchement=? WHERE id=?",
                          (datetime.now().isoformat(),aid))
                envoyer_embed_http(f"ALERTE — {ticker}",msg,0xFF6B00,
                                   [{"name":"Action","value":f"Tape `!analyse {ticker}` pour l'analyse complete","inline":False}])
        except: pass
    conn.commit(); conn.close()

def get_stats_semaine():
    conn=db();c=conn.cursor()
    date_min=(datetime.now()-timedelta(days=7)).isoformat()
    rows=c.execute("""SELECT ticker,AVG(score),MAX(var_1j),MIN(var_1j),COUNT(*)
                      FROM scans WHERE date>? GROUP BY ticker
                      ORDER BY AVG(score) DESC""", (date_min,)).fetchall()
    conn.close(); return rows

# =====================================================
#   RAPPORT PRE-MARCHE (08h15 — avant l'ouverture)
# =====================================================
def rapport_pre_marche():
    """
    Rapport de pre-ouverture : contexte macro, earnings du jour,
    actions a surveiller, signaux de la nuit.
    """
    print("Rapport pre-marche en cours...")
    # Stats automatiques (le bot avait-il raison ?)
    if HAS_STATS_AUTO:
        try:
            _stats_auto(DB_PATH, DISCORD_TOKEN, CHANNEL_ID)
            time.sleep(1)
        except Exception as e:
            print(f"Stats auto : {e}")
    fg      = get_fear_greed()
    macro   = get_macro()
    earnings= get_earnings_semaine()

    # Actions de la watchlist
    wl      = charger_watchlist()

    champs  = [
        {"name":"Fear & Greed","value":f"{fg['emoji']} **{fg['score']}/100 — {fg['rating']}**","inline":False},
    ]
    for k,m in macro.items():
        sg="+" if m["variation"]>=0 else ""
        champs.append({"name":m["nom"],"value":f"**{m['valeur']}** ({sg}{m['variation']})","inline":True})

    if earnings:
        e_txt = "\n".join(f"• **{e['ticker']}** — {e['date']} (J+{e['jours']})" for e in earnings[:5])
        champs.append({"name":"Earnings du jour / semaine","value":e_txt,"inline":False})

    if wl:
        champs.append({"name":"Ta watchlist","value":"  ".join(f"**{t}**" for t in wl),"inline":False})

    # Conseil selon Fear & Greed
    if fg["score"] <= 25:    conseil = "Peur extreme -> chercher les solides fondamentaux en zone de support"
    elif fg["score"] <= 45:  conseil = "Marche craintif -> etre selectif, attendre les confirmations"
    elif fg["score"] <= 55:  conseil = "Marche neutre -> suivre les signaux techniques"
    elif fg["score"] <= 75:  conseil = "Marche optimiste -> momentum favorable, surveiller les surachats"
    else:                    conseil = "Avidite extreme -> risque eleve de correction, proteger les gains"

    champs.append({"name":"Strategie du jour","value":conseil,"inline":False})
    champs.append({"name":"Planning","value":
                   "09:00 -> Ouverture EU\n14:30 -> Ouverture US\n17:30 -> Rapport cloture EU\n22:15 -> Rapport final US",
                   "inline":False})

    envoyer_embed_http(
        f"Briefing Pre-Marche — {datetime.now().strftime('%d/%m/%Y %H:%M')}",
        "Resume du contexte avant l'ouverture des marches",
        0x5865F2, champs
    )

# =====================================================
#   RAPPORT DE CLOTURE + VERIFICATION PREDICTIONS
# =====================================================
def rapport_cloture(marche="EU"):
    """Rapport de fin de seance avec les meilleures et pires performances."""
    print(f"Rapport cloture {marche}...")
    resultats, par_secteur = analyser_actions()
    top5  = resultats[:5]
    flop3 = resultats[-3:]

    # Top 5
    champs = []
    for i,a in enumerate(top5,1):
        taux = get_taux_reussite_ticker(a["ticker"])
        taux_txt = f" | Historique : {taux}%" if taux else ""
        champs.append({"name":f"#{i} {a['ticker']} — {a['score']}/100",
                       "value":f"{a['prix']} | {a['var_1j']:+.2f}% | RSI {a['rsi']}{taux_txt}\n{a['secteur']}",
                       "inline":False})

    envoyer_embed_http(
        f"Cloture {marche} — {datetime.now().strftime('%H:%M')}",
        f"**{len(resultats)} actions** | Secteur fort : **{max(par_secteur,key=par_secteur.get)}**",
        0x1D9E75, champs
    )

def rapport_final_et_verification():
    """
    Rapport final de la journee + verification des predictions
    + envoi du bilan + arret du bot.
    """
    print("Rapport final + verification predictions...")

    # Verification des predictions
    res = verifier_predictions()
    if res["verifications"] > 0:
        taux_j1 = round(res["reussites_j1"]/res["verifications"]*100,1) if res["verifications"]>0 else 0
        envoyer_embed_http(
            "Verification des Predictions J+1",
            f"**{res['verifications']}** predictions verifiees aujourd'hui",
            0x5865F2,
            [{"name":"Reussites J+1","value":f"**{res['reussites_j1']}/{res['verifications']}** ({taux_j1}%)","inline":True},
             {"name":"Conseil","value":"Tape `!stats` pour voir les stats completes par action","inline":False}]
        )

    # Stats globales
    stats = get_stats_globales()
    if stats["par_ticker"]:
        champs_stats = []
        for ticker,nb,reussites,gain_moy in stats["par_ticker"][:6]:
            taux=round(reussites/nb*100,1) if nb>0 else 0
            emoji="🟢" if taux>=60 else "🟡" if taux>=40 else "🔴"
            champs_stats.append({"name":f"{emoji} {ticker}",
                                 "value":f"Taux : **{taux}%** | Gain moy : **{gain_moy:+.2f}%** | {int(nb)} signaux",
                                 "inline":True})
        envoyer_embed_http("Meilleurs Tickers (Taux de Reussite)","",0x1D9E75,champs_stats)

    # Message d'arret
    envoyer_embed_http(
        "Bot en veille jusqu'a demain 08h00",
        f"Journee terminee — le bot va s'arreter dans quelques minutes.\n\n"
        f"Il redemarrera automatiquement demain a **08h00** via Railway.\n\n"
        f"Bonne nuit !",
        0x888780,
        [{"name":"Resume","value":f"Signaux verifies aujourd'hui : {res['verifications']}\nTaux reussite J+1 : {round(res['reussites_j1']/max(1,res['verifications'])*100,1)}%","inline":False}]
    )

    time.sleep(30)
    print("Arret du bot — bonne nuit !")
    os._exit(0)

# =====================================================
#   DISCORD — ENVOI HTTP
# =====================================================
def envoyer_embed_http(titre,description,couleur,champs=None):
    url=f"https://discord.com/api/v10/channels/{CHANNEL_ID}/messages"
    hdrs={"Authorization":f"Bot {DISCORD_TOKEN}","Content-Type":"application/json"}
    embed={"title":titre[:256],"description":description[:4096],"color":couleur,
           "fields":(champs or [])[:25],
           "footer":{"text":"Scanner Bourse V6 — Railway — Analyse educative"},
           "timestamp":datetime.utcnow().isoformat()}
    try: requests.post(url,headers=hdrs,json={"embeds":[embed]},timeout=10); time.sleep(0.5)
    except Exception as e: print(f"Discord : {e}")

# =====================================================
#   BOT DISCORD
# =====================================================
if DISCORD_PY:
    intents=discord.Intents.default(); intents.message_content=True
    bot=commands.Bot(command_prefix="!",intents=intents,help_command=None)

    def envoyer_embed(titre,description,couleur,champs=None,channel=None):
        async def _s():
            ch=bot.get_channel(channel or CHANNEL_ID)
            if not ch: return
            em=discord.Embed(title=titre[:256],description=description[:4096],color=couleur)
            for f in (champs or [])[:25]:
                em.add_field(name=f["name"][:256],value=f["value"][:1024],inline=f.get("inline",False))
            em.set_footer(text="Scanner V6 — Railway — Analyse educative")
            em.timestamp=datetime.utcnow()
            await ch.send(embed=em)
        if bot.loop and bot.loop.is_running():
            bot.loop.call_soon_threadsafe(lambda:bot.loop.create_task(_s()))

    def envoyer_message(contenu,channel=None):
        async def _s():
            ch=bot.get_channel(channel or CHANNEL_ID)
            if ch: await ch.send(contenu[:2000])
        if bot.loop and bot.loop.is_running():
            bot.loop.call_soon_threadsafe(lambda:bot.loop.create_task(_s()))

    _cache={"resultats":[],"secteurs":{},"momentum":[]}

    def lancer_scan_bg():
        def _run():
            try:
                resultats,par_secteur=analyser_actions()
                _cache["resultats"]=resultats; _cache["secteurs"]=par_secteur
                top=resultats[:5]; surv=[r for r in resultats if r["rsi"]<32][:3]
                fg=get_fear_greed()
                envoyer_embed(f"Scanner V6 — {datetime.now().strftime('%d/%m/%Y %H:%M')}",
                              f"**{len(resultats)} actions** | Fear & Greed : {fg['emoji']} {fg['score']}/100 — {fg['rating']}\n"
                              f"Secteur fort : **{max(par_secteur,key=par_secteur.get)}**",
                              0x5865F2)
                time.sleep(0.8)
                champs_top=[]
                for i,a in enumerate(top,1):
                    taux=get_taux_reussite_ticker(a["ticker"])
                    taux_txt=f" | Histo : {taux}%" if taux else ""
                    champs_top.append({"name":f"#{i} {a['ticker']} — {a['score']}/100",
                                       "value":f"{a['prix']} | {a['var_1j']:+.2f}% | RSI {a['rsi']}{taux_txt}\n{a['secteur']}",
                                       "inline":False})
                envoyer_embed("Top 5 Opportunites","",0x1D9E75,champs_top)
                time.sleep(0.8)
                sec=sorted(par_secteur.items(),key=lambda x:x[1],reverse=True)
                envoyer_embed("Force par Secteur","",0x378ADD,
                              [{"name":f"{'OK' if s>=65 else 'MOY' if s>=50 else 'KO'} {n}",
                                "value":f"`{'#'*int(s/10)}{'.'*(10-int(s/10))}` {s}/100","inline":True}
                               for n,s in sec])
                verifier_alertes_prix()
                actions_m=detecter_momentum(); _cache["momentum"]=actions_m
                if actions_m:
                    envoyer_embed(f"ALERTE MOMENTUM +{SEUIL_MOMENTUM}%",
                                  f"**{len(actions_m)} action(s)** ! Tape `!momentum` pour l'analyse.",
                                  0xFF0000,
                                  [{"name":a["ticker"],"value":f"+{a['variation']:.2f}% | RSI {a['rsi']}","inline":True}
                                   for a in actions_m[:8]])
                print("Scan V6 termine !")
            except Exception as e: envoyer_message(f"Erreur scan : {e}")
        threading.Thread(target=_run,daemon=True).start()

    @bot.event
    async def on_ready():
        print(f"Bot V6 connecte : {bot.user}")
        horaires_auto.start(); verif_alertes_loop.start()
        envoyer_embed("Scanner Bourse V6 — En ligne !",
                      f"**{len(TOUTES_ACTIONS_MOMENTUM)} actions** US + France\n\n"
                      f"Horaires : **08h00** demarrage -> **22h45** arret auto\n"
                      f"Nouveaute V6 : Verification des predictions + taux de reussite reel\n\n"
                      f"Dashboard : http://localhost:5000\n"
                      f"Tape `!help` pour les commandes",
                      0x1D9E75)
        threading.Thread(target=rapport_pre_marche,daemon=True).start()
        lancer_scan_bg()

    @tasks.loop(minutes=1)
    async def horaires_auto():
        h = heure_actuelle()
        # Rapport pre-marche
        if h == HORAIRES["pre_marche"]:
            threading.Thread(target=rapport_pre_marche,daemon=True).start()
        # Scan ouverture EU
        elif h == HORAIRES["ouverture_eu"]:
            envoyer_message("Ouverture marche europeen — Lancement du scan...")
            lancer_scan_bg()
        # Scan ouverture US
        elif h == HORAIRES["ouverture_us"]:
            envoyer_message("Ouverture marche US — Lancement du scan...")
            lancer_scan_bg()
        # Rapport cloture EU
        elif h == HORAIRES["cloture_eu"]:
            threading.Thread(target=lambda:rapport_cloture("EU"),daemon=True).start()
        # Rapport final + verification + arret
        elif h == HORAIRES["verification"]:
            threading.Thread(target=rapport_final_et_verification,daemon=True).start()

    @tasks.loop(hours=1)
    async def verif_alertes_loop():
        verifier_alertes_prix()

    # ── COMMANDES ──

    @bot.command(name="scan")
    async def cmd_scan(ctx):
        await ctx.send("Scan en cours (environ 30 secondes)...")
        lancer_scan_bg()

    @bot.command(name="momentum")
    async def cmd_momentum(ctx, seuil: float=None):
        seuil = seuil or SEUIL_MOMENTUM
        await ctx.send(f"Scan momentum +{seuil}% sur {len(TOUTES_ACTIONS_MOMENTUM)} actions...")
        def _run():
            actions=detecter_momentum(seuil)
            if not actions: envoyer_message(f"Aucune action ne fait +{seuil}% actuellement."); return
            fg=get_fear_greed()
            envoyer_embed(f"Momentum +{seuil}%",
                          f"**{len(actions)} action(s)** | Fear & Greed : {fg['emoji']} {fg['score']}/100",
                          0xFF6B00,
                          [{"name":f"{a['ticker']} ({a['marche']})","value":f"**+{a['variation']:.2f}%** | RSI {a['rsi']} | Vol x{a['vol_ratio']}","inline":True}
                           for a in actions[:10]])
            time.sleep(1)
            for action in actions[:3]:
                analyse=analyser_action_momentum(action)
                a=analyse["action"]; sc=analyse["score"]; t=a["ticker"]; v=a["variation"]
                taux=get_taux_reussite_ticker(t)
                taux_txt=f"\nHistorique reel : **{taux}% de reussite** sur ce ticker" if taux else ""
                champs=[
                    {"name":"Donnees","value":f"Prix : {a['prix']} | +{v:.2f}% | RSI {a['rsi']} | Vol x{a['vol_ratio']}","inline":False},
                    {"name":"Causes","value":" | ".join(analyse["causes"]),"inline":False},
                    {"name":"News","value":"\n".join(f"- {n['titre'][:80]}" for n in analyse["news"][:3]) or "Aucune","inline":False},
                    {"name":"Historique J+3",
                     "value":f"**{analyse['hist_cont']:+.1f}%** en moy apres +{SEUIL_MOMENTUM}%{taux_txt}","inline":False},
                ]
                if analyse["reddit"]: champs.append({"name":"Reddit","value":"\n".join(f"- {r['titre'][:70]}" for r in analyse["reddit"][:2]),"inline":False})
                envoyer_embed(f"{t} ({a['nom']}) — +{v:.2f}% {a['marche']}","",0xFF6B00,champs)
                time.sleep(0.8)
                detail="\n".join(f"{d[0]} -> {d[1]}" for d in sc["details"][:7])
                envoyer_embed(f"Score continuation — {t}",
                              f"J+1 : **{sc['j1']}/10** | J+3 : **{sc['j3']}/10**",
                              0x1D9E75 if sc["j1"]>=7 else 0xBA7517 if sc["j1"]>=5 else 0xE24B4A,
                              [{"name":"Demain (J+1)","value":f"{barre(sc['j1'])}\n{sc['verdict_j1']}","inline":False},
                               {"name":"J+3","value":f"{barre(sc['j3'])}\n{sc['verdict_j3']}","inline":False},
                               {"name":"Detail","value":detail,"inline":False}])
                time.sleep(0.8)
        threading.Thread(target=_run,daemon=True).start()

    @bot.command(name="stats")
    async def cmd_stats(ctx, ticker: str=None):
        """Stats reelles des predictions pour un ticker ou globalement."""
        if ticker:
            t = ticker.upper()
            await ctx.send(f"Stats reelles pour **{t}**...")
            stats = get_stats_ticker(t)
            champs = []
            for horizon,label in [("j1","J+1 (lendemain)"),("j3","J+3 (3 jours)")]:
                s = stats[horizon]
                if not s:
                    champs.append({"name":label,"value":"Pas encore assez de donnees (min 3 signaux)","inline":False})
                    continue
                emoji = "🟢" if s["taux"]>=60 else "🟡" if s["taux"]>=40 else "🔴"
                champs.append({"name":label,
                               "value":(f"{emoji} Taux reussite : **{s['taux']}%** ({s['reussites']}/{s['total']} signaux)\n"
                                        f"Gain moyen : **{s['gain_moyen']:+.2f}%**\n"
                                        f"Gain quand OK : **{s['gain_si_ok']:+.2f}%** | Perte quand KO : **{s['perte_si_ko']:+.2f}%**\n"
                                        f"Meilleur : **{s['meilleur']:+.2f}%** | Pire : **{s['pire']:+.2f}%**"),
                               "inline":False})
            envoyer_embed(f"Stats Reelles — {t}",
                          "Basees sur les predictions passees verifiees automatiquement",
                          0x5865F2,champs)
        else:
            await ctx.send("Stats globales de tous les signaux...")
            def _run():
                stats = get_stats_globales()
                if not stats["par_ticker"]:
                    envoyer_message("Pas encore assez de donnees. Lance `!momentum` pour commencer a enregistrer des signaux."); return
                champs=[]
                for ticker,nb,reussites,gain_moy in stats["par_ticker"][:8]:
                    taux=round(reussites/nb*100,1) if nb>0 else 0
                    emoji="🟢" if taux>=60 else "🟡" if taux>=40 else "🔴"
                    champs.append({"name":f"{emoji} {ticker}",
                                   "value":f"Taux : **{taux}%** | Gain moy : **{gain_moy:+.2f}%** | {int(nb)} signaux",
                                   "inline":True})
                envoyer_embed("Stats Globales — Taux de Reussite par Ticker",
                              "Top tickers selon les predictions verifiees",
                              0x5865F2,champs)
            threading.Thread(target=_run,daemon=True).start()

    @bot.command(name="verifie")
    async def cmd_verifie(ctx):
        """Force la verification des predictions en attente."""
        await ctx.send("Verification des predictions en cours...")
        def _run():
            res=verifier_predictions()
            if res["verifications"]==0:
                envoyer_message("Aucune prediction a verifier pour l'instant."); return
            taux=round(res["reussites_j1"]/res["verifications"]*100,1) if res["verifications"]>0 else 0
            envoyer_embed("Verification des Predictions",f"**{res['verifications']}** predictions verifiees",
                          0x5865F2,
                          [{"name":"Reussites J+1","value":f"**{res['reussites_j1']}/{res['verifications']}** ({taux}%)","inline":True},
                           {"name":"Reussites J+3","value":str(res["reussites_j3"]),"inline":True}])
        threading.Thread(target=_run,daemon=True).start()

    @bot.command(name="analyse")
    async def cmd_analyse(ctx, ticker: str):
        t=ticker.upper()
        await ctx.send(f"Analyse de **{t}**...")
        async with ctx.typing():
            data=await bot.loop.run_in_executor(None,generer_graphique,t)
            if data:
                f=discord.File(io.BytesIO(data),filename=f"{t}.png")
                em=discord.Embed(title=f"{t} — Graphique 3 mois",color=0x5865F2)
                em.set_image(url=f"attachment://{t}.png")
                em.set_footer(text="MM50 orange | MM200 vert | Bollinger bleu")
                await ctx.send(file=f,embed=em)
        def _run():
            try:
                hist=yf.Ticker(t).history(period="5d")
                if hist.empty or len(hist)<2: envoyer_message(f"Donnees introuvables pour {t}."); return
                pa=hist["Close"].iloc[-1]; ph=hist["Close"].iloc[-2]
                var=round(((pa-ph)/ph)*100,2)
                vr=round(hist["Volume"].tail(3).mean()/hist["Volume"].mean(),2) if hist["Volume"].mean()>0 else 1.0
                boll_d=calculer_bollinger(hist["Close"]); mm_d=calculer_mm(hist["Close"])
                action={"ticker":t,"prix":round(pa,2),"variation":var,"vol_ratio":vr,
                        "rsi":calculer_rsi(hist["Close"]),"macd":calculer_macd(hist["Close"]),
                        "boll":boll_d,"mm":mm_d,
                        "marche":"France" if ".PA" in t else "US",
                        "nom":NOMS.get(t,t.replace(".PA","").replace(".DE",""))}
                analyse=analyser_action_momentum(action)
                a=analyse["action"]; sc=analyse["score"]; v=a["variation"]
                taux=get_taux_reussite_ticker(t)
                taux_txt=f"\n\nHistorique reel : **{taux}% de reussite** sur ce ticker" if taux else ""
                champs=[
                    {"name":"Donnees","value":f"Prix : {a['prix']} | {v:+.2f}% | RSI {a['rsi']} | Vol x{a['vol_ratio']}","inline":False},
                    {"name":"Bollinger","value":f"Signal : **{boll_d['signal']}** | MM20 : {boll_d['mm']} | Haut : {boll_d['haut']} | Bas : {boll_d['bas']}","inline":False},
                    {"name":"MM50/MM200","value":f"MM50 : {mm_d.get('mm50','?')} ({mm_d.get('mm50_signal','')}) | MM200 : {mm_d.get('mm200','?')} ({mm_d.get('mm200_signal','')})","inline":False},
                    {"name":"Causes","value":" | ".join(analyse["causes"]),"inline":False},
                    {"name":"News","value":"\n".join(f"- {n['titre'][:80]}" for n in analyse["news"][:3]) or "Aucune","inline":False},
                ]
                envoyer_embed(f"Analyse — {t}",taux_txt,0x5865F2,champs)
                time.sleep(0.8)
                detail="\n".join(f"{d[0]} -> {d[1]}" for d in sc["details"][:7])
                envoyer_embed(f"Score continuation — {t}",
                              f"J+1 : **{sc['j1']}/10** | J+3 : **{sc['j3']}/10**",
                              0x1D9E75 if sc["j1"]>=7 else 0xBA7517 if sc["j1"]>=5 else 0xE24B4A,
                              [{"name":"Demain (J+1)","value":f"{barre(sc['j1'])}\n{sc['verdict_j1']}","inline":False},
                               {"name":"J+3","value":f"{barre(sc['j3'])}\n{sc['verdict_j3']}","inline":False},
                               {"name":"Detail","value":detail,"inline":False}])
            except Exception as e: envoyer_message(f"Erreur : {e}")
        threading.Thread(target=_run,daemon=True).start()

    @bot.command(name="marche")
    async def cmd_marche(ctx):
        def _run():
            fg=get_fear_greed(); macro=get_macro()
            champs=[{"name":"Fear & Greed","value":f"**{fg['emoji']} {fg['score']}/100 — {fg['rating']}**","inline":False}]
            for k,m in macro.items():
                sg="+" if m["variation"]>=0 else ""
                champs.append({"name":m["nom"],"value":f"**{m['valeur']}** ({sg}{m['variation']})","inline":True})
            if fg["score"]<=25:   conseil="Peur extreme -> opportunites sur fondamentaux solides"
            elif fg["score"]<=45: conseil="Marche craintif -> etre selectif"
            elif fg["score"]<=55: conseil="Marche neutre -> suivre les signaux techniques"
            elif fg["score"]<=75: conseil="Marche optimiste -> momentum favorable"
            else:                 conseil="Avidite extreme -> risque correction, proteger les gains"
            champs.append({"name":"Analyse","value":conseil,"inline":False})
            envoyer_embed(f"Contexte de Marche — {datetime.now().strftime('%d/%m/%Y %H:%M')}","",0x5865F2,champs)
        threading.Thread(target=_run,daemon=True).start()

    @bot.command(name="macro")
    async def cmd_macro(ctx):
        def _run():
            macro=get_macro()
            champs=[]
            for k,m in macro.items():
                sg="+" if m["variation"]>=0 else ""
                champs.append({"name":m["nom"],"value":f"**{m['valeur']}** ({sg}{m['variation']})","inline":True})
            envoyer_embed(f"Donnees Macro — {datetime.now().strftime('%d/%m/%Y')}","",0x378ADD,champs)
        threading.Thread(target=_run,daemon=True).start()

    @bot.command(name="earnings")
    async def cmd_earnings(ctx):
        def _run():
            earnings=get_earnings_semaine()
            if not earnings: envoyer_message("Aucun earnings majeur cette semaine."); return
            champs=[{"name":f"{e['ticker']} — {e['date']}",
                     "value":f"Dans **{e['jours']} jour(s)** | Tape `!analyse {e['ticker']}` pour preparer","inline":True}
                    for e in earnings]
            envoyer_embed(f"Earnings Calendar — {datetime.now().strftime('%d/%m/%Y')}",
                          f"**{len(earnings)} publication(s)** de resultats attendues.",
                          0xF0997B,champs)
        threading.Thread(target=_run,daemon=True).start()

    @bot.command(name="graphique")
    async def cmd_graphique(ctx, ticker: str):
        t=ticker.upper()
        async with ctx.typing():
            data=await bot.loop.run_in_executor(None,generer_graphique,t)
            if data:
                f=discord.File(io.BytesIO(data),filename=f"{t}.png")
                em=discord.Embed(title=f"{t} — Graphique 3 mois",color=0x5865F2)
                em.set_image(url=f"attachment://{t}.png")
                await ctx.send(file=f,embed=em)
            else: await ctx.send(f"Impossible de generer le graphique pour {t}.")

    @bot.command(name="alerte")
    async def cmd_alerte(ctx, ticker: str, valeur: str):
        t=ticker.upper()
        try:
            if "%" in valeur:
                v=float(valeur.replace("%","").replace("+","").replace("-",""))
                ta="hausse_pct" if "+" in valeur else "baisse_pct"
                desc=f"hausse de +{v}%" if ta=="hausse_pct" else f"baisse de -{v}%"
            else:
                v=float(valeur.replace("+",""))
                ta="dessus" if "+" in valeur or v>0 else "sous"
                desc=f"au-dessus de {v}$" if ta=="dessus" else f"sous {v}$"
            ajouter_alerte_db(t,ta,v)
            await ctx.send(f"Alerte creee : **{t}** -> si {desc}")
        except Exception as e: await ctx.send(f"Usage : `!alerte NVDA 150` ou `!alerte NVDA +5%`\nErreur : {e}")

    @bot.command(name="alertes")
    async def cmd_alertes(ctx):
        al=get_alertes()
        if not al: await ctx.send("Aucune alerte. `!alerte TICKER VALEUR`"); return
        em=discord.Embed(title=f"Alertes actives ({len(al)})",color=0xF0997B)
        for a in al: em.add_field(name=a[1],value=f"{a[2]} | {a[3]}",inline=True)
        await ctx.send(embed=em)

    @bot.command(name="supprimer")
    async def cmd_supprimer(ctx, ticker: str):
        n=supprimer_alerte_db(ticker)
        await ctx.send(f"{'OK' if n>0 else 'Aucune alerte'} pour **{ticker.upper()}** ({n} supprimee(s)).")

    @bot.command(name="watch")
    async def cmd_watch(ctx, ticker: str):
        ok=ajouter_watchlist(ticker)
        await ctx.send(f"{'Ajoute' if ok else 'Deja dans'} ta watchlist : **{ticker.upper()}**.")

    @bot.command(name="unwatch")
    async def cmd_unwatch(ctx, ticker: str):
        ok=retirer_watchlist(ticker)
        await ctx.send(f"{'Retire' if ok else 'Non trouve'} : **{ticker.upper()}**.")

    @bot.command(name="watchlist")
    async def cmd_watchlist(ctx):
        wl=charger_watchlist()
        if not wl: await ctx.send("Watchlist vide. `!watch TICKER`"); return
        em=discord.Embed(title=f"Watchlist ({len(wl)} actions)",
                         description="  ".join(f"**{t}**" for t in wl),color=0x5865F2)
        await ctx.send(embed=em)

    @bot.command(name="backtest")
    async def cmd_backtest(ctx, ticker: str):
        t=ticker.upper(); await ctx.send(f"Backtest **{t}**...")
        def _run():
            try:
                hist=yf.Ticker(t).history(period="6mo")
                if hist.empty or len(hist)<20: envoyer_message("Pas assez de donnees."); return
                signaux,nb_g,nb_p=[],0,0
                for i in range(14,len(hist)-5):
                    if calculer_rsi(hist["Close"].iloc[:i])<35:
                        pe=hist["Close"].iloc[i]; ps=hist["Close"].iloc[i+5]
                        g=round(((ps-pe)/pe)*100,2)
                        signaux.append({"date":hist.index[i].strftime("%d/%m/%Y"),"gain":g})
                        nb_g+=1 if g>0 else 0; nb_p+=1 if g<=0 else 0
                if not signaux: envoyer_message("Aucun signal RSI<35 sur 6 mois."); return
                taux=round(nb_g/len(signaux)*100,1)
                envoyer_embed(f"Backtest {t} — 6 mois (RSI<35 -> J+5)",
                              f"{len(signaux)} signaux detectes.",
                              0x1D9E75 if sum(s["gain"] for s in signaux)>0 else 0xE24B4A,
                              [{"name":"Taux reussite","value":f"{taux}%","inline":True},
                               {"name":"Gain moyen","value":f"{round(sum(s['gain'] for s in signaux)/len(signaux),2):+.2f}%","inline":True},
                               {"name":"Gain total","value":f"{round(sum(s['gain'] for s in signaux),2):+.2f}%","inline":True},
                               {"name":"Meilleur","value":f"{max(signaux,key=lambda x:x['gain'])['date']} : {max(signaux,key=lambda x:x['gain'])['gain']:+.2f}%","inline":True},
                               {"name":"Pire","value":f"{min(signaux,key=lambda x:x['gain'])['date']} : {min(signaux,key=lambda x:x['gain'])['gain']:+.2f}%","inline":True},
                               {"name":"3 derniers","value":"\n".join(f"- {s['date']} : {s['gain']:+.2f}%" for s in signaux[-3:]),"inline":False}])
            except Exception as e: envoyer_message(f"Erreur : {e}")
        threading.Thread(target=_run,daemon=True).start()

    @bot.command(name="buy")
    async def cmd_buy(ctx, ticker: str, quantite: float, prix: float):
        acheter(ticker,quantite,prix)
        await ctx.send(f"Achat : **{ticker.upper()}** — {quantite} x {prix}$ = {round(quantite*prix,2)}$")

    @bot.command(name="sell")
    async def cmd_sell(ctx, ticker: str, quantite: float):
        pnl,pa=vendre(ticker,quantite)
        if pnl is None: await ctx.send(f"Erreur : {pa}"); return
        await ctx.send(f"{'Gain' if pnl>=0 else 'Perte'} — Vente **{ticker.upper()}** — {quantite} x {pa}$ | P&L : **{pnl:+.2f}$**")

    @bot.command(name="portfolio")
    async def cmd_portfolio(ctx):
        positions=charger_portefeuille()
        if not positions: await ctx.send("Portefeuille vide. `!buy TICKER QTE PRIX`"); return
        champs=[]; pnl_tot=0; val_tot=0
        for pos in positions:
            try: pa=round(yf.Ticker(pos["ticker"]).history(period="1d")["Close"].iloc[-1],2)
            except: pa=pos["prix_achat"]
            pnl=round((pa-pos["prix_achat"])*pos["quantite"],2)
            pct=round((pa-pos["prix_achat"])/pos["prix_achat"]*100,2)
            val=round(pa*pos["quantite"],2); pnl_tot+=pnl; val_tot+=val
            champs.append({"name":f"{'OK' if pnl>=0 else 'KO'} {pos['ticker']} x{pos['quantite']}",
                           "value":f"PRU {pos['prix_achat']}$ -> {pa}$ | {val}$ | **{pnl:+.2f}$ ({pct:+.2f}%)**","inline":False})
        envoyer_embed(f"Portefeuille — {round(val_tot,2)}$",f"P&L total : **{pnl_tot:+.2f}$**",
                      0x1D9E75 if pnl_tot>=0 else 0xE24B4A,champs)

    @bot.command(name="semaine")
    async def cmd_semaine(ctx):
        await ctx.send("Rapport hebdomadaire...")
        def _run():
            stats=get_stats_semaine()
            if not stats: envoyer_message("Pas assez de donnees."); return
            top5=stats[:5]
            envoyer_embed(f"Rapport Hebdo — {(datetime.now()-timedelta(days=7)).strftime('%d/%m')} -> {datetime.now().strftime('%d/%m/%Y')}",
                          f"Resume des **{len(stats)} actions** cette semaine.",
                          0x5865F2,
                          [{"name":f"#{i+1} {r[0]}","value":f"Score moy : **{round(r[1],1)}/100** | Max : +{round(r[2],2)}%","inline":True}
                           for i,r in enumerate(top5)])
        threading.Thread(target=_run,daemon=True).start()

    @bot.command(name="prix")
    async def cmd_prix(ctx, ticker: str):
        try:
            t=ticker.upper(); h=yf.Ticker(t).history(period="2d")
            pa=round(h["Close"].iloc[-1],2); ph=round(h["Close"].iloc[-2],2)
            v=round(((pa-ph)/ph)*100,2)
            await ctx.send(f"**{t}** : {pa} | {'+' if v>=0 else ''}{v}%")
        except: await ctx.send(f"Ticker introuvable : {ticker}")

    @bot.command(name="help")
    async def cmd_help(ctx):
        em=discord.Embed(title="Scanner V6 — Commandes",color=0x5865F2)
        cmds=[
            ("!scan","Scan general complet"),
            ("!momentum [seuil]","Actions +10% + score J+1/J+3"),
            ("!stats NVDA","Taux de reussite reel des signaux"),
            ("!stats","Stats globales tous les tickers"),
            ("!verifie","Force la verification des predictions"),
            ("!analyse TICKER","Analyse + graphique + score"),
            ("!marche","Fear & Greed + contexte"),
            ("!macro","Taux, VIX, or, petrole"),
            ("!earnings","Resultats entreprises semaine"),
            ("!graphique TICKER","Graphique MM50/200 + Bollinger"),
            ("!alerte NVDA 150","Alerte si NVDA < 150$"),
            ("!alerte NVDA +5%","Alerte si NVDA +5%"),
            ("!alertes","Voir alertes actives"),
            ("!supprimer TICKER","Supprimer alerte"),
            ("!watch TICKER","Ajouter watchlist"),
            ("!unwatch TICKER","Retirer watchlist"),
            ("!watchlist","Voir watchlist"),
            ("!backtest TICKER","Backtest RSI 6 mois"),
            ("!buy T QTE PRIX","Enregistrer achat"),
            ("!sell T QTE","Enregistrer vente"),
            ("!portfolio","Portefeuille + P&L"),
            ("!semaine","Rapport hebdomadaire"),
            ("!prix TICKER","Prix actuel"),
        ]
        for c,d in cmds: em.add_field(name=c,value=d,inline=True)
        em.set_footer(text="Dashboard : http://localhost:5000 | Horaires : 08h00-22h45")
        await ctx.send(embed=em)

# =====================================================
#   DASHBOARD FLASK
# =====================================================
HTML="""<!DOCTYPE html><html lang="fr"><head>
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
.taux-badge{display:inline-block;padding:2px 8px;border-radius:100px;font-size:11px}
.taux-ok{background:#0a2d1a;color:#1D9E75}
.taux-moy{background:#2d1a00;color:#BA7517}
.taux-ko{background:#2d0a0a;color:#E24B4A}
</style></head><body>
<h1>Scanner Bourse V6 — Railway <span class="refresh">{{ now }} — <a href="/refresh" style="color:#5865F2">Actualiser</a></span></h1>

<div class="section"><h2>Resume</h2>
<div class="grid">
  <div class="card"><div class="label">Actions analysees</div><div class="value">{{ s.total }}</div></div>
  <div class="card"><div class="label">Score moyen</div><div class="value">{{ s.score_moy }}<span style="font-size:14px;color:#666">/100</span></div></div>
  <div class="card"><div class="label">RSI&lt;32 (rebonds)</div><div class="value">{{ s.survente }}</div></div>
  <div class="card"><div class="label">Predictions verifiees</div><div class="value" style="color:#5865F2">{{ s.predictions }}</div></div>
  <div class="card"><div class="label">Taux reussite global</div>
    <div class="value {% if s.taux_global>=60 %}up{% elif s.taux_global>=40 %}{% else %}down{% endif %}">
      {{ s.taux_global }}%
    </div>
  </div>
</div></div>

<div class="section"><h2>Top 20 Opportunites</h2>
<table>
  <tr><th>#</th><th>Ticker</th><th>Secteur</th><th>Prix</th><th>1j</th><th>5j</th><th>RSI</th><th>Bollinger</th><th>Score</th><th>Taux Reel</th></tr>
{% for a in top20 %}<tr>
  <td>{{ loop.index }}</td><td><strong>{{ a.ticker }}</strong></td>
  <td style="font-size:11px;color:#666">{{ a.secteur }}</td><td>{{ a.prix }}</td>
  <td class="{{ 'up' if a.var_1j>=0 else 'down' }}">{{ '+' if a.var_1j>=0 else '' }}{{ a.var_1j }}%</td>
  <td class="{{ 'up' if a.var_5j>=0 else 'down' }}">{{ '+' if a.var_5j>=0 else '' }}{{ a.var_5j }}%</td>
  <td>{{ a.rsi }}</td>
  <td style="font-size:11px">{{ a.boll.signal if a.boll else '-' }}</td>
  <td class="{{ 'sh' if a.score>=75 else 'sm' if a.score>=55 else 'sl' }}">{{ a.score }}/100</td>
  <td>
    {% if a.taux_reel is not none %}
      <span class="taux-badge {{ 'taux-ok' if a.taux_reel>=60 else 'taux-moy' if a.taux_reel>=40 else 'taux-ko' }}">
        {{ a.taux_reel }}%
      </span>
    {% else %}<span style="color:#666;font-size:11px">En cours</span>{% endif %}
  </td>
</tr>{% endfor %}</table></div>

{% if stats_tickers %}
<div class="section"><h2>Taux de Reussite Reel par Ticker</h2>
<table>
  <tr><th>Ticker</th><th>Signaux</th><th>Taux J+1</th><th>Gain Moyen</th></tr>
{% for r in stats_tickers %}<tr>
  <td><strong>{{ r[0] }}</strong></td>
  <td>{{ r[1]|int }}</td>
  <td class="{{ 'up' if r[1]>0 and r[2]/r[1]*100>=60 else 'down' }}">
    {{ (r[2]/r[1]*100)|round(1) if r[1]>0 else 0 }}%
  </td>
  <td class="{{ 'up' if r[3] and r[3]>=0 else 'down' }}">{{ '+' if r[3] and r[3]>=0 else '' }}{{ r[3]|round(2) if r[3] else 0 }}%</td>
</tr>{% endfor %}</table></div>
{% endif %}

{% if portfolio %}
<div class="section"><h2>Portefeuille</h2>
<table><tr><th>Ticker</th><th>Qte</th><th>PRU</th><th>Actuel</th><th>Valeur</th><th>P&L</th></tr>
{% for p in portfolio %}<tr>
  <td><strong>{{ p.ticker }}</strong></td><td>{{ p.quantite }}</td>
  <td>{{ p.prix_achat }}$</td><td>{{ p.prix_actuel }}$</td><td>{{ p.valeur }}$</td>
  <td class="{{ 'pp' if p.pnl>=0 else 'pn' }}">{{ '+' if p.pnl>=0 else '' }}{{ p.pnl }}$</td>
</tr>{% endfor %}</table></div>
{% endif %}

<script>setTimeout(()=>location.reload(),300000);</script>
</body></html>"""

app=Flask(__name__)
_cache={"resultats":[],"secteurs":{}}

@app.route("/")
def dashboard():
    global _cache
    if not _cache["resultats"]:
        _cache["resultats"],_cache["secteurs"]=analyser_actions()
    r=_cache["resultats"]

    # Ajoute le taux reel a chaque action
    top20=[]
    for a in r[:20]:
        taux=get_taux_reussite_ticker(a["ticker"])
        top20.append({**a,"taux_reel":taux})

    # Stats globales
    conn=db();c=conn.cursor()
    verif_total=c.execute("SELECT COUNT(*) FROM signaux_momentum WHERE verifie_j1=1").fetchone()[0]
    reussites_total=c.execute("SELECT SUM(succes_j1) FROM signaux_momentum WHERE verifie_j1=1").fetchone()[0] or 0
    stats_tickers=c.execute("""SELECT ticker,COUNT(*),SUM(succes_j1),AVG(var_reelle_j1)
                                FROM signaux_momentum WHERE verifie_j1=1
                                GROUP BY ticker HAVING COUNT(*)>=3
                                ORDER BY AVG(var_reelle_j1) DESC LIMIT 10""").fetchall()
    conn.close()

    taux_global=round(reussites_total/verif_total*100,1) if verif_total>0 else 0

    pf=charger_portefeuille();port=[]
    for pos in pf:
        try: pa=round(yf.Ticker(pos["ticker"]).history(period="1d")["Close"].iloc[-1],2)
        except: pa=pos["prix_achat"]
        port.append({**pos,"prix_actuel":pa,"valeur":round(pa*pos["quantite"],2),"pnl":round((pa-pos["prix_achat"])*pos["quantite"],2)})

    s={"total":len(r),"score_moy":round(sum(x["score"] for x in r)/len(r),1) if r else 0,
       "survente":sum(1 for x in r if x["rsi"]<32),
       "predictions":verif_total,"taux_global":taux_global}

    return render_template_string(HTML,
                                  now=datetime.now().strftime("%d/%m/%Y %H:%M"),
                                  top20=top20,portfolio=port,
                                  s=s,stats_tickers=stats_tickers)

@app.route("/refresh")
def refresh():
    global _cache
    _cache["resultats"],_cache["secteurs"]=analyser_actions()
    return {"ok":True,"actions":len(_cache["resultats"])}

@app.route("/health")
def health():
    """Endpoint de sante pour Railway."""
    return {"status":"ok","time":datetime.now().isoformat(),"actions":len(_cache["resultats"])}

# =====================================================
#   LANCEMENT
# =====================================================
if __name__=="__main__":
    print("="*55)
    print("  SCANNER BOURSE V6 — RAILWAY OPTIMISE")
    print("="*55)
    print(f"  Actions scanner : {len(TOUTES_ACTIONS_SCANNER)}")
    print(f"  Actions momentum : {len(TOUTES_ACTIONS_MOMENTUM)}")
    print(f"  Demarrage : {HORAIRES['demarrage']}")
    print(f"  Arret automatique : {HORAIRES['arret']}")
    print(f"  Port : {PORT}")
    print("="*55)

    init_db()

    # Flask dans un thread
    threading.Thread(
        target=lambda:app.run(host="0.0.0.0",port=PORT,debug=False,use_reloader=False),
        daemon=True
    ).start()
    print(f"Dashboard demarre sur le port {PORT}")

    if DISCORD_PY:
        bot.run(DISCORD_TOKEN)
    else:
        print("Installe discord.py : pip install discord.py")
        while True: time.sleep(60)
