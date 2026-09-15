"""
stats_auto.py — Module de stats automatiques pour scanner_v6.py
Place ce fichier dans le meme dossier que scanner_v6.py
Il est importe automatiquement au demarrage.
"""
import sqlite3
import time
import requests
from datetime import datetime, timedelta


def rapport_stats_automatique(db_path, discord_token, channel_id):
    """
    Envoye automatiquement chaque matin :
    - Taux de reussite global du bot (le bot avait-il raison ?)
    - Top 5 tickers les plus fiables
    - Ce qui s est passe hier (signal prevu vs resultat reel)
    """
    def db():
        return sqlite3.connect(db_path, check_same_thread=False)

    def envoyer(titre, description, couleur, champs=None):
        url  = f"https://discord.com/api/v10/channels/{channel_id}/messages"
        hdrs = {"Authorization": f"Bot {discord_token}", "Content-Type": "application/json"}
        embed = {
            "title":       titre[:256],
            "description": description[:4096],
            "color":       couleur,
            "fields":      (champs or [])[:25],
            "footer":      {"text": "Scanner V6 — Stats automatiques"},
            "timestamp":   datetime.utcnow().isoformat(),
        }
        try:
            requests.post(url, headers=hdrs, json={"embeds": [embed]}, timeout=10)
            time.sleep(0.5)
        except Exception as e:
            print(f"Discord stats : {e}")

    try:
        conn = db()
        c    = conn.cursor()

        # Taux global
        row = c.execute(
            "SELECT COUNT(*), SUM(succes_j1), AVG(var_reelle_j1) "
            "FROM signaux_momentum WHERE verifie_j1=1"
        ).fetchone()

        if not row or not row[0] or row[0] < 3:
            conn.close()
            return

        total, reussites, gain_moy = row
        taux_global = round((reussites or 0) / total * 100, 1)

        # Top 5 tickers les plus fiables (meilleur gain moyen)
        top = c.execute(
            "SELECT ticker, COUNT(*) as nb, SUM(succes_j1) as ok, AVG(var_reelle_j1) as gain "
            "FROM signaux_momentum WHERE verifie_j1=1 "
            "GROUP BY ticker HAVING COUNT(*)>=3 "
            "ORDER BY AVG(var_reelle_j1) DESC LIMIT 5"
        ).fetchall()

        # Verifications d hier
        hier = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        hier_rows = c.execute(
            "SELECT ticker, var_reelle_j1, succes_j1, variation_signal "
            "FROM signaux_momentum "
            "WHERE date_verif_j1 LIKE ? AND verifie_j1=1 "
            "ORDER BY var_reelle_j1 DESC LIMIT 5",
            (hier + "%",)
        ).fetchall()

        conn.close()

        if not top:
            return

        champs = []

        # 1. Taux global
        emoji_g = "OK" if taux_global >= 60 else "MOY" if taux_global >= 40 else "KO"
        val_global = (
            emoji_g + " " + str(taux_global) + "% de reussite"
            + " sur " + str(int(total)) + " signaux verifies"
            + " | Gain moyen : " + str(round(gain_moy or 0, 2)) + "%"
        )
        champs.append({"name": "Taux de reussite global", "value": val_global, "inline": False})

        # 2. Top tickers
        top_lines = []
        for ticker, nb, ok, gain in top:
            taux  = round((ok or 0) / nb * 100, 1)
            emoji = "OK" if taux >= 60 else "MOY" if taux >= 40 else "KO"
            ligne = (
                emoji + " **" + str(ticker) + "** : "
                + str(taux) + "% (" + str(int(nb)) + " signaux)"
                + " | gain moy : " + str(round(gain or 0, 2)) + "%"
            )
            top_lines.append(ligne)
        champs.append({
            "name":   "Top tickers les plus fiables",
            "value":  "\n".join(top_lines) or "En cours d apprentissage...",
            "inline": False,
        })

        # 3. Verifications d hier
        if hier_rows:
            hier_lines = []
            for ticker, var_reelle, succes, var_signal in hier_rows:
                emoji = "OK" if succes else "KO"
                ligne = (
                    emoji + " **" + str(ticker) + "**"
                    + " : signal +" + str(round(var_signal or 0, 1)) + "%"
                    + " -> reel " + str(round(var_reelle or 0, 2)) + "%"
                )
                hier_lines.append(ligne)
            champs.append({
                "name":   "Verifications d hier",
                "value":  "\n".join(hier_lines),
                "inline": False,
            })

        envoyer(
            "Stats Predictions — " + datetime.now().strftime("%d/%m/%Y"),
            "Le bot avait-il raison hier ? Bilan automatique.",
            0x5865F2,
            champs
        )

    except Exception as e:
        print(f"Erreur stats auto : {e}")
