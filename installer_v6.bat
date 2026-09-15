@echo off
chcp 1252 >nul
title Scanner Bourse V6 - Installation

echo.
echo =====================================================
echo    SCANNER BOURSE V6 - INSTALLATION AUTOMATIQUE
echo    Railway + Verification predictions + Stats reelles
echo =====================================================
echo.

:: Verification Python
echo [1/6] Verification de Python...
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERREUR : Python n'est pas installe !
    echo Va sur https://python.org/downloads
    echo Installe Python et coche "Add Python to PATH"
    pause
    exit /b 1
)
python --version
echo OK - Python trouve !
echo.

:: Installation librairies
echo [2/6] Installation des librairies (2-3 minutes)...
echo.
pip install --upgrade pip --quiet
pip install yfinance requests feedparser flask beautifulsoup4 numpy discord.py matplotlib schedule ntscraper gunicorn --quiet
if %errorlevel% neq 0 (
    echo ERREUR lors de l'installation.
    echo Essaie : clic droit puis "Executer en tant qu'administrateur"
    pause
    exit /b 1
)
echo OK - Librairies installees !
echo.

:: Verification fichiers
echo [3/6] Verification des fichiers...
if not exist "%~dp0scanner_v6.py" (
    echo ERREUR : scanner_v6.py introuvable !
    echo Mets tous les fichiers dans le meme dossier.
    pause
    exit /b 1
)
echo OK - scanner_v6.py trouve !
echo.

:: Configuration Discord
echo [4/6] Configuration Discord...
echo.
echo -----------------------------------------------------
echo TOKEN DISCORD :
echo   1. Va sur discord.com/developers/applications
echo   2. Clique sur ton bot
echo   3. Bot - Reset Token - Copie le token
echo -----------------------------------------------------
echo.
set /p "DTOKEN=Entre ton Token Discord : "
if "%DTOKEN%"=="" (
    echo ERREUR : Token vide !
    pause
    exit /b 1
)

echo.
echo -----------------------------------------------------
echo CHANNEL ID :
echo   Discord - Parametres - Avance - Mode developpeur ON
echo   Clic droit sur ton salon - Copier l'identifiant
echo -----------------------------------------------------
echo.
set /p "DCHANNEL=Entre ton Channel ID : "
if "%DCHANNEL%"=="" (
    echo ERREUR : Channel ID vide !
    pause
    exit /b 1
)

:: Ecriture config
echo.
echo Ecriture de la configuration...
python -c "
import re, sys
token   = sys.argv[1]
channel = sys.argv[2]
fichier = sys.argv[3]
with open(fichier, 'r', encoding='utf-8') as f:
    c = f.read()
c = re.sub(r'DISCORD_TOKEN\s*=\s*os\.environ\.get\([^)]+\)', 'DISCORD_TOKEN = os.environ.get(\"DISCORD_TOKEN\", \"' + token + '\")', c)
c = re.sub(r'CHANNEL_ID\s*=\s*int\(os\.environ\.get\([^)]+\)\)', 'CHANNEL_ID = int(os.environ.get(\"CHANNEL_ID\", \"' + channel + '\"))', c)
with open(fichier, 'w', encoding='utf-8') as f:
    f.write(c)
print('OK')
" "%DTOKEN%" "%DCHANNEL%" "%~dp0scanner_v6.py"

if %errorlevel% neq 0 (
    echo ERREUR lors de la configuration.
    pause
    exit /b 1
)
echo OK - Configuration enregistree !
echo.

:: Info Railway
echo [5/6] Information Railway (pour le 24/7)...
echo.
echo -----------------------------------------------------
echo Pour faire tourner le bot 24h/24 sur Railway :
echo.
echo   1. Va sur railway.app et cree un compte gratuit
echo   2. Clique "New Project" - "Deploy from GitHub"
echo   3. Upload tes fichiers OU connecte GitHub
echo   4. Dans Settings - Variables, ajoute :
echo        DISCORD_TOKEN = ton_token
echo        CHANNEL_ID    = ton_channel_id
echo   5. Railway demarre automatiquement le bot !
echo.
echo Le bot demarre a 08h00 et s'arrete a 22h45
echo (jours de bourse uniquement - lundi a vendredi)
echo -----------------------------------------------------
echo.

:: Creation lancer_v6.bat
echo [6/6] Creation du raccourci lancer_v6.bat...
(
echo @echo off
echo chcp 1252 ^>nul
echo title Scanner Bourse V6
echo echo.
echo echo =====================================================
echo echo    SCANNER BOURSE V6 - EN COURS
echo echo =====================================================
echo echo.
echo echo Dashboard  : http://localhost:5000
echo echo Demarrage  : 08h00
echo echo Arret auto : 22h45
echo echo.
echo echo Ferme cette fenetre pour arreter le bot.
echo echo.
echo cd /d "%%~dp0"
echo python scanner_v6.py
echo pause
) > "%~dp0lancer_v6.bat"

echo OK - lancer_v6.bat cree !
echo.

echo.
echo =====================================================
echo    INSTALLATION V6 TERMINEE !
echo =====================================================
echo.
echo Nouveautes V6 :
echo   - Verification auto des predictions J+1 et J+3
echo   - Taux de reussite reel par action
echo   - Score ameliore par l'historique reel
echo   - Rapport pre-marche a 08h15
echo   - Rapport de cloture EU a 17h30
echo   - Rapport final + arret auto a 22h45
echo   - Compatible Railway pour le 24/7
echo.
echo Prochaine etape : depose les fichiers sur Railway
echo pour que le bot tourne sans ton PC !
echo.
echo Demarrage local dans 5 secondes...
timeout /t 5 /nobreak >nul

cd /d "%~dp0"
python scanner_v6.py
pause
