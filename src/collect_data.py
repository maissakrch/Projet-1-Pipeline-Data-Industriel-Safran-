import requests
import pandas as pd
import sqlite3
import os
from glob import glob

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------

API_URL = "https://jsonplaceholder.typicode.com/posts"  # API publique pour simuler une API Safran
RAW_DATA_DIR = "data/raw/"
BIGDATA_DIR = "data/raw/bigdata/"
SQLITE_DB = "data/raw/source.db"

# ---------------------------------------------------------
# 1. Collecte depuis une API REST
# ---------------------------------------------------------

def collect_from_api():
    print("📡 Collecte depuis API REST...")
    response = requests.get(API_URL)
    response.raise_for_status()
    data = response.json()
    df_api = pd.DataFrame(data)
    print(f"✔️ API : {len(df_api)} lignes récupérées")
    return df_api

# ---------------------------------------------------------
# 2. Collecte depuis des fichiers CSV (NASA)
# ---------------------------------------------------------

def collect_from_csv():
    print("📁 Collecte depuis fichiers CSV NASA...")
    csv_files = glob(os.path.join(RAW_DATA_DIR, "*.csv"))
    
    if not csv_files:
        print("⚠️ Aucun fichier CSV trouvé dans data/raw/")
        return pd.DataFrame()

    df_list = [pd.read_csv(f) for f in csv_files]
    df_csv = pd.concat(df_list, ignore_index=True)
    print(f"✔️ CSV : {len(df_csv)} lignes récupérées")
    return df_csv

# ---------------------------------------------------------
# 3. Collecte depuis une base SQL (NASA)
# ---------------------------------------------------------

def collect_from_sql():
    print("🗄️ Collecte depuis base SQL NASA...")
    conn = sqlite3.connect(SQLITE_DB)
    query = "SELECT * FROM capteurs;"
    df_sql = pd.read_sql_query(query, conn)
    conn.close()
    print(f"✔️ SQL : {len(df_sql)} lignes récupérées")
    return df_sql

# ---------------------------------------------------------
# 4. Collecte depuis une source “big data” simulée
# ---------------------------------------------------------

def collect_from_bigdata():
    print("📦 Collecte depuis source big data NASA...")
    big_files = glob(os.path.join(BIGDATA_DIR, "*.csv"))

    if not big_files:
        print("⚠️ Aucun fichier big data trouvé.")
        return pd.DataFrame()

    df_list = [pd.read_csv(f) for f in big_files]
    df_big = pd.concat(df_list, ignore_index=True)
    print(f"✔️ Big Data : {len(df_big)} lignes récupérées")
    return df_big

# ---------------------------------------------------------
# POINT D’ENTRÉE PRINCIPAL
# ---------------------------------------------------------

if __name__ == "__main__":
    print("🚀 DÉMARRAGE DE LA COLLECTE MULTI-SOURCES NASA")

    df_api = collect_from_api()
    df_csv = collect_from_csv()
    df_sql = collect_from_sql()
    df_big = collect_from_bigdata()

    print("\n🎉 Collecte terminée.")
