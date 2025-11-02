from velodata import lib as velo
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta

CACHE_FILE = "/tmp/market_cap_cache.json"  # DigitalOcean için /tmp kullan
CACHE_DURATION_DAYS = 7

def should_refresh_cache():
    """Cache'in yenilenmesi gerekip gerekmediğini kontrol et"""
    if not os.path.exists(CACHE_FILE):
        return True
    
    try:
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            cache_time = datetime.fromisoformat(data['timestamp'])
            age = datetime.now() - cache_time
            return age > timedelta(days=CACHE_DURATION_DAYS)
    except:
        return True

def get_market_cap_thresholds(client, coins_no_usdt):
    """Market cap verilerini çek ve threshold'ları hesapla"""
    print(f"📊 Market cap verileri çekiliyor... (Cache: {CACHE_FILE})")
    
    records = []
    skipped = []
    
    for coin in coins_no_usdt:
        try:
            df = client.get_market_caps([coin])
            if df is None or df.empty or not {"coin", "circ_dollars"} <= set(df.columns):
                skipped.append(coin)
                continue
            
            row = df.iloc[0]
            records.append({
                "coin": str(row["coin"]).upper(),
                "market_cap": row["circ_dollars"],
            })
        except Exception:
            skipped.append(coin)
    
    caps = pd.DataFrame.from_records(records, columns=["coin", "market_cap"])
    
    # 10 milyon üstündekileri filtrele
    caps_filtered = caps[caps["market_cap"] >= 10_000_000].copy()
    
    # Market cap bazlı threshold'ları belirle
    conditions = [
        (caps_filtered["market_cap"] >= 10_000_000) & (caps_filtered["market_cap"] <= 20_000_000),
        (caps_filtered["market_cap"] >= 20_000_000) & (caps_filtered["market_cap"] <= 50_000_000),
        (caps_filtered["market_cap"] >= 51_000_000) & (caps_filtered["market_cap"] <= 100_000_000),
        (caps_filtered["market_cap"] >= 101_000_000) & (caps_filtered["market_cap"] <= 250_000_000),
        (caps_filtered["market_cap"] >= 251_000_000),
    ]
    
    values = [400_000, 500_000, 1_000_000, 1_500_000, 2_000_000]
    
    caps_filtered["DELTA_THRESHOLD"] = np.select(conditions, values, default=np.nan)
    caps_filtered["coin"] = caps_filtered["coin"] + "USDT"
    
    # Cache'e kaydet
    cache_data = {
        'timestamp': datetime.now().isoformat(),
        'thresholds': caps_filtered.to_dict('records'),
        'skipped_count': len(skipped)
    }
    
    # /tmp dizininin var olduğundan emin ol
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache_data, f, indent=2)
    
    print(f"✅ {len(caps_filtered)} coin için threshold belirlendi")
    print(f"⚠️ {len(skipped)} coin atlandı (veri yok veya market cap < 10M)")
    
    return caps_filtered

def load_cached_thresholds():
    """Cache'den threshold'ları yükle"""
    try:
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            df = pd.DataFrame(data['thresholds'])
            cache_time = datetime.fromisoformat(data['timestamp'])
            age = datetime.now() - cache_time
            print(f"📦 Cache yüklendi (Yaş: {age.days} gün, {age.seconds//3600} saat)")
            return df
    except Exception as e:
        print(f"❌ Cache yüklenemedi: {e}")
        return None

def get_thresholds(all_coins):
    """Threshold'ları al (cache'den veya yeni çekerek)"""
    velo_api_key = os.environ.get('VELO_API_KEY', '95dfb6f119584a84aaadae6516c97e43')
    client = velo.client(velo_api_key)
    coins_no_usdt = [c.replace("USDT", "") for c in all_coins]
    
    if should_refresh_cache():
        print("🔄 Cache yok veya eski, yeni veriler çekiliyor...")
        return get_market_cap_thresholds(client, coins_no_usdt)
    else:
        cached = load_cached_thresholds()
        if cached is not None:
            return cached
        else:
            print("🔄 Cache yüklenemedi, yeni veriler çekiliyor...")
            return get_market_cap_thresholds(client, coins_no_usdt)
