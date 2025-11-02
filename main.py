from velodata import lib as velo
import pandas as pd
import time
import os
import requests
from datetime import datetime, timedelta
import schedule
from market_cap_cache import get_thresholds

# Environment variables
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
VELO_API_KEY = os.environ.get('VELO_API_KEY')

# Constants
ALL_COINS = ['0GUSDT', '1MBABYDOGEUSDT', '1000000BOBUSDT', '1000000MOGUSDT', 
         '1000WHYUSDT', '1000BONKUSDT', '1000CATUSDT', '1000CHEEMSUSDT', 
         '1000FLOKIUSDT', '1000LUNCUSDT', '1000PEPEUSDT', '1000RATSUSDT', 
         '1000SATSUSDT', '1000SHIBUSDT', 'TURBOUSDT', '1000XUSDT', 
         '1000XECUSDT', '1INCHUSDT', '2ZUSDT', '4USDT', 'AUSDT', 'A2ZUSDT', 
         'AAVEUSDT', 'ACEUSDT', 'ACHUSDT', 'ACTUSDT', 'ACXUSDT', 'ADAUSDT', 
         'AERGOUSDT', 'AEROUSDT', 'AEVOUSDT', 'AGLDUSDT', 'AGTUSDT', 'AIUSDT', 
         'AI16ZUSDT', 'AIAUSDT', 'AINUSDT', 'AIOUSDT', 'AIOTUSDT', 'AIXBTUSDT', 
         'AKEUSDT', 'AKTUSDT', 'ALCHUSDT', 'ALGOUSDT', 'ALICEUSDT', 'ALLUSDT', 
         'ALPINEUSDT', 'ALTUSDT', 'ANIMEUSDT', 'ANKRUSDT', 'APEUSDT', 'API3USDT', 
         'APTUSDT', 'ARUSDT', 'ARBUSDT', 'ARCUSDT', 'ARIAUSDT', 'ARKUSDT', 
         'ARKMUSDT', 'ARPAUSDT', 'ASRUSDT', 'ASTERUSDT', 'ASTRUSDT', 'ATAUSDT', 
         'ATHUSDT', 'ATOMUSDT', 'AUCTIONUSDT', 'AVAUSDT', 'AVAAIUSDT', 'AVAXUSDT', 
         'AVNTUSDT', 'AWEUSDT', 'AXLUSDT', 'AXSUSDT', 'BUSDT', 'B2USDT', 'B3USDT', 
         'BABYUSDT', 'BANUSDT', 'BANANAUSDT', 'BANANAS31USDT', 'BANDUSDT', 
         'BANKUSDT', 'BARDUSDT', 'BASUSDT', 'BATUSDT', 'BBUSDT', 'BCHUSDT', 
         'BDXNUSDT', 'BEAMXUSDT', 'BELUSDT', 'BERAUSDT', 'BICOUSDT', 'BIDUSDT', 
         'BIGTIMEUSDT', 'BIOUSDT', 'BLESSUSDT', 'BLURUSDT', 'BMTUSDT', 'BNBUSDT', 
         'BNTUSDT', 'BOMEUSDT', 'BRUSDT', 'BRETTUSDT', 'BROCCOLI714USDT', 
         'BROCCOLIF3BUSDT', 'BSVUSDT', 'BTRUSDT', 'BULLAUSDT', 'CUSDT', 
         'C98USDT', 'CAKEUSDT', 'CARVUSDT', 'CATIUSDT', 'CELOUSDT', 'CELRUSDT', 
         'CETUSUSDT', 'CFXUSDT', 'CGPTUSDT', 'CHESSUSDT', 'CHILLGUYUSDT', 'CHRUSDT', 
         'CHZUSDT', 'CKBUSDT', 'CLOUSDT', 'COAIUSDT', 'COMPUSDT', 'COOKIEUSDT', 
         'COSUSDT', 'COTIUSDT', 'COWUSDT', 'CROSSUSDT', 'CRVUSDT', 'CTKUSDT', 
         'CTSIUSDT', 'CUDISUSDT', 'CVCUSDT', 'CVXUSDT', 'CYBERUSDT', 'DUSDT', 
         'DAMUSDT', 'DASHUSDT', 'DEEPUSDT', 'DEGENUSDT', 'DEGOUSDT', 'DENTUSDT', 
         'DEXEUSDT', 'DFUSDT', 'DIAUSDT', 'DMCUSDT', 'DODOXUSDT', 'DOGEUSDT', 
         'DOGSUSDT', 'DOLOUSDT', 'DOODUSDT', 'DOTUSDT', 'DRIFTUSDT', 'DUSKUSDT', 
         'DYDXUSDT', 'DYMUSDT', 'EDENUSDT', 'EDUUSDT', 'EGLDUSDT', 'EIGENUSDT', 
         'ENAUSDT', 'ENJUSDT', 'ENSUSDT', 'ENSOUSDT', 'EPICUSDT', 'EPTUSDT', 
         'ESPORTSUSDT', 'ETCUSDT', 'ETHFIUSDT', 'ETHWUSDT', 'EULUSDT', 
         'EVAAUSDT', 'FUSDT', 'FARTCOINUSDT', 'FETUSDT', 'FFUSDT', 'FHEUSDT', 
         'FIDAUSDT', 'FILUSDT', 'FIOUSDT', 'FISUSDT', 'FLMUSDT', 'FLOCKUSDT', 
         'FLOWUSDT', 'FLUIDUSDT', 'FLUXUSDT', 'FORMUSDT', 'FORTHUSDT', 'FUNUSDT', 
         'FXSUSDT', 'GUSDT', 'GALAUSDT', 'GASUSDT', 'GHSTUSDT', 'GIGGLEUSDT', 
         'GLMUSDT', 'GMTUSDT', 'GMXUSDT', 'GOATUSDT', 'GPSUSDT', 'GRASSUSDT', 
         'GRIFFAINUSDT', 'GRTUSDT', 'GTCUSDT', 'GUNUSDT', 'HUSDT', 'HAEDALUSDT', 
         'HANAUSDT', 'HBARUSDT', 'HEIUSDT', 'HEMIUSDT', 'HFTUSDT', 'HIGHUSDT', 
         'HIPPOUSDT', 'HIVEUSDT', 'HMSTRUSDT', 'HOLOUSDT', 'HOMEUSDT', 'HOOKUSDT', 
         'HOTUSDT', 'HUMAUSDT', 'HYPEUSDT', 'HYPERUSDT', 'ICNTUSDT', 'ICPUSDT', 
         'ICXUSDT', 'IDUSDT', 'IDOLUSDT', 'ILVUSDT', 'IMXUSDT', 'INUSDT', 'INITUSDT', 
         'INJUSDT', 'IOUSDT', 'IOSTUSDT', 'IOTAUSDT', 'IOTXUSDT', 'IPUSDT', 
         'JASMYUSDT', 'JELLYJELLYUSDT', 'JOEUSDT', 'JSTUSDT', 'JTOUSDT', 'JUPUSDT', 
         'KAIAUSDT', 'KAITOUSDT', 'KASUSDT', 'KAVAUSDT', 'KDAUSDT', 'KERNELUSDT', 
         'KGENUSDT', 'KMNOUSDT', 'KNCUSDT', 'KOMAUSDT', 'KSMUSDT', 'LAUSDT', 
         'LABUSDT', 'LAYERUSDT', 'LDOUSDT', 'LINEAUSDT', 'LINKUSDT', 'LISTAUSDT', 
         'LPTUSDT', 'LQTYUSDT', 'LRCUSDT', 'LSKUSDT', 'LTCUSDT', 'LUMIAUSDT', 
         'LUNA2USDT', 'LYNUSDT', 'MUSDT', 'MAGICUSDT', 'MANAUSDT', 'MANTAUSDT', 
         'MASKUSDT', 'MAVUSDT', 'MAVIAUSDT', 'MBOXUSDT', 'MEUSDT', 'MELANIAUSDT', 
         'MEMEUSDT', 'MERLUSDT', 'METISUSDT', 'MEWUSDT', 'MILKUSDT', 'MINAUSDT', 
         'MIRAUSDT', 'MITOUSDT', 'MLNUSDT', 'MOCAUSDT', 'MONUSDT', 'MOODENGUSDT', 
         'MORPHOUSDT', 'MOVEUSDT', 'MOVRUSDT', 'MTLUSDT', 'MUBARAKUSDT', 'MYROUSDT', 
         'MYXUSDT', 'NAORISUSDT', 'NEARUSDT', 'NEIROUSDT', 'NEOUSDT', 'NEWTUSDT', 
         'NFPUSDT', 'NILUSDT', 'NKNUSDT', 'NMRUSDT', 'NOMUSDT', 'NOTUSDT', 'NTRNUSDT', 
         'NXPCUSDT', 'OBOLUSDT', 'OGUSDT', 'OGNUSDT', 'OLUSDT', 'OMUSDT', 'ONDOUSDT', 
         'ONEUSDT', 'ONGUSDT', 'ONTUSDT', 'OPUSDT', 'OPENUSDT', 'ORCAUSDT', 
         'ORDERUSDT', 'ORDIUSDT', 'OXTUSDT', 'PARTIUSDT', 'PAXGUSDT', 'PENDLEUSDT', 
         'PENGUUSDT', 'PEOPLEUSDT', 'PERPUSDT', 'PHAUSDT', 'PHBUSDT', 'PIPPINUSDT', 
         'PIXELUSDT', 'PLAYUSDT', 'PLUMEUSDT', 'PNUTUSDT', 'POLUSDT', 'POLYXUSDT', 
         'PONKEUSDT', 'POPCATUSDT', 'PORT3USDT', 'PORTALUSDT', 'POWRUSDT', 'PROMUSDT', 
         'PROMPTUSDT', 'PROVEUSDT', 'PTBUSDT', 'PUFFERUSDT', 'PUMPUSDT', 'PUMPBTCUSDT', 
         'PUNDIXUSDT', 'PYTHUSDT', 'QUSDT', 'QNTUSDT', 'QTUMUSDT', 'QUICKUSDT', 
         'RAREUSDT', 'RAYSOLUSDT', 'RDNTUSDT', 'RECALLUSDT', 'REDUSDT', 'REIUSDT', 
         'RENDERUSDT', 'RESOLVUSDT', 'REZUSDT', 'RIFUSDT', 'RIVERUSDT', 'RLCUSDT', 
         'RONINUSDT', 'ROSEUSDT', 'RPLUSDT', 'RSRUSDT', 'RUNEUSDT', 'RVNUSDT', 
         'SUSDT', 'SAFEUSDT', 'SAGAUSDT', 'SAHARAUSDT', 'SANDUSDT', 'SANTOSUSDT', 
         'SAPIENUSDT', 'SCRUSDT', 'SCRTUSDT', 'SEIUSDT', 'SFPUSDT', 'SHELLUSDT', 
         'SIGNUSDT', 'SIRENUSDT', 'SKATEUSDT', 'SKLUSDT', 'SKYUSDT', 'SKYAIUSDT', 
         'SLERFUSDT', 'SLPUSDT', 'SNXUSDT', 'SOLUSDT', 'SOMIUSDT', 'SONICUSDT', 
         'SOONUSDT', 'SOPHUSDT', 'SPELLUSDT', 'SPKUSDT', 'SPXUSDT', 'SQDUSDT', 
         'SSVUSDT', 'STBLUSDT', 'STEEMUSDT', 'STGUSDT', 'STOUSDT', 'STORJUSDT', 
         'STRKUSDT', 'STXUSDT', 'SUIUSDT', 'SUNUSDT', 'SUPERUSDT', 'SUSHIUSDT', 
         'SWARMSUSDT', 'SWELLUSDT', 'SXPUSDT', 'SXTUSDT', 'SYNUSDT', 'SYRUPUSDT', 
         'SYSUSDT', 'TUSDT', 'TAUSDT', 'TACUSDT', 'TAGUSDT', 'TAIKOUSDT', 'TAKEUSDT', 
         'TANSSIUSDT', 'TAOUSDT', 'THEUSDT', 'THETAUSDT', 'TIAUSDT', 'TLMUSDT', 
         'TNSRUSDT', 'TOKENUSDT', 'TONUSDT', 'TOSHIUSDT', 'TOWNSUSDT', 'TRADOORUSDT', 
         'TRBUSDT', 'TREEUSDT', 'TRUUSDT', 'TRUMPUSDT', 'TRUTHUSDT', 'TRXUSDT', 
         'TSTUSDT', 'TUTUSDT', 'TWTUSDT', 'UBUSDT', 'UMAUSDT', 'UNIUSDT', 'USDCUSDT', 
         'USELESSUSDT', 'USTCUSDT', 'USUALUSDT', 'VANAUSDT', 'VANRYUSDT', 
         'VELODROMEUSDT', 'VELVETUSDT', 'VETUSDT', 'VFYUSDT', 'VICUSDT', 'VINEUSDT', 
         'VIRTUALUSDT', 'VOXELUSDT', 'VTHOUSDT', 'VVVUSDT', 'WUSDT', 'WALUSDT', 
         'WAXPUSDT', 'WCTUSDT', 'WIFUSDT', 'WLDUSDT', 'WLFIUSDT', 'WOOUSDT', 'XAIUSDT', 
         'XANUSDT', 'XCNUSDT', 'XLMUSDT', 'XMRUSDT', 'XNYUSDT', 'XPINUSDT', 'XPLUSDT', 
         'XTZUSDT', 'XVGUSDT', 'XVSUSDT', 'YALAUSDT', 'YBUSDT', 'YFIUSDT', 
         'YGGUSDT', 'ZBTUSDT', 'ZECUSDT', 'ZENUSDT', 'ZEREBROUSDT', 'ZETAUSDT', 
         'ZILUSDT', 'ZKUSDT', 'ZKCUSDT', 'ZKJUSDT', 'ZORAUSDT', 'ZRCUSDT', 'ZROUSDT', 
         'ZRXUSDT']

TIME_WINDOW_MINUTES = 30

def send_telegram_message(message):
    """Telegram'a mesaj gönder"""
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ Telegram credentials eksik!")
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        'chat_id': CHAT_ID,
        'text': message,
        'parse_mode': 'HTML'
    }
    
    for attempt in range(3):
        try:
            print(f"  📤 Telegram'a gönderiliyor (Deneme {attempt + 1}/3)...")
            response = requests.post(url, data=payload, timeout=30)
            if response.status_code == 200:
                print("  ✓ Telegram mesajı gönderildi!")
                return True
            else:
                print(f"  ✗ Telegram hatası: {response.status_code}")
        except Exception as e:
            print(f"  ✗ Hata: {e}")
            if attempt < 2:
                time.sleep(5)
    
    return False

def format_alert_message(alerted_coins, recent_delta_sum, price_changes, threshold_map):
    """Uyarı mesajını formatla"""
    if not alerted_coins:
        return None
    
    message = f"🚨 <b>DELTA UYARISI</b> 🚨\n\n"
    message += f"Son <b>{TIME_WINDOW_MINUTES} dakika</b> içinde büyük hareketler!\n\n"
    
    for coin in alerted_coins:
        delta_val = recent_delta_sum[coin]
        threshold_val = threshold_map[coin]
        price_chg = price_changes.get(coin, 0)
        
        emoji = "🟢" if delta_val > 0 else "🔴"
        price_emoji = "📈" if price_chg > 0 else "📉"
        
        message += f"{emoji} <b>{coin}</b>\n"
        message += f"   Delta: <code>{delta_val:,.0f}$</code>\n"
        message += f"   {price_emoji} Fiyat: <code>{price_chg:+.2f}%</code>\n"
        message += f"   Threshold: <code>{threshold_val:,.0f}$</code>\n\n"
    
    message += f"📊 Toplam {len(alerted_coins)} coin uyarısı"
    return message

def analyze_coins():
    """Coin analizi yap"""
    start_time = time.time()
    print(f"\n{'='*90}")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🔍 Analiz başlatılıyor...")
    print(f"{'='*90}")
    
    try:
        # Threshold'ları al
        caps_filtered = get_thresholds(ALL_COINS)
        threshold_map = dict(zip(caps_filtered["coin"], caps_filtered["DELTA_THRESHOLD"]))
        market_cap_map = dict(zip(caps_filtered["coin"], caps_filtered["market_cap"]))
        VALID_COINS = list(threshold_map.keys())
        
        print(f"📋 {len(VALID_COINS)} coin analiz edilecek")
        
        if not VALID_COINS:
            print("\n❌ Analiz edilecek coin bulunamadı!")
            return
        
        # Client oluştur
        client = velo.client(VELO_API_KEY)
        
        # Delta verilerini çek
        print("\n📈 Delta verileri çekiliyor...")
        one_hour_in_ms = 1000 * 60 * 60
        
        params = {
            'type': 'futures',
            'columns': ['buy_dollar_volume', 'sell_dollar_volume', 'close_price'],
            'exchanges': ['binance-futures'],
            'products': VALID_COINS,
            'begin': client.timestamp() - one_hour_in_ms,
            'end': client.timestamp(),
            'resolution': '2m'
        }
        
        batches = client.batch_rows(params)
        frames = []
        for part in client.stream_rows(batches):
            frames.append(part)
        
        if not frames:
            print("❌ Veri alınamadı!")
            return
        
        df_raw = pd.concat(frames, ignore_index=True)
        
        # Delta hesapla
        agg = (
            df_raw
            .groupby(['time', 'product'], as_index=False)
            .agg({
                'buy_dollar_volume': 'sum',
                'sell_dollar_volume': 'sum',
                'close_price': 'last'
            })
        )
        
        agg['delta'] = agg['buy_dollar_volume'] - agg['sell_dollar_volume']
        agg['time'] = pd.to_datetime(agg['time'], unit='ms', utc=True)
        
        # Son 30 dakika analizi
        print(f"\n🔎 Son {TIME_WINDOW_MINUTES} dakika analiz ediliyor...")
        latest_time = agg['time'].max()
        cutoff_time = latest_time - pd.Timedelta(minutes=TIME_WINDOW_MINUTES)
        recent_data = agg[agg['time'] >= cutoff_time]
        
        recent_delta_sum = recent_data.groupby('product')['delta'].sum()
        
        # Fiyat değişimi hesapla
        price_changes = {}
        for coin in recent_delta_sum.index:
            coin_data = agg[agg['product'] == coin].sort_values('time')
            recent_coin_data = coin_data[coin_data['time'] >= cutoff_time]
            
            if len(recent_coin_data) >= 2:
                first_price = recent_coin_data.iloc[0]['close_price']
                last_price = recent_coin_data.iloc[-1]['close_price']
                
                if first_price > 0:
                    price_change_pct = ((last_price - first_price) / first_price) * 100
                    price_changes[coin] = price_change_pct
                else:
                    price_changes[coin] = 0
            else:
                price_changes[coin] = 0
        
        # Threshold kontrolü
        alerted_coins = []
        for coin in recent_delta_sum.index:
            if coin in threshold_map:
                coin_threshold = threshold_map[coin]
                coin_delta = recent_delta_sum[coin]
                
                if coin_delta > coin_threshold:
                    alerted_coins.append(coin)
        
        # Sonuçları göster
        if alerted_coins:
            alerted_coins.sort(key=lambda x: abs(recent_delta_sum[x]), reverse=True)
            
            print(f"\n{'='*90}")
            print(f"⚠️ UYARI: Son {TIME_WINDOW_MINUTES} dakikada threshold'unu aşan coinler:")
            print(f"{'='*90}")
            print(f"{'Coin':<20} {'Delta':>15} {'Price Chg':>12} {'Threshold':>15} {'Market Cap':>20}")
            print(f"{'-'*90}")
            
            for coin in alerted_coins:
                delta_val = recent_delta_sum[coin]
                threshold_val = threshold_map[coin]
                mc_val = market_cap_map.get(coin, 0)
                price_chg = price_changes.get(coin, 0)
                
                mc_str = f"${mc_val:,.0f}"
                price_chg_str = f"{price_chg:+.2f}%"
                
                print(f"{coin:<20} ${delta_val:>13,.0f} {price_chg_str:>12} ${threshold_val:>13,.0f} {mc_str:>20}")
            
            print(f"{'='*90}\n")
            print(f"📊 Toplam {len(alerted_coins)} coin threshold'unu aştı")
            
            # Telegram mesajı gönder
            alert_message = format_alert_message(alerted_coins, recent_delta_sum, price_changes, threshold_map)
            if alert_message:
                send_telegram_message(alert_message)
        else:
            print(f"\n✅ Bilgi: Son {TIME_WINDOW_MINUTES} dakikada hiçbir coin kendi threshold'unu aşmadı.")
        
        total_time = time.time() - start_time
        print(f"\n⏱️ Toplam çalışma süresi: {total_time:.2f} saniye")
        
    except Exception as e:
        print(f"\n❌ Hata oluştu: {e}")
        import traceback
        traceback.print_exc()

def job():
    """Zamanlanmış görev"""
    analyze_coins()

def main():
    """Ana fonksiyon - sürekli çalışır"""
    print("="*70)
    print("🤖 CRYPTO ALERT BOT BAŞLATILDI")
    print("="*70)
    print(f"⏱️ Kontrol aralığı: 15 dakika")
    print(f"📱 Telegram Chat ID: {CHAT_ID}")
    print(f"⏰ Analiz penceresi: Son {TIME_WINDOW_MINUTES} dakika")
    print("="*70)
    
    # İlk analizi hemen yap
    print("\n⏳ İlk kontrol başlıyor...\n")
    analyze_coins()
    
    # Her 15 dakikada bir zamanla
    schedule.every(15).minutes.do(job)
    
    print("\n💤 Zamanlanmış görevler aktif. Her 15 dakikada bir kontrol edilecek...")
    
    # Sürekli çalış
    while True:
        schedule.run_pending()
        time.sleep(60)  # Her dakika kontrol et

if __name__ == "__main__":
    main()
