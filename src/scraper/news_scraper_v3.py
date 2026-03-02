"""
News Scraper V3 - TradingView News-Flow API kullanarak haber çeker

V2'den farkı: HTML parsing yerine TradingView'ın JSON API'sini kullanır.
News-flow sayfası JavaScript ile render edildiği için doğrudan API kullanıyoruz.

API Endpoint:
https://news-headlines.tradingview.com/v2/headlines?client=web&lang=tr

Response formatı:
{
    "items": [
        {
            "id": "cointelegraph:xxx:0",
            "title": "Haber başlığı",
            "provider": "cointelegraph",
            "source": "Cointelegraph",
            "published": 1772364240,  # Unix timestamp
            "storyPath": "/news/cointelegraph:xxx:0/",
            "link": "https://..."  # Orijinal kaynak linki
        },
        ...
    ]
}
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os
import time
import re
import json

# Base directory - Airflow container'ında /opt/airflow olacak
BASE_DIR = os.environ.get(
    'AIRFLOW_HOME',
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

# HTTP istekleri için ortak headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Origin': 'https://tr.tradingview.com',
    'Referer': 'https://tr.tradingview.com/news-flow/',
}

# TradingView Headlines API
NEWS_HEADLINES_API = "https://news-headlines.tradingview.com/v2/headlines?client=web&lang=tr"

LEGAL_DISCLAIMER = """***
Yasal Uyarı

Burada yer alan yatırım bilgi, yorum ve tavsiyeler yatırım danışmanlığı kapsamında değildir.Yatırım danışmanlığı hizmeti ; aracı kurumlar, portföy yönetim şirketleri, mevduat kabul etmeyen bankalar ile müşteri arasında imzalanacak yatırım danışmanlığı sözleşmesi çerçevesinde sunulmaktadır.Burada yer alan yorum ve tavsiyeler, yorum ve tavsiyede bulunanların kişisel görüşlerine dayanmaktadır.Bu görüşler mali durumunuz ile risk ve getiri tercihlerinize uygun olmayabılır.Bu nedenle, sadece burada yer alan bilgilere dayanılarak yatırım kararı verilmesi beklentilerinize uygun sonuçlar doğurmayabilir."""


def get_news_directory():
    """Returns the absolute path to news directory"""
    return os.path.join(BASE_DIR, 'src', 'scraper', 'news')


def extract_news_distributor(content):
    """Extracts the news distributor name from content"""
    start_mark = "www."
    end_mark = ".com"
    start_idx = content.find(start_mark) + len(start_mark)
    end_idx = content.find(end_mark)

    if start_idx == -1 or end_idx == -1 or start_idx >= end_idx:
        return "undetermined"

    news_distributor = content[start_idx:end_idx]

    if news_distributor == "" or len(news_distributor) > 15:
        return "undetermined"

    return news_distributor


def save_news_content(content, date_file_name, news_distributor):
    """Saves the news content to appropriate file"""
    news_base_dir = get_news_directory()
    directory_path = os.path.join(news_base_dir, date_file_name)
    os.makedirs(directory_path, exist_ok=True)

    file_path = os.path.join(directory_path, f"{news_distributor}.txt")

    with open(file_path, "a+") as distributor_file:
        distributor_file.seek(0)
        existing_content = distributor_file.read()

        if content in existing_content:
            print("Haber zaten çekilmiş.")
            return False
        else:
            distributor_file.write(content)
            print("Haber çekildi.")
            return True


def parse_news_datetime(datetime_str):
    """Parses datetime string from TradingView, handles multiple formats"""
    for fmt in [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%a, %d %b %Y %H:%M:%S %Z",  # news-flow formatı: "Sun, 01 Mar 2026 11:24:00 GMT"
    ]:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Tarih formatı tanınamadı: {datetime_str}")


def fetch_news_flow_links():
    """
    TradingView Headlines API'sinden haber listesini çeker.
    
    API endpoint: https://news-headlines.tradingview.com/v2/headlines
    
    Returns:
        list: Haber bilgilerinin listesi (url, title, provider, published timestamp)
    """
    print("TradingView Headlines API'si çağrılıyor...")

    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        response = session.get(NEWS_HEADLINES_API, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        print(f"❌ Headlines API çağrılamadı: {e}")
        raise
    except json.JSONDecodeError as e:
        print(f"❌ JSON parse hatası: {e}")
        raise

    items = data.get('items', [])
    
    news_items = []
    for item in items:
        story_path = item.get('storyPath', '')
        if not story_path:
            continue
            
        full_url = "https://tr.tradingview.com" + story_path
        
        # Unix timestamp'i datetime'a çevir
        published_ts = item.get('published', 0)
        if published_ts:
            try:
                dt_object = datetime.fromtimestamp(published_ts)
                date_file_name = dt_object.strftime("%Y.%m.%d")
            except (ValueError, OSError):
                date_file_name = datetime.now().strftime("%Y.%m.%d")
        else:
            date_file_name = datetime.now().strftime("%Y.%m.%d")

        news_items.append({
            'url': full_url,
            'title': item.get('title', ''),
            'provider': item.get('source', item.get('provider', '')),
            'provider_id': item.get('provider', ''),
            'published': published_ts,
            'date_file_name': date_file_name,
            'original_link': item.get('link', ''),
        })

    print(f"✅ {len(news_items)} adet haber bulundu (API).")
    return news_items


def extract_article_content(url, session=None):
    """
    Tek bir haber makalesinin içeriğini requests ile çeker.
    
    Args:
        url: Haber URL'si
        session: requests.Session (connection reuse için)
    
    Returns:
        dict: {title, content, datetime_str, date_file_name} veya None
    """
    if session is None:
        session = requests.Session()
        session.headers.update(HEADERS)

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  ❌ Haber çekilemedi: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    # === BAŞLIK ===
    # Yöntem 1: CSS selector ile title div'i (partial match)
    title_el = soup.find('div', class_=re.compile(r'title-.*block-'))
    # Yöntem 2: og:title meta tag'i
    if title_el is None:
        og_title = soup.find('meta', property='og:title')
        title = og_title['content'] if og_title else None
    else:
        title = title_el.get_text(strip=True)

    if not title:
        print(f"  ❌ Başlık bulunamadı: {url}")
        return None

    # === İÇERİK ===
    # Yöntem 1: Body/content div (partial match)
    content_el = soup.find('div', class_=re.compile(r'body-.*content-'))
    if content_el:
        content_text = content_el.get_text(separator='\n', strip=True)
    else:
        # Yöntem 2: og:description meta tag'i
        og_desc = soup.find('meta', property='og:description')
        content_text = og_desc['content'] if og_desc else ""

    # Başlık + İçerik birleştir
    full_content = title + "\n" + content_text

    # === TARİH ===
    # Yöntem 1: <time> elementi
    time_el = soup.find('time')
    if time_el and time_el.get('datetime'):
        datetime_str = time_el['datetime']
    else:
        # Yöntem 2: relative-time elementi
        rel_time = soup.find('relative-time')
        if rel_time and rel_time.get('event-time'):
            datetime_str = rel_time['event-time']
        else:
            # Yöntem 3: Article structured data (JSON-LD)
            json_ld = soup.find('script', type='application/ld+json')
            if json_ld:
                try:
                    data = json.loads(json_ld.string)
                    datetime_str = data.get('datePublished', data.get('dateCreated', ''))
                except (json.JSONDecodeError, AttributeError):
                    datetime_str = None
            else:
                datetime_str = None

    if not datetime_str:
        print(f"  ⚠️  Tarih bulunamadı, bugünün tarihi kullanılıyor: {url}")
        now = datetime.now()
        date_file_name = now.strftime("%Y.%m.%d")
    else:
        try:
            dt_object = parse_news_datetime(datetime_str)
            date_file_name = dt_object.strftime("%Y.%m.%d")
        except ValueError as e:
            print(f"  ⚠️  Tarih parse edilemedi ({e}), bugünün tarihi kullanılıyor")
            date_file_name = datetime.now().strftime("%Y.%m.%d")

    return {
        'title': title,
        'content': full_content,
        'date_file_name': date_file_name,
    }


def content_extractor(news_items):
    """
    Haber içeriklerini çeker ve kaydeder.
    
    API'den gelen temel bilgiler kullanılır, gerekirse detay sayfası da çekilir.
    
    Args:
        news_items: fetch_news_flow_links() tarafından döndürülen liste
    
    Returns:
        int: Kaydedilen haber sayısı
    """
    session = requests.Session()
    # HTML sayfaları için farklı header
    html_headers = HEADERS.copy()
    html_headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    session.headers.update(html_headers)

    extracted_count = 0
    extracted_news = []
    total = len(news_items)

    for idx, item in enumerate(news_items, 1):
        link = item['url']
        print(f"[{idx}/{total}] İşleniyor: {item['title'][:60]}...")

        # API'den gelen tarih bilgisini kullan
        date_file_name = item.get('date_file_name', datetime.now().strftime("%Y.%m.%d"))
        
        # Detay sayfasından içerik çekmeyi dene
        article = extract_article_content(link, session=session)
        
        if article and article.get('content'):
            content = article['content']
            # Detay sayfasından gelen tarihi kullan
            date_file_name = article.get('date_file_name', date_file_name)
        else:
            # API'den gelen bilgiyi kullan
            content = item['title']
            if item.get('provider'):
                content += f"\nKaynak: {item['provider']}"
            if item.get('original_link'):
                content += f"\nLink: {item['original_link']}"

        # Yasal uyarıyı kaldır
        if LEGAL_DISCLAIMER in content:
            content = content.replace(LEGAL_DISCLAIMER, "")

        pure_content = content
        content += "\n"
        content += "_" * 50
        content += "\n"

        # Provider'ı distributor olarak kullan
        news_distributor = extract_news_distributor(content)
        
        # API'den gelen provider bilgisini tercih et
        if news_distributor == "undetermined" and item.get('provider_id'):
            provider_clean = item['provider_id'].lower().replace(' ', '').replace('.', '').replace('-', '')
            if provider_clean and len(provider_clean) <= 20:
                news_distributor = provider_clean

        if save_news_content(content, date_file_name, news_distributor):
            extracted_news.append({
                "date": date_file_name,
                "content": pure_content,
                "url": link,
                "provider": news_distributor,
            })
            extracted_count += 1
            print(f"  ✅ Kaydedildi: {news_distributor}/{date_file_name}")
        else:
            print(f"  ⏭️  Zaten mevcut, atlandı.")

        # Rate limiting - sunucuyu yormamak için kısa bekleme
        if idx < total:
            time.sleep(0.3)

    print(f"\n{'='*50}")
    print(f"Toplam {total} haberden {extracted_count} tanesi yeni olarak kaydedildi.")
    print(f"{'='*50}")
    return extracted_count, extracted_news


def fetch_news_page():
    """
    TradingView news-flow sayfasından linkleri çeker.
    
    Returns:
        dict: İşlem durumu
    """
    print("=" * 50)
    print("📰 News Scraper V3 (News-Flow)")
    print("=" * 50)

    news_items = fetch_news_flow_links()

    return {
        "status": "success",
        "link_count": len(news_items),
        "links": [item['url'] for item in news_items]
    }


def scrape_bist_news():
    """
    News-flow sayfasından haberleri çeker ve kaydeder.
    """
    print("BIST haber çekme işlemi başlıyor (V3 - news-flow)...")

    news_items = fetch_news_flow_links()

    if not news_items:
        print("Çekilecek haber bulunamadı.")
        return {"status": "no_links", "count": 0}

    print(f"{len(news_items)} adet haber kartı bulundu.")

    extracted_count, extracted_news = content_extractor(news_items)

    print(f"Toplam {extracted_count} haber çekildi.")

    return {
        "status": "success", 
        "count": extracted_count,
        "extracted_news": extracted_news
    }


def full_pipeline():
    """
    Tek adımda tüm pipeline'ı çalıştırır.
    """
    return scrape_bist_news()


# Script olarak çalıştırıldığında
if __name__ == "__main__":
    result = full_pipeline()
    print(f"\nSonuç: {result}")
