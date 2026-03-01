"""
News Scraper V2 - Selenium'suz versiyon

Selenium + Chrome/ChromeDriver yerine sadece requests + BeautifulSoup kullanır.
Docker'da çalıştırmak çok daha kolaydır:
  - Chrome/Chromium binary'ye gerek yok
  - ChromeDriver'a gerek yok
  - --no-sandbox, --headless vs. ayarlarına gerek yok
  - Sadece requests ve beautifulsoup4 pip paketleri yeterli

TradingView haber sayfası server-side rendered (SSR) olduğu için,
haber linkleri ve detayları doğrudan HTML'den parse edilebilir.
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os
import time
import re

# Base directory - Airflow container'ında /opt/airflow olacak
BASE_DIR = os.environ.get(
    'AIRFLOW_HOME',
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

# HTTP istekleri için ortak headers (bot korumasını aşmak için)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
}

# TradingView haber sayfası URL'si
NEWS_PAGE_URL = "https://tr.tradingview.com/news/"

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
    """Parses datetime string from TradingView, handles both old and new formats"""
    for fmt in [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%a, %d %b %Y %H:%M:%S %Z",  # Eski format
    ]:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Tarih formatı tanınamadı: {datetime_str}")


def fetch_news_links():
    """
    TradingView haber sayfasını requests ile çeker ve
    haber linklerini BeautifulSoup ile parse eder.
    
    Selenium'a gerek yok çünkü TradingView haber sayfası SSR (Server-Side Rendered).
    Haber kartları HTML'de doğrudan mevcut.
    
    Returns:
        list: Haber URL'lerinin listesi
    """
    print("TradingView haber sayfası çekiliyor (requests)...")

    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        response = session.get(NEWS_PAGE_URL, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"❌ Haber sayfası çekilemedi: {e}")
        raise

    soup = BeautifulSoup(response.text, 'html.parser')

    # TradingView haber grid'i - CSS class hash içerdiğinden partial match kullanıyoruz
    # Önce tam class adıyla dene, sonra partial match
    div_list = soup.find('div', class_=re.compile(r'^grid-'))

    if div_list is None:
        print("⚠️  Haber grid'i bulunamadı, tüm sayfadaki haber kartlarını arıyoruz...")
        # Fallback: data-qa-id ile haber kartlarını bul  
        news_cards = soup.find_all('a', href=re.compile(r'^/news/'))
    else:
        news_cards = div_list.find_all('a', href=True)

    href_list = []
    for card in news_cards:
        href = card.get('href', '')
        if href.startswith('/news/') and href != '/news/':
            full_url = "https://tr.tradingview.com" + href
            if full_url not in href_list:
                href_list.append(full_url)

    print(f"✅ {len(href_list)} adet haber linki bulundu.")
    print(f"   Sayfa boyutu: {len(response.text)} karakter")
    return href_list


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
                import json
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


def content_extractor(anchor_list):
    """
    Haber içeriklerini requests ile çeker (Selenium'suz).
    
    Connection reuse için tek bir session kullanır,
    her istek arasında kısa bir bekleme süresi ekler (rate limiting).
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    extracted_count = 0
    total = len(anchor_list)

    for idx, link in enumerate(anchor_list, 1):
        print(f"[{idx}/{total}] Haber okunuyor: {link}")

        article = extract_article_content(link, session=session)
        if article is None:
            print(f"  ❌ Haber çekilemedi: {link}")
            continue

        content = article['content']

        # Yasal uyarıyı kaldır
        if LEGAL_DISCLAIMER in content:
            content = content.replace(LEGAL_DISCLAIMER, "")

        content += "\n"
        content += "_" * 50
        content += "\n"

        news_distributor = extract_news_distributor(content)

        if save_news_content(content, article['date_file_name'], news_distributor):
            extracted_count += 1
            print(f"  ✅ Başlık: {article['title'][:80]}")
        else:
            print(f"  ⏭️  Zaten mevcut, atlandı.")

        # Rate limiting - sunucuyu yormamak için kısa bekleme
        if idx < total:
            time.sleep(0.5)

    print(f"\n{'='*50}")
    print(f"Toplam {total} haberden {extracted_count} tanesi yeni olarak kaydedildi.")
    print(f"{'='*50}")
    return extracted_count


def fetch_news_page():
    """
    TradingView haber sayfasından linkleri çeker.
    V1'deki Selenium versiyonunun yerine geçer.
    
    Returns:
        dict: İşlem durumu
    """
    print("=" * 50)
    print("📰 News Scraper V2 (Selenium'suz)")
    print("=" * 50)

    href_list = fetch_news_links()

    return {
        "status": "success",
        "link_count": len(href_list),
        "links": href_list
    }


def scrape_bist_news():
    """
    Haberleri çeker ve kaydeder.
    V1 ile aynı arayüz, ama Selenium yerine requests kullanır.
    """
    print("BIST haber çekme işlemi başlıyor (V2 - requests)...")

    href_list = fetch_news_links()

    if not href_list:
        print("Çekilecek haber linki bulunamadı.")
        return {"status": "no_links", "count": 0}

    print(f"{len(href_list)} adet haber linki bulundu.")

    extracted_count = content_extractor(href_list)

    print(f"Toplam {extracted_count} haber çekildi.")

    return {"status": "success", "count": extracted_count}


def full_pipeline():
    """
    Tek adımda tüm pipeline'ı çalıştırır.
    V1'deki gibi fetch_news_page() + scrape_bist_news() ayrımına gerek yok
    çünkü Selenium'suz yöntemde ara dosya (content.txt) gerekmiyor.
    """
    return scrape_bist_news()


# Script olarak çalıştırıldığında
if __name__ == "__main__":
    result = full_pipeline()
    print(f"\nSonuç: {result}")
