from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options

from datetime import datetime

import os
import time

# Base directory - Airflow container'ında /opt/airflow olacak
BASE_DIR = os.environ.get('AIRFLOW_HOME', os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# ChromeDriver path - Docker container'da env variable, lokalde chromedriver_py kullanılır
CHROMEDRIVER_PATH = os.environ.get('CHROMEDRIVER_PATH')
if not CHROMEDRIVER_PATH:
    try:
        from chromedriver_py import binary_path as CHROMEDRIVER_PATH
    except ImportError:
        CHROMEDRIVER_PATH = '/usr/bin/chromedriver'

# Chrome binary path for Docker
CHROME_BIN = os.environ.get('CHROME_BIN')

LEGAL_DISCLAIMER = """***
Yasal Uyarı

Burada yer alan yatırım bilgi, yorum ve tavsiyeler yatırım danışmanlığı kapsamında değildir.Yatırım danışmanlığı hizmeti ; aracı kurumlar, portföy yönetim şirketleri, mevduat kabul etmeyen bankalar ile müşteri arasında imzalanacak yatırım danışmanlığı sözleşmesi çerçevesinde sunulmaktadır.Burada yer alan yorum ve tavsiyeler, yorum ve tavsiyede bulunanların kişisel görüşlerine dayanmaktadır.Bu görüşler mali durumunuz ile risk ve getiri tercihlerinize uygun olmayabılır.Bu nedenle, sadece burada yer alan bilgilere dayanılarak yatırım kararı verilmesi beklentilerinize uygun sonuçlar doğurmayabilir."""


def get_content_file_path():
    """Returns the absolute path to content.txt"""
    return os.path.join(BASE_DIR, 'src', 'scraper', 'data', 'content.txt')


def get_news_directory():
    """Returns the absolute path to news directory"""
    return os.path.join(BASE_DIR, 'src', 'scraper', 'news')


def anchor_finder():
    """Extracts anchor links from content.txt HTML file"""
    content_path = get_content_file_path()
    
    with open(content_path, "r") as f:
        soup = BeautifulSoup(f.read(), 'html.parser')

    div_list = soup.find('div', class_="grid-iTt_Zp4a")
    
    if div_list is None:
        print("Haber listesi bulunamadı.")
        return []

    anchor_tags = div_list.find_all('a')

    href_list = []
    for anchor in anchor_tags:
        href_list.append("https://tr.tradingview.com" + anchor["href"])
    
    return href_list


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
    # Yeni format: ISO 8601 (örn: 2026-02-12T19:00:00.000Z)
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


def content_extractor(anchor_list):
    """Extracts content from each anchor link"""
    service = Service(CHROMEDRIVER_PATH)
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    
    # Docker container'da Chromium binary'sini kullan
    if CHROME_BIN:
        chrome_options.binary_location = CHROME_BIN
    
    extracted_count = 0
    
    total = len(anchor_list)
    
    for idx, link in enumerate(anchor_list, 1):
        print(f"[{idx}/{total}] Haber okunuyor: {link}")
        driver = webdriver.Chrome(service=service, options=chrome_options)

        try:
            driver.get(link)
            driver.implicitly_wait(3)

            # CSS class hash'leri TradingView deploy'larında değişebilir,
            # bu yüzden partial match (CSS_SELECTOR) kullanıyoruz
            title_el = driver.find_element(By.CSS_SELECTOR, "div[class*='title-'][class*='block-']")
            title = title_el.text

            # Body elementi: body- ve content- class'larını içeren div
            content_el = driver.find_element(By.CSS_SELECTOR, "div[class*='body-'][class*='content-']")
            content = content_el.text

            content = title + "\n" + content

            time_element = driver.find_element(By.CSS_SELECTOR, "time").get_attribute("datetime")
            dt_object = parse_news_datetime(time_element)
            day = str(dt_object.day).zfill(2)
            month = str(dt_object.month).zfill(2)
            year = str(dt_object.year).zfill(4)
            date_file_name = f"{year}.{month}.{day}"

            if LEGAL_DISCLAIMER in content:
                content = content.replace(LEGAL_DISCLAIMER, "")
            
            content += "\n"
            content += "_" * 50
            content += "\n"

            news_distributor = extract_news_distributor(content)
            
            if save_news_content(content, date_file_name, news_distributor):
                extracted_count += 1
                print(f"  ✅ Başlık: {title[:80]}")
            else:
                print(f"  ⏭️  Zaten mevcut, atlandı.")

        except NoSuchElementException:
            print(f"  ❌ Haber çekilemedi: {link}")

        finally:
            driver.quit()
    
    print(f"\n{'='*50}")
    print(f"Toplam {total} haberden {extracted_count} tanesi yeni olarak kaydedildi.")
    print(f"{'='*50}")
    return extracted_count


def fetch_news_page():
    """
    TradingView haber sayfasını Selenium ile açar ve content.txt'ye kaydeder.
    DAG'ın ilk adımı olarak çalışır.
    """
    print("TradingView haber sayfası çekiliyor...")
    
    service = Service(CHROMEDRIVER_PATH)
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    
    if CHROME_BIN:
        chrome_options.binary_location = CHROME_BIN
    
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    try:
        driver.get("https://tr.tradingview.com/news/")
        driver.implicitly_wait(5)
        
        # Sayfayı aşağı kaydırarak daha fazla haber yükle
        for i in range(3):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            print(f"  Sayfa kaydırıldı ({i+1}/3)")
        
        page_source = driver.page_source
        
        content_path = get_content_file_path()
        os.makedirs(os.path.dirname(content_path), exist_ok=True)
        
        with open(content_path, "w", encoding="utf-8") as f:
            f.write(page_source)
        
        print(f"✅ Haber sayfası kaydedildi: {content_path}")
        print(f"   Dosya boyutu: {len(page_source)} karakter")
        return {"status": "success", "file": content_path}
        
    except Exception as e:
        print(f"❌ Haber sayfası çekilemedi: {e}")
        raise
    finally:
        driver.quit()


def scrape_bist_news():
    """
    Haber içeriklerini çeker. DAG'ın ikinci adımı.
    fetch_news_page() çalıştıktan sonra güncel content.txt'den okur.
    """
    print("BIST haber çekme işlemi başlıyor...")
    
    href_list = anchor_finder()
    
    if not href_list:
        print("Çekilecek haber linki bulunamadı.")
        return {"status": "no_links", "count": 0}
    
    print(f"{len(href_list)} adet haber linki bulundu.")
    
    extracted_count = content_extractor(href_list)
    
    print(f"Toplam {extracted_count} haber çekildi.")
    
    return {"status": "success", "count": extracted_count}


# Script olarak çalıştırıldığında
if __name__ == "__main__":
    fetch_news_page()
    scrape_bist_news()