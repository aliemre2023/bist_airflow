from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options

from datetime import datetime

import os

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

    div_list = soup.find('div', class_="list-iTt_Zp4a")
    
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
    
    for link in anchor_list:
        driver = webdriver.Chrome(service=service, options=chrome_options)

        try:
            driver.get(link)
            driver.implicitly_wait(1)

            # Elements depends on website, it may change in time, need to be controlled
            title = driver.find_element(By.CLASS_NAME, "title-KX2tCBZq").text
            content = driver.find_element(By.XPATH, "//div[@class='body-KX2tCBZq body-pIO_GYwT content-pIO_GYwT body-RYg5Gq3E']").text

            content = title + "\n" + content

            time_element = driver.find_element(By.CSS_SELECTOR, "time").get_attribute("datetime")
            dt_object = datetime.strptime(time_element, "%a, %d %b %Y %H:%M:%S %Z")
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

        except NoSuchElementException:
            print(f"Haber çekilemedi: {link}")

        finally:
            driver.quit()
    
    return extracted_count


def scrape_bist_news():
    """
    Main function to scrape BIST news.
    This is the entry point for Airflow DAG.
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
    scrape_bist_news()