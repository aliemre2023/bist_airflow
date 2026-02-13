# BIST100 Airflow Scraper

Bu proje, TradingView üzerinden BIST100 haberlerini çekmek için geliştirilmiştir.

## Kurulum ve Çalıştırma (Lokal)

Docker veya Airflow kullanmadan script'i direkt olarak Python ile çalıştırabilirsiniz.

### 1. Proje Dizinine Gidin

Terminalde proje kök dizinine gidin:

```bash
cd /Users/aliemre2023/Desktop/apache_project/bist100_airflow
```

### 2. Scripti Çalıştırın

Aşağıdaki komutu kullanarak scraper'ı başlatın:

```bash
/usr/local/bin/python3.10 -m src.scraper.news_scraper
```

Bu komut sırasıyla şunları yapar:
1.  TradingView haber sayfasını (`https://tr.tradingview.com/news/`) açar ve HTML kaynağını `src/scraper/data/content.txt` dosyasına kaydeder.
2.  Kaydedilen HTML'den haber linklerini ayıklar.
3.  Her bir haber linkine giderek içeriği çeker ve `src/scraper/news/YYYY.MM.DD/` klasörüne kaydeder.

## Çıktılar

Haberler `src/scraper/news/` dizini altında tarih bazlı klasörlerde saklanır.
Örneğin: `src/scraper/news/2026.02.13/matriks.txt`
