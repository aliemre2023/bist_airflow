import os
import json
import yfinance as yf

WORKSPACE_DIR = os.path.join(
    os.environ.get(
        'AIRFLOW_HOME',
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    ),
    'src', 'workspace'
)
WALLET_FILE = os.path.join(WORKSPACE_DIR, 'wallet.json')

# Kaç adet alınacak / satılacak (sabit birim)
TRADE_UNIT = 4
# Ne kadar alınacak / satılacak (sabit miktar)
TRADE_PRICE = 10_000

class Wallet:
    def __init__(self, name="default"):
        self.name = name
        self.money = 0.0
        self.portfolio = {}  # {"THYAO.IS": 5, ...}
        self._load()

    def _load(self):
        os.makedirs(WORKSPACE_DIR, exist_ok=True)
        if not os.path.exists(WALLET_FILE):
            self._save()
            return
        with open(WALLET_FILE, 'r') as f:
            data = json.load(f)
        wallet_data = data.get(self.name, {"money": 100000, "portfolio": {}})
        self.money = float(wallet_data.get("money", 100000))
        self.portfolio = wallet_data.get("portfolio", {})

    def _save(self):
        os.makedirs(WORKSPACE_DIR, exist_ok=True)
        if os.path.exists(WALLET_FILE):
            with open(WALLET_FILE, 'r') as f:
                data = json.load(f)
        else:
            data = {}
        data[self.name] = {
            "money": round(self.money, 4),
            "portfolio": self.portfolio
        }
        with open(WALLET_FILE, 'w') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

    def get_price(self, ticker):
        """Yahoo Finance'dan güncel fiyat çeker. Ticker: 'THYAO.IS' formatında"""
        try:
            hist = yf.Ticker(ticker).history(period="5d")
            if len(hist) == 0:
                return None
            return float(hist["Close"].iloc[-1])
        except Exception as e:
            print(f"  ⚠️  Fiyat çekilemedi ({ticker}): {e}")
            return None

    def buy_v1(self, ticker, unit=TRADE_UNIT):
        """Istenen unit'ten başla, para yetmiyorsa birer azalt (fiyat sınırı yok)."""
        price = self.get_price(ticker)
        if price is None:
            return False
        for attempt_unit in range(unit, 0, -1):
            total_cost = price * attempt_unit
            if self.money >= total_cost:
                self.money -= total_cost
                self.portfolio[ticker] = self.portfolio.get(ticker, 0) + attempt_unit
                print(f"  ✅ ALINDI (v1): {attempt_unit} x {ticker} @ {price:.2f} = {total_cost:.2f} TL → kalan para: {self.money:.2f}")
                self._save()
                return True
        print(f"  💸 Yetersiz bakiye: {self.money:.2f} < {price:.2f} x 1 ({ticker})")
        return False

    def buy_v2(self, ticker, unit=TRADE_UNIT):
        """TRADE_PRICE bütçe sınırlı alım: önce fiyat kapından geç, sonra bakiye fallback."""
        price = self.get_price(ticker)
        if price is None:
            return False
        # TRADE_PRICE bütçesine göre alınabilecek max unit'i hesapla
        max_by_price = int(TRADE_PRICE // price) if price > 0 else unit
        if max_by_price < 1:
            print(f"  💸 Tek hisse fiyatı bütçeyi aşıyor: {price:.2f} > {TRADE_PRICE} ({ticker})")
            return False
        # Belirtilen miktardan başla, yetersizse birer azalt
        for attempt_unit in range(max_by_price, 0, -1):
            total_cost = price * attempt_unit
            if self.money >= total_cost:
                self.money -= total_cost
                self.portfolio[ticker] = self.portfolio.get(ticker, 0) + attempt_unit
                print(f"  ✅ ALINDI (v2): {attempt_unit} x {ticker} @ {price:.2f} = {total_cost:.2f} TL → kalan para: {self.money:.2f}")
                self._save()
                return True
        print(f"  💸 Yetersiz bakiye: {self.money:.2f} < {price:.2f} x 1 ({ticker})")
        return False

    def buy(self, ticker, unit=TRADE_UNIT):
        """Varsayılan alım -> buy_v2 (TRADE_PRICE sınırlı)."""
        return self.buy_v2(ticker, unit)

    def sell(self, ticker, unit=None):
        if ticker not in self.portfolio or self.portfolio[ticker] == 0:
            print(f"  ⚠️  Portföyde yok: {ticker}")
            return False
        price = self.get_price(ticker)
        if price is None:
            return False
        sell_unit = unit if unit is not None else self.portfolio[ticker]
        sell_unit = min(sell_unit, self.portfolio[ticker])
        self.money += sell_unit * price
        self.portfolio[ticker] -= sell_unit
        if self.portfolio[ticker] == 0:
            del self.portfolio[ticker]
        print(f"  ✅ SATILDI: {sell_unit} x {ticker} @ {price:.2f} → toplam para: {self.money:.2f}")
        self._save()
        return True

    def sell_others(self, keep_ticker):
        """keep_ticker dışındaki tüm hisseleri sat (agresif alım için alan aç)"""
        for ticker in list(self.portfolio.keys()):
            if ticker != keep_ticker:
                print(f"  🔄 Diğer pozisyon satılıyor: {ticker}")
                self.sell(ticker)

    def show(self):
        print(f"👛 Wallet [{self.name}]")
        print(f"   Nakit: {self.money:.2f} TL")
        for ticker, unit in self.portfolio.items():
            print(f"   {ticker}: {unit} adet")


def stockbroker(wallet: Wallet, companies: list, sentiment_type: int):
    """
    Sentiment tipine göre alım/satım kararı verir.

    Args:
        wallet:         Wallet instance
        companies:      Eşleşen şirket kod listesi (örn. ["THYAO", "SISE"])
        sentiment_type: 2=Güçlü al, 1=Al, 0=Tut, -1=Hafif sat, -2=Sat

    Sentiment mantığı:
        2  → Agresif al: para yetmiyorsa diğer poziysyonları sat, sonra al
        1  → Al: eğer para yetiyorsa al
        0  → Tut: işlem yapma
       -1  → Hafif sat: pozisyon varsa yarısını sat
       -2  → Sat: tüm pozisyonu sat
    """
    if not companies:
        return

    for kod in companies:
        ticker = kod.strip().upper() + ".IS"

        if sentiment_type == 2:
            # Para yetmiyorsa diğerlerini sat, sonra al
            price = wallet.get_price(ticker)
            if price is not None and wallet.money < price * TRADE_UNIT:
                print(f"  🔄 Agresif alım: diğer pozisyonlar satılıyor ({ticker} için)")
                wallet.sell_others(ticker)
            wallet.buy(ticker, TRADE_UNIT)

        elif sentiment_type == 1:
            wallet.buy(ticker, max(1, TRADE_UNIT // 2))

        elif sentiment_type == 0:
            # Tut
            print(f"  ⏸️  TUT: {ticker}")

        elif sentiment_type == -1:
            # Tümünü sat
            wallet.sell(ticker)

        elif sentiment_type == -2:
            # Tümünü sat
            wallet.sell(ticker)
