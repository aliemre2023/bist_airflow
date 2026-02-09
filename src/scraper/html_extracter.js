const puppeteer = require('puppeteer');
const fs = require('fs');


(async () => {
  const browser = await puppeteer.launch();
  const page = await browser.newPage();
  
  // Navigate to the webpage
  await page.goto('https://tr.tradingview.com/news/markets/?category=stock&market_country=TR');
  await page.waitForSelector('.card-DmjQR0Aa'); // classname may be change in time
  // Get the HTML source of the page
  const htmlSource = await page.content();
  
  fs.writeFileSync("scraper/data/content.txt", htmlSource)

  await browser.close();
})();
