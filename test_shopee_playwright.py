import asyncio
import json
import re
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        url = "https://shopee.com.br/Caixa-de-Som-Bluetooth-Potente-Com-4-Alto-Falantes-30W-Soundbar-PC-Notebook-TV-i.1351679961.21498150153"
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(3000)
        
        html = await page.content()
        with open("shopee.html", "w", encoding="utf-8") as f:
            f.write(html)
            
        print("Title:", await page.title())
        
        # We can also execute JS to grab data
        # Often shopee exposes `window.__APP_STATE__` or similar
        try:
            app_state = await page.evaluate("() => window.__APP_STATE__")
            print("Has APP_STATE:", bool(app_state))
        except:
            print("No APP_STATE")
        
        # Let's just grab elements directly
        try:
            title = await page.locator("div[class*='page-product'] h1, h1").first.text_content()
            print("Title from DOM:", title)
            
            # For description
            desc = await page.locator("div[class*='product-detail']").text_content()
            if not desc:
                desc = await page.locator("div[style*='white-space: pre-wrap']").first.text_content()
            print("Desc length:", len(desc) if desc else 0)
            
            # For images
            images = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll('picture img')).map(img => img.src).filter(src => src.includes('http'));
            }""")
            print("Images count:", len(images))
            print("Images:", images[:3])
            
            # For video
            video = await page.evaluate("""() => {
                let v = document.querySelector('video');
                return v ? v.src : null;
            }""")
            print("Video:", video)
        except Exception as e:
            print("Error parsing DOM:", e)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())
