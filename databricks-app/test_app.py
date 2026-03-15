#!/usr/bin/env python3
"""
Browser test script to navigate to the Databricks app and take screenshots.
"""

import asyncio
from pathlib import Path
from playwright.async_api import async_playwright

async def test_app():
    async with async_playwright() as p:
        # Launch browser
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        url = "https://tpcdi-benchmark-8259553627480283.gcp.databricksapps.com/"

        print(f"Navigating to: {url}")

        try:
            # Navigate to the URL
            await page.goto(url, wait_until="networkidle", timeout=30000)
            print("Page loaded successfully")

            # Wait a moment for any dynamic content to load
            await page.wait_for_timeout(2000)

            # Take initial screenshot
            screenshot_path1 = "/tmp/app_initial.png"
            await page.screenshot(path=screenshot_path1, full_page=True)
            print(f"Initial screenshot saved to: {screenshot_path1}")

            # Get page title and URL
            title = await page.title()
            print(f"Page title: {title}")
            current_url = page.url
            print(f"Current URL: {current_url}")

            # Check if there's a "Databricks" tab/link and click it
            # Look for links or buttons with "Databricks" in the text
            databricks_elements = await page.locator("text=Databricks").all()
            print(f"Found {len(databricks_elements)} elements with 'Databricks' text")

            if databricks_elements:
                print("Clicking first Databricks element")
                await databricks_elements[0].click()

                # Wait for navigation/content to load
                await page.wait_for_timeout(2000)

                # Take screenshot after clicking Databricks
                screenshot_path2 = "/tmp/app_after_databricks_click.png"
                await page.screenshot(path=screenshot_path2, full_page=True)
                print(f"Screenshot after Databricks click saved to: {screenshot_path2}")
            else:
                print("No 'Databricks' elements found on the page")

            # Get console messages (if any errors)
            console_messages = []
            def handle_console(msg):
                console_messages.append({"type": msg.type, "text": msg.text})

            page.on("console", handle_console)

            print("\n=== Page Content Summary ===")
            # Get some basic page info
            html = await page.content()
            if "login" in html.lower():
                print("Status: Login page detected")
            elif "dashboard" in html.lower() or "app" in html.lower():
                print("Status: App/Dashboard page detected")
            else:
                print("Status: Page loaded (content type unclear)")

        except Exception as e:
            print(f"Error navigating to page: {e}")
            screenshot_path_error = "/tmp/app_error.png"
            try:
                await page.screenshot(path=screenshot_path_error, full_page=True)
                print(f"Error screenshot saved to: {screenshot_path_error}")
            except:
                print("Could not take error screenshot")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_app())
