"""
This class is used to login to a LinkedIn personal account and scrape the results
for a specified search result for the last 24 hours.

Developer Notes:
    xpath vs. css selector:
        - xpath performance is subpar to css selector
        - only xpath can be used to get the text of an element
        - syntax is different for each of them
        - xpath is bi-direction while css selector is one-directional
    
    xpath syntax:

    css selector syntax:
        result.find_element_by_css_selector(
            "div > div > div[class*='feed-shared-actor'] > a > div[class*='feed-shared-actor__meta'] > span[class*='feed-shared-actor__title']"
        )
"""
import os
import sys
import csv
from bs4 import BeautifulSoup
import pyperclip
import time

# Firefox & Chrome
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Microsoft Edge
# from msedge.selenium_tools import Edge, EdgeOptions

CHROME_DRIVER = "/usr/lib/chromium/chromedriver"
FIREFOX_DRIVER = os.path.join("drivers", "geckodriver.exe") # not installed in the Dockerfile!

BASE_URL = "https://www.linkedin.com"


class LinkedInWebScraper:
    def __init__(self, browser):
        self.web_driver = self.get_webdriver(browser)

    def get_webdriver(self, browser):
        # Startup the web driver
        if browser == "Chrome":
            options = webdriver.ChromeOptions()
            options.add_argument("--no-sandbox")
            options.add_argument("--remote-debugging-port=9222")
            options.add_argument("--crash-dumps-dir=/tmp")
            options.add_argument("--headless")
            options.add_experimental_option("excludeSwitches", ["enable-logging"])
            driver = webdriver.Chrome(CHROME_DRIVER, options=options)
        elif browser == "Firefox":
            driver = webdriver.Firefox(FIREFOX_DRIVER)
        elif browser == "Edge":
            options = EdgeOptions()
            options.use_chromium = True
            driver = Edge(options=options)

        driver.maximize_window()

        return driver

    def go_to(self, url):
        pass

    def login(self, user_name, password):
        url = f"{BASE_URL}/login?"
        url += "fromSignIn=true&trk=guest_homepage-basic_nav-header-signin"

        driver = self.web_driver

        driver.get(url)
        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Email field has several different id variations, depending
        # on which page loads
        # email-or-phone
        # email-address
        user_name_field = None
        password_field = None

        for id in ("email-address", "email-or-phone", "username"):
            try:
                user_name_field = driver.find_element_by_id(id)
            except:
                continue

        try:
            password_field = driver.find_element_by_id("password")
        except:
            print("Couldn't find password.")

        # If inputs for user name and password were both found,
        # then input user name and password.
        if (user_name_field is not None) and (password_field is not None):
            user_name_field.send_keys(user_name)
            password_field.send_keys(password)

            driver.find_element_by_xpath("//button[@type='submit']").click()
        else:
            print("Could not login.")

    def search(
        self,
        _keywords,
        _date_posted="past-24h",
        _origin="FACETED_SEARCH",
        _sort_by="date_posted",
    ):
        """
        Sample URL:
        https://www.linkedin.com/search/results/content/?datePosted=%22past-24h%22&keywords=okrs&origin=FACETED_SEARCH&sid=!lL&sortBy=%22date_posted%22

        Anatomy of the URL params:
            - datePosted="past-24h"
            - keywords=<>
            -
        """

        # URL Params
        keywords = _keywords.replace(" ", "%20")
        date_posted = f"%22{_date_posted}%22"
        origin = _origin
        sort_by = f"%22{_sort_by}%22"

        url = (
            BASE_URL
            + "/search/results/content/?"
            + f"datePosted={date_posted}"
            + f"&keywords={keywords}&origin={origin}&sortBy={sort_by}"
        )

        driver = self.web_driver
        action_chains = ActionChains(driver)

        # Navigate to search page
        driver.get(url)

        # Steps:
        # 1) Scroll to bottom (aka load all page results)
        # 2) Scroll to top
        # 3) For each element in results:
        # 4) Scroll element into view
        # 5) Collect information from element result

        # Scroll to the bottom of the infinite scroll page
        self._scroll_to_bottom()

        results = []

        # Scroll so the body is in view (top?)
        # body = driver.find_element_by_xpath("//body")
        # driver.execute_script("return arguments[0].scrollIntoView();", body)

        # Find search results
        search_container = driver.find_element_by_css_selector(
            "div[class='search-results-container']"
        )  # soup.find("div", {"class": "search-results-container"})

        # Top-most search result. During testing, it was determined that this
        # isn't always related to the search-term.
        results.append(
            search_container.find_element_by_css_selector(
                "div[id^='ember'][data-urn*='urn:li:activity:']"
            )
        )

        [
            results.append(result)
            for result in search_container.find_elements_by_css_selector(
                "div[id^='ember'][class*='occludable-update ember-view']"
            )
        ]

        print(f"# of results: {len(results)}")

        data = self._extract_data_from_results(results)

        return data

    def _extract_data_from_results(self, results):
        driver = self.web_driver
        header_offset = 150

        data = []

        for count, result in enumerate(results, 1):
            # if count > 5:
            #     break

            print("-" * 40)

            # FOR TESTING KEEP HERE
            # AFTERWARDS MOVE BELOW SCROLL INTO VIEW

            # Identify user / company name of the post.
            try:
                name = result.find_element_by_css_selector(
                    "div > div > div[class*='feed-shared-actor'] > a > div[class*='feed-shared-actor__meta'] > span[class*='feed-shared-actor__title'] > span[class*='feed-shared-actor__name'] > span"
                ).text

                print("Posted by:", name)
            except:
                print("Could not find 'name' field in post.")
                name = ""

            # Identify whether the poster is a person or company.
            try:
                connection_level = result.find_element_by_css_selector(
                    "div > div > div[class*='feed-shared-actor'] > a > div[class*='feed-shared-actor__meta'] > span[class*='feed-shared-actor__title'] > span[class*='feed-shared-actor__supplement']"
                ).text

                author_type = "person"

                print("Author type:", author_type)
                print("Connection level:", connection_level)
            except:
                print("Could not find 'supplement' field in post. This is a company")
                author_type = "company"

            # Identify the post description if available. Depends on the type of post.
            try:
                description = result.find_element_by_css_selector(
                    "div > div > div[class*='feed-shared-actor'] > a > div[class*='feed-shared-actor__meta'] > span[class*='feed-shared-actor__description']"  # not done yet
                ).text

                print("Description:", description)
            except:
                print("Could not find 'description' field in post.")
                description = ""

            self._scroll_element_into_view(result, header_offset=header_offset)

            # driver.implicitly_wait(20)

            # continue

            # Extract urn from the result element
            urn = result.get_attribute("data-urn")

            if urn is None:
                element = result.find_element_by_css_selector("*:first-child")
                urn = element.get_attribute("data-urn")

            post_url = "" if urn is None else self._get_link_to_post(urn)

            record = {
                "name": name,
                "description": description,
                "post_url": post_url,
                "author_type": author_type,
            }

            data.append(record)

        return data

    def _scroll_to_bottom(self):
        """Simulate scrolling to capture all posts"""

        driver = self.web_driver
        SCROLL_PAUSE_TIME = 1.5

        # Get scroll height
        last_height = driver.execute_script("return document.body.scrollHeight")

        # FOR TESTING
        count = 0

        while True:
            # Scroll down to bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Wait to load page
            time.sleep(SCROLL_PAUSE_TIME)

            # Calculate new scroll height and compare with last scroll height
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

            # count += 1

            # if count >= 2:
            #     break

    def _scroll_element_into_view(self, element, header_offset=150):
        print("Scrolling...")

        driver = self.web_driver
        action_chains = ActionChains(driver)

        action_chains.move_to_element(element).perform()

        return True

        # Get distance of element's top from the top of the viewable screen
        # result.getBoundingClientRect().y
        result_y_offset = driver.execute_script(
            "return arguments[0].getBoundingClientRect().y", element
        )

        y = element.location["y"]

        scroll_delta = result_y_offset - header_offset

        print("Before scrolling:")
        print(
            result_y_offset,
            scroll_delta,
            element.location["y"],
            driver.execute_script("return window.innerHeight"),
            driver.execute_script("return window.pageYOffset"),
        )

        # Scroll until the element is a certain # of pixels from the top
        # ie - (element's y offset) +- (some amount) = desired_offset_from_top
        # scroll_distance =

        # Wait to see
        time.sleep(20)

        print("Scrolling element into view...")

        # window.scrollBy(0, scroll_distance) # negative goes up
        #
        # Scroll element into view
        # driver.execute_script("return arguments[0].scrollIntoView();", result)
        driver.execute_script("window.scrollBy(0, arguments[0]);", scroll_delta)

        result_y_offset = driver.execute_script(
            "return arguments[0].getBoundingClientRect().y", element
        )

        scroll_delta = result_y_offset - header_offset

        print("After scrolling:")
        print(
            result_y_offset,
            scroll_delta,
            element.location["y"],
            driver.execute_script("return window.innerHeight"),
            driver.execute_script("return window.pageYOffset"),
        )

        # Offset the scroll for LinkedIn's sticky header
        # driver.execute_script("return ")

    def _get_link_to_post(self, urn):
        return f"https://www.linkedin.com/feed/update/urn:li:activity:{urn}/"

    def close(self):
        self.web_driver.close()
