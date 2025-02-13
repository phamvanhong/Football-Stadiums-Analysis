import requests
from bs4 import BeautifulSoup

class WebScraping:
    def __init__(self, url):
        self.url = url

    def get_html(self):
        try:
            response = requests.get(self.url).text
            return response
        except requests.RequestException as e:
            print(f"An error occured: {e}")

    def get_soup(self):
        html = self.get_html()
        return BeautifulSoup(html, "html.parser")

    def get_html_element(self, tag_name: str) -> list:
        """
        Extracts all HTML tab elements from the webpage

        Args:
            tag_name: str - the tag name to extract
        Returns:
            list - a list of all the elements with the tag name
        """
        soup = self.get_soup()
        elements = soup.find_all(tag_name)
        return elements
    