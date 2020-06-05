from bs4 import BeautifulSoup
import requests
from dataclasses import dataclass
import urllib.parse



@dataclass
class ScrapeResult:
    title: str
    n_images: int
    n_links: int
    local_links: list


def load_url(url):
    return requests.get(url).content


def build_url(domain, parts):
    scheme = "https"
    url = f"{parts.scheme or scheme}://{domain}{parts.path}"
    if parts.query != "":
       url += f"?{parts.query}"

    return url


def scrape_url(url):
    content = load_url(url)

    url_parts = urllib.parse.urlparse(url)
    original_domain = url_parts.netloc

    elements = BeautifulSoup(content)

    title = elements.title.text
    n_images = len(elements.select("img"))
    all_links = elements.select("a")
    n_links = len(all_links)

    extracted_urls = [link.get("href") for link in all_links]


    parsed_urls = [urllib.parse.urlparse(url)
                   for url in extracted_urls
                   if url is not None]




    local_links = [build_url(original_domain, url)
                   for url in parsed_urls
                   if url.netloc in [original_domain, ""]
                   if url.path != ""]


    return ScrapeResult(title=title,
                        n_images=n_images,
                        n_links=n_links,
                        local_links=local_links)
