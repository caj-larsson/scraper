import urllib.parse
import random


def mock_scrape(scrape_url):
    """ In order to prevent overload of the webserver, pretend to scrape.
    """
    url_parts = urllib.parse.urlparse(scrape_url)

    def build_sub_url(extension):
        path = url_parts.path + '/' + extension
        return url_parts.scheme + "://" + url_parts.netloc + path

    mock_pages = [
        "page1", "page3", "about", "home", "testing", "course", "mouse",
        "tape", "cable", "headphones", "filler2", "unsinkable2", "last"
    ]

    n_links = random.randint(0, 3)

    if n_links > 0:
        links = [build_sub_url(extension)
                    for extension
                    in random.choices(mock_pages, k=n_links)
                ]
    else:
        links = []

    n_images = random.randint(0, 100)

    # Make sure we have cycles in the structure by always adding the root.
    links.append("https://www.pentesteracademy.com")
    n_links = len(links) + random.randint(0, 10) # Add out of domain links.


    return n_images, n_links, links, "mock_title"
