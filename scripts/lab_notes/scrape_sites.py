from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
import requests
from urllib.parse import urljoin, urlparse
import html2text
from readability import Document
import json
from concurrent.futures import ThreadPoolExecutor
import tldextract


OLLAMA_API_URL = "http://localhost:11434/api/generate"


def extract_links(website_url: str, css_selector: Optional[str] = None) -> List[str]:
    """
    Extract href links using CSS selectors for more flexible matching.

    Args:
        website_url (str): The URL to scrape
        css_selector (str, optional): CSS selector (e.g., 'div[class*="text parbase"]')

    Returns:
        List[str]: List of href attribute values from matching anchor tags
    """

    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    "Referer": website_url
    }
    session = requests.Session()
    response = session.get(website_url, headers=headers, allow_redirects=True)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')
    if css_selector is None:
        links = [anchor.get('href') for anchor in soup.find_all('a', href=True)]
    else:
        target_elements = soup.select(css_selector)
        links = [
            anchor.get('href')
            for element in target_elements
            for anchor in element.find_all('a', href=True)
        ]

    return links

def convert_to_full_urls(href_list: List[str], base_url: str) -> List[str]:
    """
    Convert relative URLs to absolute URLs using the provided base URL.

    Args:
        href_list (List[str]): List of href values (relative or absolute)
        base_url (str): Base URL to resolve relative URLs against

    Returns:
        List[str]: List of absolute URLs
    """
    absolute_urls = []

    for href in href_list:
        # Check if URL is already absolute (has scheme like http/https)
        if urlparse(href).scheme:
            absolute_urls.append(href)
        else:
            # Convert relative URL to absolute using urljoin
            absolute_url = urljoin(base_url, href)
            absolute_urls.append(absolute_url)
    return absolute_urls

def get_base_url(url: str) -> str:
    """
    Extract the base URL (scheme + domain) from a full URL, converting http to https.

    Args:
        url (str): Full URL (e.g., 'http://lsa.umich.edu/psych/some-page.html')

    Returns:
        str: Base URL with https (e.g., 'https://lsa.umich.edu')
    """
    parsed = urlparse(url)
    # Convert http to https
    scheme = "https" if parsed.scheme == "http" else parsed.scheme
    return f"{scheme}://{parsed.netloc}"

def get_all_lab_sites(lab_categories: List[str]) -> List[str]:
    """Extract all individual lab sites from category pages."""
    class_id = "text parbase aem-GridColumn--default--none aem-GridColumn--offset--xs--0 aem-GridColumn--md--8 aem-GridColumn aem-GridColumn--xs--none aem-GridColumn--offset--md--0 aem-GridColumn--default--12 aem-GridColumn--offset--default--0 aem-GridColumn--xs--12 aem-GridColumn--md--none"
    lab_sites = []
    for link in lab_categories:
        base = get_base_url(link)
        labs = extract_links(link,
            'div[class*="text parbase"]'
        )
        labs = [item for item in labs if "/psych/people/faculty/" not in item
                and "docs.google.com" not in item
                ] # removes excess links from some category pages
        labs = convert_to_full_urls(labs, base)
        lab_sites.extend(labs)
    #lab_sites = list(set(lab_sites)) #de-duplicate webpages
    return lab_sites

def extract_and_clean_page_content(url: str) -> tuple[str, str]:
    """Extract and clean page content, returning title and markdown."""
    headers = {
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
               'Accept-Language': 'en-US,en;q=0.5',
               'Accept-Encoding': 'gzip, deflate',
               'Connection': 'keep-alive',
               'Upgrade-Insecure-Requests': '1',
           }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        doc = Document(response.text)
        page_title = doc.title()
        page_content = doc.content()

        soup = BeautifulSoup(page_content, 'html.parser')
        for script in soup(["script", "style", "head"]):
            script.decompose()
        for tag in soup.select('[class*="nav"], [id*="nav"], [class*="menu"], [class*="foot"], [id*="foot"], [id*="menu"]'):
            tag.decompose()
        for a in soup.find_all('a'):
            a.decompose()
        for img in soup.find_all('img'):
            img.decompose()

        markdown = html2text.html2text(str(soup))
        return page_title, markdown
    except Exception as e:
        print(f"Error processing {url}: {e}")
        return None, None

def process_lab_site(lab_url: str) -> None:
    """Process a single lab site - extract pages and identify member pages."""
    def extract_all_links(lab_url, visited=None, max_depth=3, current_depth=0):
           # Prevent infinite recursion and track visited URLs
           if visited is None:
               visited = set()
           if lab_url in visited or current_depth > max_depth:
               return []
           visited.add(lab_url)

           base = get_base_url(lab_url)
           site_links = extract_links(lab_url)
           # Recursive case: if only one link and it's not already visited, follow it
           if len(site_links) == 1 and site_links[0] not in visited:
               print(f"Only one link found, following: {site_links[0]} to find more pages")
               return extract_all_links(site_links[0], visited, max_depth, current_depth + 1)
           site_links = list(set(site_links))
           site_links = [item for item in site_links if "#" not in item
                       and ".pdf" not in item
                       and ".png" not in item
                       and ".jpg" not in item
                        ]
           cleaned_site_links = convert_to_full_urls(site_links, base)
           cleaned_site_links = [link for link in cleaned_site_links if tldextract.extract(base).domain in link]

           if not cleaned_site_links:
               base = "sites.lsa.umich.edu"
               cleaned_site_links = [link for link in site_links if base in link]



           return cleaned_site_links
    cleaned_site_links = extract_all_links(lab_url)

    pages = {}
    page_titles = []

    for site in cleaned_site_links:
        #print(f"     {site}")
        page_title, markdown = extract_and_clean_page_content(site)
        #print(f"Page Title: {page_title}")
        if page_title and markdown:  # Only add if extraction was successful
            page_titles.append(page_title)
            pages[page_title] = markdown

    #print(page_titles)
    if not page_titles:
        print(f"{lab_url} pages not found.")
    if page_titles:  # Only proceed if we have pages
        pages_to_check = get_lab_member_pages(page_titles)
        print(f"Pages to Check: {pages_to_check}")
        for page in pages_to_check:
            if page in pages:  # Check if page exists in dictionary
                pass
                #print(json.dumps(pages[page], indent=4))

def get_lab_member_pages(page_titles: List[str]) -> List[str]:
    """
    Query Ollama server to identify pages most likely to contain lab member information.

    Args:
        page_titles (List[str]): List of page titles from a psychology research lab website

    Returns:
        List[str]: Two page titles most likely to contain lab member information
    """
    # Construct the prompt
    titles_str = "\n".join([f"- {title}" for title in page_titles])
    prompt = f"""Below is a list of page titles for a psychology research laboratory's website. Return the two pages most likely to give you information on lab members as a python style list. Return the entirety of each name found between the quotation marks list items may include dashes '-' or pipes '|' including everything before and after them in the list items. Do not return anything else.
    {titles_str}"""

    # Prepare the request payload
    payload = {
        "model": "gemma3:27b",
        "prompt": prompt,
        "stream": False
    }

    # Make the request to Ollama
    response = requests.post(OLLAMA_API_URL, json=payload)
    response.raise_for_status()

    # Parse the response
    response_data = response.json()
    response_text = response_data.get('response', '').strip()

    # Extract and evaluate the Python list from the response
    try:
        # Use eval to convert the string representation of the list to an actual list
        result_list = eval(response_text)
        return result_list
    except:
        # If eval fails, try to extract list manually or return empty list
        return []

#all_links = extract_links("https://lsa.umich.edu/psych/research/research-laboratories.html")
#print(f"\n\n\n All Links: {all_links}")

#lab_links = extract_links("https://lsa.umich.edu/psych/program-areas/social-psychology/social-psychology-labs---research.html","div", {"class":"text parbase aem-GridColumn--default--none aem-GridColumn--offset--xs--0 aem-GridColumn--md--8 aem-GridColumn aem-GridColumn--xs--none aem-GridColumn--offset--md--0 aem-GridColumn--default--12 aem-GridColumn--offset--default--0 aem-GridColumn--xs--12 aem-GridColumn--md--none"})
#print(f"\n\n\n Lab links {lab_links}")

def main():
    url = "https://lsa.umich.edu/psych/research/research-laboratories.html"
    base_url = get_base_url(url)

    # Get list of labs by category listing
    lab_categories = extract_links(url,"div[class='accordion-body text']")
    lab_categories = convert_to_full_urls(lab_categories, base_url)
    #print(f" Lab Categories: {lab_categories}")

    # Get all individual lab sites
    lab_sites = get_all_lab_sites(lab_categories)
    #print(f"Lab Websites: {lab_sites}")

    for lab_site in lab_sites:
        #print(f"Lab Website to Process: {lab_site}")
        process_lab_site(lab_site)
'''
    #multi-thread lab site processing
    def threaded_process(lab_site):
        print(f"Lab Website to Process: {lab_site}")
        process_lab_site(lab_site)

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(threaded_process, lab_sites)
'''


if __name__ == '__main__':
    #process_lab_site("http://www.umich.edu/~deldin/")
    #process_lab_site("https://faculty.isr.umich.edu/gonzo/")
    #process_lab_site("http://www-personal.umich.edu/~jjonides/") # base url does not match because of redirect
    #process_lab_site("https://gideon-rothschild.squarespace.com/")
    #process_lab_site("https://www.rothschild-lab.com/")
    main()
