from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
import requests
from urllib.parse import urljoin, urlparse
import html2text
from readability import Document
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor
import tldextract
import os
import pickle
from pathlib import Path
import pandas
from ai.ai_facade import AIFacade


def extract_links(website_url: str, css_selector: Optional[str] = None) -> List[str]:
    """
    Extract href links using CSS selectors. Falls back to Selenium on 403 errors.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": website_url,
        "Cache-Control": "no-cache",
        "DNT": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Ch-Ua": '"Not/A)Brand";v="8", "Chromium";v="126"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"'
    }

    try:
        session = requests.Session()
        session.headers.update(headers)

        # Initial request to establish session/cookies
        initial_response = session.get(website_url, timeout=10)
        initial_response.raise_for_status()

        # Main request
        response = session.get(website_url, timeout=10)
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

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            return extract_links_selenium(website_url, css_selector)
        else:
            raise

def extract_links_selenium(website_url: str, css_selector: Optional[str] = None) -> List[str]:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import time

    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    try:
        driver.get(website_url)

        # Handle multiple Cloudflare challenges
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                # Check if still on challenge page
                if "Just a moment" not in driver.title:
                    break

                # Try to click challenge
                checkbox = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='checkbox'], .cb-lb, button, .cf-button"))
                )
                checkbox.click()

                # Wait for next challenge or completion
                time.sleep(3)

            except:
                break  # No more challenges

        # Wait for final page load
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "a"))
        )

        if css_selector is None:
            elements = driver.find_elements(By.TAG_NAME, "a")
        else:
            parent_elements = driver.find_elements(By.CSS_SELECTOR, css_selector)
            elements = []
            for parent in parent_elements:
                elements.extend(parent.find_elements(By.TAG_NAME, "a"))

        links = [elem.get_attribute('href') for elem in elements if elem.get_attribute('href')]
        return links

    finally:
        driver.quit()

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
    lab_sites = []
    for link in lab_categories:
        base = get_base_url(link)
        labs = extract_links(link, 'div[class*="text parbase"]')
        labs = [item for item in labs if "/psych/people/faculty/" not in item
                and "docs.google.com" not in item
                ] # removes excess links from some category pages
        labs = convert_to_full_urls(labs, base)
        lab_sites.extend(labs)
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
        #print(f"page content for {page_title}")
        soup = BeautifulSoup(page_content, 'html.parser')

        # Extract figcaption content first
        figcaptions = soup.find_all('figcaption')
        preserved_figcaptions = [str(fig) for fig in figcaptions]

        # Replace anchors with their text content (removes URLs, keeps text)
        for a in soup.find_all('a'):
            if a.get_text(strip=True):  # Only if anchor has text
                a.replace_with(a.get_text())
            else:
                a.decompose()  # Remove empty anchors

        # Remove navigation/menu elements entirely
        for tag in soup.select('[class*="nav"]:not(body), [id*="nav"]:not(body), [class*="menu"]:not(body), [class*="foot"]:not(body), [id*="foot"]:not(body)'):
            #print(f"Removing tag: {tag.name} with class/id: {tag.get('class', [])} {tag.get('id', '')}")
            tag.decompose()

        # Remove images but preserve alt text if useful
        for img in soup.find_all('img'):
            alt_text = img.get('alt', '').strip()
            if alt_text and len(alt_text) > 3:  # Keep meaningful alt text
                img.replace_with(f" {alt_text} ")
            else:
                img.decompose()

        # Re-inject preserved figcaptions
        for fig_html in preserved_figcaptions:
            soup.append(BeautifulSoup(fig_html, 'html.parser'))

        markdown = html2text.html2text(str(soup))
        #print(markdown)
        return page_title, markdown
    except Exception as e:
        print(f"Error processing {url}: {e}")
        return None, None

def get_lab_member_pages(page_titles: List[str]) -> List[str]:
    """
    Query AI service to identify pages most likely to contain lab member information.

    Args:
        page_titles (List[str]): List of page titles from a psychology research lab website

    Returns:
        List[str]: Two page titles most likely to contain lab member information
    """
    if not page_titles:
        return []

    # Domain-specific prompt construction
    titles_str = "\n".join([f"- {title}" for title in page_titles])
    prompt = f"""
    Below is a list of page titles for a psychology research laboratory's website.
    Return the two pages most likely to give you information on lab members as a python style list.
    Return the entirety of each name found between the quotation marks list items may include dashes '-' or pipes '|'
    including everything before and after them in the list items. Do not return anything else.
    {titles_str}"""

    # Use generic AI capabilities instead of hardcoded Ollama
    ai_service = AIFacade()

    # Check if service is available
    if not ai_service.is_service_available():
        print(f"Warning: {ai_service.get_current_provider()} AI service is not available")
        print(f"Using provider: {ai_service.get_current_provider()}, model: {ai_service.get_current_model()}")
        return []

    # Generate structured response
    response = ai_service.generate_structured_response(
        prompt=prompt,
        format_type="python_list"
    )

    if response.success and response.parsed_data:
        #print(f"AI analysis successful using {response.provider} - {response.model_used}")
        return response.parsed_data
    else:
        print(f"AI analysis failed: {response.error_message}")
        return []

def extract_all_links(lab_url, visited=None, max_depth=3, current_depth=0):
    """Extract all links from a lab site with recursive following."""
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

def process_lab_site(lab_url: str) -> None:
    """Process a single lab site - extract pages and identify member pages."""
    cleaned_site_links = extract_all_links(lab_url)

    pages = {}
    page_titles = []

    for site in cleaned_site_links:
        page_title, markdown = extract_and_clean_page_content(site)
        if page_title and markdown:  # Only add if extraction was successful
            page_titles.append(page_title)
            pages[page_title] = markdown

    if not page_titles:
        print(f"{lab_url} pages not found.")
        return

    if page_titles:  # Only proceed if we have pages
        pages_to_check = get_lab_member_pages(page_titles)
        print(f"Pages to Check: {pages_to_check}")
        for page in pages_to_check:
            if page in pages:  # Check if page exists in dictionary
                #pass
                print(f"Page: {page}")
                #print(json.dumps(pages[page], indent=4))  # Uncomment to see page content

def extract_lab_personnel(pages_content: Dict[str, str]) -> Dict[str, Any]:
    """
    Extract lab personnel information from website content using AI analysis.

    Args:
        pages_content (Dict[str, str]): Dictionary with page names as keys and markdown content as values

    Returns:
        Dict[str, Any]: JSON object with lab personnel info, easily convertible to DataFrame
    """
    if not pages_content:
        return {"lab_name": "", "personnel": []}

    # Combine all page content for analysis
    combined_content = ""
    for page_name, content in pages_content.items():
        combined_content += f"\n\n=== PAGE: {page_name} ===\n{content[:2000]}"  # Limit content length

    # Construct prompt for personnel extraction
    prompt = f"""Analyze the following psychology research lab website content and extract personnel information.

        Content:
        {combined_content}

        Return a JSON object with this exact structure:
        {{
        "lab_name": "Name of the lab or research group",
        "personnel": [
            {{
            "name": "Full name",
            "role": "Principal Investigator|Lab Manager|Graduate Student|Postdoc|Research Assistant|Undergraduate|Staff|Other",
            "title": "Academic title or position",
            "email": "email@domain.com or null if not found",
            "description": "Brief description or research focus if available"
            }}
        ]
        }}

        Guidelines:
        - Principal Investigator: Usually a faculty member, the lab director, or who the lab is named after
        - Lab Manager: Administrative coordinator, point of contact, lab administrator
        - Include all identifiable lab members
        - If email not explicitly stated, use null
        - For role field, use exactly one of the specified categories
        - Order by: Principal Investigator first, then Lab Manager, then others
        - If uncertain about role, use best judgment based on context"""

    # Use AI facade to analyze content
    ai_service = AIFacade()

    if not ai_service.is_service_available():
        print(f"Warning: {ai_service.get_current_provider()} AI service not available")
        return {"lab_name": "", "personnel": []}

    response = ai_service.generate_structured_response(
        prompt=prompt,
        format_type="json"
    )

    if response.success and response.parsed_data:
        # Validate and clean the response
        personnel_data = response.parsed_data

        # Ensure required structure
        if "personnel" not in personnel_data:
            personnel_data["personnel"] = []
        if "lab_name" not in personnel_data:
            personnel_data["lab_name"] = ""

        # Clean and validate personnel entries
        cleaned_personnel = []
        for person in personnel_data.get("personnel", []):
            if isinstance(person, dict) and "name" in person:
                cleaned_person = {
                    "name": person.get("name", ""),
                    "role": person.get("role", "Other"),
                    "title": person.get("title", ""),
                    "email": person.get("email"),
                    "description": person.get("description", "")
                }
                cleaned_personnel.append(cleaned_person)

        personnel_data["personnel"] = cleaned_personnel

        print(f"Successfully extracted {len(cleaned_personnel)} lab members using {response.provider}")
        return personnel_data
    else:
        print(f"Personnel extraction failed: {response.error_message}")
        return {"lab_name": "", "personnel": []}

def convert_personnel_to_dataframe(personnel_data: Dict[str, Any]) -> 'pd.DataFrame':
    """Convert personnel data to pandas DataFrame."""
    try:
        import pandas as pd
    except ImportError:
        print("pandas not installed. Install with: pip install pandas")
        return None

    if not personnel_data.get("personnel"):
        return pd.DataFrame()

    # Create DataFrame and add lab name to each row
    df = pd.DataFrame(personnel_data["personnel"])
    df["lab_name"] = personnel_data.get("lab_name", "")

    # Reorder columns for better readability
    column_order = ["lab_name", "name", "role", "title", "email", "description"]
    df = df.reindex(columns=[col for col in column_order if col in df.columns])

    return df

def save_personnel_to_csv(personnel_data: Dict[str, Any], filename: str) -> bool:
    """Save personnel data directly to CSV file."""
    try:
        df = convert_personnel_to_dataframe(personnel_data)
        if df is not None and not df.empty:
            df.to_csv(filename, index=False)
            print(f"Personnel data saved to {filename}")
            return True
        else:
            print("No personnel data to save")
            return False
    except Exception as e:
        print(f"Error saving to CSV: {e}")
        return False

def process_lab_site_with_personnel(lab_url: str) -> Dict[str, Any]:
    """
    Enhanced version that extracts both member pages and personnel information.

    Args:
        lab_url (str): URL of the lab website

    Returns:
        Dict[str, Any]: Complete lab information including personnel
    """
    cleaned_site_links = extract_all_links(lab_url)

    pages = {}
    page_titles = []

    # Extract all page content
    for site in cleaned_site_links:
        page_title, markdown = extract_and_clean_page_content(site)
        if page_title and markdown:
            page_titles.append(page_title)
            pages[page_title] = markdown

    if not page_titles:
        print(f"{lab_url} pages not found.")
        return {"lab_url": lab_url, "personnel_data": {"lab_name": "", "personnel": []}}

    # Get recommended member pages
    pages_to_check = get_lab_member_pages(page_titles)
    print(f"Recommended member pages: {pages_to_check}")

    # Extract content from recommended pages for personnel analysis
    member_pages_content = {}
    for page in pages_to_check:
        if page in pages:
            member_pages_content[page] = pages[page]

    # If no specific member pages found, use all pages
    if not member_pages_content:
        member_pages_content = pages

    # Extract personnel information
    personnel_data = extract_lab_personnel(member_pages_content)
    print(f"Personnel Data:\n {personnel_data}")
    return {
        "lab_url": lab_url,
        "all_pages": list(pages.keys()),
        "member_pages": pages_to_check,
        "personnel_data": personnel_data
    }

def process_multiple_labs_to_csv(lab_urls: List[str], output_file: str = "lab_personnel.csv"):
    """Process multiple labs and combine all personnel into a single CSV."""
    try:
        import pandas as pd
    except ImportError:
        print("pandas required for this function. Install with: pip install pandas")
        return

    all_personnel = []

    for lab_url in lab_urls:
        print(f"\nProcessing: {lab_url}")
        lab_data = process_lab_site_with_personnel(lab_url)

        personnel_data = lab_data["personnel_data"]
        if personnel_data.get("personnel"):
            # Add lab URL to each person's record
            for person in personnel_data["personnel"]:
                person["lab_url"] = lab_url
                person["lab_name"] = personnel_data.get("lab_name", "")
            all_personnel.extend(personnel_data["personnel"])

    if all_personnel:
        df = pd.DataFrame(all_personnel)
        df.to_csv(output_file, index=False)
        print(f"\nCombined personnel data saved to {output_file}")
        print(f"Total personnel extracted: {len(all_personnel)}")
    else:
        print("No personnel data extracted")

def save_lab_sites(lab_sites: List[str], filename: str = "lab_sites.json") -> None:
    """Save lab sites list to JSON file."""
    import json
    with open(filename, 'w') as f:
        json.dump(lab_sites, f, indent=2)
    print(f"Saved {len(lab_sites)} lab sites to {filename}")

def load_lab_sites(filename: str = "lab_sites.json") -> List[str]:
    """Load lab sites list from JSON file."""
    import json
    try:
        with open(filename, 'r') as f:
            lab_sites = json.load(f)
        print(f"Loaded {len(lab_sites)} lab sites from {filename}")
        return lab_sites
    except FileNotFoundError:
        print(f"No existing {filename} found")
        return []

def get_processed_labs(csv_file: str) -> set:
    """Get set of already processed lab URLs from existing CSV."""
    processed = set()
    if os.path.exists(csv_file):
        try:
            import pandas as pd
            df = pd.read_csv(csv_file)
            if 'lab_url' in df.columns:
                processed = set(df['lab_url'].dropna().unique())
                print(f"Found {len(processed)} already processed labs in {csv_file}")
        except Exception as e:
            print(f"Error reading existing CSV: {e}")
    return processed

def append_personnel_to_csv(personnel_data: Dict[str, Any], lab_url: str, csv_file: str) -> bool:
    """Append personnel data for one lab to CSV file."""
    try:
        import pandas as pd

        if not personnel_data.get("personnel"):
            return False

        # Add lab_url to each person's record
        personnel_list = []
        for person in personnel_data["personnel"]:
            person_copy = person.copy()
            person_copy["lab_url"] = lab_url
            person_copy["lab_name"] = personnel_data.get("lab_name", "")
            personnel_list.append(person_copy)

        df_new = pd.DataFrame(personnel_list)

        # Append to existing CSV or create new one
        if os.path.exists(csv_file):
            df_new.to_csv(csv_file, mode='a', header=False, index=False)
        else:
            df_new.to_csv(csv_file, index=False)

        print(f"Added {len(personnel_list)} people from {lab_url} to {csv_file}")
        return True

    except Exception as e:
        print(f"Error appending to CSV: {e}")
        return False

def save_processing_state(processed_urls: set, state_file: str = "processing_state.pkl") -> None:
    """Save current processing state."""
    with open(state_file, 'wb') as f:
        pickle.dump(processed_urls, f)

def load_processing_state(state_file: str = "processing_state.pkl") -> set:
    """Load processing state."""
    try:
        with open(state_file, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        return set()

def process_multiple_labs_to_csv_persistent(
    lab_urls: List[str] = None,
    output_file: str = "lab_personnel.csv",
    lab_sites_file: str = "lab_sites.json",
    resume: bool = True
    ) -> None:
    """
    Process multiple labs with persistence and resume capability.

    Args:
        lab_urls: List of lab URLs (if None, will try to load from file)
        output_file: CSV output filename
        lab_sites_file: JSON file to save/load lab sites
        resume: Whether to resume from previous run
    """
    try:
        import pandas as pd
    except ImportError:
        print("pandas required. Install with: pip install pandas")
        return

    # Load or use provided lab URLs
    if lab_urls is None:
        lab_urls = load_lab_sites(lab_sites_file)
        if not lab_urls:
            print("No lab URLs provided and none found in file")
            return
    else:
        # Save lab URLs for future use
        save_lab_sites(lab_urls, lab_sites_file)

    # Get already processed labs if resuming
    processed_labs = set()
    if resume:
        processed_labs = get_processed_labs(output_file)

    total_labs = len(lab_urls)
    remaining_labs = [url for url in lab_urls if url not in processed_labs]

    print(f"Total labs: {total_labs}")
    print(f"Already processed: {len(processed_labs)}")
    print(f"Remaining: {len(remaining_labs)}")

    if not remaining_labs:
        print("All labs already processed!")
        return

    # Process remaining labs
    success_count = 0
    for i, lab_url in enumerate(remaining_labs, 1):
        print(f"\n[{i}/{len(remaining_labs)}] Processing: {lab_url}")

        try:
            lab_data = process_lab_site_with_personnel(lab_url)
            personnel_data = lab_data["personnel_data"]

            if personnel_data.get("personnel"):
                if append_personnel_to_csv(personnel_data, lab_url, output_file):
                    success_count += 1
                    processed_labs.add(lab_url)
                else:
                    print(f"Failed to save data for {lab_url}")
            else:
                print(f"No personnel found for {lab_url}")
                processed_labs.add(lab_url)  # Mark as processed even if empty

            # Save progress every 5 labs
            if i % 5 == 0:
                save_processing_state(processed_labs)
                print(f"Progress saved: {i}/{len(remaining_labs)} labs processed")

        except Exception as e:
            print(f"Error processing {lab_url}: {e}")
            continue

    # Final save
    save_processing_state(processed_labs)
    print(f"\nCompleted! Successfully processed {success_count} labs")
    print(f"Total personnel records in {output_file}")

def scrape_and_save_lab_sites(
    url: str = "https://lsa.umich.edu/psych/research/research-laboratories.html",
    lab_sites_file: str = "lab_sites.json"
    ) -> List[str]:
    """
    Scrape lab sites and save to file for future use.

    Args:
        url: Main psychology labs page URL
        lab_sites_file: Filename to save lab sites

    Returns:
        List of lab site URLs
    """
    print("Scraping lab sites...")
    base_url = get_base_url(url)

    # Get list of labs by category listing
    lab_categories = extract_links(url, "div[class='accordion-body text']")
    lab_categories = convert_to_full_urls(lab_categories, base_url)

    # Get all individual lab sites
    lab_sites = get_all_lab_sites(lab_categories)

    # Save for future use
    save_lab_sites(lab_sites, lab_sites_file)

    return lab_sites

def clean_restart(
    lab_sites_file: str = "lab_sites.json",
    output_file: str = "lab_personnel.csv",
    state_file: str = "processing_state.pkl"
    ) -> None:
    """Remove all saved state files for a clean restart."""
    files_to_remove = [output_file, state_file]

    for file in files_to_remove:
        if os.path.exists(file):
            os.remove(file)
            print(f"Removed {file}")

    print("Clean restart - all state files removed")

def main():
    """Replace your current main() with this."""
    # Configuration
    lab_sites_file = "lab_sites.json"
    output_file = "lab_personnel.csv"

    # Try to load existing lab sites, otherwise scrape them
    lab_sites = load_lab_sites(lab_sites_file)

    if not lab_sites:
        lab_sites = scrape_and_save_lab_sites(
            url="https://lsa.umich.edu/psych/research/research-laboratories.html",
            lab_sites_file=lab_sites_file
        )

    # Process with persistence (will automatically resume)
    process_multiple_labs_to_csv_persistent(
        lab_urls=lab_sites,
        output_file=output_file,
        resume=True  # Set False for fresh start
    )


if __name__ == '__main__':
    main()
