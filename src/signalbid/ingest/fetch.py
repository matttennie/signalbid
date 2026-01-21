"""Multi-source fetcher for opportunity documents"""

import re
from typing import Any, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class MultiSourceFetcher:
    """Fetches opportunities from multiple configured sources"""

    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.session = self._create_retry_session()

    def _create_retry_session(self) -> requests.Session:
        """Create requests session with retry logic and exponential backoff"""
        session = requests.Session()
        session.headers.update(
            {"User-Agent": "SignalBid/0.1.0 (Opportunity Intelligence; +https://signalbid.com)"}
        )

        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def fetch_source(self, source: dict[str, Any]) -> list[dict[str, Any]]:
        """Fetch opportunities from a single source configuration"""
        source_type = source.get("type")

        if source_type == "html_index":
            return self._fetch_html_index(source)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

    def _fetch_html_index(self, source: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Fetch from HTML index pages with configurable CSS selectors.

        Flow:
        1. Fetch index page
        2. Extract listing links using listing_link_selectors
        3. Follow each listing link
        4. Extract PDF links using pdf_link_selectors
        5. Return structured opportunity records
        """
        items = []
        index_url = source.get("base_url") or source.get("url")  # support both schemas
        crawl_config = source.get("crawl", {})
        normalize_config = source.get("normalize", {})

        listing_selectors = crawl_config.get("listing_link_selectors", ["a"])
        pdf_selectors = crawl_config.get("pdf_link_selectors", ["a[href$='.pdf']"])
        max_listings = crawl_config.get("max_listings", 50)

        # Support old schema fallback
        if not crawl_config:
            listing_selectors = [source.get("listing_link_selectors", "a")]
            pdf_selectors = [source.get("pdf_link_selectors", "a[href$='.pdf']")]

        # Fetch index page
        try:
            response = self.session.get(index_url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to fetch index {index_url}: {e}")

        soup = BeautifulSoup(response.content, "lxml")

        # Try each selector and collect all matching links
        all_links = []
        for selector in listing_selectors:
            links = soup.select(selector)
            if links:
                all_links.extend(links)

        # Deduplicate by href and limit
        seen_hrefs = set()
        unique_links = []
        for link in all_links:
            href = link.get("href")
            if href and href not in seen_hrefs:
                seen_hrefs.add(href)
                unique_links.append(link)
                if len(unique_links) >= max_listings:
                    break

        for link in unique_links:
            href = link.get("href")
            if not href:
                continue

            canonical_url = urljoin(index_url, href)
            title = link.get_text(strip=True) or "Untitled"

            # Follow listing link to get detail page
            detail_data = self._fetch_listing_detail(
                canonical_url,
                pdf_selectors,
                normalize_config.get("buyer_org") or source.get("buyer_org", "Unknown"),
            )

            item = {
                "title": title,
                "canonical_url": canonical_url,
                "buyer_org": normalize_config.get("buyer_org")
                or source.get("buyer_org", "Unknown"),
                "buyer_type": normalize_config.get("buyer_type")
                or source.get("buyer_type", "unknown"),
                "region": normalize_config.get("region") or source.get("region", "unknown"),
                "pdf_url": detail_data.get("pdf_url"),
                "deadline": detail_data.get("deadline"),
                "description": detail_data.get("description", ""),
            }

            items.append(item)

        return items

    def _fetch_listing_detail(
        self, url: str, pdf_selectors: list[str], buyer_org: str
    ) -> dict[str, Any]:
        """Fetch detail page and extract PDF links and metadata"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
        except requests.RequestException:
            return {"pdf_url": None, "deadline": None, "description": ""}

        soup = BeautifulSoup(response.content, "lxml")

        # Extract PDF link - try each selector
        pdf_url = None
        for pdf_selector in pdf_selectors:
            pdf_links = soup.select(pdf_selector)
            if pdf_links:
                pdf_href = pdf_links[0].get("href")
                if pdf_href:
                    pdf_url = urljoin(url, pdf_href)
                    break

        # Extract deadline (best effort - look for common patterns)
        deadline = self._extract_deadline(soup)

        # Extract description (first paragraph or meta description)
        description = ""
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content"):
            description = meta_desc["content"]
        else:
            first_p = soup.find("p")
            if first_p:
                description = first_p.get_text(strip=True)[:500]

        return {
            "pdf_url": pdf_url,
            "deadline": deadline,
            "description": description,
        }

    def _extract_deadline(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract deadline from page text using common patterns.
        Returns ISO-8601 date string or None.
        """
        text = soup.get_text()

        # Look for patterns like:
        # "Deadline: January 15, 2025"
        # "Due Date: 2025-01-15"
        # "Applications due: 01/15/2025"

        patterns = [
            r"deadline[:\s]+([A-Z][a-z]+\s+\d{1,2},\s+\d{4})",
            r"due\s+date[:\s]+(\d{4}-\d{2}-\d{2})",
            r"due[:\s]+(\d{1,2}/\d{1,2}/\d{4})",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                # Return raw match for now - parser will handle normalization
                return match.group(1)

        return None
