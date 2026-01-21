"""Selector test mode - validates source configurations without scoring or persistence"""

import argparse
import json
import sys
from typing import Any
from urllib.parse import urljoin

import requests
import yaml
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def create_retry_session() -> requests.Session:
    """Create requests session with retry logic"""
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


def test_source_selectors(
    source: dict[str, Any], limit: int = 10, fetch_pdf: bool = True
) -> dict[str, Any]:
    """
    Test source configuration by extracting items without scoring.

    Returns:
        dict with keys: source_id, base_url, count, items, errors
    """
    session = create_retry_session()
    result = {
        "source_id": source.get("id"),
        "base_url": source.get("base_url"),
        "count": 0,
        "items": [],
        "errors": [],
    }

    try:
        base_url = source["base_url"]
        crawl_config = source.get("crawl", {})
        listing_selectors = crawl_config.get("listing_link_selectors", ["a"])
        pdf_selectors = crawl_config.get("pdf_link_selectors", ["a[href$='.pdf']"])
        max_listings = min(limit, crawl_config.get("max_listings", 10))

        # Fetch index page
        response = session.get(base_url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "lxml")

        # Try each listing selector until we find links
        all_links = []
        for selector in listing_selectors:
            links = soup.select(selector)
            if links:
                all_links.extend(links)

        # Deduplicate by href
        seen_hrefs = set()
        unique_links = []
        for link in all_links:
            href = link.get("href")
            if href and href not in seen_hrefs:
                seen_hrefs.add(href)
                unique_links.append(link)

        # Process up to max_listings
        for link in unique_links[:max_listings]:
            href = link.get("href")
            if not href:
                continue

            canonical_url = urljoin(base_url, href)
            title = link.get_text(strip=True) or "Untitled"

            item = {"title": title, "canonical_url": canonical_url, "pdf_url": None}

            # If this looks like a direct PDF link, use it
            if href.endswith(".pdf") or ".pdf?" in href:
                item["pdf_url"] = canonical_url
            else:
                # Try to fetch detail page and find PDF
                if fetch_pdf:
                    try:
                        detail_response = session.get(canonical_url, timeout=30)
                        detail_response.raise_for_status()
                        detail_soup = BeautifulSoup(detail_response.content, "lxml")

                        # Try each PDF selector
                        for pdf_selector in pdf_selectors:
                            pdf_links = detail_soup.select(pdf_selector)
                            if pdf_links:
                                pdf_href = pdf_links[0].get("href")
                                if pdf_href:
                                    item["pdf_url"] = urljoin(canonical_url, pdf_href)
                                    break
                    except requests.RequestException as e:
                        result["errors"].append(
                            {"url": canonical_url, "error": f"Failed to fetch detail: {e}"}
                        )

            result["items"].append(item)

        result["count"] = len(result["items"])

    except Exception as e:
        result["errors"].append({"stage": "index_fetch", "error": str(e)})

    return result


def main():
    parser = argparse.ArgumentParser(description="Test source selector configurations")
    parser.add_argument("--sources", required=True, help="Path to sources.yml configuration")
    parser.add_argument("--test-source", required=True, help="Source ID to test")
    parser.add_argument("--limit", type=int, default=10, help="Max items to extract (default: 10)")
    parser.add_argument(
        "--no-fetch-pdf",
        action="store_true",
        help="Don't fetch detail pages to find PDFs",
    )
    parser.add_argument("--out", help="Optional output file path for JSON results")

    args = parser.parse_args()

    # Load sources config
    with open(args.sources) as f:
        config = yaml.safe_load(f)

    # Find the requested source
    source = None
    for src in config.get("sources", []):
        if src.get("id") == args.test_source:
            source = src
            break

    if not source:
        print(f"Error: Source '{args.test_source}' not found in {args.sources}", file=sys.stderr)
        sys.exit(1)

    # Run the test
    result = test_source_selectors(source, limit=args.limit, fetch_pdf=not args.no_fetch_pdf)

    # Output results
    output_json = json.dumps(result, indent=2)

    if args.out:
        with open(args.out, "w") as f:
            f.write(output_json)
        print(f"Results written to {args.out}")
    else:
        print(output_json)


if __name__ == "__main__":
    main()
