#!/usr/bin/env python3
"""Fetch RSS feeds and generate a static JSON data file for the reader."""
import asyncio
import aiohttp
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
import re
import html
import os
import hashlib

MAX_CONCURRENT = 30
TIMEOUT = 20
HOURS_BACK = 48  # fetch last 48h to ensure good coverage for 24h visits

def parse_date(date_str):
    """Try to parse various RSS date formats."""
    if not date_str:
        return None
    date_str = date_str.strip()

    formats = [
        '%a, %d %b %Y %H:%M:%S %z',
        '%a, %d %b %Y %H:%M:%S %Z',
        '%Y-%m-%dT%H:%M:%S%z',
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%dT%H:%M:%S.%f%z',
        '%Y-%m-%dT%H:%M:%S.%fZ',
        '%Y-%m-%d %H:%M:%S',
        '%a, %d %b %Y %H:%M:%S',
        '%d %b %Y %H:%M:%S %z',
    ]

    # Clean up timezone abbreviations
    cleaned = re.sub(r'\s+(GMT|UTC|EST|CST|PST|EDT|CDT|PDT|CET|CEST|EET|EEST|AST|IST)\s*$', ' +0000', date_str)

    for fmt in formats:
        try:
            dt = datetime.strptime(cleaned, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None

def clean_html(text):
    """Strip HTML tags and decode entities."""
    if not text:
        return ''
    text = html.unescape(text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:500]  # limit description length

def parse_rss(xml_text, cutoff_time):
    """Parse RSS/Atom XML and return items newer than cutoff."""
    items = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return items

    ns = {
        'atom': 'http://www.w3.org/2005/Atom',
        'dc': 'http://purl.org/dc/elements/1.1/',
        'content': 'http://purl.org/rss/1.0/modules/content/',
        'media': 'http://search.yahoo.com/mrss/',
    }

    feed_title = ''

    # Try RSS 2.0
    channel = root.find('.//channel')
    if channel is not None:
        feed_title = (channel.findtext('title') or '').strip()
        for item in channel.findall('.//item'):
            pub_date = item.findtext('pubDate') or item.findtext('{http://purl.org/dc/elements/1.1/}date') or ''
            dt = parse_date(pub_date)

            title = clean_html(item.findtext('title') or '')
            link = item.findtext('link') or ''
            desc = clean_html(item.findtext('description') or '')

            if dt and dt >= cutoff_time:
                items.append({
                    'title': title,
                    'link': link,
                    'description': desc,
                    'pubDate': dt.isoformat(),
                    'timestamp': dt.timestamp()
                })
            elif dt is None:
                # Include items without dates (might be recent)
                items.append({
                    'title': title,
                    'link': link,
                    'description': desc,
                    'pubDate': '',
                    'timestamp': 0
                })
        return items

    # Try Atom
    feed = root if root.tag == '{http://www.w3.org/2005/Atom}feed' else root.find('{http://www.w3.org/2005/Atom}feed')
    if feed is not None or root.tag == '{http://www.w3.org/2005/Atom}feed':
        target = feed if feed is not None else root
        feed_title = (target.findtext('{http://www.w3.org/2005/Atom}title') or '').strip()

        for entry in target.findall('{http://www.w3.org/2005/Atom}entry'):
            updated = entry.findtext('{http://www.w3.org/2005/Atom}updated') or entry.findtext('{http://www.w3.org/2005/Atom}published') or ''
            dt = parse_date(updated)

            title = clean_html(entry.findtext('{http://www.w3.org/2005/Atom}title') or '')
            link_el = entry.find('{http://www.w3.org/2005/Atom}link[@rel="alternate"]')
            if link_el is None:
                link_el = entry.find('{http://www.w3.org/2005/Atom}link')
            link = link_el.get('href', '') if link_el is not None else ''
            desc = clean_html(entry.findtext('{http://www.w3.org/2005/Atom}summary') or entry.findtext('{http://www.w3.org/2005/Atom}content') or '')

            if dt and dt >= cutoff_time:
                items.append({
                    'title': title,
                    'link': link,
                    'description': desc,
                    'pubDate': dt.isoformat(),
                    'timestamp': dt.timestamp()
                })

    # Try RDF/RSS 1.0
    for item in root.findall('.//{http://purl.org/rss/1.0/}item'):
        pub_date = item.findtext('{http://purl.org/dc/elements/1.1/}date') or ''
        dt = parse_date(pub_date)
        title = clean_html(item.findtext('{http://purl.org/rss/1.0/}title') or '')
        link = item.findtext('{http://purl.org/rss/1.0/}link') or ''
        desc = clean_html(item.findtext('{http://purl.org/rss/1.0/}description') or '')

        if dt and dt >= cutoff_time:
            items.append({
                'title': title,
                'link': link,
                'description': desc,
                'pubDate': dt.isoformat(),
                'timestamp': dt.timestamp()
            })

    return items

async def fetch_feed(session, feed, cutoff_time):
    """Fetch and parse a single feed."""
    url = feed['xmlUrl']
    try:
        timeout = aiohttp.ClientTimeout(total=TIMEOUT)
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; RSSReader/1.0)',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*'
        }
        async with session.get(url, timeout=timeout, headers=headers,
                               allow_redirects=True, ssl=False) as resp:
            if resp.status == 200:
                text = await resp.text(errors='replace')
                items = parse_rss(text, cutoff_time)
                source_title = feed.get('title') or url
                for item in items:
                    item['source'] = source_title
                    item['sourceUrl'] = feed.get('htmlUrl') or url
                return items
    except Exception:
        pass
    return []

async def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_dir = os.path.dirname(script_dir)
    feeds_path = os.path.join(repo_dir, 'feeds.json')
    output_path = os.path.join(repo_dir, 'data.json')

    with open(feeds_path, 'r', encoding='utf-8') as f:
        categories = json.load(f)

    cutoff = datetime.now(timezone.utc) - timedelta(hours=HOURS_BACK)
    print(f"Fetching feeds newer than {cutoff.isoformat()}")

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        sem = asyncio.Semaphore(MAX_CONCURRENT)

        result = {}
        total_items = 0

        for cat, feeds in categories.items():
            async def bounded_fetch(feed):
                async with sem:
                    return await fetch_feed(session, feed, cutoff)

            tasks = [bounded_fetch(feed) for feed in feeds]
            feed_results = await asyncio.gather(*tasks)

            cat_items = []
            for items in feed_results:
                cat_items.extend(items)

            # Sort by date descending, remove items without dates last
            cat_items.sort(key=lambda x: x.get('timestamp', 0), reverse=True)

            # Remove duplicates by link
            seen_links = set()
            unique_items = []
            for item in cat_items:
                link = item.get('link', '')
                if link and link in seen_links:
                    continue
                if link:
                    seen_links.add(link)
                unique_items.append(item)
                # Remove timestamp helper field
                if 'timestamp' in item:
                    del item['timestamp']

            if unique_items:
                result[cat] = unique_items
                total_items += len(unique_items)
                print(f"  {cat}: {len(unique_items)} items")

    output = {
        'lastUpdated': datetime.now(timezone.utc).isoformat(),
        'categories': result
    }

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False)

    print(f"\nTotal: {total_items} items written to data.json")

if __name__ == '__main__':
    asyncio.run(main())
