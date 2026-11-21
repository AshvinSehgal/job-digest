#!/usr/bin/env python3
"""Scrape job career pages and boards with crawl4ai (free, runs locally, no API key).

Setup (once):
    pip install -U crawl4ai
    crawl4ai-setup

Run:
    python scrape_jobs.py                      # uses the built-in source list
    python scrape_jobs.py --sources my.txt     # one URL per line, # for comments
    python scrape_jobs.py --out results --concurrency 5

Output (in --out, default ./out/<YYYY-MM-DD>/):
    digest.md        job-looking links grouped by site (read this first)
    pages/<site>.md  full markdown of every page
    manifest.json    status of every URL (ok or error message)
"""
import argparse
import asyncio
import json
import re
import sys
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig

# Edit freely. Add or remove lines. JavaScript boards (Getro, Consider, Wellfound,
# Workday) are rendered by a real browser here, which is the main reason to use crawl4ai.
DEFAULT_SOURCES = """
# Big tech and large companies
https://www.google.com/about/careers/applications/jobs/results/?q=software%20engineer&target_level=EARLY
https://careers.microsoft.com/v2/global/en/search?q=software%20engineer&exp=Students%20and%20graduates
https://jobs.apple.com/en-us/search?search=software%20engineer
https://www.amazon.jobs/en/search?base_query=software+engineer&category[]=software-development
https://www.metacareers.com/jobs
https://nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite?q=new%20grad
https://careers.salesforce.com/en/jobs/?search=software+engineer
https://careers.adobe.com/us/en/search-results?keywords=software%20engineer
https://jobs.cisco.com/jobs/SearchJobs/software%20engineer
https://careers.qualcomm.com/careers?query=software%20engineer
https://www.databricks.com/company/careers/open-positions
https://careers.snowflake.com/us/en/search-results?keywords=software%20engineer
https://www.palantir.com/careers/
https://stripe.com/jobs/search?query=software%20engineer
https://www.uber.com/us/en/careers/list/?query=software%20engineer
https://careers.airbnb.com/positions/
https://jobs.netflix.com/search?q=software%20engineer
https://careers.linkedin.com/
https://jobs.bytedance.com/en/position?keywords=software%20engineer
https://lifeattiktok.com/search?keyword=software%20engineer

# AI labs and AI companies
https://openai.com/careers/search/
https://www.anthropic.com/jobs
https://deepmind.google/about/careers/
https://jobs.lever.co/mistral
https://cohere.com/careers
https://job-boards.greenhouse.io/xai
https://apply.workable.com/huggingface/
https://jobs.ashbyhq.com/langchain
https://jobs.ashbyhq.com/openai
https://jobs.ashbyhq.com/chalk
https://jobs.ashbyhq.com/harvey
https://jobs.ashbyhq.com/wispr-flow
https://jobs.ashbyhq.com/mirendil
https://jobs.ashbyhq.com/odysseyml
https://jobs.ashbyhq.com/ande
https://jobs.lever.co/palantir
https://job-boards.greenhouse.io/graphonai

# VC and startup boards
https://www.ycombinator.com/jobs/role/software-engineer
https://www.workatastartup.com/jobs
https://jobs.a16z.com/jobs
https://jobs.sequoiacap.com/jobs
https://jobs.khoslaventures.com/jobs
https://jobs.signalfire.com/jobs
https://wellfound.com/role/software-engineer
https://startup.jobs/?q=software+engineer
https://builtin.com/jobs/dev-engineering

# Aggregators
https://simplify.jobs/jobs?query=new%20grad%20software%20engineer
https://hiring.cafe/
https://jobright.ai/jobs/search?value=software%20engineer%20new%20grad
https://github.com/SimplifyJobs/New-Grad-Positions
https://github.com/vanshb03/New-Grad-2027
https://www.ai-jobs.net/
https://news.ycombinator.com/jobs

# India (large companies, startups, boards)
https://www.naukri.com/software-engineer-jobs-in-bangalore
https://www.naukri.com/software-engineer-jobs-in-hyderabad
https://www.naukri.com/software-engineer-jobs-in-gurgaon
https://www.naukri.com/software-engineer-jobs-in-noida
https://www.instahyre.com/search-jobs/
https://cutshort.io/jobs
https://hirist.tech/
https://www.foundit.in/srp/results?query=software+engineer
https://in.indeed.com/jobs?q=software+engineer&l=Bengaluru
https://wellfound.com/location/india
https://careers.razorpay.com/
https://www.postman.com/company/careers/
https://careers.swiggy.com/
https://www.flipkartcareers.com/
https://careers.cred.club/
https://www.zepto.com/careers
https://careers.phonepe.com/
https://jobs.lever.co/meesho
https://www.freshworks.com/company/careers/
https://careers.sarvam.ai/

# Europe
https://relocate.me/search?query=software+engineer
https://landing.jobs/jobs?q=software+engineer
https://berlinstartupjobs.com/engineering/
https://swissdevjobs.ch/jobs
https://www.eurotechjobs.com/
""".strip()

# A link text must match one of these to land in the digest...
INCLUDE = re.compile(
    r"software|engineer|developer|sde|swe|new grad|graduate|early career|university|"
    r"machine learning|\bml\b|\bai\b|inference|llm|full[- ]?stack|backend|frontend|"
    r"deployed|member of technical staff|\bmts\b|platform|infrastructure",
    re.I,
)
# ...and must not match any of these.
EXCLUDE = re.compile(
    r"senior|\bsr\b|staff|principal|director|manager|head of|vp\b|intern(ship)?\b|"
    r"recruit|sales|marketing|legal|counsel|finance|account exec|designer|support",
    re.I,
)


def load_sources(path):
    text = Path(path).read_text() if path else DEFAULT_SOURCES
    urls = []
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            urls.append(line)
    return list(dict.fromkeys(urls))


def slug(url):
    p = urlparse(url)
    s = re.sub(r"[^a-zA-Z0-9]+", "-", f"{p.netloc}{p.path}").strip("-")
    return (s or "page")[:90]


def job_links(result):
    """Return (text, href) pairs for links that look like job titles."""
    found, seen = [], set()
    links = getattr(result, "links", None) or {}
    for group in ("internal", "external"):
        for item in links.get(group, []) or []:
            href = (item.get("href") or "").strip()
            text = re.sub(r"\s+", " ", (item.get("text") or "")).strip()
            if not href or not text or len(text) > 140 or href in seen:
                continue
            if INCLUDE.search(text) and not EXCLUDE.search(text):
                seen.add(href)
                found.append((text, href))
    return found


class JinaResult:
    """Minimal stand-in for a crawl4ai result, built from Jina Reader markdown."""

    def __init__(self, md):
        self.success, self.markdown, self.error_message = True, md, None
        pairs = re.findall(r"\[([^\]]{2,140})\]\((https?://[^)\s]+)\)", md)
        self.links = {"internal": [{"text": t, "href": h} for t, h in pairs]}


def jina_fetch(url, timeout=45):
    import urllib.request
    try:
        req = urllib.request.Request("https://r.jina.ai/" + url,
                                     headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            md = r.read().decode("utf-8", "ignore")
        return md if len(md.strip()) > 200 else None
    except Exception:
        return None


def publish(repo, out, manifest, ok, total):
    """Copy the digest into a git clone and push it, so the cloud job search can read it."""
    import shutil
    import subprocess

    header = (f"<!-- scraped {date.today().isoformat()} 03:30 local, {ok}/{total} pages ok -->\n")
    failed = [m for m in manifest if not m["ok"]]
    footer = "\n\n## Pages that failed\n" + "\n".join(
        f"- {m['url']} ({m['error']})" for m in failed) if failed else ""
    text = header + (out / "digest.md").read_text() + footer
    (repo / "latest_digest.md").write_text(text)
    shutil.copy(out / "manifest.json", repo / "latest_manifest.json")
    optional = {"pull"}  # pull fails harmlessly on a brand-new empty repo
    for cmd in (["git", "add", "latest_digest.md", "latest_manifest.json"],
                ["git", "commit", "-m", f"digest {date.today().isoformat()}"],
                ["git", "pull", "--rebase", "--autostash"],
                ["git", "push", "-u", "origin", "HEAD"]):
        r = subprocess.run(cmd, cwd=repo, capture_output=True, text=True)
        if cmd[1] in optional:
            continue
        if r.returncode and "nothing to commit" not in (r.stdout + r.stderr):
            print(f"git step failed: {' '.join(cmd)}\n{r.stderr.strip()}", file=sys.stderr)
            return
    print(f"Published digest to {repo}")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", help="text file with one URL per line")
    ap.add_argument("--out", default="out", help="output folder")
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--timeout", type=int, default=60, help="seconds per page")
    ap.add_argument("--wait", type=float, default=3.0,
                    help="seconds to let JavaScript finish before reading a page")
    ap.add_argument("--publish-dir",
                    help="a local git clone; digest is copied there as latest_digest.md "
                         "and pushed (git must already be able to push without a prompt)")
    args = ap.parse_args()

    urls = load_sources(args.sources)
    out = Path(args.out) / date.today().isoformat()
    (out / "pages").mkdir(parents=True, exist_ok=True)

    browser = BrowserConfig(headless=True, verbose=False)
    run = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,          # always fetch fresh pages
        page_timeout=args.timeout * 1000,
        delay_before_return_html=args.wait,   # JS boards need a moment to render
        wait_until="domcontentloaded",
        scan_full_page=True,                  # scroll to trigger lazy-loaded lists
        remove_overlay_elements=True,         # try to close cookie and popup overlays
    )

    manifest, digest = [], [f"# Job scrape digest {date.today().isoformat()}\n"]
    sem = asyncio.Semaphore(args.concurrency)

    async with AsyncWebCrawler(config=browser) as crawler:

        async def fetch(url):
            async with sem:
                err = None
                for attempt in (1, 2):  # one retry with a longer JavaScript wait
                    try:
                        cfg = run if attempt == 1 else run.clone(
                            delay_before_return_html=args.wait * 2.5)
                        res = await crawler.arun(url=url, config=cfg)
                        if res.success and str(res.markdown or "").strip():
                            return url, res, None
                        err = (res.error_message or "empty page").strip()
                    except Exception as e:  # network error, timeout, browser crash
                        err = f"{type(e).__name__}: {e}"
                # Last resort: Jina Reader (free, no key). Works for some bot-blocked pages.
                md = await asyncio.to_thread(jina_fetch, url)
                if md:
                    return url, JinaResult(md), None
                return url, None, err

        tasks = [asyncio.create_task(fetch(u)) for u in urls]
        for i, task in enumerate(asyncio.as_completed(tasks), 1):
            url, res, err = await task
            if err or res is None or not res.success:
                msg = err or (res.error_message if res else "unknown error")
                msg = (msg or "unknown error").strip().splitlines()[0][:200]
                manifest.append({"url": url, "ok": False, "error": msg})
                print(f"[{i}/{len(urls)}] FAIL {url}  ({msg})", file=sys.stderr)
                continue

            md = str(res.markdown or "")
            (out / "pages" / f"{slug(url)}.md").write_text(f"<!-- {url} -->\n{md}")
            links = job_links(res)
            manifest.append({"url": url, "ok": True, "chars": len(md),
                             "job_links": len(links)})
            print(f"[{i}/{len(urls)}] ok   {url}  ({len(md)} chars, {len(links)} job links)")

            digest.append(f"\n## {urlparse(url).netloc}\n{url}\n")
            if links:
                digest.extend(f"- [{t}]({h})" for t, h in links[:80])
            else:
                digest.append("- no job-looking links found (open pages/ file to check)")

    (out / "digest.md").write_text("\n".join(digest) + "\n")
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    ok = sum(1 for m in manifest if m["ok"])
    print(f"\nDone: {ok}/{len(urls)} pages fetched. See {out}/digest.md")
    if args.publish_dir:
        publish(Path(args.publish_dir), out, manifest, ok, len(urls))
    if ok < len(urls):
        print("Failed pages are listed in manifest.json (login walls and bot blocks are normal).")


if __name__ == "__main__":
    asyncio.run(main())
