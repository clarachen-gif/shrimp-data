"""
每日 VC 博客抓取
- RSS 全文 (已验证可用的源)
- 网页抓取 (其他重要 VC)
输出: data/vc/YYYY-MM-DD.md
"""

import requests
import feedparser
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import os

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# ── 已验证可用的 RSS 源 ──────────────────────────────────
RSS_FEEDS = {
    "Sequoia Capital": "https://www.sequoiacap.com/feed/",
    "Elad Gil": "https://blog.eladgil.com/feed",
    "Fred Wilson (AVC)": "https://avc.com/feed/",
}

# ── 需要网页抓取的 VC 博客 ────────────────────────────────
BLOG_PAGES = {
    "a16z": "https://a16z.com/blog/",
    "Greylock": "https://greylock.com/greymatter/",
    "Bessemer": "https://www.bvp.com/atlas",
    "Insight Partners": "https://www.insightpartners.com/ideas/",
    "Khosla Ventures": "https://www.khoslaventures.com/",
    "Index Ventures": "https://www.indexventures.com/perspectives/",
    "First Round Review": "https://review.firstround.com/",
    "Lightspeed": "https://lsvp.com/insights/",
    "General Catalyst": "https://www.generalcatalyst.com/perspectives",
    "Kleiner Perkins": "https://www.kleinerperkins.com/perspectives/",
}

# ── 对冲基金/投行研究页面 ─────────────────────────────────
RESEARCH_PAGES = {
    "Bridgewater": "https://www.bridgewater.com/research-and-insights",
    "AQR": "https://www.aqr.com/insights/research",
    "Man Group": "https://www.man.com/insights",
    "Oaktree (Howard Marks)": "https://www.oaktreecapital.com/insights/memos",
    "Goldman Sachs": "https://www.goldmansachs.com/insights/",
    "Morgan Stanley": "https://www.morganstanley.com/ideas",
    "JPMorgan": "https://www.jpmorgan.com/insights",
}


def fetch_rss(name, url):
    """抓取 RSS feed, 返回最近 7 天的文章"""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        feed = feedparser.parse(resp.content)
        cutoff = datetime.now() - timedelta(days=7)
        articles = []
        for entry in feed.entries[:10]:
            pub = entry.get("published_parsed") or entry.get("updated_parsed")
            if pub:
                pub_dt = datetime(*pub[:6])
                if pub_dt < cutoff:
                    continue
            # 提取正文
            content = ""
            if hasattr(entry, "content"):
                content = entry.content[0].get("value", "")
            elif hasattr(entry, "summary"):
                content = entry.summary
            # 清理 HTML
            if content:
                soup = BeautifulSoup(content, "html.parser")
                content = soup.get_text(separator="\n", strip=True)[:3000]
            articles.append({
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "date": entry.get("published", entry.get("updated", "")),
                "content": content,
                "author": entry.get("author", ""),
            })
        return articles
    except Exception as e:
        print(f"  [ERROR] {name}: {e}")
        return []


def fetch_page_links(name, url):
    """抓取博客页面, 提取最近的文章标题和链接"""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            print(f"  [WARN] {name}: HTTP {resp.status_code}")
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        articles = []
        # 通用策略: 找所有链接, 过滤出像文章的
        for a in soup.find_all("a", href=True):
            title = a.get_text(strip=True)
            href = a["href"]
            if not title or len(title) < 15 or len(title) > 200:
                continue
            if any(skip in href.lower() for skip in
                   ["twitter", "linkedin", "facebook", "mailto:", "#", "javascript:"]):
                continue
            if not href.startswith("http"):
                href = url.rstrip("/") + "/" + href.lstrip("/")
            articles.append({"title": title, "link": href})
        # 去重 (按 title)
        seen = set()
        unique = []
        for a in articles:
            if a["title"] not in seen:
                seen.add(a["title"])
                unique.append(a)
        return unique[:15]
    except Exception as e:
        print(f"  [ERROR] {name}: {e}")
        return []


def fetch_article_content(url):
    """尝试抓取单篇文章正文"""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")
        # 尝试常见的文章容器
        for selector in ["article", "[class*='post-content']",
                         "[class*='article-body']", "[class*='entry-content']",
                         "main", "[role='main']"]:
            el = soup.select_one(selector)
            if el:
                text = el.get_text(separator="\n", strip=True)
                if len(text) > 200:
                    return text[:5000]
        return ""
    except Exception:
        return ""


def main():
    today = datetime.now().strftime("%Y-%m-%d")
    output_lines = [f"# VC & Research Daily Fetch — {today}\n"]

    # ── RSS Feeds ──
    output_lines.append("## RSS Feeds (全文)\n")
    for name, url in RSS_FEEDS.items():
        print(f"[RSS] {name}...")
        articles = fetch_rss(name, url)
        output_lines.append(f"### {name}")
        if not articles:
            output_lines.append("_今日无新文章_\n")
            continue
        for a in articles:
            output_lines.append(f"\n**{a['title']}**")
            output_lines.append(f"- Link: {a['link']}")
            output_lines.append(f"- Date: {a['date']}")
            if a.get("author"):
                output_lines.append(f"- Author: {a['author']}")
            if a.get("content"):
                output_lines.append(f"\n{a['content'][:2000]}")
            output_lines.append("")

    # ── VC Blog Pages (标题+链接+前3篇正文) ──
    output_lines.append("\n## VC Blog Pages\n")
    for name, url in BLOG_PAGES.items():
        print(f"[WEB] {name}...")
        links = fetch_page_links(name, url)
        output_lines.append(f"### {name}")
        if not links:
            output_lines.append("_无法获取或无内容_\n")
            continue
        for i, a in enumerate(links[:8]):
            output_lines.append(f"- [{a['title']}]({a['link']})")
            # 前 3 篇尝试抓正文
            if i < 3 and a.get('link', '').startswith('http'):
                print(f"  [CONTENT] {a['title'][:40]}...")
                content = fetch_article_content(a['link'])
                if content:
                    output_lines.append(f"\n> **摘要:** {content[:1500]}\n")
        output_lines.append("")

    # ── Research Pages (标题+链接+前2篇正文) ──
    output_lines.append("\n## Hedge Fund & Investment Bank Research\n")
    for name, url in RESEARCH_PAGES.items():
        print(f"[RESEARCH] {name}...")
        links = fetch_page_links(name, url)
        output_lines.append(f"### {name}")
        if not links:
            output_lines.append("_无法获取或无内容_\n")
            continue
        for i, a in enumerate(links[:8]):
            output_lines.append(f"- [{a['title']}]({a['link']})")
            if i < 2 and a.get('link', '').startswith('http'):
                print(f"  [CONTENT] {a['title'][:40]}...")
                content = fetch_article_content(a['link'])
                if content:
                    output_lines.append(f"\n> **摘要:** {content[:1500]}\n")
        output_lines.append("")

    # ── 写入文件 ──
    os.makedirs("data/vc", exist_ok=True)
    output_path = f"data/vc/{today}.md"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(output_lines))
    print(f"\n✅ Written to {output_path}")

    # 同时更新 latest.md 方便 agent 读取
    with open("data/vc/latest.md", "w", encoding="utf-8") as f:
        f.write("\n".join(output_lines))
    print("✅ Updated data/vc/latest.md")


if __name__ == "__main__":
    main()
