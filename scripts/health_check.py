#!/usr/bin/env python3
"""每周健康检查精选源，更新 status.json。"""
import json, urllib.request, ssl
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

ROOT = Path(__file__).resolve().parent.parent
curated = json.loads((ROOT / "config" / "curated.json").read_text(encoding="utf-8"))

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def check_url(url, timeout=10):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "okhttp/3.12.0"})
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.status == 200
    except Exception:
        return False

def check_site(s):
    """返回 (key, ok, checked_url)"""
    # CMS: 检查 api 根
    if s.get("type") in (0, 1):
        api = s.get("api", "")
        # 取 ?ac=videolist 测试
        test_url = api.rstrip("/") + "/?ac=videolist&pg=1" if "?" not in api else api
        ok = check_url(test_url)
        return (s["key"], ok, test_url)
    # 爬虫: 检查 ext 或 jar
    for field in ("ext", "jar"):
        url = s.get(field, "")
        if url and url.startswith("http"):
            # jar 可能带 ;md5;xxx 后缀
            clean = url.split(";")[0]
            ok = check_url(clean)
            return (s["key"], ok, clean)
    return (s["key"], None, "no-url")

sites = []
for s in curated.get("cms", []):
    sites.append({"key": s["key"], "name": s["name"], "type": s["type"], "api": s["api"]})
for s in curated.get("spiders", []):
    sites.append(s)

results = []
with ThreadPoolExecutor(max_workers=10) as ex:
    results = list(ex.map(check_site, sites))

alive = sum(1 for _, ok, _ in results if ok)
dead = [(k, u) for k, ok, u in results if ok is False]
unknown = [k for k, ok, _ in results if ok is None]

status = {
    "mode": "curated",
    "site_count": len(sites),
    "alive": alive,
    "dead": [{"key": k, "url": u} for k, u in dead],
    "unknown": unknown,
}
(ROOT / "output" / "status.json").write_text(
    json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")

print(f"健康检查：{alive}/{len(sites)} 存活，{len(dead)} 失效，{len(unknown)} 未检测")
for k, u in dead:
    print(f"  死：{k} ({u[:60]})")
