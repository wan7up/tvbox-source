#!/usr/bin/env python3
"""从精选源清单生成 TVBox 配置。替代旧的聚合流程。"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
curated = json.loads((ROOT / "config" / "curated.json").read_text(encoding="utf-8"))

sites = []
for s in curated.get("cms", []):
    sites.append({
        "key": s["key"],
        "name": s["name"],
        "type": s["type"],
        "api": s["api"],
        "searchable": 1,
        "quickSearch": 1,
        "filterable": 1,
    })

for s in curated.get("spiders", []):
    site = {
        "key": s["key"],
        "name": s["name"],
        "type": s["type"],
        "api": s["api"],
        "searchable": s.get("searchable", 1),
        "quickSearch": s.get("quickSearch", 1),
        "filterable": s.get("filterable", 0),
    }
    if "ext" in s:
        site["ext"] = s["ext"]
    if "jar" in s:
        site["jar"] = s["jar"]
    if "changeable" in s:
        site["changeable"] = s["changeable"]
    sites.append(site)

out = {
    "sites": sites,
    "parses": [],
    "lives": [],
    "flags": ["youku", "qq", "iqiyi", "qiyi", "letv", "sohu", "tudou", "pptv",
              "mgtv", "wasu", "bilibili", "renrenmi"],
}

out_dir = ROOT / "output"
out_dir.mkdir(exist_ok=True)
(out_dir / "tvbox.json").write_text(
    json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
(out_dir / "tvbox-curated.json").write_text(
    json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

# 状态文件
status = {
    "mode": "curated",
    "site_count": len(sites),
    "cms_count": len(curated.get("cms", [])),
    "spider_count": len(curated.get("spiders", [])),
}
(out_dir / "status.json").write_text(
    json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")

print(f"已生成 {len(sites)} 个精选源（CMS {status['cms_count']} + 爬虫 {status['spider_count']}）")
