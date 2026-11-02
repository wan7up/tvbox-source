#!/usr/bin/env python3
"""从 douer index.js 提取「直」源清单，用于每周对比发现新源。"""
import re, json, sys
from pathlib import Path

p = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("miraplay/index.js")
src = p.read_text(encoding="utf-8", errors="ignore")

# 匹配 meta:{key:"...",name:"\u300C\u76F4\u300D..."}（Unicode 转义的「直」/「盘」）
# \u300C = 「, \u300D = 」, \u76F4 = 直, \u76D8 = 盘
pattern = re.compile(r'meta:\{key:"([^"]+)",name:"((?:\\u300C\\u76F4\\u300D|\\u300C\\u76D8\\u300D)[^"]*)"')
matches = pattern.findall(src)

def decode(s):
    return s.encode().decode('unicode_escape')

zhi = sorted(set((k, decode(n)) for k, n in matches if '\\u76F4' in n))
pan = sorted(set((k, decode(n)) for k, n in matches if '\\u76D8' in n))

out = {
    "zhi": [{"key": k, "name": n} for k, n in zhi],
    "pan": [{"key": k, "name": n} for k, n in pan],
    "zhi_count": len(zhi),
    "pan_count": len(pan),
}

out_path = Path("miraplay/douer-sources.json")
out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"直 {len(zhi)} 个，盘 {len(pan)} 个 → {out_path}")
for k, n in zhi:
    print(f"  {n} ({k})")
