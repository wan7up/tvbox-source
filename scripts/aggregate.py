#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TVBox 源聚合器主脚本
拉取上游 -> 剥离注释与清洗 -> 相对路径与 spider 下发 -> 站点级分层健康检查 -> 指纹去重聚合 -> 状态与 README 更新
"""

import concurrent.futures
import copy
import datetime
import json
import logging
import os
import re
import sys
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("tvbox-aggregator")

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CONFIG_PATH = os.path.join(ROOT_DIR, "config", "upstreams.json")
OUTPUT_DIR = os.path.join(ROOT_DIR, "output")
README_PATH = os.path.join(ROOT_DIR, "README.md")
STATUS_PATH = os.path.join(OUTPUT_DIR, "status.json")

DEFAULT_UA = "okhttp/3.12.0"
UPSTREAM_TIMEOUT = 20
UPSTREAM_RETRIES = 2
SITE_CHECK_TIMEOUT = 8
SITE_CHECK_CONCURRENCY = 20
MAX_CONSECUTIVE_FAILURES = 3

README_START_MARKER = "<!-- STATUS_TABLE_START -->"
README_END_MARKER = "<!-- STATUS_TABLE_END -->"


# ---------- URL 处理 ----------

def to_punycode_url(url: str) -> str:
    """中文域名转 punycode，路径非 ASCII 做 quote。"""
    url = url.strip()
    if not url:
        return url
    try:
        parsed = urllib.parse.urlsplit(url)
        if not parsed.scheme or not parsed.netloc:
            return url
        netloc = parsed.netloc
        userinfo = ""
        if "@" in netloc:
            userinfo, netloc = netloc.rsplit("@", 1)
            userinfo += "@"
        host, port = netloc, ""
        if ":" in netloc and not netloc.endswith("]"):
            host, port = netloc.rsplit(":", 1)
            port = ":" + port
        ascii_host = host.encode("idna").decode("ascii")
        new_netloc = f"{userinfo}{ascii_host}{port}"
        safe_path = urllib.parse.quote(urllib.parse.unquote(parsed.path), safe="/:@!$&'()*+,;=-._~")
        safe_query = urllib.parse.quote(urllib.parse.unquote(parsed.query), safe="=/:&?@!$'()*+,;-._~")
        return urllib.parse.urlunsplit((parsed.scheme, new_netloc, safe_path, safe_query, parsed.fragment))
    except Exception as exc:
        logger.debug("Punycode 转换回退原 URL (%s): %s", url, exc)
        return url


def resolve_relative(base_url: str, ref: str) -> str:
    """把上游配置中的相对路径转为绝对 URL。"""
    if not ref or not isinstance(ref, str):
        return ref
    ref = ref.strip()
    if not ref or "://" in ref or ref.startswith("data:"):
        return ref
    # 去掉 ;md5;xxx 后缀再判断
    core = ref.split(";")[0]
    if "://" in core:
        return ref
    try:
        abs_core = urllib.parse.urljoin(base_url, core)
        # 把 md5 后缀接回去
        suffix = ref[len(core):]
        return abs_core + suffix
    except Exception:
        return ref


# ---------- 网络抓取 ----------

def fetch_url_with_retry(
    url: str,
    timeout: int = UPSTREAM_TIMEOUT,
    retries: int = UPSTREAM_RETRIES,
    headers: Optional[Dict[str, str]] = None,
) -> Tuple[Optional[str], Optional[str], float]:
    """拉取 URL。返回 (文本, 最终URL, 耗时)。4xx/5xx 不重试，瞬态错误退避重试。"""
    target_url = to_punycode_url(url)
    req_headers = {"User-Agent": DEFAULT_UA, "Accept": "*/*"}
    if headers:
        req_headers.update(headers)

    for attempt in range(retries + 1):
        start_ts = time.monotonic()
        try:
            resp = requests.get(target_url, headers=req_headers, timeout=timeout, allow_redirects=True)
            elapsed = round(time.monotonic() - start_ts, 3)
            if 400 <= resp.status_code < 600:
                logger.warning("HTTP %d 不重试: %s", resp.status_code, url)
                return None, None, elapsed
            # 编码处理
            if resp.encoding == "ISO-8859-1":
                resp.encoding = resp.apparent_encoding or "utf-8"
            text = (resp.text or "").strip()
            if not text:
                raise ValueError("Empty response body")
            return text, resp.url, elapsed
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, ValueError) as exc:
            elapsed = round(time.monotonic() - start_ts, 3)
            if attempt < retries:
                backoff = 1.5 * (2 ** attempt)
                logger.warning("瞬态错误 %s [%d/%d]，%.1fs 后重试: %s",
                               exc.__class__.__name__, attempt + 1, retries + 1, backoff, url)
                time.sleep(backoff)
            else:
                logger.warning("拉取最终失败 (%s): %s", exc, url)
                return None, None, elapsed
        except Exception as exc:
            elapsed = round(time.monotonic() - start_ts, 3)
            logger.warning("未预期异常 (%s): %s", exc, url)
            return None, None, elapsed
    return None, None, 0.0


# ---------- JSON 清洗 ----------

def strip_json_comments(text: str) -> str:
    """剥离 // 和 /* */ 注释，但不误删字符串内的 //（如 http://）。"""
    result = []
    i, n = 0, len(text)
    in_str = False
    esc = False
    while i < n:
        c = text[i]
        if in_str:
            result.append(c)
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
            i += 1
            continue
        # 字符串外
        if c == '"':
            in_str = True
            result.append(c)
            i += 1
        elif c == "/" and i + 1 < n and text[i + 1] == "/":
            # 行注释，跳到行尾
            while i < n and text[i] != "\n":
                i += 1
        elif c == "/" and i + 1 < n and text[i + 1] == "*":
            # 块注释
            i += 2
            while i + 1 < n and not (text[i] == "*" and text[i + 1] == "/"):
                i += 1
            i += 2
        else:
            result.append(c)
            i += 1
    return "".join(result)


def escape_ctrl_in_strings(text: str) -> str:
    """把 JSON 字符串内的字面控制字符转义（字符串外保留换行结构）。"""
    result = []
    i, n = 0, len(text)
    in_str = False
    esc = False
    while i < n:
        c = text[i]
        if in_str:
            if esc:
                result.append(c)
                esc = False
            elif c == "\\":
                result.append(c)
                esc = True
            elif c == '"':
                result.append(c)
                in_str = False
            elif c == "\n":
                result.append("\\n")
            elif c == "\r":
                result.append("\\r")
            elif c == "\t":
                result.append("\\t")
            elif ord(c) < 0x20 or ord(c) == 0x7f:
                result.append(f"\\u{ord(c):04x}")
            else:
                result.append(c)
        else:
            result.append(c)
            if c == '"':
                in_str = True
        i += 1
    return "".join(result)


def parse_tvbox_json(text: str) -> Optional[Dict[str, Any]]:
    """解析 TVBox 配置 JSON，兼容 BOM、注释、尾随逗号、控制字符、多余尾巴。"""
    # 去 BOM
    text = text.lstrip("\ufeff")
    cleaned = strip_json_comments(text)
    # 去尾随逗号
    cleaned = re.sub(r",\s*([}\]])", r"\1", cleaned)
    # 字符串内控制字符转义（含字符串内的字面换行）
    cleaned = escape_ctrl_in_strings(cleaned)
    # 字符串外只留结构字符，去掉杂散控制字符
    cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass
    # 兜底：Extra data 时只取第一个完整 JSON 对象
    try:
        obj, _ = json.JSONDecoder().raw_decode(cleaned.strip())
        if isinstance(obj, dict):
            logger.debug("用了 raw_decode 兜底解析")
            return obj
    except json.JSONDecodeError as exc:
        logger.warning("JSON 解析失败: %s", exc)
    return None


# ---------- 上游处理 ----------

def process_upstream(up: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """抓取并处理单个上游。返回 (配置dict或None, 状态信息)。"""
    uid = up["id"]
    name = up.get("name", uid)
    status = {"id": uid, "name": name, "ok": False, "latency": None, "sites": 0, "error": None, "url": None}

    for url in up.get("urls", []):
        text, final_url, elapsed = fetch_url_with_retry(url)
        if text is None:
            continue
        # 多仓订阅格式：{"urls": [...]}
        # 单仓格式：{"sites": [...], ...}
        cfg = parse_tvbox_json(text)
        if cfg is None:
            status["error"] = "JSON解析失败"
            continue
        # 多仓订阅不含 sites，视为特殊类型
        if "urls" in cfg and "sites" not in cfg:
            status["ok"] = True
            status["latency"] = elapsed
            status["url"] = final_url or url
            status["type"] = "multi"
            return cfg, status

        sites = cfg.get("sites", [])
        if not isinstance(sites, list) or not sites:
            status["error"] = "sites为空"
            continue

        base = final_url or url
        # 相对路径归一化 + spider 下发
        top_spider = cfg.get("spider", "")
        if top_spider:
            top_spider = resolve_relative(base, top_spider)

        for site in sites:
            if not isinstance(site, dict):
                continue
            for field in ("api", "ext"):
                if site.get(field):
                    site[field] = resolve_relative(base, site[field])
            # spider 下发：type:3 且无独立 jar 的站点继承上游顶层 spider
            if site.get("type") == 3 and not site.get("jar") and top_spider:
                site["jar"] = top_spider
        if top_spider:
            cfg["spider"] = top_spider

        status["ok"] = True
        status["latency"] = elapsed
        status["sites"] = len(sites)
        status["url"] = final_url or url
        status["type"] = "single"
        return cfg, status

    if not status["error"]:
        status["error"] = "所有地址均失败"
    return None, status


# ---------- 去重指纹 ----------

def normalize_url(u: str) -> str:
    """归一化 URL 用于指纹：去尾斜杠、小写 host。"""
    if not u or not isinstance(u, str):
        return ""
    u = u.strip().split(";")[0]  # 去 ;md5 后缀
    try:
        p = urllib.parse.urlsplit(u)
        host = p.netloc.lower()
        path = p.path.rstrip("/") or "/"
        return urllib.parse.urlunsplit((p.scheme.lower(), host, path, p.query, ""))
    except Exception:
        return u.strip().lower()


def site_fingerprint(site: Dict[str, Any]) -> str:
    """站点去重指纹。type 0/1 用 api，type 3 用 (api, ext)。"""
    stype = site.get("type")
    api = normalize_url(site.get("api", ""))
    if stype == 3:
        ext = site.get("ext", "")
        if isinstance(ext, dict):
            ext = json.dumps(ext, sort_keys=True)
        ext = normalize_url(str(ext))
        return f"t3:{api}|{ext}"
    return f"t{stype}:{api}"


# ---------- 站点健康检查 ----------

def check_site_health(site: Dict[str, Any]) -> Tuple[bool, float]:
    """分层健康检查。返回 (健康, 耗时)。"""
    stype = site.get("type")
    start = time.monotonic()
    try:
        if stype in (0, 1):
            # CMS 接口：请求 ?ac=list 验证返回
            api = site.get("api", "")
            if not api:
                return False, 0
            sep = "&" if "?" in api else "?"
            test_url = f"{api}{sep}ac=list"
            text, _, _ = fetch_url_with_retry(test_url, timeout=SITE_CHECK_TIMEOUT, retries=0)
            elapsed = round(time.monotonic() - start, 3)
            if not text:
                return False, elapsed
            # 简单验证：包含 class 或 list 关键字
            low = text[:2000].lower()
            if "class" in low or "list" in low or "vod" in low:
                return True, elapsed
            return False, elapsed
        elif stype == 3:
            # 爬虫源：检查 ext 和 jar 可达性
            ext = site.get("ext", "")
            jar = site.get("jar", "")
            ok = False
            for target in (ext, jar):
                if not target or not isinstance(target, str):
                    continue
                t = target.split(";")[0].strip()
                if not t.startswith("http"):
                    continue
                try:
                    r = requests.head(to_punycode_url(t), headers={"User-Agent": DEFAULT_UA},
                                      timeout=SITE_CHECK_TIMEOUT, allow_redirects=True)
                    if r.status_code < 400:
                        ok = True
                        break
                except Exception:
                    continue
                # HEAD 不行试 GET（部分服务器禁 HEAD）
                try:
                    r = requests.get(to_punycode_url(t), headers={"User-Agent": DEFAULT_UA},
                                     timeout=SITE_CHECK_TIMEOUT, allow_redirects=True, stream=True)
                    if r.status_code < 400:
                        ok = True
                        r.close()
                        break
                    r.close()
                except Exception:
                    continue
            elapsed = round(time.monotonic() - start, 3)
            return ok, elapsed
        else:
            # 未知类型：只做 api 连通性
            api = site.get("api", "")
            if not api or not isinstance(api, str) or not api.startswith("http"):
                return False, 0
            text, _, _ = fetch_url_with_retry(api.split(";")[0], timeout=SITE_CHECK_TIMEOUT, retries=0)
            elapsed = round(time.monotonic() - start, 3)
            return bool(text), elapsed
    except Exception as exc:
        logger.debug("站点检查异常 %s: %s", site.get("key"), exc)
        return False, round(time.monotonic() - start, 3)


def batch_health_check(sites: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """并发健康检查，返回健康的站点（按耗时排序）。"""
    healthy = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=SITE_CHECK_CONCURRENCY) as pool:
        future_map = {pool.submit(check_site_health, s): s for s in sites}
        for fut in concurrent.futures.as_completed(future_map):
            site = future_map[fut]
            try:
                ok, latency = fut.result()
            except Exception:
                ok, latency = False, 0
            if ok:
                site = copy.deepcopy(site)
                site["_latency"] = latency
                healthy.append(site)
            else:
                logger.debug("站点不健康已过滤: %s", site.get("key") or site.get("name"))
    healthy.sort(key=lambda s: s.get("_latency", 999))
    # 去掉内部字段
    for s in healthy:
        s.pop("_latency", None)
    return healthy


# ---------- 聚合 ----------

def merge_sources(results: List[Tuple[str, Dict[str, Any]]]) -> Dict[str, Any]:
    """多上游合并去重。results 为 [(上游id, 配置)]，顺序即优先级。"""
    merged_sites: List[Dict[str, Any]] = []
    seen_fp = set()
    merged_parses: List[Dict[str, Any]] = []
    seen_parse_urls = set()
    merged_lives: List[Dict[str, Any]] = []
    seen_live_urls = set()

    for uid, cfg in results:
        for site in cfg.get("sites", []):
            if not isinstance(site, dict):
                continue
            fp = site_fingerprint(site)
            if fp in seen_fp:
                continue
            seen_fp.add(fp)
            site = copy.deepcopy(site)
            # key 撞车加后缀
            existing_keys = {s.get("key") for s in merged_sites}
            if site.get("key") in existing_keys:
                site["key"] = f"{site['key']}_{uid}"
            merged_sites.append(site)

        for p in cfg.get("parses", []):
            if not isinstance(p, dict):
                continue
            u = normalize_url(p.get("url", ""))
            if u and u not in seen_parse_urls:
                seen_parse_urls.add(u)
                merged_parses.append(p)

        for lv in cfg.get("lives", []):
            if not isinstance(lv, dict):
                continue
            u = normalize_url(lv.get("url", ""))
            if u and u not in seen_live_urls:
                seen_live_urls.add(u)
                merged_lives.append(lv)

    out: Dict[str, Any] = {"sites": merged_sites}
    if merged_parses:
        out["parses"] = merged_parses
    if merged_lives:
        out["lives"] = merged_lives
    return out


# ---------- 状态与 README ----------

def load_prev_status() -> Dict[str, Any]:
    if os.path.exists(STATUS_PATH):
        try:
            with open(STATUS_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"sources": {}}


def status_icon(ok: bool, quarantined: bool = False) -> str:
    if quarantined:
        return "🚫"
    return "✅" if ok else "❌"


def update_readme(statuses: List[Dict[str, Any]], quarantined: set):
    """更新 README 状态表（只改标记区间）。"""
    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    ok_count = sum(1 for s in statuses if s["ok"] and s["id"] not in quarantined)

    lines = [
        README_START_MARKER,
        f"> 🔄 最近更新：{now_str}（北京时间）· 成功 **{ok_count}/{len(statuses)}** 个源",
        "",
        "| 配置源 | 状态 | 站点数 | 延迟 | 备注 |",
        "| --- | --- | --- | --- | --- |",
    ]
    for s in statuses:
        q = s["id"] in quarantined
        icon = status_icon(s["ok"], q)
        note = ""
        if q:
            note = "连续失败，已隔离"
        elif not s["ok"]:
            note = s.get("error", "失败")
        elif s.get("type") == "multi":
            note = "多仓订阅"
        latency = f"{s['latency']}s" if s.get("latency") else "-"
        lines.append(f"| {s['name']} | {icon} | {s.get('sites', '-')} | {latency} | {note} |")
    lines.append(README_END_MARKER)
    table = "\n".join(lines)

    if not os.path.exists(README_PATH):
        return
    with open(README_PATH, "r", encoding="utf-8") as f:
        content = f.read()
    pattern = re.compile(re.escape(README_START_MARKER) + r".*?" + re.escape(README_END_MARKER), re.DOTALL)
    if pattern.search(content):
        content = pattern.sub(table, content)
    else:
        content = content.rstrip() + "\n\n## 源可用性状态\n\n" + table + "\n"
    with open(README_PATH, "w", encoding="utf-8") as f:
        f.write(content)


# ---------- 主流程 ----------

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        upstreams = json.load(f)
    upstreams = [u for u in upstreams if u.get("enabled", True)]
    logger.info("上游清单：%d 个启用", len(upstreams))

    prev = load_prev_status()
    prev_sources = prev.get("sources", {})

    results: List[Tuple[str, Dict[str, Any]]] = []
    statuses: List[Dict[str, Any]] = []
    quarantined: set = set()
    new_status_data: Dict[str, Any] = {}

    for up in upstreams:
        uid = up["id"]
        logger.info("处理上游: %s", up.get("name", uid))
        cfg, status = process_upstream(up)

        # 连续失败计数
        prev_fail = prev_sources.get(uid, {}).get("consecutive_failures", 0)
        if status["ok"]:
            status["consecutive_failures"] = 0
        else:
            status["consecutive_failures"] = prev_fail + 1
            if status["consecutive_failures"] >= MAX_CONSECUTIVE_FAILURES:
                quarantined.add(uid)
                logger.warning("上游 %s 连续失败 %d 次，已隔离", uid, status["consecutive_failures"])

        statuses.append(status)
        new_status_data[uid] = {
            "name": status["name"],
            "ok": status["ok"],
            "latency": status["latency"],
            "sites": status["sites"],
            "error": status.get("error"),
            "consecutive_failures": status["consecutive_failures"],
            "quarantined": uid in quarantined,
            "checked_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }

        if cfg is None or uid in quarantined:
            continue

        # 多仓订阅：只存独立文件，不并入单仓聚合
        if status.get("type") == "multi":
            out_path = os.path.join(OUTPUT_DIR, f"{uid}.json")
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(cfg, f, ensure_ascii=False, indent=2)
            logger.info("多仓 %s 已保存独立文件", uid)
            continue

        # 单仓：站点级健康检查
        sites = cfg.get("sites", [])
        logger.info("%s 站点数 %d，开始健康检查...", uid, len(sites))
        healthy_sites = batch_health_check(sites)
        logger.info("%s 健康站点 %d/%d", uid, len(healthy_sites), len(sites))
        status["sites"] = len(healthy_sites)
        status["sites_total"] = len(sites)
        new_status_data[uid]["sites"] = len(healthy_sites)
        new_status_data[uid]["sites_total"] = len(sites)

        cfg["sites"] = healthy_sites
        # 独立文件
        out_path = os.path.join(OUTPUT_DIR, f"{uid}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        results.append((uid, cfg))

    # 聚合输出
    merged = merge_sources(results)
    logger.info("聚合完成：%d 个去重站点", len(merged.get("sites", [])))

    tvbox_path = os.path.join(OUTPUT_DIR, "tvbox.json")
    with open(tvbox_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    # 多仓订阅 urls.json
    urls_cfg = {"urls": []}
    for s in statuses:
        if s["ok"] and s["id"] not in quarantined:
            urls_cfg["urls"].append({
                "url": f"https://raw.githubusercontent.com/wan7up/tvbox-source/main/output/{s['id']}.json",
                "name": s["name"],
            })
    urls_path = os.path.join(OUTPUT_DIR, "urls.json")
    with open(urls_path, "w", encoding="utf-8") as f:
        json.dump(urls_cfg, f, ensure_ascii=False, indent=2)

    # 状态文件
    with open(STATUS_PATH, "w", encoding="utf-8") as f:
        json.dump({"sources": new_status_data,
                   "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat()},
                  f, ensure_ascii=False, indent=2)

    # README
    update_readme(statuses, quarantined)
    logger.info("完成。聚合 %d 站点，隔离 %d 上游", len(merged.get("sites", [])), len(quarantined))


if __name__ == "__main__":
    sys.exit(main())
