"""
CryptoPunks 全量交易历史下载器
==================================
数据源: https://cryptopunks.app/api  (NODE Foundation 公开 API, 无需 API key)
覆盖范围: 原生 CryptoPunksMarket 合约的全部链上事件
           (Claimed / Sold / Offered / Bid / BidWithdrawn / Transfer / Wrap / Unwrap)
不覆盖: Wrapped 状态下在 OpenSea/Blur 上的交易

用法:
    pip install requests tqdm
    python cryptopunks_scraper.py

输出:
    cryptopunks_transactions.csv   — 全部交易记录 (flat table)
    cryptopunks_checkpoint.json    — 断点续传文件 (跑完后可删)

注意:
    - 10,000 个 punk，每个一次 API 调用，预计总共 ~30-60 分钟
    - 脚本有断点续传: 中途 Ctrl+C 退出后重新运行会从上次停的地方继续
    - 默认每次请求间隔 0.3 秒，如遇 rate limit 可调大 DELAY_SECONDS
"""

import requests
import json
import csv
import time
import os
import sys
from pathlib import Path
from datetime import datetime

# ============================================================
#  配置
# ============================================================
API_BASE = "https://cryptopunks.app/api"
TOTAL_PUNKS = 10_000           # punk 0 ~ 9999
DELAY_SECONDS = 1.0            # 请求间隔 (秒), 防止 429 限流
MAX_RETRIES = 5                # 单次请求最大重试
RETRY_BACKOFF = 2              # 指数退避倍数
TIMEOUT = 30                   # 请求超时 (秒)

OUTPUT_CSV = "cryptopunks_transactions.csv"
CHECKPOINT_FILE = "cryptopunks_checkpoint.json"
RAW_DIR = "cryptopunks_raw"    # 保存每个 punk 的原始 JSON (可选, 设为 None 则不保存)

# CSV 列定义
CSV_COLUMNS = [
    "punk_id",
    "event_type",           # Claimed / Sold / Offered / Bid / BidWithdrawn / OfferWithdrawn / Transfer / Wrap / Unwrap / ...
    "from_address",
    "from_ens",
    "to_address",
    "to_ens",
    "amount_wei",
    "amount_eth",
    "amount_usd",
    "timestamp",            # ISO 格式
    "block_number",
    "tx_hash",
]


# ============================================================
#  工具函数
# ============================================================
def load_checkpoint() -> int:
    """返回下一个要抓的 punk_id (默认 0)"""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            data = json.load(f)
            return data.get("next_punk_id", 0)
    return 0


def save_checkpoint(next_punk_id: int):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"next_punk_id": next_punk_id, "updated_at": datetime.utcnow().isoformat()}, f)


def fetch_punk_details(punk_id: int, session: requests.Session) -> dict:
    """
    调用 POST /api/punks/{punk_id}/details 获取完整交易历史。
    MCP server 源码确认这是获取 full history 的可靠端点。
    """
    url = f"{API_BASE}/punks/{punk_id}/details"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.post(url, json={}, timeout=TIMEOUT)

            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = RETRY_BACKOFF ** attempt
                print(f"  [429 Rate limited] punk {punk_id}, 等待 {wait}s 后重试...")
                time.sleep(wait)
                continue
            else:
                print(f"  [HTTP {resp.status_code}] punk {punk_id}, attempt {attempt}/{MAX_RETRIES}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF ** attempt)
                    continue
                else:
                    print(f"  ⚠ punk {punk_id} 跳过 (HTTP {resp.status_code})")
                    return None

        except requests.exceptions.RequestException as e:
            print(f"  [网络错误] punk {punk_id}: {e}, attempt {attempt}/{MAX_RETRIES}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF ** attempt)
            else:
                print(f"  ⚠ punk {punk_id} 跳过 (网络错误)")
                return None

    return None


def parse_transactions(punk_id: int, raw: dict) -> list[dict]:
    """
    从 API 返回的 JSON 中提取交易记录。
    
    API 返回格式可能因版本而异, 这里做了多种 key 的兼容:
    - raw 本身可能是 list (直接就是 events)
    - raw 可能有 "events", "transactions", "history" 等 key
    - 每条 event 的字段名也做了兼容映射
    """
    # 找到 events 列表
    # 实际 API 结构: {"success": ..., "data": {"transactionHistory": [...]}}
    events = []
    if isinstance(raw, list):
        events = raw
    elif isinstance(raw, dict):
        data = raw.get("data")
        if isinstance(data, dict):
            for key in ["transactionHistory", "events", "transactions", "history"]:
                if key in data and isinstance(data[key], list):
                    events = data[key]
                    break
        if not events:
            for key in ["events", "transactions", "history", "transactionHistory"]:
                if key in raw and isinstance(raw[key], list):
                    events = raw[key]
                    break
        if not events:
            for val in raw.values():
                if isinstance(val, list) and len(val) > 0:
                    first = val[0]
                    if isinstance(first, dict) and any(k in first for k in ["type", "event_type", "txn_type"]):
                        events = val
                        break

    rows = []
    for ev in events:
        if not isinstance(ev, dict):
            continue

        row = {"punk_id": punk_id}

        # event type
        row["event_type"] = (
            ev.get("type") or ev.get("event_type") or ev.get("txn_type") or ""
        )

        # from — API 返回 "from" 为地址字符串, "fromEns" 为 ENS
        from_info = ev.get("from") or {}
        if isinstance(from_info, dict):
            row["from_address"] = from_info.get("address") or from_info.get("id") or ""
            row["from_ens"] = from_info.get("ens") or from_info.get("name") or ""
        elif isinstance(from_info, str):
            row["from_address"] = from_info
            row["from_ens"] = ev.get("fromEns") or ev.get("fromName") or ""
        else:
            row["from_address"] = ev.get("fromAddress") or ev.get("from_address") or ""
            row["from_ens"] = ev.get("fromEns") or ev.get("fromName") or ""

        # to — 同上
        to_info = ev.get("to") or {}
        if isinstance(to_info, dict):
            row["to_address"] = to_info.get("address") or to_info.get("id") or ""
            row["to_ens"] = to_info.get("ens") or to_info.get("name") or ""
        elif isinstance(to_info, str):
            row["to_address"] = to_info
            row["to_ens"] = ev.get("toEns") or ev.get("toName") or ""
        else:
            row["to_address"] = ev.get("toAddress") or ev.get("to_address") or ""
            row["to_ens"] = ev.get("toEns") or ev.get("toName") or ""

        # amount
        row["amount_wei"] = ev.get("amount") or ev.get("value") or ev.get("price") or ""
        row["amount_eth"] = ev.get("ethAmount") or ev.get("amount_eth") or ev.get("eth") or ""
        row["amount_usd"] = ev.get("amountUsd") or ev.get("usdAmount") or ev.get("amount_usd") or ev.get("usd") or ""

        # 如果 eth 没有单独字段, 尝试从 wei 转换
        if not row["amount_eth"] and row["amount_wei"]:
            try:
                wei = int(str(row["amount_wei"]))
                row["amount_eth"] = f"{wei / 1e18:.6f}"
            except (ValueError, TypeError):
                pass

        # timestamp — API 返回 Unix 秒, 转为 ISO 格式
        ts = ev.get("timestamp") or ev.get("date") or ev.get("blockTimestamp") or ""
        if isinstance(ts, (int, float)) and ts > 0:
            row["timestamp"] = datetime.utcfromtimestamp(ts).isoformat()
        else:
            row["timestamp"] = ts

        # block & tx
        row["block_number"] = ev.get("blockNumber") or ev.get("block_number") or ev.get("block") or ""
        row["tx_hash"] = (
            ev.get("transactionHash") or ev.get("txHash") or
            ev.get("transaction_hash") or ev.get("txn") or ev.get("hash") or ""
        )

        rows.append(row)

    return rows


# ============================================================
#  主流程
# ============================================================
def main():
    start_from = load_checkpoint()
    print(f"CryptoPunks 交易历史下载器")
    print(f"{'='*50}")
    print(f"  数据源:   {API_BASE}")
    print(f"  范围:     punk #{start_from} ~ #{TOTAL_PUNKS - 1}")
    print(f"  请求间隔: {DELAY_SECONDS}s")
    print(f"  输出文件: {OUTPUT_CSV}")
    print(f"{'='*50}")

    if start_from > 0:
        print(f"  ℹ 从断点恢复: 上次停在 punk #{start_from}")

    # 创建 raw JSON 存储目录 (可选)
    if RAW_DIR:
        os.makedirs(RAW_DIR, exist_ok=True)

    # 准备 CSV
    csv_exists = os.path.exists(OUTPUT_CSV) and start_from > 0
    csv_file = open(OUTPUT_CSV, "a" if csv_exists else "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS)
    if not csv_exists:
        writer.writeheader()

    session = requests.Session()
    session.headers.update({
        "Content-Type": "application/json",
        "User-Agent": "CryptoPunks-Research-Scraper/1.0",
    })

    total_events = 0
    failed_punks = []

    try:
        for punk_id in range(start_from, TOTAL_PUNKS):
            # 进度显示
            pct = (punk_id - start_from + 1) / (TOTAL_PUNKS - start_from) * 100
            print(f"\r  [{pct:5.1f}%] punk #{punk_id:>5d} / {TOTAL_PUNKS - 1}", end="", flush=True)

            raw = fetch_punk_details(punk_id, session)

            if raw is None:
                failed_punks.append(punk_id)
                save_checkpoint(punk_id + 1)
                time.sleep(DELAY_SECONDS)
                continue

            # 保存原始 JSON (可选)
            if RAW_DIR:
                with open(os.path.join(RAW_DIR, f"{punk_id}.json"), "w", encoding="utf-8") as jf:
                    json.dump(raw, jf, ensure_ascii=False)

            # 解析并写入 CSV
            rows = parse_transactions(punk_id, raw)
            for row in rows:
                writer.writerow(row)
            total_events += len(rows)

            # 定期 flush
            if punk_id % 100 == 0:
                csv_file.flush()

            save_checkpoint(punk_id + 1)
            time.sleep(DELAY_SECONDS)

    except KeyboardInterrupt:
        print(f"\n\n  ⏸ 用户中断, 已保存断点 (punk #{punk_id})")
        print(f"  重新运行脚本即可从断点继续")

    finally:
        csv_file.close()

    print(f"\n\n{'='*50}")
    print(f"  完成!")
    print(f"  总事件数:     {total_events:,}")
    print(f"  失败 punk 数: {len(failed_punks)}")
    if failed_punks:
        print(f"  失败 ID:      {failed_punks[:20]}{'...' if len(failed_punks) > 20 else ''}")
    print(f"  输出文件:     {OUTPUT_CSV}")
    print(f"{'='*50}")

    # 清理断点文件
    if not failed_punks and os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print(f"  (断点文件已清理)")


if __name__ == "__main__":
    main()
