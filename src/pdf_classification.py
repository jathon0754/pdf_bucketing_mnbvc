# -*- coding: utf-8 -*-
# Author: jathon
# Date: 2025/12/02
# Description: PDF 分类器

import os
import csv
import fitz  # PyMuPDF
import logging
import subprocess
from pathlib import Path
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from threading import Thread
from queue import Queue
import time

# ----------------- 配置 -----------------
LOG_FILE_PATH = './pdf_processor.log'
DEFAULT_CSV_FILE = './pdf_analysis.csv'

PAGE_COUNT_THRESHOLD = 100  # 页数阈值
FILE_SIZE_THRESHOLD_BYTES = 10 * 1024 * 1024    # 文件大小阈值

BATCH_WRITE_SIZE = 1000
CSV_QUEUE_MAXSIZE = 5000
PROGRESS_INTERVAL = 3
# ---------------------------------------

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(LOG_FILE_PATH, encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ----------------- 工具函数 -----------------
def find_pdf_files(root_path):
    """生成器：高效查找 PDF 文件"""
    root_path = Path(root_path)
    seen = set()

    try:
        cmd = ["find", str(root_path), "-type", "f", "-iname", "*.pdf"]
        with subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True, encoding='utf-8') as proc:
            for line in proc.stdout:
                path = line.strip()
                if not path:
                    continue
                norm = os.path.normcase(path)
                if norm in seen:
                    continue
                seen.add(norm)
                yield path
    except Exception:
        for pdf_path in root_path.rglob("*.[pP][dD][fF]"):
            norm = os.path.normcase(str(pdf_path))
            if norm in seen:
                continue
            seen.add(norm)
            yield str(pdf_path)

def process_single_pdf(pdf_path_str):
    """处理单个 PDF，只返回路径、页数、大小(MB)、类别"""
    pdf_path = Path(pdf_path_str)
    info = {
        'file_path': str(pdf_path),
        'page_count': 0,
        'file_size_mb': 0.0,
        'category': 'N/A'
    }

    try:
        file_size_bytes = pdf_path.stat().st_size
        file_size_mb = file_size_bytes / (1024 * 1024)
        info['file_size_mb'] = round(file_size_mb, 2)

        with fitz.open(str(pdf_path)) as doc:
            info['page_count'] = doc.page_count

            page_cat = 'L' if info['page_count'] > PAGE_COUNT_THRESHOLD else 'S'
            size_cat = 'L' if file_size_bytes > FILE_SIZE_THRESHOLD_BYTES else 'S'
            info['category'] = f"{page_cat}-{size_cat}"

    except Exception:
        pass

    return info

def load_processed_csv(csv_file):
    """加载已处理记录"""
    processed = set()
    if not os.path.exists(csv_file):
        return processed
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                p = row.get("file_path")
                if p:
                    processed.add(os.path.normcase(p))
        logger.info(f"已加载 {len(processed)} 条已处理记录")
    except Exception as e:
        logger.warning(f"加载 CSV 失败: {e}")
    return processed

# ----------------- 核心处理函数 -----------------
def process_pdfs(root_path, csv_file=DEFAULT_CSV_FILE, workers=None, resume=True):
    """主处理函数：多进程 + 写线程 + 实时进度"""
    # CSV 写线程嵌套
    def csv_writer_thread(csv_file, queue: Queue):
        file_exists = os.path.exists(csv_file)
        with open(csv_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['file_path', 'page_count', 'file_size_mb', 'category'])

            while True:
                rows = queue.get()
                if rows is None:
                    break
                for row in rows:
                    if row is None:
                        continue
                    writer.writerow([
                        row['file_path'],
                        row['page_count'],
                        row['file_size_mb'],
                        row['category']
                    ])
                queue.task_done()

    workers = workers or (os.cpu_count() or 4)
    processed = load_processed_csv(csv_file) if resume else set()

    stats = {
        "processed": 0,
        "category_counts": {'S-S':0, 'S-L':0, 'L-S':0, 'L-L':0, 'N/A':0},
        "start": datetime.now()
    }

    csv_queue = Queue(maxsize=CSV_QUEUE_MAXSIZE)
    writer_thread = Thread(target=csv_writer_thread, args=(csv_file, csv_queue), daemon=True)
    writer_thread.start()

    batch_rows = []
    pdf_gen = (p for p in find_pdf_files(root_path) if os.path.normcase(p) not in processed)

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_single_pdf, p): p for p in pdf_gen}
        last_time = time.time()

        for fut in as_completed(futures):
            res = fut.result()
            batch_rows.append(res)
            stats["processed"] += 1
            stats["category_counts"].setdefault(res['category'], 0)
            stats["category_counts"][res['category']] += 1

            if len(batch_rows) >= BATCH_WRITE_SIZE:
                csv_queue.put(batch_rows.copy())
                batch_rows.clear()

            now = time.time()
            if now - last_time >= PROGRESS_INTERVAL:
                elapsed = (now - stats["start"].timestamp())
                speed = stats["processed"] / elapsed if elapsed > 0 else 0
                logger.info(f"已处理 {stats['processed']} 个文件，速率 {speed:.1f} 文件/秒")
                last_time = now

    if batch_rows:
        csv_queue.put(batch_rows.copy())
    csv_queue.put(None)
    writer_thread.join()

    total_time = (datetime.now() - stats["start"]).total_seconds()
    logger.info("="*50)
    logger.info(f"处理完成！总文件数: {stats['processed']}")
    logger.info(f"总用时: {total_time/60:.1f} 分钟")
    logger.info(f"平均速度: {stats['processed']/total_time:.1f} 文件/秒")
    logger.info("分类统计:")
    for k, v in stats["category_counts"].items():
        logger.info(f"{k}: {v}")
    logger.info("="*50)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="PDF 分类器")
    parser.add_argument("root_path", help="PDF 根目录")
    parser.add_argument("--output", "-o", default=DEFAULT_CSV_FILE)
    parser.add_argument("--workers", "-w", type=int, help="进程数")
    parser.add_argument("--no-resume", action="store_true", help="不加载已有 CSV")
    args = parser.parse_args()

    process_pdfs(args.root_path, csv_file=args.output, workers=args.workers, resume=not args.no_resume)

if __name__ == "__main__":
    main()
