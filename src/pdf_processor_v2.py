# -*- coding: utf-8 -*-
# Author: Jathon
# Date: 2025/12/02
# Description: 高性能跨平台 PDF 处理器（进程池 + 批量写 CSV）

import os
import csv
import fitz
import logging
import platform
import subprocess
from pathlib import Path
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

# ----------------- 配置 -----------------
PAGE_COUNT_THRESHOLD = 100
FILE_SIZE_THRESHOLD_BYTES = 10 * 1024 * 1024  # 10MB
BATCH_WRITE_SIZE = 200  # 每批处理多少文件写一次 CSV
# ---------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('out/v6/pdf_processor.log', encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def process_single_pdf(pdf_path_str):
    """处理单个 PDF，返回 dict"""
    pdf_path = Path(pdf_path_str)
    start_time = datetime.now()

    info = {
        'file_path': str(pdf_path),
        'file_size_bytes': 0,
        'can_open': False,
        'page_count': 0,
        'processing_time': 0.0,
        'error_message': '',
        'category': 'N/A'
    }

    try:
        size = pdf_path.stat().st_size
        info['file_size_bytes'] = size
        if size < 100:
            info['error_message'] = 'too small'
            return info

        with fitz.open(str(pdf_path)) as doc:
            if doc.is_encrypted:
                info['error_message'] = 'encrypted'
                return info
            page_count = len(doc)
            info['page_count'] = page_count
            info['can_open'] = True
            page_cat = 'L' if page_count > PAGE_COUNT_THRESHOLD else 'S'
            size_cat = 'L' if size > FILE_SIZE_THRESHOLD_BYTES else 'S'
            info['category'] = f"{page_cat}-{size_cat}"

    except Exception as e:
        info['error_message'] = str(e)

    finally:
        info['processing_time'] = round((datetime.now() - start_time).total_seconds(), 3)

    return info


def find_pdf_files(root_path):
    """生成器：流式查找 PDF 文件"""
    root_path = Path(root_path)
    seen = set()
    if platform.system() == "Windows":
        for pdf_path in root_path.rglob("*.[pP][dD][fF]"):
            norm = os.path.normcase(str(pdf_path))
            if norm in seen:
                continue
            seen.add(norm)
            yield str(pdf_path)
    else:
        # Linux / Mac
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
            # 回退到 Python 查找
            for pdf_path in root_path.rglob("*.[pP][dD][fF]"):
                norm = os.path.normcase(str(pdf_path))
                if norm in seen:
                    continue
                seen.add(norm)
                yield str(pdf_path)


def load_processed_csv(csv_file):
    """加载已处理记录，返回 set"""
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


def write_csv_batch(csv_file, rows):
    """批量写入 CSV"""
    if not rows:
        return
    file_exists = os.path.exists(csv_file)
    with open(csv_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['file_path', 'file_size_bytes', 'can_open',
                             'page_count', 'processing_time', 'error_message', 'category'])
        for row in rows:
            writer.writerow([
                row['file_path'],
                row['file_size_bytes'],
                row['can_open'],
                row['page_count'],
                row['processing_time'],
                row['error_message'],
                row['category']
            ])


def process_pdfs(root_path, csv_file="pdf_analysis.csv", workers=None, resume=True):
    workers = workers or (os.cpu_count() or 4)
    processed = set()
    if resume:
        processed = load_processed_csv(csv_file)

    stats = {
        "processed": 0,
        "category_counts": {'S-S': 0, 'S-L': 0, 'L-S': 0, 'L-L': 0, 'N/A': 0},
        "start": datetime.now()
    }

    batch_rows = []

    pdf_gen = (p for p in find_pdf_files(root_path) if os.path.normcase(p) not in processed)

    with ProcessPoolExecutor(max_workers=workers) as executor:
        future_to_path = {}
        batch_size = workers * 10

        # 提交任务
        pdf_batch = []
        for pdf in pdf_gen:
            pdf_batch.append(pdf)
            if len(pdf_batch) >= batch_size:
                for p in pdf_batch:
                    future_to_path[executor.submit(process_single_pdf, p)] = p
                pdf_batch = []

                # 处理完成任务
                for future in as_completed(future_to_path):
                    res = future.result()
                    batch_rows.append(res)
                    stats["processed"] += 1
                    stats["category_counts"].setdefault(res['category'], 0)
                    stats["category_counts"][res['category']] += 1

                    if len(batch_rows) >= BATCH_WRITE_SIZE:
                        write_csv_batch(csv_file, batch_rows)
                        batch_rows.clear()

                    # 打印进度
                    if stats["processed"] % 100 == 0:
                        elapsed = (datetime.now() - stats["start"]).total_seconds()
                        speed = stats["processed"] / elapsed if elapsed > 0 else 0
                        logger.info(f"已处理 {stats['processed']} 个文件；速率 {speed:.1f} file/s")

                future_to_path.clear()

        # 提交剩余 batch
        for p in pdf_batch:
            future_to_path[executor.submit(process_single_pdf, p)] = p

        for future in as_completed(future_to_path):
            res = future.result()
            batch_rows.append(res)
            stats["processed"] += 1
            stats["category_counts"].setdefault(res['category'], 0)
            stats["category_counts"][res['category']] += 1
            if len(batch_rows) >= BATCH_WRITE_SIZE:
                write_csv_batch(csv_file, batch_rows)
                batch_rows.clear()

        # 写入剩余未写入的数据
        if batch_rows:
            write_csv_batch(csv_file, batch_rows)
            batch_rows.clear()

    # 输出最终统计
    total_time = (datetime.now() - stats["start"]).total_seconds()
    logger.info("=" * 50)
    logger.info(f"处理完成！总文件数: {stats['processed']}")
    logger.info(f"总用时: {total_time / 60:.1f} 分钟")
    logger.info(f"平均速度: {stats['processed'] / total_time:.1f} 文件/秒")
    logger.info("分类统计:")
    for k, v in stats["category_counts"].items():
        logger.info(f"  {k}: {v}")
    logger.info("=" * 50)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="高性能 PDF 扫描器")
    parser.add_argument("root_path", help="PDF 根目录")
    parser.add_argument("--output", "-o", default="pdf_analysis.csv")
    parser.add_argument("--workers", "-w", type=int, help="进程数，默认 CPU 核心数")
    parser.add_argument("--no-resume", action="store_true", help="不加载已有 CSV，重新开始")
    args = parser.parse_args()

    process_pdfs(args.root_path, csv_file=args.output, workers=args.workers, resume=not args.no_resume)


if __name__ == "__main__":
    main()
