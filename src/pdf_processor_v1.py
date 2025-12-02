# -*- coding: utf-8 -*-
# Author: Jathon
# Date: 2025/11/29
# Description: XiXi.
# -*- coding: utf-8 -*-
# Author: Jathon（增强版）
# High-performance streaming + multiprocessing PDF scanner

import os
import csv
import fitz
import logging
import subprocess
from pathlib import Path
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed


PAGE_COUNT_THRESHOLD = 100
FILE_SIZE_THRESHOLD_BYTES = 10 * 1024 * 1024  # 10 MB


def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.FileHandler('out/v6/pdf_processor.log', encoding='utf-8'),
                  logging.StreamHandler()]
    )
    return logging.getLogger(__name__)


logger = setup_logger()


def process_single_pdf(pdf_path):
    """Worker 子进程执行的任务，负责打开 PDF 并返回结果 dict"""
    pdf_path = Path(pdf_path)
    start_time = datetime.now()

    info = {
        "file_path": str(pdf_path),
        "file_size_bytes": 0,
        "can_open": False,
        "page_count": 0,
        "processing_time": 0.0,
        "error_message": "",
        "category": "N/A"
    }

    try:
        size = pdf_path.stat().st_size
        info["file_size_bytes"] = size

        if size < 100:
            info["error_message"] = "文件过小 (too small)"
            return info

        with fitz.open(str(pdf_path)) as doc:
            if doc.is_encrypted:
                info["error_message"] = "加密PDF"
                return info

            page_count = len(doc)
            info["page_count"] = page_count
            info["can_open"] = True

            page_cat = 'L' if page_count > PAGE_COUNT_THRESHOLD else 'S'
            size_cat = 'L' if size > FILE_SIZE_THRESHOLD_BYTES else 'S'
            info["category"] = f"{page_cat}-{size_cat}"

    except Exception as e:
        info["error_message"] = f"{e}"

    finally:
        info["processing_time"] = round(
            (datetime.now() - start_time).total_seconds(), 3
        )

    return info


def main(root_path, output_csv="pdf_analysis.csv", workers=8):
    root_path = Path(root_path)
    if not root_path.exists():
        logger.error(f"路径不存在: {root_path}")
        return

    logger.info(f"开始扫描目录 (仅查找 *.pdf): {root_path}")

    # 使用 Linux find 流式遍历，避免 Python 大量耗 RAM
    find_cmd = ["find", str(root_path), "-type", "f", "-name", "*.pdf"]
    find_proc = subprocess.Popen(find_cmd, stdout=subprocess.PIPE, text=True)

    pdf_paths = (line.rstrip("\n") for line in find_proc.stdout)

    # 初始化 CSV
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "file_path",
            "file_size_bytes",
            "can_open",
            "page_count",
            "processing_time",
            "error_message",
            "category",
        ])

    # 处理 PDF
    logger.info(f"启用 {workers} 个进程并行解析 PDF")

    processed = 0
    start = datetime.now()

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(process_single_pdf, pdf): pdf
            for pdf in pdf_paths
        }

        with open(output_csv, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            for future in as_completed(futures):
                info = future.result()
                writer.writerow([
                    info["file_path"],
                    info["file_size_bytes"],
                    info["can_open"],
                    info["page_count"],
                    info["processing_time"],
                    info["error_message"],
                    info["category"],
                ])
                processed += 1

                if processed % 500 == 0:
                    elapsed = (datetime.now() - start).total_seconds()
                    speed = processed / elapsed
                    logger.info(f"已处理 {processed} 个文件 (速度 {speed:.1f} 文件/秒)")

    total_time = (datetime.now() - start).total_seconds()
    logger.info(f"全部完成: {processed} 个 PDF，用时 {total_time/60:.1f} 分钟")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("root_path", help="扫描根目录")
    parser.add_argument("--output", "-o", default="pdf_analysis.csv")
    parser.add_argument("--workers", "-w", type=int, default=os.cpu_count())

    args = parser.parse_args()
    main(args.root_path, args.output, args.workers)
