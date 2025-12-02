# -*- coding: utf-8 -*-
# Author: Jathon
# Date: 2025/12/2
# Description: XiXi.
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# gen_many_pdfs_linux.py
# 批量生成大量 PDF，每个 PDF 可多页，支持分目录、并发、进度显示
# Author: Jathon

import os
from pathlib import Path
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import argparse
import random
import json

# 辅助日志文件
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("gen_many_pdfs.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 生成单个 PDF 文件
def make_pdf_file(out_path: str, page_count: int, pagesize=A4, font="Helvetica", fontsize=48):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    c = canvas.Canvas(out_path, pagesize=pagesize)
    width, height = pagesize
    c.setFont(font, fontsize)
    for i in range(1, page_count + 1):
        text = f"Page {i}"
        x = (width - c.stringWidth(text, font, fontsize)) / 2
        y = height / 2
        c.drawString(x, y, text)
        c.showPage()
    c.save()
    return os.path.getsize(out_path)

# Worker：生成编号连续 PDF
def worker_generate(start_idx, end_idx, out_dir, prefix, padding, files_per_dir, min_pages, max_pages, fontsize):
    generated, total_bytes = 0, 0
    out_dir = Path(out_dir)
    for idx in range(start_idx, end_idx):
        dir_index = idx // files_per_dir
        subdir = out_dir / f"{dir_index:06d}"
        subdir.mkdir(parents=True, exist_ok=True)

        filename = f"{prefix}{idx:0{padding}d}.pdf"
        out_path = subdir / filename

        page_count = random.randint(min_pages, max_pages)
        try:
            b = make_pdf_file(str(out_path), page_count, fontsize=fontsize)
            generated += 1
            total_bytes += b
        except Exception as e:
            logger.warning(f"[ERROR] {out_path}: {e}")

    return generated, total_bytes

# 分块生成编号区间
def chunk_ranges(total, chunk_size):
    for start in range(0, total, chunk_size):
        end = min(start + chunk_size, total)
        yield start, end

# 保存/加载已生成记录
def save_progress(progress_file, generated_count):
    try:
        with open(progress_file, "w", encoding="utf-8") as f:
            json.dump({"generated": generated_count}, f)
    except Exception as e:
        logger.warning(f"保存进度失败: {e}")

def load_progress(progress_file):
    if not os.path.exists(progress_file):
        return 0
    try:
        with open(progress_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("generated", 0)
    except Exception as e:
        logger.warning(f"加载进度失败: {e}")
        return 0

def main():
    parser = argparse.ArgumentParser(description="批量生成多页 PDF")
    parser.add_argument("out_dir", help="输出根目录")
    parser.add_argument("--total", type=int, default=1000000, help="总文件数")
    parser.add_argument("--workers", "-w", type=int, default=os.cpu_count(), help="并发进程数")
    parser.add_argument("--files-per-dir", type=int, default=1000, help="每个子目录的文件数")
    parser.add_argument("--prefix", type=str, default="page_", help="文件名前缀")
    parser.add_argument("--padding", type=int, default=7, help="编号零填充宽度")
    parser.add_argument("--chunk-size", type=int, default=1000, help="每个任务块包含文件数")
    parser.add_argument("--min-pages", type=int, default=1, help="每个 PDF 最少页数")
    parser.add_argument("--max-pages", type=int, default=10, help="每个 PDF 最大页数")
    parser.add_argument("--fontsize", type=int, default=48, help="页码字体大小")
    parser.add_argument("--resume", action="store_true", help="断点续生成")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    progress_file = out_dir / ".progress.json"

    generated_total = load_progress(progress_file) if args.resume else 0
    logger.info(f"已生成 {generated_total} 个 PDF（续生成模式: {args.resume}）")

    ranges = list(chunk_ranges(args.total, args.chunk_size))
    start_time = datetime.now()
    futures = []

    with ProcessPoolExecutor(max_workers=args.workers) as exe:
        for s, e in ranges:
            if e <= generated_total:
                continue
            s_adjusted = max(s, generated_total)
            futures.append(exe.submit(worker_generate, s_adjusted, e, str(out_dir),
                                       args.prefix, args.padding, args.files_per_dir,
                                       args.min_pages, args.max_pages, args.fontsize))

        for fut in tqdm(as_completed(futures), total=len(futures), desc="总体进度"):
            gen, b = fut.result()
            generated_total += gen
            save_progress(progress_file, generated_total)

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"完成：生成 {generated_total} 个 PDF，总大小约 {b/1024/1024:.2f} MB，耗时 {elapsed/60:.2f} 分钟，速率 {generated_total/elapsed:.1f} 文件/秒")

if __name__ == "__main__":
    main()
