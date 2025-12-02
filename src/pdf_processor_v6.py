# -*- coding: utf-8 -*-
# Author: jathon
# Date: 2025/12/02
# Description: 极致高性能 PDF 扫描器（多进程 + 写线程 + 实时进度）

import os
import csv
import fitz  # PyMuPDF 库，用于高效处理 PDF
import logging
import platform  # 用于判断操作系统类型（Windows/Linux/Mac）
import subprocess  # 用于在非 Windows 系统下执行 find 命令
from pathlib import Path  # 现代文件路径操作
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed  # 进程池和任务完成回调
from threading import Thread  # 线程模块，用于 CSV 写入
from queue import Queue  # 队列，用于进程与写线程间的通信
import time  # 时间模块

# ----------------- 配置 -----------------
PAGE_COUNT_THRESHOLD = 100  # 分类阈值：超过此页数视为 L，否则为 S
FILE_SIZE_THRESHOLD_BYTES = 10 * 1024 * 1024  # 10 MB，超过此大小视为 L，否则为S
BATCH_WRITE_SIZE = 1000  # CSV 写线程：每积累多少条结果批量写入一次磁盘
CSV_QUEUE_MAXSIZE = 5000  # 队列最大容量，防止进程生产速度过快导致内存溢出
PROGRESS_INTERVAL = 2  # 实时进度输出的间隔时间（秒）
# ---------------------------------------

# 配置日志系统，将日志输出到文件 'out/v6/pdf_processor.log' 和控制台
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('out/v6/pdf_processor.log', encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def process_single_pdf(pdf_path_str):
    """处理单个 PDF，在子进程中运行，返回包含分析结果的字典"""
    pdf_path = Path(pdf_path_str)
    start_time = datetime.now()
    # 初始化结果字典
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
        # 获取文件大小
        size = pdf_path.stat().st_size
        info['file_size_bytes'] = size
        if size < 100:
            info['error_message'] = 'too small'  # 标记文件过小
            return info

        # 使用 fitz (PyMuPDF) 打开 PDF
        with fitz.open(str(pdf_path)) as doc:
            if doc.is_encrypted:
                info['error_message'] = 'encrypted'  # 标记加密文件
            else:
                info['page_count'] = doc.page_count  # 获取页数
                info['can_open'] = True

                # 分类逻辑：页数和大小时长/短
                page_cat = 'L' if info['page_count'] > PAGE_COUNT_THRESHOLD else 'S'
                size_cat = 'L' if size > FILE_SIZE_THRESHOLD_BYTES else 'S'
                info['category'] = f"{page_cat}-{size_cat}"  # 组合分类，如 L-S

    except Exception as e:
        info['error_message'] = str(e)  # 捕获其他处理异常（如文件损坏）

    finally:
        # 计算处理时间并保留四位小数, 可以去除，但感觉没大差别
        # info['processing_time'] = round((datetime.now() - start_time).total_seconds(), 4)
        pass
    return info  # 返回结果字典


def find_pdf_files(root_path):
    """生成器：流式查找 PDF 文件，兼容 Windows 和 Linux/Mac"""
    root_path = Path(root_path)
    seen = set()  # 集合用于去重 (针对软链接或系统返回重复路径)

    # --- Windows 查找逻辑 ---
    if platform.system() == "Windows":
        # rglob 通配符查找，使用 *.[pP][dD][fF] 确保大小写不敏感匹配
        for pdf_path in root_path.rglob("*.[pP][dD][fF]"):
            norm = os.path.normcase(str(pdf_path))  # 规范化路径 (Windows 关键步骤)
            if norm in seen:
                continue
            seen.add(norm)
            yield str(pdf_path)

    # --- Linux/Mac 查找逻辑 ---
    else:
        try:
            # 尝试使用高效的 find 命令（比 Python rglob 更快）
            cmd = ["find", str(root_path), "-type", "f", "-iname", "*.pdf"]
            # 启动子进程执行 find 命令
            with subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True, encoding='utf-8') as proc:
                for line in proc.stdout:
                    path = line.strip()
                    if not path:
                        continue
                    norm = os.path.normcase(path)  # 规范化路径
                    if norm in seen:
                        continue
                    seen.add(norm)
                    yield path
        except Exception:
            # 如果 find 命令失败（如权限问题或系统缺失），回退到 Python rglob
            for pdf_path in root_path.rglob("*.[pP][dD][fF]"):
                norm = os.path.normcase(str(pdf_path))
                if norm in seen:
                    continue
                seen.add(norm)
                yield str(pdf_path)


def load_processed_csv(csv_file):
    """加载已处理记录，用于断点续传"""
    processed = set()
    if not os.path.exists(csv_file):
        return processed
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                p = row.get("file_path")
                if p:
                    # 将路径规范化后存入集合，与 find_pdf_files 的输出保持一致
                    processed.add(os.path.normcase(p))
        logger.info(f"已加载 {len(processed)} 条已处理记录")
    except Exception as e:
        logger.warning(f"加载 CSV 失败: {e}")
    return processed


def csv_writer_thread(csv_file, queue: Queue):
    """写线程：专门负责从队列读取数据并批量写入 CSV 文件"""
    file_exists = os.path.exists(csv_file)
    with open(csv_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # 如果文件不存在，写入 CSV 表头
        if not file_exists:
            writer.writerow(['file_path', 'file_size_bytes', 'can_open',
                             'page_count', 'processing_time', 'error_message', 'category'])
        while True:
            rows = queue.get()  # 阻塞等待队列中的数据
            if rows is None:
                break  # 收到 None 信号，表示所有数据已处理完毕，退出循环
            # 写入批量数据
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
            queue.task_done()  # 通知队列此批次任务已完成


def process_pdfs(root_path, csv_file="pdf_analysis.csv", workers=None, resume=True):
    """主处理函数：协调多进程、写线程和任务流"""
    # 确定工作进程数
    workers = workers or (os.cpu_count() or 4)
    # 加载已处理记录（如果启用断点续传）
    processed = load_processed_csv(csv_file) if resume else set()

    # 统计数据初始化
    stats = {
        "processed": 0,
        "category_counts": {'S-S': 0, 'S-L': 0, 'L-S': 0, 'L-L': 0, 'N/A': 0},
        "start": datetime.now()
    }

    # 启动 CSV 写线程
    csv_queue = Queue(maxsize=CSV_QUEUE_MAXSIZE)
    writer_thread = Thread(target=csv_writer_thread, args=(csv_file, csv_queue), daemon=True)
    writer_thread.start()

    batch_rows = []  # 存储待写入 CSV 的批量结果

    # 生成器：过滤掉已处理的文件
    pdf_gen = (p for p in find_pdf_files(root_path) if os.path.normcase(p) not in processed)

    with ProcessPoolExecutor(max_workers=workers) as executor:
        # 将 PDF 路径提交到进程池，并创建一个 future 到 path 的映射
        futures = {executor.submit(process_single_pdf, p): p for p in pdf_gen}

        last_time = time.time()
        # 实时获取已完成的 future 结果
        for fut in as_completed(futures):
            res = fut.result()  # 获取子进程返回的结果字典
            batch_rows.append(res)  # 结果添加到待写入批次

            # 更新统计数据
            stats["processed"] += 1
            stats["category_counts"].setdefault(res['category'], 0)
            stats["category_counts"][res['category']] += 1

            # 如果批次达到预设大小，将其放入队列
            if len(batch_rows) >= BATCH_WRITE_SIZE:
                csv_queue.put(batch_rows.copy())  # 注意：必须使用 copy() 避免多进程/线程共享同一个列表
                batch_rows.clear()  # 清空批次列表

            # 实时进度输出
            now = time.time()
            if now - last_time >= PROGRESS_INTERVAL:
                elapsed = (now - stats["start"].timestamp())
                speed = stats["processed"] / elapsed if elapsed > 0 else 0
                logger.info(f"已处理 {stats['processed']} 个文件，速率 {speed:.1f} 文件/秒")
                last_time = now

    # 处理循环结束后，将剩余的不足一批次的数据放入队列
    if batch_rows:
        csv_queue.put(batch_rows.copy())

    # 发送 None 信号给写线程，通知其退出
    csv_queue.put(None)
    # 等待写线程彻底完成所有写入任务
    writer_thread.join()

    # 打印总结报告
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
    """命令行参数解析和主函数调用"""
    import argparse
    parser = argparse.ArgumentParser(description="极致高性能 PDF 扫描器")
    # 必选参数：根目录
    parser.add_argument("root_path", help="PDF 根目录")
    # 可选参数：输出文件，默认为 pdf_analysis.csv
    parser.add_argument("--output", "-o", default="pdf_analysis.csv")
    # 可选参数：进程数
    parser.add_argument("--workers", "-w", type=int, help="进程数")
    # 可选标志：如果设置了 --no-resume，则不加载已有 CSV
    parser.add_argument("--no-resume", action="store_true", help="不加载已有 CSV")
    args = parser.parse_args()

    # 调用主处理函数
    process_pdfs(args.root_path, csv_file=args.output, workers=args.workers, resume=not args.no_resume)


if __name__ == "__main__":
    main()  # 脚本执行入口



