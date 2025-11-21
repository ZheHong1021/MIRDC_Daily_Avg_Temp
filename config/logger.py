import logging
import logging.handlers
from datetime import datetime
from pathlib import Path

def setup_logger(name, level=logging.INFO):
    """
    配置日志系统

    @param name: logger 名稱
    @param level: 日誌級別 (預設為 INFO)
    @return: 配置好的 logger 實例
    """

    # 創建 logs 目錄
    logs_dir = Path(__file__).parent.parent / 'logs'
    logs_dir.mkdir(exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # 避免重複添加 handler
    if logger.handlers:
        return logger

    # 日誌格式
    formatter = logging.Formatter(
        '[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 檔案 handler (每日輪轉)
    log_file = logs_dir / f'app_{datetime.now().strftime("%Y%m%d")}.log'
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # 控制台 handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
