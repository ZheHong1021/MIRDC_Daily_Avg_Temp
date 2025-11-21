import os
from dotenv import load_dotenv
import pymysql

load_dotenv()

# Database Configuration
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '3306')),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'cwa_weather'),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor # 使用字典游標
}