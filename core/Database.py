import logging
import pymysql
from config.settings import DATABASE_CONFIG
from config.logger import setup_logger
from datetime import datetime, timedelta

logger = setup_logger(__name__)

class Database:
    def __init__(self, date):
        self.config = DATABASE_CONFIG
        self.connection = None
        self.date = date

    def connect(self):
        try:
            self.connection = pymysql.connect(**self.config)
            # logger.info("資料庫連線建立成功")
            return True
        except pymysql.Error as e:
            logger.error(f"資料庫連線錯誤: {e}")
            return False

    def disconnect(self):
        if self.connection:
            self.connection.close()
            # logger.info("資料庫連線已關閉")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def selectWeatherTemp(self, stationId):
        """
        查詢指定測站的溫度資料 
        (近24小時的資料，資料為1小時更新一次)
        限制溫度在0~60度之間的合理範圍內

        @param stationId: 測站編號
        @return: 包含溫度資料的列表，若無資料則回傳 None
        """

        try:
            with self.connection.cursor() as cursor:
                sql = """
                    SELECT 
                        `id`, `station_id`, `temp`, `pressure`, `obs_time` 
                    FROM `weather_temperature`
                    WHERE 
                        `station_id` = %s AND `temp` > 0 AND `temp` < 60
                        AND DATE(`obs_time`) <= %s
                    ORDER BY `obs_time` DESC 
                    LIMIT 24;
                """
                cursor.execute(sql, (stationId, self.date))
                result = cursor.fetchall()
                return result
        except pymysql.Error as e:
            logger.error(f'查詢 [selectWeatherTemp] 資料庫時發生錯誤: {e}')
            return None

    def selectPositionStatusTemp(self, stationName):
        """
        查詢指定測站的溫度資料 (以近7筆的資料為主)用於計算權重溫度

        @param stationName: 測站名稱
        @return: 包含溫度資料的列表，若無資料則回傳 None
        """
        try:
            with self.connection.cursor() as cursor:
                sql = """
                    SELECT 
                        `id`, `station`, `temp`, `adjusted_temp`, `date` 
                    FROM `position_status`
                    WHERE `station` = %s AND `date` <= %s
                    ORDER BY `date` DESC 
                    LIMIT 7;
                """
                cursor.execute(sql, (stationName, self.date))
                result = cursor.fetchall()
                return result
        except pymysql.Error as e:
            logger.error(f'查詢 [selectPositionStatusTemp] 資料庫時發生錯誤: {e}')
            return None

    def selectBeforeDatePositionStatusTemp(self, stationName):
        """
        查詢指定日期最近一筆的 position_status 溫度資料 (用於判斷調整後溫度)

        @param stationName: 測站名稱
        @return: 返回最近一筆的溫度資料，若無資料則回傳 None
        """
        try:
            with self.connection.cursor() as cursor:
                sql = """
                    SELECT
                        `id`, `station`, `temp`, `date`, `max_Temp`, `min_Temp`, `pressure`
                    FROM `position_status`
                    WHERE `station` = %s AND `date` < %s
                    ORDER BY `date` DESC
                    LIMIT 1;
                """
                cursor.execute(sql, (stationName, self.date))
                result = cursor.fetchone()
                return result
        except pymysql.Error as e:
            logger.error(f'查詢 [selectPositionStatusTemp] 資料庫時發生錯誤: {e}')
            return None

    def create_or_update_position_status(self, station, data):
        """
        新增或更新 position_status 資料

        根據 station 和 date 判斷是否已存在記錄：
        - 若存在則更新
        - 若不存在則新增

        @param station: 測站名稱
        @param data: 要新增/更新的資料字典，必須包含以下鍵值：
                     - temp: 溫度
                     - adjusted_temp: 調整後溫度
                     - weight_temp: 權重溫度
                     - max_Temp: 最高溫度
                     - min_Temp: 最低溫度
                     - date: 日期
                     - pressure: 氣壓值
                     - city: 縣市
        @return: True 表示成功，False 表示失敗
        """
        try:
            logger.debug(f"準備新增/更新資料: {data}")
            with self.connection.cursor() as cursor:
                # 檢查是否已存在此 station 和 date 的記錄
                check_sql = """
                    SELECT `id` FROM `position_status`
                    WHERE `station` = %s AND `date` = %s
                """
                cursor.execute(check_sql, (station, self.date))
                existing = cursor.fetchone()

                if existing:
                    # 更新已存在的記錄
                    update_sql = """
                        UPDATE `position_status`
                        SET
                            `temp` = %s,
                            `adjusted_temp` = %s,
                            `weight_temp` = %s,
                            `max_Temp` = %s,
                            `min_Temp` = %s,
                            `pressure` = %s,
                            `city` = %s
                        WHERE `station` = %s AND `date` = %s
                    """
                    cursor.execute(update_sql, (
                        data.get('temp'),
                        data.get('adjusted_temp'),
                        data.get('weight_temp'),
                        data.get('max_Temp'),
                        data.get('min_Temp'),
                        data.get('pressure'),
                        data.get('city'),
                        station,
                        self.date
                    ))
                    logger.info(f'已更新 position_status: {station} - {self.date}')
                else:
                    # 新增記錄
                    insert_sql = """
                        INSERT INTO `position_status`
                        (`station`, `temp`, `adjusted_temp`, `weight_temp`, `max_Temp`, `min_Temp`, `pressure`, `city`, `date`)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_sql, (
                        station,
                        data.get('temp'),
                        data.get('adjusted_temp'),
                        data.get('weight_temp'),
                        data.get('max_Temp'),
                        data.get('min_Temp'),
                        data.get('pressure'),
                        data.get('city'),
                        self.date
                    ))
                    logger.info(f'已新增 position_status: {station} - {self.date}')

                self.connection.commit()
                return True

        except pymysql.Error as e:
            logger.error(f'[create_or_update_position_status] 資料庫錯誤: {e}')
            self.connection.rollback()
            return False
