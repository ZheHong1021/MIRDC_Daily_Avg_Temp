from utils.load_stations import load_stations
from core.Station import Station
from core.Database import Database
from config.logger import setup_logger
from datetime import datetime, timedelta

logger = setup_logger(__name__)

if __name__ == "__main__":
    stations = load_stations()


    date = datetime.now().date()
    # date = "2025-11-15"

    logger.info(f"程式開始執行，共有 {len(stations)} 個測站")

    for station in stations:
        logger.info("=" * 50)
        logger.info(f"處理測站: {station.get('StationName')} (ID: {station.get('StationId')})")
        station_instance = Station(
            station=station,
            date=date
        )

        # 取得測站溫度資料
        station_instance.get_weather_temp()
        station_instance.calculate_weight_temp()
        station_instance.fetch_adjusted_temp()

        logger.info(f'[結果統計] 平均溫度: {station_instance.avg_temp}')
        logger.info(f'[結果統計] 最小溫度: {station_instance.min_temp}')
        logger.info(f'[結果統計] 最大溫度: {station_instance.max_temp}')
        logger.info(f'[結果統計] 權重溫度: {station_instance.weight_temp}')
        logger.info(f'[結果統計] 調整後溫度: {station_instance.adjusted_temp}')
        logger.info(f'[結果統計] 氣壓值: {station_instance.pressure}')
        logger.info(f'[結果統計] 縣市: {station_instance.city}')

        # 保存資料到 position_status 表
        with Database(date=date) as db:
            success = db.create_or_update_position_status(
                station=station.get('StationName'),
                data={
                    'temp': station_instance.avg_temp,
                    'adjusted_temp': station_instance.adjusted_temp,
                    'weight_temp': station_instance.weight_temp,
                    'max_Temp': station_instance.max_temp,
                    'min_Temp': station_instance.min_temp,
                    'pressure': station_instance.pressure,
                    'city': station_instance.city
                }
            )

            if not success:
                logger.error(f'無法保存 {station.get("StationName")} 的資料')

    logger.info("程式執行完成")