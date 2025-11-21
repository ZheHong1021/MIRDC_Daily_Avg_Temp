import math
from core.Database import Database
from decimal import Decimal
from config.logger import setup_logger

logger = setup_logger(__name__)

class Station:
    def __init__(self, station, date):
        self.stationId = station.get('StationId')
        self.stationName = station.get('StationName')
        self.city = station.get('City')
        self.date = date
        
        
        #region (氣象相關)
        self.weather_temps = [] # 每小時溫度資料
        self.avg_temp = None
        self.pressure = None
        self.min_temp = None
        self.max_temp = None
        #endregion
        
        #region (針對 position_status 使用)
        self.weight_temp = None
        self.adjusted_temp = None
        #endregion
        
        self.db = Database(date=self.date)

    
    def get_weather_temp(self):
        """取得測站溫度資料"""
        with self.db as db:
            self.weather_temps = db.selectWeatherTemp(self.stationId)

            if self.weather_temps:
                temps = [data['temp'] for data in self.weather_temps]
                times = [data['obs_time'] for data in self.weather_temps]
                logger.info(f'取得測站 {self.stationName} 的溫度資料，時間範圍: {min(times)} ~ {max(times)}')
                
                self.avg_temp = round(sum(temps) / len(temps), 1)
                self.min_temp = min(temps)
                self.max_temp = max(temps)
                
                pressures = [data['pressure'] for data in self.weather_temps]
                self.pressure = round(sum(pressures) / len(pressures), 1)

    
    def calculate_weight_temp(self):
        """
          計算權重溫度 (以近7筆的資料為主)
          
          使用時間序列權重：最新資料權重最高（權重係數 = 資料總數）
            越往前的資料權重越低（遞減至1）
            權重溫度 = (七天前溫度 x 1 + 六天前溫度 x 2 + … 一天前溫度 x 7 ) ÷ (7 + 6 +…+ 1)
          """
        with self.db as db: 
            # 取得溫度資料
          temps = db.selectPositionStatusTemp(self.stationName)
          # 計算權重溫度
          sum_weight_temp = 0
          sum_weight_index = 0
          for index, item in enumerate(temps):
                temp_parameter = (item['adjusted_temp'] or item['temp'])
                
                # 權重比重 = 總筆數 - index
                # 例如: 7筆資料，index從0開始，則權重比重分別為7,6,5,4,3,2,1
                weight_index = len(temps) - index
                sum_weight_temp += temp_parameter * weight_index
                sum_weight_index += weight_index
                
        self.weight_temp = round(sum_weight_temp / sum_weight_index, 1)
        return self.weight_temp
        
    def fetch_adjusted_temp(self):
        """
        誤差調整後溫度的計算方式如下：
        1. 取得前一次的調整後溫度資料。
        2. 如果前一次溫度資料不存在，則直接使用當前的平均溫度作為調整後溫度。
        3. 如果當前的平均溫度不存在，則直接使用前一次的溫度作為調整後溫度。
        4. 如果當前的平均溫度不在合理範圍（0~60度）內，則使用前一次的溫度作為調整後溫度。
        5. 計算當前平均溫度與前一次溫度的誤差值。
        6. 如果誤差值超過5度，則將調整後溫度設為前一次溫度與當前平均溫度的平均值。
        7. 否則，直接使用當前的平均溫度作為調整後溫度。
        8. 最終將計算出的調整後溫度存儲在 self.adjusted_temp 屬性中。
        """
        with self.db as db:
            # 得到前一次溫度
            before_temp = db.selectBeforeDatePositionStatusTemp(self.stationName)
            logger.debug(f"前次溫度資料: {before_temp}")
            
            if not before_temp:
                logger.warning("無前一次溫度資料，直接使用自身平均溫度")
                self.adjusted_temp = self.avg_temp
                return
            
            if not self.weather_temps:
                logger.warning("無自身平均溫度資料，直接使用前一次溫度")
                self.avg_temp = before_temp['temp']
                self.adjusted_temp = before_temp['temp']
                self.max_temp = before_temp['max_Temp']
                self.min_temp = before_temp['min_Temp']
                self.pressure = before_temp['pressure']
                return
            
            # 如果自身平均溫度不合理，使用前一次溫度
            if self.avg_temp <= 0 or self.avg_temp >= 60:
                logger.warning(f"自身平均溫度不合理({self.avg_temp})，使用前一次溫度({before_temp['temp']})")
                self.adjusted_temp = before_temp['temp']
                return
            
            # 當誤差值超過 5
            before_temp_value = Decimal(str(before_temp['temp']))
            if math.fabs(self.avg_temp - before_temp_value) > 5:
                # 誤差調整後溫度 = (前一次溫度 + 當前溫度) / 2
                self.adjusted_temp = round((before_temp_value + self.avg_temp) / 2, 1)
                logger.info(f"誤差值 > 5 => 調整後溫度: {self.adjusted_temp} (原本: {self.avg_temp} ； 前次: {before_temp_value})")
                return
            # 最後都符合條件就用自己
            self.adjusted_temp = self.avg_temp
            
            
