#########################################################
# Инструмент для кастинга, корректировки и валидации типов данных TypeCaster
# Разработчик Эль Сабаяр Шевченко Нидал. 2022 г.
# elsabayar-shevchenko@yandex.ru
#########################################################

import re
import uuid
from datetime import datetime


class TypeCaster:
    def __init__(self):
        self.mode_correct = False
        self.__uuid_pattern = re.compile(r'^[\da-f]{8}-([\da-f]{4}-){3}[\da-f]{12}$', re.IGNORECASE)
        self.__date_masks = [
            '%d %m %Y',
            '%d %m %y',
            '%d %b %Y',
            '%d %B %y',
            '%b %d %y', 
            '%m %d %y',
            '%Y %d %m',
            '%Y %d %b',
            '%Y %d %B',
            '%d %B %Y'
            ]
            
        self.__time_masks = [
            '%H %M',
            '%I %M%p',
            '%H %M %S',
            '%H %M %S%p',
            ]

        self.__eng_months = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december']
        self.__eng_months_abb = [m[:3] for m in self.__eng_months]

        self.__ru_months = ['январь', 'февраль', 'март', 'апрель', 'май', 'июнь', 'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь']
        self.__ru_months_conv = [self.__ru_months[i][:-1]+('я' if self.__ru_months[i][-1] in ['й','ь'] else self.__ru_months[i][-1]+'а') for i in range(len(self.__ru_months))]
        self.__ru_months_abb = ["янв","фев","мар","апр","май","июн", "июл","авг","сен","окт","ноя","дек"]

        self._collection_eng_ru_dates = [self.__eng_months, self.__eng_months_abb, self.__ru_months, self.__ru_months_abb, self.__ru_months_conv]

    async def __is_nan(self, value):
        return value!=value
    
    async def float_cast(self, value):
        if await self.__is_nan(value):
            return {"error": True, "err_msg": "Error: NaN value", "value": 0.0}

        try:
            return {"error": False, "value": float(value)}
        except Exception as e:
            result = {
                "error": True, 
                "err_msg": str(e), 
                "value": 0.0
                }            
            try:                
                result["value"] = float(value.replace(',','.'))        
            except Exception as e_:
                result["err_msg"] = str(e_)
                
            return result
                    
    
    async def integer_cast(self, value):
        if await self.__is_nan(value):
            return {"error": True, "err_msg": "Error: NaN value", "value": 0}

        try:
            return {"error": False, "value": int(value)}
        except Exception as e:
            result = {
                "error": True, 
                "err_msg": str(e), 
                "value": 0
                }

            cast_value = await self.float_cast(value.replace(',', '.'))
                        
            if not cast_value["error"]:
                result["value"] = int(str(cast_value["value"]).split('.')[0])
            else:
                result["err_msg"] = cast_value["err_msg"]
            
            return result
    
    async def string_cast(self, value):
        return {"error": False, "value": str(value)}
    
    async def uuid_cast(self, value):
        try:
            match = await self.__uuid_pattern.match(value)
        
            if match:
                return {"error": False, "value": uuid.UUID(value)}
            return {"error": True, "err_msg": "Неправильный формат uuid", "value": None}

        except Exception as e:
            return {"error": True, "err_msg": str(e), "value": None}
            
        
    async def datetime_cast(self, dt_value):
        dt_value = dt_value.translate({ord(x): ' ' for x in ['.', '/', ':', '-', ',']}).lower().strip()
        detected_value = None

        for j in range(len(self.__ru_months)):
            if len([c[j] for c in self._collection_eng_ru_dates if c[j] in dt_value]):
                detected_value = self.__eng_months[j]

                if self.__ru_months_conv[j] in dt_value:
                    dt_value = dt_value.replace(self.__ru_months_conv[j], self.__eng_months[j])

                elif self.__ru_months[j] in dt_value:
                    dt_value = dt_value.replace(self.__ru_months[j], self.__eng_months[j])

                elif self.__ru_months_abb[j] in dt_value:
                    dt_value = dt_value.replace(self.__ru_months_abb[j], self.__eng_months[j])

                elif self.__eng_months[j] not in dt_value and self.__eng_months_abb[j] in dt_value:
                    dt_value = dt_value.replace(self.__eng_months_abb[j], self.__eng_months[j])
                break

        if self.mode_correct:
            dt_value_ = []

            for pod in dt_value.split():
                try:
                    int(pod)
                    dt_value_.append(pod)
                except:
                    if pod != detected_value:
                        continue

                    dt_value_.append(pod)

            dt_value = ' '.join(dt_value_)
            del dt_value_

        datetime_object = False
        result = {
            "error": True,
            "value": None,
            "err_msg": "Дата не распознана"
            }
        
        for date_mask in self.__date_masks:
            try:
                datetime_object = datetime.strptime(dt_value, date_mask)
            except:
                for time_mask in self.__time_masks:
                    try:
                        datetime_object = datetime.strptime(dt_value, date_mask+' '+time_mask)
                        break
                    except:
                        pass

            if datetime_object:
                result["error"] = False
                result["err_msg"] = ""
                result["value"] = str(datetime_object)
                break
                
        return result

    async def get_cast_method(self, datatype):
        if datatype == "float":
            return self.float_cast
        if datatype == "integer" or datatype == "int":
            return self.integer_cast
        if datatype == "uuid":
            return self.uuid_cast
        if datatype == "datetime":
            return self.datetime_cast
        if datatype == "string" or datatype == "str":
            return self.string_cast

        return False

    async def correct_data(self, list_values, datatype):
        self.mode_correct = True
        corrected_values = []
        cast_method = await self.get_cast_method(datatype)

        if not cast_method:
            return {"error": "Указанный тип данных "+datatype+" не поддерживается."}
        
        for v in list_values:
            res = await cast_method(v)
            corrected_values.append(res["value"])
        
        return corrected_values

    async def validate_data(self, list_values, datatype):
        self.mode_correct = False
        cast_method = await self.get_cast_method(datatype)

        if not cast_method:
            return {"error": "Указанный тип данных "+datatype+" не поддерживается."}
        
        bad_values = []

        for i, v in enumerate(list_values):
            res = await cast_method(v)
        
            res["value"] = v
            res["row"] = i
            
            if res["error"]:
                del res["error"]
                bad_values.append(res)
                
        return bad_values
