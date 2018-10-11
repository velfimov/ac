"""
Необходимо максимально оптимальным образом  прочитать файлы логов, найти кол-во
валидных и невалидных запросов с группировкой по суткам (время UTC) и по типу
запроса. Количество файлов логов и строк в них может быть очень большим.

Формат строки:
{
  "timestamp": 1538669069, # UNIX timestamp
  "event_type": "create|update|delete", # одно из значений create, update, delete
  "ids": [1, 2, 3],   # список int'ов
  "query_string": "id=1&id=5&id=3"    # Query string
}

Валидным считается запрос в котором:
* точно такой же набор id в массиве 'ids' как переданные id в 'query_string',
  при этом порядок id неважен и id не повторяются.

Пример валидного запроса:
{
    "timestamp":1525392515,
    "event_type":"delete",
    "ids":[7,3,9],
    "query_string":"btksf=btksf&id=3&id=7&id=9&dabbq=dabbq&wtcbd=wtcbd"
}

Пример невалидного запроса:
{
    "timestamp":1525208549,
    "event_type":"create",
    "ids":[7,6,5,4,8,1],
    "query_string":"query_string":"id=19&id=15&ohxhi=ohxhi&id=12&id=16&id=13&nabao=nabao&id=18&id=14&zanhw=zanhw&id=20"
}



Образцы логов можно скачать по ссылке:
https://www.dropbox.com/s/4ltuy3dzkqro3he/test_files.zip?dl=0

На экран должен быть выведен отчет в котором:
* Сначала произведена группировка по: валидный - невалидный запрос
* внутри предыдущей группировки, группировка по дням
* внутри предыдущей группировки, группировка по типам запросов
* возле каждой из группировки из предыдущего пункта суммарное количество таких запросов

Должна получиться примерно такая структура:
{
    "valid": {
        # timestamp начала суток в группировке
        1514764800: {
            "create": 20,
            "update": 20,
            "delete": 0,
        },
    },
    "non_valid": {}
}
"""

import os
import pprint

try:
    import ujson as json
except ImportError:
    import json


class Statistics:
    GROUP_KEYNAMES = ('valid', 'non_valid')
    VALID_EVENT_TYPES = ('create', 'update', 'delete')

    _result = None

    def __init__(self, datadir):
        self.datadir = datadir

    def calculate(self):
        self._flush_result()

        for entry in os.scandir(path=self.datadir):
            if not entry.is_file() or entry.name.startswith('.'):
                continue

            filename = os.path.join(self.datadir, entry.name)
            self._process_file(filename)

    def _process_file(self, entry):
        with open(entry) as f:
            for line in f:
                is_good = False

                data = json.loads(line)

                # проверка на допустимые значения в event_type
                if data['event_type'] in self.VALID_EVENT_TYPES:
                    qs_chunks = data['query_string'].split('&')
                    qs_ids = [int(chunk[3:]) for chunk in qs_chunks if len(chunk) > 3 and chunk[:3]=='id=']

                    # сравнение словарей с значениями-ключами
                    qs_dict = dict.fromkeys(qs_ids, True)
                    ids_dict = dict.fromkeys(data['ids'], True)
                    if qs_dict == ids_dict:
                        is_good = True

                key_name = 'valid' if is_good else 'non_valid'
                key_timestamp = data['timestamp']//86400*86400

                if self._result is None:
                    self._result = {}

                if key_name not in self._result:
                    self._result[key_name] = {}

                if key_timestamp not in self._result[key_name]:
                    self._result[key_name][key_timestamp] = dict.fromkeys(self.VALID_EVENT_TYPES, 0)

                self._result[key_name][key_timestamp][data['event_type']] += 1

    def _flush_result(self):
        self._result = None

    @property
    def result(self):
        return self._result


if __name__ == '__main__':
    datadir = './test_files/'

    statistics = Statistics(datadir)
    statistics.calculate()

    print('result')
    pprint.pprint(statistics.result)
