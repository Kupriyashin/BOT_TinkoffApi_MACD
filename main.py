import math
import random
import time

import pandas
import vk_api
from tinkoff.invest.utils import now

from datetime import timedelta

from tinkoff.invest import (
    CandleInstrument,
    Client,
    SubscribeCandlesRequest,
)

# глобальные переменные для хранения
dr_pozdr = 0
long = 0
sell = 0
cena_pokupki = 0
pribil = 0

token_tinkoff_api = ''
with open('tokens.tink.txt', 'r', encoding='UTF-8') as tok_tin:
    for lin in tok_tin:
        token_tinkoff_api = lin
        break

marina_vk_token = ''
with open('token.marina.txt', 'r', encoding='UTF-8') as marvktok:
    for lin in marvktok:
        marina_vk_token = lin
        break
session_marina = vk_api.VkApi(token=marina_vk_token)

kupriyashin_token = ''
with open('token_kupriyashin.txt', 'r', encoding='UTF-8') as kupriyashin_tok:
    for lin in kupriyashin_tok:
        kupriyashin_token = lin
        break
session_kupr = vk_api.VkApi(token=kupriyashin_token)

#
# def users_holly_day():
#     try:
#         count = session_kupr.method('friends.get', {  # количество пользователей
#         })['count']
#
#         # print
#         item = []
#         import tqdm
#         for elem in tqdm.tqdm(range(math.ceil(int(count) / 100))):
#             item = session_kupr.method('friends.get', {
#                 'offset': elem * 100,
#                 'count': 100,
#             })['items']
#             print(item, '\n')
#             print(f"Количество: {count}")
#
#             for i in tqdm.tqdm(range(len(item))):
#                 if 'bdate' in item[i]:
#                     import datetime
#                     if ((int(item[i]['bdate'].split('.')[0])) == datetime.datetime.now().day) and (
#                             (int(item[i]['bdate'].split('.')[1])) == datetime.datetime.now().month):
#                         print(f"\nСегодня др у {item[i]['id']}")
#                         session_kupr.method('messages.send', {
#                             'user_id': str(item[i]['id']),
#                             'random_id': random.randint(-2147483648, 2147483647),
#                             'peer_id': str(item[i]['id']),
#                             'message': str("Добрейшего вечера!\n"
#                                            "Поздравляю с днем рождения и желаю всего самого наилучшего🌚")
#                         })
#                         time.sleep(0.1)
#
#         time.sleep(0.1)
#         del item
#         print('Скрипт завершен')
#     except Exception as error:
#         print('Ошибка при отправке поздравления', error)
#

def request_eterator():
    global dr_pozdr

    from tinkoff.invest.grpc.marketdata_pb2 import SUBSCRIPTION_ACTION_SUBSCRIBE
    from tinkoff.invest.grpc.marketdata_pb2 import SUBSCRIPTION_INTERVAL_FIVE_MINUTES
    from tinkoff.invest import MarketDataRequest

    yield MarketDataRequest(
        subscribe_candles_request=SubscribeCandlesRequest(
            waiting_close=True,
            subscription_action=SUBSCRIPTION_ACTION_SUBSCRIBE,
            instruments=[
                CandleInstrument(
                    figi='BBG004S68507',  # Магнитогорский металлургический комбинат
                    interval=SUBSCRIPTION_INTERVAL_FIVE_MINUTES
                )
            ],
        )
    )
    while True:
        time.sleep(1)

        if (now() + timedelta(hours=3)).time().hour == 12 and dr_pozdr:
            dr_pozdr = 0
        elif (now() + timedelta(hours=3)).time().hour == 0 and not dr_pozdr:
            dr_pozdr = 1
            users_holly_day()

        print(f'-----{(now() + timedelta(hours=3)).time()}-----\n')


# figis = (
#     'BBG004S68BH6',#ПИК
#     'BBG000NL6ZD9',#Иркутскэнерго
#     'BBG00475KHX6',#транснефть
#     'BBG0047315D0'#сургут нефтегаз 25р,
#     'BBG000NLC9Z6',#Ленэнерго,
#     'BBG004S68507'#Магнитогорский металлургический комбинат
# )



def main():
    try:
        with Client(token_tinkoff_api) as client:

            from tinkoff.invest import MarketDataServerSideStreamRequest
            from tinkoff.invest.grpc.instruments_pb2 import INSTRUMENT_ID_TYPE_FIGI

            for data in client.market_data_stream.market_data_stream(
                    request_eterator()
            ):
                try:
                    if data.candle:
                        # информация об бумаге
                        instr = client.instruments.get_instrument_by(
                            id_type=INSTRUMENT_ID_TYPE_FIGI,
                            id=data.candle.figi
                        )
                        print('\n-----Основная информация о бумаге------')
                        print(f'Лот: {instr.instrument.lot}')
                        print(f'Клас код: {instr.instrument.class_code}')
                        print(f'Имя: {instr.instrument.name}')
                        print(f'Доступ в шорт: {instr.instrument.short_enabled_flag}')
                        print(f'Цента закрытия свечи: {data.candle.close}')
                        print(f'Время закрытия свечи: {data.candle.last_trade_ts.utcnow()}')

                        # получение массива 5-ых свечек за 22 часа
                        from tinkoff.invest.grpc.marketdata_pb2 import CANDLE_INTERVAL_5_MIN
                        candles_5_minutes = client.market_data.get_candles(
                            figi='BBG004S68507',
                            from_=now() - timedelta(hours=22, minutes=30),
                            to=now(),
                            interval=CANDLE_INTERVAL_5_MIN
                        ).candles
                        candles_5_minutes = candles_5_minutes[:-2]
                        print("\n-----КОличество полученных свеч-----")
                        print(len(candles_5_minutes), '\n')

                        # Получение цен закрытия этих свечек
                        data_5_candles = {
                            'close': [],
                        }
                        for cand in candles_5_minutes:
                            data_5_candles['close'].append(
                                round(float(f"{int(cand.close.units)}.{int(cand.close.nano)}"), 3))

                        print("-----data['close']-----\n")
                        print(data_5_candles['close'], '\n')

                        # создание базы пандаса из цен закрытия
                        db = pandas.Series(data_5_candles['close'])
                        db = pandas.Series.dropna(db)

                        # расчет макди при помощи библиотеки
                        from ta import trend
                        macd_deff = trend.macd_diff(close=db, window_fast=20, window_slow=34, window_sign=12)
                        macd_deff = pandas.Series.dropna(macd_deff)
                        macd_deff = round(float(macd_deff.iloc[-1]), 7)
                        print('-----Значение гистограммы-----\n')
                        print(macd_deff, '\n')

                        ####################################################################################################
                        global sell
                        global long
                        global cena_pokupki
                        global pribil

                        if macd_deff < 0 and not sell:  ###SHORT

                            print('-----Покупаю в ШОРТ------\n')
                            sell = 1
                            long = 0

                            if not cena_pokupki:
                                cena_pokupki = float(f"{int(data.candle.close.units)}.{int(data.candle.close.nano)}")

                            pribil = (-cena_pokupki + float(
                                f"{int(data.candle.close.units)}.{int(data.candle.close.nano)}")) / cena_pokupki * 100
                            try:
                                session_marina.method('messages.send', {
                                    'random_id': random.randint(-2147483648, 2147483647),
                                    'peer_id': '2000000209',
                                    'chat_id': '209',
                                    'message': str(f"🆘🆘🆘🆘🆘🆘🆘🆘\n"
                                                   f"Основная информация о бумаге.\n"
                                                   f" Время и дата - {now().utcnow() + timedelta(hours=3)}\n"
                                                   f"-----------------------------------------------\n"
                                                   f"Имя: {instr.instrument.name}\n"
                                                   f"Доступ в шорт: {instr.instrument.short_enabled_flag}\n"
                                                   f"Цена на момент покупки: {float(f'{int(data.candle.close.units)}.{int(data.candle.close.nano)}')}\n"
                                                   f"Время покупки: {data.candle.time.utcnow() + timedelta(hours=3)}\n"
                                                   f"-----------------------------------------------\n"
                                                   f"❗Покупаю в ШОРТ📉 за {float(f'{int(data.candle.close.units)}.{int(data.candle.close.nano)}')} рублей\n"
                                                   f"Прибыль от прошлой сделки: {pribil - 0.3}%, рублей: {-cena_pokupki + float(f'{int(data.candle.close.units)}.{int(data.candle.close.nano)}')}\n"
                                                   f"MACD: {round(macd_deff, 7)}\n"
                                                   f"-----------------------------------------------\n")
                                })
                            except Exception as vk_err:
                                print('В блоке отправки сообщений вк (ШОРТ) произошла ошибка: ', vk_err)

                            cena_pokupki = float(f"{int(data.candle.close.units)}.{int(data.candle.close.nano)}")

                            try:
                                with open('logs.txt','a',encoding='UTF-8') as logs:
                                    logs.write(str(
                                        f"Время: {(data.candle.time.utcnow() + timedelta(hours=3))}\n"
                                        f"Прибыль от прошлой сделки: "
                                        f"{pribil - 0.3}%\n"
                                        f"В рублях: {cena_pokupki - float(f'{int(data.candle.close.units)}.{int(data.candle.close.nano)}')}\n"
                                        f"\n"
                                    ))
                            except Exception as file_err:
                                print(f"Ошибка в открытии файла логов: {file_err}")

                        ####################################################################################################
                        elif macd_deff > 0 and not long:  ###LONG

                            print('-----Покупаю в ЛОНГ------\n')
                            sell = 0
                            long = 1

                            if not cena_pokupki:
                                cena_pokupki = float(f"{int(data.candle.close.units)}.{int(data.candle.close.nano)}")
                            pribil = (cena_pokupki - float(
                                f"{int(data.candle.close.units)}.{int(data.candle.close.nano)}")) / cena_pokupki * 100

                            try:
                                session_marina.method('messages.send', {
                                    'random_id': random.randint(-2147483648, 2147483647),
                                    'peer_id': '2000000209',
                                    'chat_id': '209',
                                    'message': str(f"🆘🆘🆘🆘🆘🆘🆘🆘\n"
                                                   f"Основная информация о бумаге.\n"
                                                   f" Время и дата - {now().utcnow() + timedelta(hours=3)}\n"
                                                   f"-----------------------------------------------\n"
                                                   f"Имя: {instr.instrument.name}\n"
                                                   f"Доступ в шорт: {instr.instrument.short_enabled_flag}\n"
                                                   f"Цена на момент покупки: {float(f'{int(data.candle.close.units)}.{int(data.candle.close.nano)}')}\n"
                                                   f"Время покупки: {data.candle.time.utcnow() + timedelta(hours=3)}\n"
                                                   f"-----------------------------------------------\n"
                                                   f"❗Покупаю в ЛОНГ📈 за {float(f'{int(data.candle.close.units)}.{int(data.candle.close.nano)}')} рублей\n"
                                                   f"Прибыль от прошлой сделки: {pribil - 0.3}%, "
                                                   f"рублей: {cena_pokupki - float(f'{int(data.candle.close.units)}.{int(data.candle.close.nano)}')}\n"
                                                   f"MACD: {round(macd_deff, 7)}\n"
                                                   f"-----------------------------------------------\n")
                                })
                            except Exception as vk_err:
                                print('В блоке отправки сообщений вк (ЛОНГ) произошла ошибка: ', vk_err)

                            cena_pokupki = float(f"{int(data.candle.close.units)}.{int(data.candle.close.nano)}")

                            try:
                                with open('logs.txt','a',encoding='UTF-8') as logs:
                                    logs.write(str(
                                        f"Время: {(data.candle.time.utcnow() + timedelta(hours=3))}\n"
                                        f"Прибыль от прошлой сделки: "
                                        f"{pribil - 0.3}%\n"
                                        f"В рублях: {cena_pokupki - float(f'{int(data.candle.close.units)}.{int(data.candle.close.nano)}')}\n"
                                        f"\n"
                                    ))
                            except Exception as file_err:
                                print(f"Ошибка в открытии файла логов: {file_err}")

                except Exception as stream_error:
                    print(f"Ошибка при подключении к стриму: {stream_error}")
                    session_marina.method('messages.send', {
                        'random_id': random.randint(-2147483648, 2147483647),
                        'peer_id': '2000000209',
                        'chat_id': '209',
                        'message': str(f'Ошибка при подключении к стриму: {stream_error}')
                    })
                    break

    except Exception as error:
        print('Ошибка в основной функции: ', error)
        session_marina.method('messages.send', {
            'random_id': random.randint(-2147483648, 2147483647),
            'peer_id': '2000000209',
            'chat_id': '209',
            'message': str(f'Ошибка в функции main(): {error}')
        })


if __name__ == '__main__':
    while True:
        try:
            main()
        except Exception as errr:
            print(f"В основной функции ошибка: {errr}")
            session_marina.method('messages.send', {
                'random_id': random.randint(-2147483648, 2147483647),
                'peer_id': '2000000209',
                'chat_id': '209',
                'message': str(f"Ошибка в конструкции if __name__ == '__main__': {errr}")
            })
