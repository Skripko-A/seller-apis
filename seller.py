import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """
    Получает список продуктов с Ozon Seller API.

    Запрашивает список продуктов с Ozon Seller API, начиная с указанного идентификатора, с максимальным лимитом
    в 1000 продуктов за запрос.

    Args:
        last_id (str): Последний идентификатор продукта для пагинации.
        client_id (str): Идентификатор клиента для аутентификации.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        dict: Словарь, содержащий результат запроса со списком продуктов.

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился с ошибкой.

    Examples:
        Успешное выполнение:
            last_id = '100'
            client_id = 'example_client_id'
            seller_token = 'example_seller_token'

            products = get_product_list(last_id, client_id, seller_token)
            print(products)

            # Вывод:
            # {'items': [...], 'total': 1500}

        Неудачное выполнение:
            last_id = '100'
            client_id = 'example_client_id'
            seller_token = 'invalid_seller_token'

            try:
                products = get_product_list(last_id, client_id, seller_token)
            except requests.exceptions.HTTPError as e:
                print(f"Ошибка: {e}")

            # Возможный вывод:
            # Ошибка: 401 Client Error: Unauthorized for url: https://api-seller.ozon.ru/v2/product/list
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """
    Получает список идентификаторов предложений (offer_id) всех продуктов с Ozon Seller API.

    Выполняет постраничный запрос к Ozon Seller API,
    чтобы собрать все продукты и извлечь из них идентификаторы предложений.

    Args:
        client_id (str): Идентификатор клиента для аутентификации.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        list: Список идентификаторов предложений (offer_id).

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился с ошибкой.

    Examples:
        Успешное выполнение:
            client_id = 'example_client_id'
            seller_token = 'example_seller_token'

            offer_ids = get_offer_ids(client_id, seller_token)
            print(offer_ids)

            # Вывод:
            # ['offer_id_1', 'offer_id_2', 'offer_id_3', ...]

        Неудачное выполнение:
            client_id = 'example_client_id'
            seller_token = 'invalid_seller_token'

                try:
                offer_ids = get_offer_ids(client_id, seller_token)
            except requests.exceptions.HTTPError as e:
                print(f"Ошибка: {e}")

                # Возможный вывод:
            # Ошибка: 401 Client Error: Unauthorized for url: https://api-seller.ozon.ru/v2/product/list
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """
    Обновляет цены на продукты с помощью Ozon Seller API.

    Отправляет запрос на обновление цен на продукты, используя предоставленный список цен.

    Args:
        prices (list): Список словарей с информацией о ценах.
                       Каждый словарь должен содержать ключи, необходимые для API Ozon.
        client_id (str): Идентификатор клиента для аутентификации.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        dict: Словарь с ответом от API, содержащий информацию о статусе обновления цен.

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился с ошибкой.

    Examples:
        Успешное выполнение:
            prices = [
                {"offer_id": "offer_id_1", "price": "100.00"},
                {"offer_id": "offer_id_2", "price": "150.00"},
            ]
            client_id = 'example_client_id'
            seller_token = 'example_seller_token'

              response = update_price(prices, client_id, seller_token)
            print(response)

              # Вывод:
            # {'result': 'success'}

        Неудачное выполнение:
            prices = [
                {"offer_id": "offer_id_1", "price": "100.00"},
                {"offer_id": "offer_id_2", "price": "150.00"},
            ]
            client_id = 'example_client_id'
            seller_token = 'invalid_seller_token
            try:
                response = update_price(prices, client_id, seller_token)
            except requests.exceptions.HTTPError as e:
                print(f"Ошибка: {e}"
            # Возможный вывод:
            # Ошибка: 401 Client Error: Unauthorized for url: https://api-seller.ozon.ru/v1/product/import/prices
      """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """
    Обновляет запасы продуктов с помощью Ozon Seller API.

    Отправляет запрос на обновление количества запасов для продуктов, используя предоставленный список запасов.

    Args:
        stocks (list): Список словарей с информацией о запасах.
                       Каждый словарь должен содержать ключи, необходимые для API Ozon.
        client_id (str): Идентификатор клиента для аутентификации.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        dict: Словарь с ответом от API, содержащий информацию о статусе обновления запасов.

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился с ошибкой.

    Examples:
        Успешное выполнение:
            stocks = [
                {"offer_id": "offer_id_1", "stock": 10},
                {"offer_id": "offer_id_2", "stock": 5},
            ]
            client_id = 'example_client_id'
            seller_token = 'example_seller_token'
            response = update_stocks(stocks, client_id, seller_token)
            print(response)
            # Вывод:
            # {'result': 'success'}

        Неудачное выполнение:
            stocks = [
                {"offer_id": "offer_id_1", "stock": 10},
                {"offer_id": "offer_id_2", "stock": 5},
            ]
            client_id = 'example_client_id'
            seller_token = 'invalid_seller_token'
            try:
                response = update_stocks(stocks, client_id, seller_token)
            except requests.exceptions.HTTPError as e:
                print(f"Ошибка: {e}"
           # Возможный вывод:
           # Ошибка: 401 Client Error: Unauthorized for url: https://api-seller.ozon.ru/v1/product/import/stocks
       """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """
    Скачивает и извлекает данные о запасах часов из файла на сайте TimeWorld.

    Функция загружает zip-архив с указанного URL, извлекает содержимое, читает данные из Excel файла,
    и возвращает их в виде списка словарей. После этого удаляет временный Excel файл.

    Returns:
        list: Список остатков часов в виде словарей, где каждый словарь представляет одну запись из Excel файла.

    Raises:
        requests.exceptions.HTTPError: Если запрос на скачивание завершился с ошибкой.
        zipfile.BadZipFile: Если содержимое не является допустимым zip-архивом.
        FileNotFoundError: Если файл Excel не найден после извлечения.
        ValueError: Если файл Excel не содержит ожидаемого листа или данных.

    Examples:
        Успешное выполнение:
            watch_remnants = download_stock()
            print(watch_remnants)

            # Вывод:
            # [{'Артикул': '12345', 'Модель': 'Casio A158WA', 'Количество': 10, ...}, ...]

        Неудачное выполнение:
            try:
                watch_remnants = download_stock()
            except requests.exceptions.HTTPError as e:
                print(f"Ошибка загрузки: {e}")
            except zipfile.BadZipFile as e:
                print(f"Ошибка распаковки архива: {e}")
            except FileNotFoundError as e:
                print(f"Excel файл не найден: {e}")
            except ValueError as e:
                print(f"Ошибка чтения данных из Excel: {e}")

            # Возможный вывод:
            # Ошибка загрузки: 404 Client Error: Not Found for url: https://timeworld.ru/upload/files/ostatki.zip
            # или
            # Ошибка распаковки архива: File is not a zip file
            # или
            # Excel файл не найден: [Errno 2] No such file or directory: './ostatki.xls'
            # или
            # Ошибка чтения данных из Excel: No sheet named <'Sheet1'>
    """
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """
    Создает список остатков товаров на основе данных о запасах часов и списка идентификаторов предложений.

    Функция обрабатывает данные о запасах часов, сопоставляет их с идентификаторами предложений и формирует
    список остатков. Если количество товаров указано как ">10", запас устанавливается на 100.
    Если количество товаров равно "1", запас устанавливается на 0. В остальных случаях запас устанавливается
    в соответствии с указанным количеством. Для предложений, отсутствующих в данных о запасах, запас устанавливается на 0.

    Args:
        watch_remnants (list): Список остатков часов в виде словарей, где каждый словарь представляет
                               одну запись из Excel файла.
        offer_ids (list): Список идентификаторов предложений, для которых необходимо создать остатки.

    Returns:
        list: Список остатков товаров, где каждый элемент - словарь с ключами "offer_id" и "stock".

    Examples:
        # Успешное выполнение
            watch_remnants = [
                {"Код": "12345", "Количество": ">10"},
                {"Код": "67890", "Количество": "1"},
                {"Код": "54321", "Количество": "5"}
            ]
            offer_ids = ["12345", "67890", "54321", "09876"]

            stocks = create_stocks(watch_remnants, offer_ids)

            print(stocks)

            # Вывод:
            # [{'offer_id': '12345', 'stock': 100},
            #  {'offer_id': '67890', 'stock': 0},
            #  {'offer_id': '54321', 'stock': 5},
            #  {'offer_id': '09876', 'stock': 0}]

        # Неудачное выполнение: некорректный формат данных
            watch_remnants = [
                {"Код": "12345", "Количество": ">10"},
                {"Код": "67890", "Количество": "один"},
                {"Код": "54321", "Количество": "5"}
            ]
            offer_ids = ["12345", "67890", "54321", "09876"]

            try:
                stocks = create_stocks(watch_remnants, offer_ids)
            except ValueError as e:
                print(f"Ошибка: {e}")

            # Ожидаемый вывод:
            # Ошибка: invalid literal for int() with base 10: 'один'
    """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Создает список цен товаров на основе данных о запасах часов и списка идентификаторов предложений.

    Функция обрабатывает данные о запасах часов, сопоставляет их с идентификаторами предложений и формирует
    список цен. Цена конвертируется с помощью функции `price_conversion`. Для каждого предложения устанавливаются
    стандартные значения для полей "auto_action_enabled" и "currency_code".

    Args:
        watch_remnants (list): Список остатков часов в виде словарей, где каждый словарь представляет одну запись из Excel файла.
        offer_ids (list): Список идентификаторов предложений, для которых необходимо создать цены.

    Returns:
        list: Список цен товаров, где каждый элемент - словарь с ключами "auto_action_enabled", "currency_code", "offer_id", "old_price", "price".

    Examples:
        # Успешное выполнение
            def price_conversion(price_str):
                return price_str.replace(" ", "")

            watch_remnants = [
                {"Код": "12345", "Цена": "1 000"},
                {"Код": "67890", "Цена": "2 000"},
                {"Код": "54321", "Цена": "3 000"}
            ]
            offer_ids = ["12345", "67890", "54321", "09876"]

            prices = create_prices(watch_remnants, offer_ids)

            print(prices)

            # Вывод:
            # [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '12345', 'old_price': '0', 'price': '1000'},
            #  {'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '67890', 'old_price': '0', 'price': '2000'},
            #  {'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '54321', 'old_price': '0', 'price': '3000'}]

        # Неудачное выполнение: некорректный формат данных
            watch_remnants = [
                {"Код": "12345", "Цена": "1 000"},
                {"Код": "67890", "Цена": "две тысячи"},
                {"Код": "54321", "Цена": "3 000"}
            ]
            offer_ids = ["12345", "67890", "54321", "09876"]

            try:
                prices = create_prices(watch_remnants, offer_ids)
            except ValueError as e:
                print(f"Ошибка: {e}")

            # Ожидаемый вывод:
            # Ошибка: invalid literal for int() with base 10: 'две тысячи'
    """

    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """
    Конвертирует строку цены, удаляя все символы, кроме цифр.

    Функция принимает строку, представляющую цену, и удаляет из неё все нецифровые символы.
    Если цена содержит дробную часть, она отбрасывается.

    Args:
        price (str): Строка, представляющая цену, например, "1 000.50 руб."

    Returns:
        str: Строка, содержащая только цифры из исходной строки.

    Examples:
        # Успешное выполнение
            price = "1 000.50 руб."
            converted_price = price_conversion(price)
            print(converted_price)
            # Вывод:
            # "1000"

        # Успешное выполнение с другой строкой
            price = "2,345.67"
            converted_price = price_conversion(price)
            print(converted_price)
            # Вывод:
            # "2345"

        # Обработка пустой строки
            price = ""
            converted_price = price_conversion(price)
            print(converted_price)
            # Вывод:
            # ""

        # Неудачное выполнение: строка содержит точку в начале суммы
        price = "1.999 р, 90 коп"
        converted_price = price_conversion(price)
        print(converted_price)
        # Вывод:
        # "1"
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    Разделяет список на более мелкие списки фиксированной длины.

    Генерирует подсписки длины `n` из исходного списка `lst`. Последний подсписок может быть менее длины `n`,
    если количество элементов в исходном списке не делится нацело на `n`. API Озон ограничивает количество записей на
    изменение в одном post запросе записей, для этого и нужно разбивать списки словарей, которые идут в post на
    отрезки, например для метода api по обновлению цен ограничение в 1000 записеей, а для обновления остатков - 100.

    Args:
        lst (list): Исходный список, который нужно разделить.
        n (int): Длина каждого подсписка.

    Returns:
        list: Подсписки длины `n` из исходного списка `lst`.

    Examples:
        Успешное выполнение:
            lst = [1, 2, 3, 4, 5, 6, 7]
            n = 3

            for sublist in divide(lst, n):
                print(sublist)

            # Вывод:
            # [1, 2, 3]
            # [4, 5, 6]
            # [7]
        Неудачное выполнение: n меньше или равно 0
        lst = [1, 2, 3, 4, 5, 6, 7]
            n = 0
        ValueError: n должно быть больше 0

        Неудачное выполнение: нецелое число n
        lst = [1, 2, 3, 4, 5, 6, 7]
            n = 2.5
        TypeError: n должно быть целым числом
    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """
    Загружает и обновляет цены товаров на основе остатков и идентификаторов предложений.

    Получает id товаров, создает цены на основе остатков и id, и обновляет цены группами по 1000.

    Args:
        watch_remnants (list): Список остатков товаров, который нужно обработать.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        list: Список обновленных цен.

    Examples:
        watch_remnants = [{'id': '001', 'quantity': 10}, {'id': '002', 'quantity': 5}]
        client_id = 'example_client_id'
        seller_token = 'example_seller_token'

        prices = await upload_prices(watch_remnants, client_id, seller_token)
        print(prices)

        # Вывод:
        # [{'offer_id': '001', 'price': 100}, {'offer_id': '002', 'price': 50}, ...]
    Неудачное выполнение: пустой `client_id`
        watch_remnants = [{"item_id": "123", "price": 100}]
        client_id = ""
        seller_token = "token_abc123"
        prices = await upload_prices(watch_remnants, client_id, seller_token)
        # Вывод:
        # ValueError: `client_id` не может быть пустым

        Неудачное выполнение: пустой `seller_token`
        watch_remnants = [{"item_id": "123", "price": 100}]
        client_id = "client_001"
        seller_token = ""
        prices = await upload_prices(watch_remnants, client_id, seller_token)
        # Вывод:
        # ValueError: `seller_token` не может быть пустым
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """
    Загружает и обновляет остатки товаров на основе остатков и идентификаторов предложений.

    Получает идентификаторы предложений, создает остатки на основе остатков и идентификаторов, и обновляет остатки
    группами по 100. Возвращает список не пустых остатков и общий список остатков.

    Args:
        watch_remnants (list): Список остатков товаров для обработки.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        tuple: Кортеж, содержащий два списка:
            - list: Список остатков, где количество на складе не равно нулю.
            - list: Общий список остатков.

    Пример:
        watch_remnants = [{'id': '001', 'quantity': 10}, {'id': '002', 'quantity': 0}]
        client_id = 'example_client_id'
        seller_token = 'example_seller_token'

        not_empty, stocks = await upload_stocks(watch_remnants, client_id, seller_token)
        print(not_empty)
        print(stocks)

        # Вывод:
        # [{'offer_id': '001', 'stock': 10}]
        # [{'offer_id': '001', 'stock': 10}, {'offer_id': '002', 'stock': 0}]
    Неудачное выполнение: пустой список `watch_remnants`
        watch_remnants = []
        client_id = "client_001"
        seller_token = "token_abc123"
        not_empty, stocks = await upload_stocks(watch_remnants, client_id, seller_token)
        # Вывод:
        # ValueError: `watch_remnants` не может быть пустым списком

        Неудачное выполнение: пустой `client_id`
        watch_remnants = [{"item_id": "123", "stock": 10}]
        client_id = ""
        seller_token = "token_abc123"
        not_empty, stocks = await upload_stocks(watch_remnants, client_id, seller_token)
        # Вывод:
        # ValueError: `client_id` не может быть пустым

        Неудачное выполнение: пустой `seller_token`
        watch_remnants = [{"item_id": "123", "stock": 10}]
        client_id = "client_001"
        seller_token = ""
        not_empty, stocks = await upload_stocks(watch_remnants, client_id, seller_token)
        # Вывод:
        # ValueError: `seller_token` не может быть пустым
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """
    Основная функция для обновления остатков и цен товаров.

    Эта функция выполняет следующие шаги:
    1. Инициализирует переменные окружения.
    2. Получает токен продавца и идентификатор клиента.
    3. Получает идентификаторы предложений.
    4. Загружает остатки товаров.
    5. Обновляет остатки товаров партиями по 100 элементов.
    6. Создает и обновляет цены товаров партиями по 900 элементов.

    При возникновении исключений соответствующие ошибки обрабатываются и выводятся сообщения.

    Raises:
        requests.exceptions.ReadTimeout: Если превышено время ожидания запроса.
        requests.exceptions.ConnectionError: Если произошла ошибка соединения.
        Exception: Любые другие исключения, которые могут возникнуть.

    Examples:
        Успешное выполнение:

        main()
        (функция выполнит все шаги без ошибок и обновит остатки и цены товаров)

        Ошибка соединения:

        main()
        (выведет сообщение об ошибке соединения)

        Превышено время ожидания:

        main()
        (выведет сообщение о превышении времени ожидания)
        """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
