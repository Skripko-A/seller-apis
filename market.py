import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """
    Получает список товарных предложений из API Яндекс Маркет

    Args:
        page (str): Токен страницы для получения. Используется для пагинации.
        campaign_id (str): ID кампании, для которой необходимо получить товарные предложения.
        access_token (str): Токен доступа для аутентификации запросов к API.

    Returns:
        dict: Словарь, содержащий результат вызова API с товарными предложениями.

    Raises:
        requests.exceptions.HTTPError: Если HTTP-запрос вернул неуспешный статус-код.

    Пример:
        >>> get_product_list("page_token", "campaign_id", "access_token")
        {'result': {...}, ...}

    Пример неудачного выполнения:
        >>> get_product_list("invalid_page", "invalid_campaign_id", "invalid_access_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url: https://api.partner.market.yandex.ru/campaigns/invalid_campaign_id/offer-mapping-entries?page_token=invalid_page&limit=200
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """
    Обновляет информацию о запасах товаров в API Яндекс

    Args:
        stocks (list): Список товаров с обновленной информацией о запасах.
        campaign_id (str): ID кампании, для которой необходимо обновить запасы товаров.
        access_token (str): Токен доступа для аутентификации запросов к API.

    Returns:
        dict: Словарь, содержащий результат вызова API с обновленной информацией о запасах.

    Raises:
        requests.exceptions.HTTPError: Если HTTP-запрос вернул неуспешный статус-код.

    Пример:
        >>> update_stocks([{"sku": "12345", "stock": 10}], "campaign_id", "access_token")
        {'result': {...}, ...}

    Пример неудачного выполнения:
        >>> update_stocks([{"sku": "invalid_sku", "stock": 10}], "invalid_campaign_id", "invalid_access_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url: https://api.partner.market.yandex.ru/campaigns/invalid_campaign_id/offers/stocks
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """
    Обновляет информацию о ценах товаров в API Яндекс Маркет

    Args:
        prices (list): Список товаров с обновленной информацией о ценах.
        campaign_id (str): ID кампании, для которой необходимо обновить цены товаров.
        access_token (str): Токен доступа для аутентификации запросов к API.

    Returns:
        dict: Словарь, содержащий результат вызова API с обновленной информацией о ценах.

    Raises:
        requests.exceptions.HTTPError: Если HTTP-запрос вернул неуспешный статус-код.

    Пример:
        >>> update_price([{"sku": "12345", "price": {"value": 1000, "currencyId": "RUR"}}], "campaign_id", "access_token")
        {'result': {...}, ...}

    Пример неудачного выполнения:
        >>> update_price([{"sku": "invalid_sku", "price": {"value": 1000, "currencyId": "RUR"}}], "invalid_campaign_id", "invalid_access_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url: https://api.partner.market.yandex.ru/campaigns/invalid_campaign_id/offer-prices/updates
        """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """
    Получить артикулы товаров Яндекс Маркета

    Args:
        campaign_id (str): ID кампании, для которой необходимо получить артикулы товаров.
        market_token (str): Токен доступа для аутентификации запросов к API.

    Returns:
        list: Список артикулов товаров

    Пример удачного выполнения:
        >>> get_offer_ids("valid_campaign_id", "valid_market_token")
        ['12345', '67890', ...]

    Пример неудачного выполнения:
        >>> get_offer_ids("invalid_campaign_id", "invalid_market_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url: ...
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """
    Создает список запасов для товаров на складе на основе остатков и артикулов.

    Args:
        watch_remnants (list): Список остатков товаров, содержащий словари с ключами "Код" и "Количество".
        offer_ids (list): Список артикулов товаров, которые загружены в маркет.
        warehouse_id (str): ID склада, на котором хранятся товары.

    Returns:
        list: Список запасов, где каждый элемент является словарем с информацией о товаре, его количестве и дате обновления.

    Пример удачного выполнения:
        >>> watch_remnants = [{"Код": "123", "Количество": "5"}, {"Код": "456", "Количество": ">10"}]
        >>> offer_ids = ["123", "456", "789"]
        >>> warehouse_id = "wh_001"
        >>> create_stocks(watch_remnants, offer_ids, warehouse_id)
        [
            {"sku": "123", "warehouseId": "wh_001", "items": [{"count": 5, "type": "FIT", "updatedAt": "2023-10-01T00:00:00Z"}]},
            {"sku": "456", "warehouseId": "wh_001", "items": [{"count": 100, "type": "FIT", "updatedAt": "2023-10-01T00:00:00Z"}]},
            {"sku": "789", "warehouseId": "wh_001", "items": [{"count": 0, "type": "FIT", "updatedAt": "2023-10-01T00:00:00Z"}]}
        ]

    Пример неудачного выполнения:
        >>> watch_remnants = [{"Код": "123", "Количество": "5"}, {"Код": "456", "Количество": ">10"}]
        >>> offer_ids = "123, 456, 789"  # offer_ids должен быть списком, а не строкой
        >>> warehouse_id = "wh_001"
        >>> create_stocks(watch_remnants, offer_ids, warehouse_id)
        Traceback (most recent call last):
        ...
        TypeError: 'str' object is not iterable
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Создает список цен для товаров на основе остатков и артикулов.

    Args:
        watch_remnants (list): Список остатков товаров, содержащий словари с ключами "Код" и "Цена".
        offer_ids (list): Список артикулов товаров (shopSku), которые загружены в маркет.

    Returns:
        list: Список цен, где каждый элемент является словарем с информацией о товаре и его цене.

    Пример удачного выполнения:
        >>> watch_remnants = [{"Код": "123", "Цена": "5000"}, {"Код": "456", "Цена": "15000"}]
        >>> offer_ids = ["123", "456"]
        >>> create_prices(watch_remnants, offer_ids)
        [
            {"id": "123", "price": {"value": 5000, "currencyId": "RUR"}},
            {"id": "456", "price": {"value": 15000, "currencyId": "RUR"}}
        ]

    Пример неудачного выполнения:
        >>> watch_remnants = [{"Код": "123", "Цена": "5000"}, {"Код": "456", "Цена": "15000"}]
        >>> offer_ids = "123, 456"  # offer_ids должен быть списком, а не строкой
        >>> create_prices(watch_remnants, offer_ids)
        Traceback (most recent call last):
        ...
        TypeError: 'str' object is not iterable

        >>> watch_remnants = [{"Код": "123", "Цена": "5000"}, {"Код": "456", "Цена": "15000"}]
        >>> offer_ids = ["123", "456"]
        >>> create_prices(watch_remnants, offer_ids)
        Traceback (most recent call last):
        ...
        NameError: name 'price_conversion' is not defined  # price_conversion должна быть определена функцией
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """
    Загружает цены для товаров на маркетплейс.

    Args:
        watch_remnants (list): Список остатков товаров, содержащий словари с ключами "Код" и "Цена".
        campaign_id (str): Идентификатор кампании на маркетплейсе.
        market_token (str): Токен для доступа к API маркетплейса.

    Returns:
        list: Список всех загруженных цен.

    Пример удачного выполнения:
        >>> watch_remnants = [{"Код": "123", "Цена": "5000"}, {"Код": "456", "Цена": "15000"}]
        >>> campaign_id = "campaign_123"
        >>> market_token = "token_abc123"
        >>> await upload_prices(watch_remnants, campaign_id, market_token)
        [
            {"id": "123", "price": {"value": 5000, "currencyId": "RUR"}},
            {"id": "456", "price": {"value": 15000, "currencyId": "RUR"}}
        ]

    Пример неудачного выполнения:
        >>> watch_remnants = [{"Код": "123", "Цена": "5000"}, {"Код": "456", "Цена": "15000"}]
        >>> campaign_id = "campaign_123"
        >>> market_token = 12345  # market_token должен быть строкой, а не числом
        >>> await upload_prices(watch_remnants, campaign_id, market_token)
        Traceback (most recent call last):
        ...
        TypeError: 'int' object is not iterable

        >>> watch_remnants = [{"Код": "123", "Цена": "5000"}, {"Код": "456", "Цена": "15000"}]
        >>> campaign_id = "invalid_campaign_id"
        >>> market_token = "token_abc123"
        >>> await upload_prices(watch_remnants, campaign_id, market_token)
        Traceback (most recent call last):
        ...
        SomeAPIError: Campaign ID is invalid  # Ошибка, если идентификатор кампании недействителен
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """
    Загружает остатки товаров на маркетплейс и возвращает списки всех остатков и не пустых остатков.

    Args:
        watch_remnants (list): Список остатков товаров, содержащий словари с ключами "Код" и "Количество".
        campaign_id (str): Идентификатор кампании на маркетплейсе.
        market_token (str): Токен для доступа к API маркетплейса.
        warehouse_id (str): Идентификатор склада.

    Returns:
        tuple: Кортеж из двух списков:
            - не пустые остатки товаров (list),
            - все остатки товаров (list).

    Пример удачного выполнения:
        >>> watch_remnants = [{"Код": "123", "Количество": "10"}, {"Код": "456", "Количество": "0"}]
        >>> campaign_id = "campaign_123"
        >>> market_token = "token_abc123"
        >>> warehouse_id = "warehouse_456"
        >>> await upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id)
        (
            [{"id": "123", "items": [{"count": 10}]}],
            [
                {"id": "123", "items": [{"count": 10}]},
                {"id": "456", "items": [{"count": 0}]}
            ]
        )

    Пример неудачного выполнения:
        >>> watch_remnants = [{"Код": "123", "Количество": "10"}, {"Код": "456", "Количество": "0"}]
        >>> campaign_id = "campaign_123"
        >>> market_token = 12345  # market_token должен быть строкой, а не числом
        >>> warehouse_id = "warehouse_456"
        >>> await upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id)
        Traceback (most recent call last):
        ...
        TypeError: 'int' object is not iterable

        >>> watch_remnants = [{"Код": "123", "Количество": "10"}, {"Код": "456", "Количество": "0"}]
        >>> campaign_id = "invalid_campaign_id"
        >>> market_token = "token_abc123"
        >>> warehouse_id = "warehouse_456"
        >>> await upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id)
        Traceback (most recent call last):
        ...
        SomeAPIError: Campaign ID is invalid  # Ошибка, если идентификатор кампании недействителен
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """
        Основная функция для обновления остатков и цен на маркетплейсе для кампаний FBS и DBS.

        Функция выполняет следующие шаги:
        1. Получает настройки окружения (токены, идентификаторы кампаний и складов).
        2. Загружает текущие остатки товаров.
        3. Обновляет остатки и цены для кампаний FBS и DBS.

        Пример удачного выполнения:
            >>> main()
            (Функция выполнится без ошибок, обновив остатки и цены для кампаний FBS и DBS)

        Пример неудачного выполнения:
            >>> main()
            Превышено время ожидания...
            (Ошибка возникает при превышении времени ожидания ответа от API)

            >>> main()
            ConnectionError: ('Connection aborted.', RemoteDisconnected('Remote end closed connection without response')) Ошибка соединения
            (Ошибка возникает при проблемах с соединением к API)

            >>> main()
            KeyError: 'MARKET_TOKEN' ERROR_2
            (Ошибка возникает, если переменная окружения 'MARKET_TOKEN' не задана)

        Исключения:
            requests.exceptions.ReadTimeout: Возникает при превышении времени ожидания ответа от API.
            requests.exceptions.ConnectionError: Возникает при проблемах с соединением к API.
            Exception: Общая ошибка, выводится с сообщением "ERROR_2".
        """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
