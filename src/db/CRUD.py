from datetime import datetime
from typing import Annotated, Any, Dict, List, Optional

import requests  # type: ignore
from fastapi import Depends
from pydantic import TypeAdapter
from sqlalchemy import select, delete
from sqlalchemy.orm import Session

from src.common.Schemas.location_schemas import LocationProductSchema
from src.common.logger import logger
from src.common.vector_store import vector_store
from src.db.database import engine, get_db
from src.db.Models import Base, Location, LocationProduct, Product


def create_db() -> str:
    """
    Создает базу данных и таблицы, если они не существуют.
    Если база уже существует, ничего не делает.

    :return: Сообщение о результате операции
    """
    try:
        Base.metadata.create_all(bind=engine)
    except Exception as exp:
        if "already exists" in str(exp):
            logger.info("Database already exists")
            return "Database already exists"
        else:
            logger.error("Failed to create database: %s", exp)
            raise
    else:
        return "Database created successfully"


# def drop_db() -> str:
#     """
#     Удаляет все таблицы из базы данных.
#
#     :return: Сообщение о результате операции
#     """
#     try:
#         Base.metadata.drop_all(bind=engine)
#     except Exception as exp:
#         if "does not exist" in str(exp):
#             logger.info("Database does not exist")
#             return "Database does not exist"
#         else:
#             logger.error("Failed to drop database: %s", exp)
#             raise
#     else:
#         return "Database dropped successfully"


def __get_json_from_url(
    address: str,
    params: Optional[Dict[Any, Any]] = None,
    headers: Optional[Dict[Any, Any]] = None,
) -> Any:
    """
    Отправляет GET-запрос по указанному URL и возвращает ответ в формате JSON.

    :param address: URL для запроса
    :param params: (опционально) параметры запроса
    :param headers: (опционально) заголовки запроса
    :return: Ответ в формате dict (JSON)
    :raises: requests.RequestException, ValueError
    """
    response = requests.get(address, params=params, headers=headers)
    response.raise_for_status()  # выбросит исключение, если код ответа не 2xx
    return response.json()


def __get_location_products_from_json(
    json_data: Optional[Dict[Any, Any]],
) -> List[LocationProductSchema]:
    """
    Получает список LocationProductSchema из JSON-данных.
    :param json_data: Словарь с ключом "Products", содержащим список продуктов
    :return: Список LocationProductSchema
    """
    if not json_data:
        raise ValueError(
            "JSON data is required. Please provide a valid JSON dictionary."
        )
    location_products = TypeAdapter(List[LocationProductSchema]).validate_python(
        json_data["Products"]
    )
    return location_products


def update_db(
    db: Annotated[Session, Depends(get_db)],
    json_url: str = "",
    json_data: Optional[Dict[Any, Any]] = None,
) -> int:
    """
    Обновляет базу данных с bulk-операциями для ускорения массовой загрузки.

    :param json_url: URL с JSON-данными
    :param json_data: (опционально) JSON-данные
    :param db: SQLAlchemy session
    :return: Количество добавленных записей
    """
    if not json_data:
        json_data = __get_json_from_url(json_url)
    pydantic_list_of_products = __get_location_products_from_json(json_data)
    counter = 0

    # Загружаем все существующие продукты и аптеки в память через scalars
    products = db.scalars(select(Product)).all()
    locations = db.scalars(select(Location)).all()
    existing_products = {p.name: p.id for p in products}
    existing_locations = {p.address: p.id for p in locations}

    # Собираем новые продукты и аптеки, исключая дубликаты внутри пачки
    new_product_names = set()
    new_location_addresses = set()
    new_products = []
    new_locations = []

    for item in pydantic_list_of_products:
        p_name = item.product.name
        ph_addr = item.location.address
        if p_name not in existing_products and p_name not in new_product_names:
            new_products.append(Product(name=p_name))
            new_product_names.add(p_name)
        if ph_addr not in existing_locations and ph_addr not in new_location_addresses:
            new_locations.append(Location(address=ph_addr))
            new_location_addresses.add(ph_addr)

    # Bulk insert новых продуктов и аптек
    if new_products:
        db.bulk_save_objects(new_products)
    if new_locations:
        db.bulk_save_objects(new_locations)
    db.commit()

    # Обновим словари id через scalars
    products = db.scalars(select(Product)).all()
    locations = db.scalars(select(Location)).all()
    existing_products = {p.name: p.id for p in products}
    existing_locations = {p.address: p.id for p in locations}

    # Очищаем таблицу LocationProduct
    db.execute(delete(LocationProduct))
    db.commit()

    # Формируем уникальные связи
    seen_links = set()
    pharm_prod_prices = []

    for item in pydantic_list_of_products:
        try:
            price_product = int(item.price)
        except Exception as exp:
            logger.error("Price error: %s | Product: %s", exp, item)
            continue

        p_name = item.product.name.strip()
        ph_addr = item.location.address.strip()
        product_id = existing_products.get(p_name)
        location_id = existing_locations.get(ph_addr)

        if not product_id or not location_id:
            continue

        key = (product_id, location_id)
        if key in seen_links:
            continue  # Пропускаем дубликаты
        seen_links.add(key)

        pharm_prod_prices.append(
            LocationProduct(
                product_id=product_id, location_id=location_id, price=price_product
            )
        )
        counter += 1
    if pharm_prod_prices:
        db.bulk_save_objects(pharm_prod_prices)
    db.commit()

    # Обновление vector store по понедельникам с 4-5 утра
    now = datetime.now()
    if now.weekday() == 0 and (4 <= now.hour <= 5):
        logger.info("Starting to rebuild vector store")
        status_update = update_vector_store()
        logger.info("Vector store rebuilt status: %s", status_update)

    return counter


def get_all_locations_by_product_name(product_name: str) -> Any:
    """
    Поиск аптек по названию продукта
    :param product_name: Название продукта
    :return: Список аптек
    """
    db = next(get_db())
    # Поиск id продукта по имени

    product = db.scalar(select(Product).where(Product.name.ilike(f"%{product_name}%")))
    if not product:
        return []
    # Поиск аптеки, где есть этот продукт
    query = (
        select(Location)
        .join(LocationProduct, Location.id == LocationProduct.location_id)
        .where(LocationProduct.product_id == product.id)
    )
    locations = db.scalars(query).all()
    return locations


def get_product_price(product_name: str, location_address: str) -> Any:
    """
    Поиск цены продукта в конкретной аптеке
    :param product_name: Название продукта
    :param location_address: Адрес аптеки
    :return: Цена продукта или None, если не найдено
    """
    db = next(get_db())
    product = db.scalar(select(Product).where(Product.name.ilike(f"%{product_name}%")))
    if not product:
        return None
    location = db.scalar(select(Location).where(Location.address == location_address))
    if not location:
        return None
    location_product = db.scalar(
        select(LocationProduct).where(
            LocationProduct.product_id == product.id,
            LocationProduct.location_id == location.id,
        )
    )
    if not location_product:
        return None
    return location_product.price


def get_products_by_name(product_name: str) -> Optional[List[str]]:
    db = next(get_db())
    products = db.scalars(
        select(Product).where(Product.name.ilike(f"%{product_name.lower()}%"))
    )
    if products:
        return [product.name for product in products]
    return None


def get_all_products() -> Optional[List[str]]:
    db = next(get_db())
    products = db.scalars(select(Product)).all()
    if products:
        return [product.name for product in products]
    return None


def update_vector_store() -> Any:
    products_names = get_all_products()
    if products_names:
        status_message = vector_store.rebuild_vector_store(
            products_names=products_names
        )
        return status_message
    return "No products found"
