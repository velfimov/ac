"""
В таблице 'orders' находится 15млн записей. Необходимо проверить все заказы в статусе 'hold' пачками по 100шт.
Статус заказа проверяется в функции 'mark_random_orders_accepted', эта функция ставит рандомное кол-во заказов
в статус 'accepted', т.е. '1'. Кол-во, переведенных в статус 'accepted' заказов неизвестно.
Необходимо написать оптимальное решение. Нельзя выгружать все заказы в память(вызов .all() в SQLAlchemy). 
"""
import random

from sqlalchemy import BigInteger, Column, Integer, Unicode, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

Base = declarative_base()
engine = None


class Order(Base):
    __tablename__ = 'orders'

    id = Column(BigInteger, nullable=False, primary_key=True, autoincrement=True)
    name = Column(Unicode, nullable=False)
    state = Column(Integer, nullable=False, index=True)   # accepted = 1, hold = 0


def mark_random_orders_accepted(session, orders):
    update_orders = random.sample(orders, random.randint(0, len(orders)))

    mappings = [
        {
            'id': o.id,
            'state': 1
        } for o in update_orders

    ]
    session.bulk_update_mappings(Order, mappings)


def process(session, chunk_size):
    filter = {'state': 0}

    orders = []
    for counter, order in enumerate(session.query(Order).filter_by(**filter).yield_per(chunk_size), 1):
        orders.append(order)

        if counter % chunk_size == 0:
            mark_random_orders_accepted(session, orders)
            orders = []

    if orders:
        mark_random_orders_accepted(session, orders)

    session.commit()


if __name__ == '__main__':
    engine = create_engine("postgresql+psycopg2://adcombo:adcombo@localhost/adcombo", echo=True)

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    session = Session(engine)

    # заполняем таблицу тестовыми данными
    for chunk in range(0, 100000, 10000):
        session.bulk_insert_mappings(Order, [
            {
                'id': i,
                'name': 'order %d' % i,
                'state': 0
            } for i in range(chunk, chunk + 10000)
        ])
    session.commit()

    """ Выше были подготовительные действия для тестирования кода """

    process(session, chunk_size=100)
