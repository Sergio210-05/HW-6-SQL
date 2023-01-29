import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship
import ast

Base = declarative_base()


class Publisher(Base):
    __tablename__ = 'publisher'

    id_publisher = sq.Column(sq.Integer, primary_key=True)
    publisher_name = sq.Column(sq.VARCHAR(length=40), nullable=False)

    def __init__(self, pk, name):
        self.id_publisher = pk
        self.publisher_name = name

    def __str__(self):
        return f'{self.id_publisher}: {self.publisher_name}'


class Book(Base):
    __tablename__ = 'book'

    id_book = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.VARCHAR, nullable=False)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey('publisher.id_publisher'), nullable=False)

    publisher = relationship(Publisher, backref='books')

    def __init__(self, pk, title, id_pub):
        self.id_book = pk
        self.title = title
        self.id_publisher = id_pub

    def __str__(self):
        return self.title


class Shop(Base):
    __tablename__ = 'shop'

    id_shop = sq.Column(sq.Integer, primary_key=True)
    shop_name = sq.Column(sq.VARCHAR, nullable=False)

    def __init__(self, pk, s_name):
        self.id_shop = pk
        self.shop_name = s_name

    def __str__(self):
        return self.shop_name


class Stock(Base):
    __tablename__ = 'stock'

    id_stock = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey('book.id_book'), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey('shop.id_shop'), nullable=False)
    count = sq.Column(sq.Integer)

    book = relationship(Book, backref='stocks_b')
    shop = relationship(Shop, backref='stocks_sh')

    def __init__(self, pk, id_shop, id_book, count):
        self.id_stock = pk
        self.id_book = id_book
        self.id_shop = id_shop
        self.count = count


class Sale(Base):
    __tablename__ = 'sale'

    id_sale = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.NUMERIC, nullable=False)
    date_sale = sq.Column(sq.DATE, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey('stock.id_stock'), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)

    stock = relationship(Stock, backref='sales')

    def __init__(self, pk, price, date, count, id_stock):
        self.id_sale = pk
        self.price = price
        self.date_sale = date
        self.id_stock = id_stock
        self.count = count

    def __str__(self):
        return f'{self.price * self.count} | {self.date_sale}'


def create_tables(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    print('Tables has successful created')


def filling_tables(session, file_json):
    with open(file_json, 'rt', encoding='utf8') as file_td:
        td = ast.literal_eval(file_td.read())

    objects = {
        "publisher": Publisher,
        "book": Book,
        "shop": Shop,
        "stock": Stock,
        "sale": Sale
    }
    rc = {}

    for index, record in enumerate(td):
        args = [field for field in record['fields'].values()]
        rc[index] = objects[record['model']](record['pk'], *args)
        session.add(rc[index])

    session.commit()
    print('Tables filling completed')


def name_by_id(session, publisher_id):
    name = session.query(Publisher).filter(Publisher.id_publisher == publisher_id).all()
    return name[0].publisher_name


def publisher_realization(session):
    producer = input('Enter publisher name or publisher id: ')
    if producer.isdigit() and int(producer) in (i[0] for i in session.query(Publisher.id_publisher)):
        producer = name_by_id(session, int(producer))

    authors = session.query(Publisher, Book, Stock, Shop, Sale). \
        join(Book, Publisher.id_publisher == Book.id_publisher). \
        join(Stock, Book.id_book == Stock.id_book). \
        join(Shop, Stock.id_shop == Shop.id_shop). \
        join(Sale, Stock.id_stock == Sale.id_stock). \
        filter(Publisher.publisher_name == producer).all()
    if len(authors) == 0:
        print('No such publisher')
    else:
        for writer in authors:
            print(f'{writer[0].publisher_name} | {str(writer[1])} | {str(writer[3])} | {str(writer[4])}')


if __name__ == '__main__':
    with open('authentication.txt', 'rt', encoding='utf8') as au:
        login_pass = ast.literal_eval(au.read())
    DSN = f"postgresql://{login_pass['login']}:{login_pass['password']}" \
          f"@localhost:{login_pass['localhost']}/{login_pass['bd_name']}"
    engine = sq.create_engine(DSN)
    create_tables(engine)
