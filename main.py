import sqlalchemy
import ast
import psycopg2
from sqlalchemy.orm import sessionmaker

from models import create_tables, filling_tables, publisher_realization

with open('authentication.txt', 'rt', encoding='utf8') as au:
    login_pass = ast.literal_eval(au.read())

DSN = f"postgresql://{login_pass['login']}:{login_pass['password']}" \
      f"@localhost:{login_pass['localhost']}/{login_pass['bd_name']}"

if __name__ == '__main__':
    engine = sqlalchemy.create_engine(DSN)
    create_tables(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    filling_tables(session=session, file_json='tests_data.json')
    publisher_realization(session=session)

    session.close()
