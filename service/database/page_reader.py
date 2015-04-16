from sqlalchemy.orm import Session, sessionmaker
from service.database.model import TitleRegisterData
from service import db

# If an updater needs to read data in a different way, it should use a different page reader
def get_next_data_page(last_title_number, last_modification_date, page_size):
    session = Session(bind=db)

    try:
        page_query = session.query(TitleRegisterData).filter(
            (
                (TitleRegisterData.last_modified == last_modification_date)
                & (TitleRegisterData.title_number > last_title_number)
            )
            | (TitleRegisterData.last_modified > last_modification_date)
        ).order_by(
            TitleRegisterData.last_modified,
            TitleRegisterData.title_number
        ).limit(page_size)

        return page_query.all()
    finally:
        session.close()



    

