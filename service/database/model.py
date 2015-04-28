from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, DateTime, String, Boolean

Base = declarative_base()


# In case of a schema change, a new model should be created and used by whatever
#  needs it, leaving this one unchanged
class TitleRegisterData(Base):
    __tablename__ = 'title_register_data'

    title_number = Column(String(10), primary_key=True)
    register_data = Column(postgresql.JSON(), nullable=True)
    last_modified = Column(DateTime(), nullable=False)
    is_deleted = Column(Boolean(), nullable=False)
