from sqlalchemy import Column, DateTime, String, Boolean  # type: ignore
from sqlalchemy.dialects import postgresql                # type: ignore
from sqlalchemy.ext.declarative import declarative_base   # type: ignore

Base = declarative_base()


# In case of a schema change, a new model should be created and used by whatever
#  needs it, leaving this one unchanged
class TitleRegisterData(Base):  # type: ignore
    __tablename__ = 'title_register_data'

    title_number = Column(String(10), primary_key=True)
    register_data = Column(postgresql.JSON(), nullable=True)
    last_modified = Column(DateTime(), nullable=False)
    is_deleted = Column(Boolean(), nullable=False)
