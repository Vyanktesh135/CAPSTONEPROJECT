from sqlalchemy import Column,Integer,String,DateTime
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import UUID,JSONB
from sqlalchemy.sql import func
import uuid

Base = declarative_base()

class DatabaseMetadata(Base):
    __tablename__ = "metadata_table"

    id = Column(UUID(as_uuid=True),primary_key=True,default=uuid.uuid4)
    file_name = Column(String,nullable=False)
    table_name = Column(String,nullable=False,unique=True)
    table_metadata =Column(JSONB,nullable=False,default=dict)
    created_at = Column(DateTime(timezone=True),default=func.now())