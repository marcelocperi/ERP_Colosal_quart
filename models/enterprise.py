from sqlalchemy import Column, Integer, String, Boolean, Text
from multiMCP.database import Base

class Enterprise(Base):
    __tablename__ = 'sys_enterprises'

    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre = Column(String(100), nullable=False)
    cuit = Column(String(20), nullable=False, unique=True)
    email = Column(String(100))
    telefono = Column(String(50))
    direccion = Column(String(255))
    localidad = Column(String(100))
    provincia = Column(String(100))
    activo = Column(Boolean, default=True)

    def __repr__(self):
        return f"<Enterprise(nombre='{self.nombre}', cuit='{self.cuit}')>"
