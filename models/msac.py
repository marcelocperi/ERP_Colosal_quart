
from sqlalchemy import Column, Integer, String, Text, DateTime, Numeric, ForeignKey, Index, func, Boolean
from multiMCP.database import Base

class CmpSourcingOrigenes(Base):
    __tablename__ = 'cmp_sourcing_origenes'

    id = Column(Integer, primary_key=True, autoincrement=True)
    enterprise_id = Column(Integer, nullable=False, index=True)
    nombre = Column(String(100), nullable=False)
    descripcion = Column(Text)
    impuestos_arancel_pct = Column(Numeric(19, 4), default=0.0000)
    flete_estimado_pct = Column(Numeric(19, 4), default=0.0000)
    seguro_estimado_pct = Column(Numeric(19, 4), default=0.0000)
    activo = Column(Boolean, default=True)
    
    # Audit standards
    user_id = Column(Integer)
    created_at = Column(DateTime, default=func.now())
    user_id_update = Column(Integer)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

class CmpArticulosProveedores(Base):
    """
    Maestro de vinculación de Artículos con sus distintos Proveedores (Sourcing Hardening).
    Permite definir qué proveedor ofrece un ítem, a qué precio y con qué lead time.
    Ahora incluye el Origen para determinar reglas impositivas/flete.
    """
    __tablename__ = 'cmp_articulos_proveedores'

    id = Column(Integer, primary_key=True, autoincrement=True)
    enterprise_id = Column(Integer, nullable=False, index=True)
    articulo_id = Column(Integer, nullable=False, index=True)
    proveedor_id = Column(Integer, nullable=False, index=True)
    origen_id = Column(Integer, index=True)
    
    codigo_articulo_proveedor = Column(String(50))
    precio_referencia = Column(Numeric(19, 4), default=0.0000)
    moneda = Column(String(10), default='ARS')
    lead_time_dias = Column(Integer, default=0)
    es_habitual = Column(Boolean, default=False)
    
    # Audit standards
    user_id = Column(Integer)
    created_at = Column(DateTime, default=func.now())
    user_id_update = Column(Integer)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index('unique_art_prov', 'enterprise_id', 'articulo_id', 'proveedor_id', unique=True),
    )
