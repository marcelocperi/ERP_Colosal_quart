from multiMCP.database import Base
from .enterprise import Enterprise
from .msac import CmpSourcingOrigenes, CmpArticulosProveedores

# This ensures all models are registered with Base
__all__ = ["Base", "Enterprise", "CmpSourcingOrigenes", "CmpArticulosProveedores"]
