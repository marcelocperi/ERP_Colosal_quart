from flask import Blueprint

contabilidad_bp = Blueprint('contabilidad', __name__, template_folder='templates')

from . import routes
