from flask import Blueprint, jsonify

from modules.admin.routes import login_required

api_bp = Blueprint("api", __name__)

@api_bp.route("/ping")
@login_required
def ping():
    return jsonify({"status": "ok", "msg": "API IndFlow ativa"})
