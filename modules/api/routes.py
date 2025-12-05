from flask import Blueprint, jsonify

api_bp = Blueprint("api", __name__)

@api_bp.route("/ping")
def ping():
    return jsonify({"status": "ok", "msg": "API IndFlow ativa"})
