from flask import Blueprint, render_template

producao_bp = Blueprint("producao", __name__, template_folder="templates")

@producao_bp.route("/")
def home():
    return render_template("producao_home.html")
