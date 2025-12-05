from flask import Blueprint, render_template

manutencao_bp = Blueprint("manutencao", __name__, template_folder="templates")

@manutencao_bp.route("/")
def home():
    return render_template("manutencao_home.html")
