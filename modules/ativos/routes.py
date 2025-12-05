from flask import Blueprint, render_template

ativos_bp = Blueprint("ativos", __name__, template_folder="templates")

@ativos_bp.route("/")
def home():
    return render_template("ativos_home.html")
