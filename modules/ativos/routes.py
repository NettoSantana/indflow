from flask import Blueprint, render_template

from modules.admin.routes import login_required

ativos_bp = Blueprint("ativos", __name__, template_folder="templates")

@ativos_bp.route("/")
@login_required
def home():
    return render_template("ativos_home.html")
