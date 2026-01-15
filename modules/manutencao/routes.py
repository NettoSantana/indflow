from flask import Blueprint, render_template

# =====================================================
# AUTH
# =====================================================
from modules.admin.routes import login_required

manutencao_bp = Blueprint("manutencao", __name__, template_folder="templates")

@manutencao_bp.route("/")
@login_required
def home():
    return render_template("manutencao_home.html")
