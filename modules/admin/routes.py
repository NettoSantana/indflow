from flask import Blueprint, render_template

admin_bp = Blueprint("admin", __name__, template_folder="templates")

@admin_bp.route("/")
def home():
    return render_template("admin_home.html")
