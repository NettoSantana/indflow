from flask import Blueprint, render_template

devices_bp = Blueprint("devices", __name__, template_folder="templates")

@devices_bp.route("/")
def home():
    return render_template("devices_home.html")
