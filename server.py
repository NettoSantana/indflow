from flask import Flask, render_template, redirect, url_for, request, flash

app = Flask(__name__)
app.secret_key = "alterar_antes_de_produzir"

@app.route("/")
def index():
    return render_template("base.html")

if __name__ == "__main__":
    app.run(debug=True)

