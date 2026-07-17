"""Laplace Multidocument — one app: control tower + ground-truth annotation.

A single Flask app serving one SPA. The topbar's day_id selector drives every
tab, annotation included, so annotating a sampled file moves the gate counter
in the same page and the same process.

  /            control-tower SPA (8 tabs, annotation is one of them)
  /dashboard   evaluation dashboard (standalone: its PNG export needs a pinned
               colour context, which fights the SPA's light/dark theme)
  /api/*       pipeline: batches, jobs, gate, errors, SFTP, review, files
  /api/annotate/*  annotation: worklist, page images, save + evaluate

Run locally:  python app.py   →  http://localhost:8000
Auth: Databricks SDK profile, or DATABRICKS_HOST + DATABRICKS_TOKEN.
"""
from __future__ import annotations

from flask import Flask, render_template

from src.annotate.routes import bp as annotate_bp
from src.pipeline.routes import bp as pipeline_bp

app = Flask(__name__)
app.register_blueprint(pipeline_bp)
app.register_blueprint(annotate_bp)


@app.get("/")
def index():
    return render_template("control_tower.html")


@app.get("/dashboard")
def dashboard():
    return render_template("dashboard.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
