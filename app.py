from flask import Flask, request, jsonify
import subprocess

app = Flask(__name__)

@app.route("/")
def home():
    return "Crawler is running"

@app.route("/crawl")
def run_crawler():
    try:
        subprocess.run(["python", "crawler.py"], check=True)

        return jsonify({
            "status": "success",
            "message": "Crawler executed successfully"
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
