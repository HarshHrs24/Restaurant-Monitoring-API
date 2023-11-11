# Import necessary libraries and modules
from urllib import response
from flask import Flask, render_template, request, jsonify
import pymongo
import openai


# Define a route for the root URL ("/") using the GET HTTP method
app = Flask(__name__)
@app.get("/")
def index_get():
    return render_template("base.html")


# Run the Flask app if this script is executed directly
if __name__ == "__main__":
    app.run(debug=True)
    







