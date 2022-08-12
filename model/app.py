from flask import Flask, request, jsonify
from predictor import Predictor
from property_index import PropertyIndex

app = Flask(__name__)
property_index = PropertyIndex("property_index.txt")
predictor = Predictor(property_index=property_index)

@app.route('/predict', methods=["POST"])
def predict():  # put application's code here
    texts = request.json
    top_k = int(request.args.get("k"))
    if top_k is None:
        top_k = 3
    predictions = predictor.predict(texts, top_k)
    return jsonify(predictions)


if __name__ == '__main__':
    app.run()
