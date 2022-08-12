import pathlib

import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification


class Predictor:
    def __init__(self, property_index, model_path="ontology-matching-base-uncased", device="cpu"):
        self.property_index = property_index
        self.device = device
        model_path = pathlib.Path(model_path).absolute()
        self.tokenizer = AutoTokenizer.from_pretrained(model_path)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_path).to(self.device)

    def predict(self, texts, k):
        # prepare our text into tokenized sequence
        inputs = self.tokenizer(texts, padding=True, truncation=True, return_tensors="pt").to(self.device)
        # perform inference to our model
        outputs = self.model(**inputs)
        # get output probabilities by doing softmax
        if len(outputs) == 0:
            return []
        probs = outputs[0].softmax(dim=1).sum(dim=0).multiply(1 / len(texts))
        # executing argmax function to get the candidate label
        probs, idxs = probs.topk(k)
        # Unwrap the tensor
        idxs = idxs.tolist()
        probs = probs.tolist()
        return [{'key': self.property_index.get_property_name(idxs[i]), 'prob': probs[i]} for i in range(len(idxs))]
