import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer


class EmailClassifier:
    def __init__(self, model_path=None):
        self.tokenizer = AutoTokenizer.from_pretrained(model_path or "bert-base-uncased")
        self.model = AutoModelForSequenceClassification.from_pretrained(model_path or "bert-base-uncased", num_labels=3)

    def categorize_email(self, subject, body):
        text = f"{subject} {body}"
        inputs = self.tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=512)

        with torch.no_grad():
            outputs = self.model(**inputs)

        probabilities = torch.softmax(outputs.logits, dim=1)
        confidence, predicted_class = torch.max(probabilities, dim=1)

        categories = {0: "question", 1: "refund", 2: "other"}
        return categories[predicted_class.item()], confidence.item()
