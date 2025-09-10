from typing import Optional

import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from utils.logger import logger


class EmailClassifier:
    def __init__(self, model_path=None):
        self.tokenizer = AutoTokenizer.from_pretrained(model_path or "bert-base-uncased")
        self.model = AutoModelForSequenceClassification.from_pretrained(model_path or "bert-base-uncased", num_labels=3)

    def categorize_email(self, subject: Optional[str], body: str):
        text = f"{subject} {body}"
        inputs = self.tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=512)
        logger.debug(f"inputs: {inputs}, type: {type(inputs)}")
        logger.debug("------------------------------------")

        with torch.no_grad():
            outputs = self.model(**inputs)

            logger.debug(f"outputs: {outputs}")
            logger.debug("------------------------------------")

        probabilities = torch.softmax(outputs.logits, dim=1)
        confidence, predicted_class = torch.max(probabilities, dim=1)

        logger.debug(f"confidence: {confidence}")
        logger.debug("------------------------------------")

        logger.debug(f"predicted_class: {predicted_class}")
        logger.debug("------------------------------------")

        categories = {0: "question", 1: "refund", 2: "other"}
        return categories[predicted_class.item()], confidence.item()
