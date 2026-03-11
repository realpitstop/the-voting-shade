import torch
import json
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from typing import List

DEFLT_MEANINGS_PATH="./../data/clean/stances.jsonl"

class PolicyClassifier:
    def __init__(self, meanings_path=DEFLT_MEANINGS_PATH, model_path="../annotation/topic_classifier_model",
                 id2code_path="../annotation/id2code.json",
                 threshold=0.5, thresh2=0.2):

        self.meanings = {}
        with open(meanings_path, 'r') as f:
            for line in f:
                data = json.loads(line)
                self.meanings[int(data['code'])] = data['text']

        if torch.backends.mps.is_available():
            self.device = torch.device("mps")
        elif torch.cuda.is_available():
            self.device = torch.device("cuda")
        else:
            self.device = torch.device("cpu")
        self.threshold = threshold
        self.thresh2 = thresh2

        self.tokenizer = AutoTokenizer.from_pretrained(model_path)

        self.model = AutoModelForSequenceClassification.from_pretrained(
            model_path,
            torch_dtype=torch.float16 if self.device == "mps" else torch.float32
        )
        self.model.to(self.device)
        self.model.eval()

        with open(id2code_path) as f:
            self.id2code = json.load(f)

    def get_policy_codes_batch(self, texts: List[str], batch_size: int = 64) -> List[List[str]]:
        if not texts:
            return []

        inputs = self.tokenizer(
            texts,
            truncation=True,
            padding=True,
            max_length=512,
            return_tensors="pt"
        ).to(self.device)

        with torch.inference_mode():
            logits = self.model(**inputs).logits
            probs = torch.sigmoid(logits.float()).cpu().tolist()

        all_batch_results = []
        for prob_set in probs:
            codes = []
            backup = None

            for i, p in enumerate(prob_set):
                code = self.id2code[str(i)]
                if p >= self.threshold:
                    codes.append(code)
                elif p >= self.thresh2:
                    if backup is None or p > backup[1]:
                        backup = (code, p)

            final_codes = codes if codes else ([backup[0]] if backup else [])
            all_batch_results.append(final_codes)

        return all_batch_results

    def get_meaning(self, code: str) -> str:
        return self.meanings.get(int(code), "Unknown")
