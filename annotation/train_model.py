import json

import torch
import torch.nn.functional as F
import numpy as np
from datasets import Dataset
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    Trainer,
    TrainingArguments
)
from sklearn.metrics import f1_score, precision_score, recall_score

# file paths
TRAIN_FILE = "./output.jsonl"
MODEL_NAME = "bert-base-uncased"
OUTPUT_DIR = "./"
# if resuming from checkpoint, set True. must be in same directory as output_dir
CHECKPOINT = False

# constant arguments
MAX_LEN = 512
EPOCHS = 12
BATCH = 8
LR = 2e-5
THRESHOLD = 0.3

# choose best device
if torch.backends.mps.is_available():
    DEVICE = torch.device("mps")
elif torch.cuda.is_available():
    DEVICE = torch.device("cuda")
else:
    DEVICE = torch.device("cpu")

print("Using device:", DEVICE)

texts = []
labels_raw = []

with open(TRAIN_FILE, encoding="utf8") as f:
    for line in f:
        row = json.loads(line)
        texts.append(row["text"])
        labels_raw.append(int(row["label"]))

unique_codes = sorted(set(labels_raw))
# set up comparison for the id of the category and it's actual code
code2id = {c:i for i,c in enumerate(unique_codes)}
id2code = {i:c for c,i in code2id.items()}

# upload id2code so you can decipher outputs
json.dump(id2code, open("id2code.json", "w"))

# convert subtopic to major topic
sub_to_major = {
    int(k): str(v)[:-2] for k,v in id2code.items()
}

major_codes = sorted(set(sub_to_major.values()))

# major code to id
major_to_id = {code: i for i, code in enumerate(major_codes)}
num_major_topics = len(major_codes)
num_subtopics = len(unique_codes)

# make matrix for major and sub topics
mapping_matrix = torch.zeros((num_subtopics, num_major_topics))

for sub_idx, major_str in sub_to_major.items():
    major_idx = major_to_id[major_str]
    mapping_matrix[sub_idx, major_idx] = 1.0

mapping_matrix = mapping_matrix.to(DEVICE)

NUM_LABELS = len(unique_codes)

labels = []

# get label vectors
for code in labels_raw:
    vec = [0.0]*NUM_LABELS
    vec[code2id[code]] = 1
    labels.append(vec)

# create dataset
dataset = Dataset.from_dict({
    "text": texts,
    "labels": labels
})

# split into training and testing
dataset = dataset.train_test_split(test_size=0.1, seed=42)

train_ds = dataset["train"]
test_ds = dataset["test"]

# get the model
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

# define tokenizer
def tokenize(batch):
    return tokenizer(
        batch["text"],
        truncation=True,
        padding="max_length",
        max_length=MAX_LEN
    )

# tokenize the dataset
train_ds = train_ds.map(tokenize, batched=True)
test_ds = test_ds.map(tokenize, batched=True)

train_ds.set_format("torch")
test_ds.set_format("torch")

model = AutoModelForSequenceClassification.from_pretrained(
    MODEL_NAME,
    num_labels=NUM_LABELS,
    problem_type="multi_label_classification",
)

model.to(DEVICE)

# loss function for the dataset
def hierarchy_aware_loss(outputs, labels, num_items_in_batch):
    logits = outputs.logits
    probs = torch.sigmoid(logits)

    # use the BCE w logit loss default
    bce = F.binary_cross_entropy_with_logits(logits, labels.float(), reduction="none")

    # hierarchize the loss so that it learns to also have other subtopics in the same topic
    # activated so that it prioritizes getting the major topic right rather than just guessing
    true_major = (torch.matmul(labels.float(), mapping_matrix) > 0).float()
    pred_major_prob = torch.matmul(probs, mapping_matrix).clamp(0, 1)

    major_gap = (true_major - pred_major_prob).clamp(min=0)

    hierarchy_penalty = torch.matmul(major_gap, mapping_matrix.t())

    final_weights = 1.0 + (0.5 * hierarchy_penalty)

    return (bce * final_weights).mean()

# compute metrics for evaluation
def compute_metrics(eval_pred):
    logits, labels = eval_pred
    probs = 1 / (1 + np.exp(-logits))
    preds = (probs >= THRESHOLD).astype(int)

    return {
        "micro_f1": f1_score(labels, preds, average="micro"),
        "macro_f1": f1_score(labels, preds, average="macro"),
        "micro_precision": precision_score(labels, preds, average="micro"),
        "macro_precision": precision_score(labels, preds, average="macro"),
        "micro_recall": recall_score(labels, preds, average="micro"),
        "macro_recall": recall_score(labels, preds, average="macro")
    }

# training args
args = TrainingArguments(
    output_dir=OUTPUT_DIR,
    per_device_train_batch_size=BATCH,
    per_device_eval_batch_size=BATCH,
    num_train_epochs=EPOCHS,
    learning_rate=LR,
    eval_strategy="epoch",
    save_strategy="epoch",
    logging_steps=100,
    fp16= DEVICE == torch.device("cuda"),
    report_to="none",
    load_best_model_at_end=True,
)

# trainer
trainer = Trainer(
    model=model,
    args=args,
    train_dataset=train_ds,
    eval_dataset=test_ds,
    processing_class=tokenizer,
    compute_metrics=compute_metrics,
    compute_loss_func=hierarchy_aware_loss
)

trainer.train(resume_from_checkpoint=CHECKPOINT)

trainer.save_model(OUTPUT_DIR)
tokenizer.save_pretrained(OUTPUT_DIR)

print("Training complete.")

# method to run prediction on a text
def predict(text, threshold=0):
    inputs = tokenizer(text, return_tensors="pt", truncation=True)
    inputs = {k:v.to(DEVICE) for k,v in inputs.items()}

    with torch.no_grad():
        logits = model(**inputs).logits

    probs = torch.sigmoid(logits)[0]

    results = []
    for i,p in enumerate(probs):
        if p >= threshold:
            results.append((id2code[i], float(p)))

    return sorted(results, key=lambda x:-x[1])
