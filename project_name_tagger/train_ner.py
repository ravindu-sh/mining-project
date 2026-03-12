import json
import random
import pickle
import spacy
from spacy.tokens import DocBin
from spacy.training.example import Example
import ast
import re
import pandas as pd
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional
from html_processor import get_article
from spacy.matcher import PhraseMatcher
from tqdm.notebook import tqdm

# Expect TRAIN_DATA to be defined in the module namespace before running.
# TRAIN_DATA = [("text", {"entities": [(start, end, "PROJECT")]})]
TRAIN_DATA = globals().get("TRAIN_DATA", [])


def make_spacy_dataset(csv_path: str):
    df = pd.read_csv(csv_path)

    dataset: List[Tuple[str, List[Tuple[int, int, str]]]] = []

    for _, row in tqdm(df.iterrows(), total=len(df)):
        path = str(row.get("path", ""))
        raw_projects = str(row.get("project_names", ""))

        if pd.isna(path) or pd.isna(raw_projects):
            continue

        try:
            parsed_projects = ast.literal_eval(raw_projects)
        except Exception:
            continue

        if isinstance(parsed_projects, (list, tuple)):
            projects = [str(p) for p in parsed_projects]
        else:
            projects = [str(parsed_projects)]
        projects.sort()

        with open(path, "r") as html_file:
            title, _, _, body = get_article(
                html_file.read(),
                redact_tables=True,
                convert_to_text=True,
                preserved_tags_on_text_convert=set(),
            )

        if not (body or title):
            continue

        sentences = re.split(r"[.!?]+(?:\s+|[.!?]+)", str(body)) if body else []
        sentences.append(title)
        sentences = [s.strip() for s in sentences]

        for sentence in sentences:
            entities: List[Tuple[int, int, str]] = []
            for proj in projects:
                proj = str(proj).strip()
                if not proj:
                    continue

                joiners = r"[ \-_.]+"
                pattern = joiners.join(map(re.escape, proj.split()))
                regex = re.compile(pattern, re.IGNORECASE)

                for match in regex.finditer(sentence):
                    start, end = match.start(), match.end()
                    overlap = False
                    for ent_start, ent_end, _ in entities:
                        if not (end <= ent_start or start >= ent_end):
                            overlap = True
                            break
                    if not overlap:
                        entities.append((start, end, "PROJECT"))
            dataset.append((sentence, entities))

    return dataset


def to_jsonl(dataset: List[Tuple[str, List[Tuple[int, int, str]]]], output_path: str):
    with open(output_path, "w") as f:
        for text, entities in dataset:
            json_line = {
                "text": text,
                "entities": [[s, e, l] for s, e, l in entities],
            }
            f.write(json.dumps(json_line) + "\n")


def load_data(csv_path: str) -> List[Tuple[str, Dict[str, Any]]]:
    out: List[Tuple[str, Dict[str, Any]]] = []
    csv_file = Path(csv_path)
    base_dir = csv_file.parent

    df = pd.read_csv(csv_file)

    for _, row in tqdm(df.iterrows(), total=len(df)):
        path = str(row.get("path", ""))
        raw_projects = str(row.get("project_names", ""))

        if not path or not raw_projects:
            continue

        try:
            with open(path, "r") as file:
                title, provider, published_date, body = get_article(
                    file.read(), redact_tables=True, convert_to_text=True
                )
        except:
            continue
        text = f"TITLE: {title}, PROVIDER: {provider}, PUBLISHED_DATE: {published_date}, BODY: {body}"
        projects = []
        if raw_projects:
            try:
                parsed = ast.literal_eval(raw_projects)
                if isinstance(parsed, (list, tuple)):
                    projects = [str(p) for p in parsed]
                else:
                    projects = [str(parsed)]
            except Exception:
                # fallback: remove surrounding brackets and split on comma
                s = raw_projects.strip()
                if s.startswith("[") and s.endswith("]"):
                    s = s[1:-1]
                projects = [p.strip().strip("'\"") for p in s.split(",") if p.strip()]

        entities: List[Tuple[int, int, str]] = []

        # Use spaCy PhraseMatcher for robust, token-aligned matching
        nlp_match = spacy.blank("en")
        matcher = PhraseMatcher(nlp_match.vocab, attr="LOWER")
        patterns = []
        for proj in projects:
            proj = str(proj).strip()
            if not proj:
                continue
            patterns.append(nlp_match.make_doc(proj))

        if patterns:
            # ensure no duplicate rule
            try:
                matcher.remove("PROJECT")
            except Exception:
                pass
            matcher.add("PROJECT", patterns)
            doc = nlp_match(text)
            for _, start, end in matcher(doc):
                span = doc[start:end]
                entities.append((span.start_char, span.end_char, "PROJECT"))

        if entities:
            entities = _resolve_overlaps(entities)
            out.append((text, {"entities": entities}))

    return out


def _resolve_overlaps(
    entities: List[Tuple[int, int, str]],
) -> List[Tuple[int, int, str]]:
    """Remove overlapping spans preferring longer spans.

    Greedy algorithm: sort candidate spans by length (descending) then by
    start position (ascending). Accept a span if it does not overlap any
    previously accepted span.
    """
    # Deduplicate exact spans first
    uniq = list({(s, e, l) for s, e, l in entities})
    # Sort by length desc, then start asc
    uniq.sort(key=lambda x: (-(x[1] - x[0]), x[0]))

    accepted: List[Tuple[int, int, str]] = []
    for s, e, l in uniq:
        conflict = False
        for as_, ae, _ in accepted:
            # overlap if not (e <= as_ or s >= ae)
            if not (e <= as_ or s >= ae):
                conflict = True
                break
        if not conflict:
            accepted.append((s, e, l))

    # return accepted sorted by start position
    accepted.sort(key=lambda x: x[0])
    return accepted


def train_project_ner(
    train_data, output_dir="model_project_only", n_iter=20, dropout=0.2
):
    if not train_data:
        raise SystemExit("TRAIN_DATA is empty — provide labeled examples in TRAIN_DATA")

    random.shuffle(train_data)
    nlp = spacy.blank("en")
    ner = nlp.add_pipe("ner")
    ner.add_label("PROJECT")

    optimizer = nlp.begin_training()

    for it in range(n_iter):
        random.shuffle(train_data)
        losses = {}
        batches = spacy.util.minibatch(
            train_data, size=spacy.util.compounding(4.0, 32.0, 1.5)
        )
        for batch in batches:
            examples = []
            for text, ann in batch:
                doc = nlp.make_doc(text)
                examples.append(Example.from_dict(doc, ann))
            nlp.update(examples, sgd=optimizer, drop=dropout, losses=losses)
        print(f"Iter {it + 1}/{n_iter} Losses: {losses}")

    nlp.to_disk(output_dir)
    return nlp


def train_project_ner_sm(
    data,
    output_dir="project_name_tagger/sm-pretrained",
    n_iter=20,
    dropout=0.4,
    dev_ratio=0.2,
    seed=42,
):
    if not data:
        raise SystemExit("DATA is empty — provide labeled examples in DATA")

    # Shuffle and split data
    random.seed(seed)
    random.shuffle(data)
    split = int(len(data) * (1 - dev_ratio))
    train_data = data[:split]
    dev_data = data[split:]

    print(f"Train size: {len(train_data)}, Dev size: {len(dev_data)}")

    nlp = spacy.load("en_core_web_lg")
    if "ner" in nlp.pipe_names:
        nlp.remove_pipe("ner")
    ner = nlp.add_pipe("ner", last=True)
    ner.add_label("PROJECT")

    optimizer = nlp.initialize()
    optimizer.learn_rate = 0.0001
    best_f = 0.1

    for itn in range(n_iter):
        random.shuffle(train_data)
        losses = {}
        batches = spacy.util.minibatch(
            train_data, size=spacy.util.compounding(4.0, 32.0, 1.5)
        )
        for batch in batches:
            examples = []
            for text, ann in batch:
                doc = nlp.make_doc(text)
                examples.append(Example.from_dict(doc, ann))
            nlp.update(examples, sgd=optimizer, drop=dropout, losses=losses)
        print(f"Iter {itn + 1}/{n_iter} Losses: {losses}")

        # Validation
        if dev_data:
            scorer = nlp.evaluate(
                [Example.from_dict(nlp.make_doc(text), ann) for text, ann in dev_data]
            )
            f_score = scorer["ents_f"]
            print(f"Validation (dev) score at epoch {itn + 1}: {f_score} F-score")
            if f_score > best_f:
                best_f = f_score
                nlp.to_disk(output_dir)
                print(f"New best model saved with F-score: {best_f}")

    return nlp


def train_data_to_spacy_docbin():
    nlp = spacy.blank("en")
    with open("ner_train_data.pkl", "rb") as f:
        training_data = pickle.load(f)
    db = DocBin()
    for text, annotations in training_data:
        doc = nlp(text)
        ents = []
        for start, end, label in annotations:
            span = doc.char_span(start, end, label=label)
            ents.append(span)
        doc.ents = ents
        db.add(doc)
    db.to_disk("./train.spacy")


if __name__ == "__main__":
    model = train_project_ner(load_data("data.csv"))
    print(model("Review Project Orion and Nova."))
