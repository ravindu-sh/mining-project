import pandas as pd
import re

tonnage_keywords = {
    "tonnage",
    "tonnes",
    "tons",
    "ton",
    "tonn",
}

grade_keywords = {"grade", "avg", "average"}

contained_keywords = {"contained", "cont", "metal", "content"}

measured_keywords = {
    "measured",
    "meas",
    "m",
}

indicated_keywords = {
    "indicated",
    "ind",
    "i",
}

inferred_keywords = {
    "inferred",
    "inf",
    "infe",
}

meas_ind_keywords = {
    "meas_ind",
    "meas-ind",
    "meas ind",
    "meas/ind",
    "meas / ind",
    "meas&ind",
    "meas & ind",
    "m+i",
    "m + i",
    "mi",
    "m/i",
    "m / i",
    "m&i",
    "m & i",
}

probable_keywords = {
    "probable",
    "prob",
    "proba",
    "pro",
    "p",
}

proven_keywords = {
    "proven",
    "prov",
    "pro",
    "p",
}

prov_prob_keywords = {
    "prov_prob",
    "prov-prob",
    "prov prob",
    "prov/prob",
    "prov / prob",
    "proven + probable",
    "proven & probable",
    "proven/probable",
    "proven / probable",
    "p + p",
    "p & p",
    "p/p",
    "p / p",
}

keyword_set1 = list(tonnage_keywords | grade_keywords | contained_keywords)
keyword_set2 = list(
    measured_keywords
    | indicated_keywords
    | inferred_keywords
    | meas_ind_keywords
    | probable_keywords
    | proven_keywords
    | prov_prob_keywords
)

# print(keyword_set1)
# print(keyword_set2)


def is_keyword_in_text(keywords: list, text: str) -> bool:
    pattern = r"\b(?:" + "|".join(re.escape(kw) for kw in keywords) + r")\b"
    return re.search(pattern, text) is not None


# TODO: optimize with dynamic programming and regex
def is_resource_table(df: pd.DataFrame) -> bool:
    rows = df.index
    cols = df.columns
    for i in range(len(rows)):
        for j in range(len(cols)):
            current_cell = str(df.iat[i, j])
            if sum(c.isdigit() for c in current_cell) < len(current_cell) / 2:
                continue
            set1 = False
            set2 = False
            for ii in range(len(rows)):
                ahead_cell = str(df.iat[ii, j]).lower()
                if not set1:
                    set1 = is_keyword_in_text(keyword_set1, ahead_cell)
                if not set2:
                    set2 = is_keyword_in_text(keyword_set2, ahead_cell)
                if set1 and set2:
                    return True
            for jj in range(len(cols)):
                ahead_cell = str(df.iat[i, jj]).lower()
                if not set1:
                    set1 = is_keyword_in_text(keyword_set1, ahead_cell)
                if not set2:
                    set2 = is_keyword_in_text(keyword_set2, ahead_cell)
                if set1 and set2:
                    return True
    return False
