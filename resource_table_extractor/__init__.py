import pandas as pd
from table_processor import get_related_cells
import re
from pprint import pprint

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
    "measured and indicated",
    "measured & indicated",
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
}

proven_keywords = {
    "proven",
    "prov",
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
    "p&p",
    "p/p",
    "p / p",
}

metric_keywords = {
    "tonnage": tonnage_keywords,
    "grade": grade_keywords,
    "contained": contained_keywords,
}

category_keywords = {
    "prov_prob": prov_prob_keywords,
    "meas_ind": meas_ind_keywords,
    "measured": measured_keywords,
    "indicated": indicated_keywords,
    "inferred": inferred_keywords,
    "probable": probable_keywords,
    "proven": proven_keywords,
}

commodity_df = pd.read_csv("keywords/commodity_keywords.csv")
commodity_keywords: dict[str, set[str]] = {}
for idx, row in commodity_df.iterrows():
    commodity_keywords[(str(row["name"]).lower())] = set(
        [str(kw).lower().strip() for kw in str(row["keywords"]).split(",")]
    )

measurement_units_df = pd.read_csv("keywords/measurement_units_keywords.csv")
measurement_units_keywords: dict[str, set[str]] = {}
for idx, row in measurement_units_df.iterrows():
    measurement_units_keywords[(str(row["name"]).lower())] = set(
        [str(kw).lower().strip() for kw in str(row["keywords"]).split(",")]
    )

minetype_df = pd.read_csv("keywords/minetype_keywords.csv")
minetype_keywords: dict[str, set[str]] = {}
for idx, row in minetype_df.iterrows():
    minetype_keywords[(str(row["name"]).lower())] = set(
        [str(kw).lower().strip() for kw in str(row["keywords"]).split(",")]
    )

oretype_df = pd.read_csv("keywords/oretype_keywords.csv")
oretype_keywords: dict[str, set[str]] = {}
for idx, row in oretype_df.iterrows():
    oretype_keywords[(str(row["name"]).lower())] = set(
        [str(kw).lower().strip() for kw in str(row["keywords"]).split(",")]
    )

processtype_df = pd.read_csv("keywords/processtype_keywords.csv")
processtype_keywords: dict[str, set[str]] = {}
for idx, row in processtype_df.iterrows():
    processtype_keywords[(str(row["name"]).lower())] = set(
        [str(kw).lower().strip() for kw in str(row["keywords"]).split(",")]
    )

outer = {
    "metric": metric_keywords,
    "category": category_keywords,
    "commodity": commodity_keywords,
    "measurement": measurement_units_keywords,
    "minetype": minetype_keywords,
    "oretype": oretype_keywords,
    "processtype": processtype_keywords,
}

pprint(outer)


def match_keywords(
    df: pd.DataFrame,
    key_dict: dict,
    row: list[str],
    col: list[str],
    col_name: str = "not specified",
):
    out = []
    sample_value = next(iter(key_dict.values()))
    if type(sample_value) is dict:
        for key, sub_dict in key_dict.items():
            out.extend(match_keywords(df, sub_dict, row, col, str(key)))
        return out
    search_items = [
        (keyword, category)
        for category, keywords in key_dict.items()
        for keyword in keywords
    ]
    search_items.sort(key=lambda x: len(x[0]), reverse=True)
    for keyword, category in search_items:
        found_in_row_idx = -1
        for i, cell in enumerate(row):
            if re.search(r"\b(?:" + keyword + r")\b", cell):
                found_in_row_idx = i
                break

        found_in_col_idx = -1
        for i, cell in enumerate(col):
            if re.search(r"\b(?:" + keyword + r")\b", cell):
                found_in_col_idx = i
                break

        if found_in_col_idx < 0 and found_in_row_idx < 0:
            continue
        out.append((col_name, category, min(found_in_row_idx, found_in_col_idx)))
    return out


def matches_to_df(matches: list[tuple[str, str, int]]) -> pd.DataFrame:
    categories = set(match[0] for match in matches)
    out_df = pd.DataFrame(columns=list(categories))
    out_df.loc[0] = [None] * len(out_df.columns)

    matches.sort(key=lambda x: x[2])
    for col_name, category, found_idx in matches:
        if not pd.isna(out_df.at[0, col_name]):
            continue
        out_df.at[0, col_name] = category

    return out_df


def parse_table(df: pd.DataFrame):
    out_df = pd.DataFrame(
        columns=[
            "value",
            "metric",
            "category",
            "commodity",
            "measurement",
            "minetype",
            "oretype",
            "processtype",
        ]
    )
    rows, cols = df.shape
    for i in range(rows):
        for j in range(cols):
            cell_value = str(df.iat[i, j]).lower()
            if len(cell_value) > 100:
                continue
            if sum(c.isdigit() for c in cell_value) < len(cell_value) / 2:
                continue
            row_cells, col_cells = get_related_cells(df, i, j)

            row_cells = [str(cell).lower() for cell in row_cells]
            col_cells = [str(cell).lower() for cell in col_cells]

            matches = match_keywords(df, outer, row_cells, col_cells)

            if len(matches) == 0:
                continue

            df_row = matches_to_df(matches)
            df_row["value"] = cell_value
            out_df = pd.concat([out_df, df_row], ignore_index=True)

    if len(out_df.dropna(subset=["metric", "category"])) == 0:
        return None

    return out_df


# parse_table(pd.DataFrame(), metric_keywords, [], [])

# pprint(
#     {
#         "tonnage_keywords": tonnage_keywords,
#         "grade_keywords": grade_keywords,
#         "contained_keywords": contained_keywords,
#         "measured_keywords": measured_keywords,
#         "indicated_keywords": indicated_keywords,
#         "inferred_keywords": inferred_keywords,
#         "meas_ind_keywords": meas_ind_keywords,
#         "probable_keywords": probable_keywords,
#         "proven_keywords": proven_keywords,
#         "prov_prob_keywords": prov_prob_keywords,
#         "metric_keywords": metric_keywords,
#         "category_keywords": category_keywords,
#         "commodity_keywords": commodity_keywords,
#         "measurement_units_keywords": measurement_units_keywords,
#     }
# )


#
# def get_found_keys(text: str, keywords_dict: dict[str, set[str]]) -> set[str]:
#     found_keys = set()
#     for key, keywords in keywords_dict.items():
#         if not keywords:
#             continue
#         pattern = r"\b(?:" + "|".join(re.escape(kw) for kw in keywords) + r")\b"
#         if re.search(pattern, text, re.IGNORECASE):
#             found_keys.add(key)
#     return found_keys
#
#
# def matched_keywords(
#     cell_values: list[str], keyword_dict: dict[str, set[str]]
# ) -> list[set[str]]:
#     out = []
#     for cell_value in cell_values:
#         out.append(get_found_keys(cell_value, keyword_dict))
#     return out
#
#
# def get_first_found_keyword(
#     matches: list[set[str]],
# ) -> tuple[tuple[str | None, int], tuple[str | None, int]]:
#     metric = (None, sys.maxsize)
#     category = (None, sys.maxsize)
#     for cell_idx, keys in enumerate(matches):
#         if metric[0] is None:
#             found = next(iter(keys & metric_keywords.keys()), None)
#             if found:
#                 metric = (found, cell_idx)
#         if category[0] is None:
#             found = next(iter(keys & category_keywords.keys()), None)
#             if found:
#                 category = (found, cell_idx)
#         if metric[0] is not None and category[0] is not None:
#             break
#     return (metric, category)
#
#
# def get_first_keyword_and_index(
#     matches: list[set[str]], keyword_dict: dict[str, set[str]]
# ) -> tuple[str | None, int]:
#     for cell_idx, keys in enumerate(matches):
#         found = next(iter(keys & keyword_dict.keys()), None)
#         if found:
#             return (found, cell_idx)
#     return (None, sys.maxsize)
#
#
# def extract_resource_table(df: pd.DataFrame) -> pd.DataFrame | None:
#     out_df = pd.DataFrame(columns=["value", "metric", "category"])
#     rows, cols = df.shape
#     for i in range(rows):
#         for j in range(cols):
#             cell_value = str(df.iloc[i, j]).lower()
#             if sum(c.isdigit() for c in cell_value) < len(cell_value) / 2:
#                 continue
#             row_cells, col_cells = get_related_cells(df, i, j)
#
#             row_cells = [str(cell).lower() for cell in row_cells]
#             col_cells = [str(cell).lower() for cell in col_cells]
#
#             # print("cells", row_cells, col_cells)
#
#             metric_row_matches = matched_keywords(row_cells, metric_keywords)
#             metric_col_matches = matched_keywords(col_cells, metric_keywords)
#             category_row_matches = matched_keywords(row_cells, category_keywords)
#             category_col_matches = matched_keywords(col_cells, category_keywords)
#
#             # print("matches", row_matches, col_matches)
#
#             row_metric = get_first_keyword_and_index(
#                 metric_row_matches, metric_keywords
#             )
#             row_category = get_first_keyword_and_index(
#                 category_row_matches, category_keywords
#             )
#             col_metric = get_first_keyword_and_index(
#                 metric_col_matches, metric_keywords
#             )
#             col_category = get_first_keyword_and_index(
#                 category_col_matches, category_keywords
#             )
#
#             # row_metric, row_category = get_first_found_keyword(row_matches)
#             # col_metric, col_category = get_first_found_keyword(col_matches)
#
#             # print("first found", row_metric, row_category, col_metric, col_category)
#
#             metric = (row_metric, col_metric)
#             category = (row_category, col_category)
#
#             combinations = {(0, 0): 0, (0, 1): 0, (1, 0): 0, (1, 1): 0}
#
#             for key in combinations.keys():
#                 combinations[key] = metric[key[0]][1] + category[key[1]][1]
#
#             combination = min(combinations, key=combinations.get)
#
#             metric = metric[combination[0]][0]
#             category = category[combination[1]][0]
#
#             if metric is None or category is None:
#                 continue
#
#             out_df = pd.concat(
#                 [
#                     out_df,
#                     pd.DataFrame(
#                         {
#                             "value": [cell_value],
#                             "metric": [metric],
#                             "category": [category],
#                         }
#                     ),
#                 ],
#                 ignore_index=True,
#             )
#
#     return out_df
