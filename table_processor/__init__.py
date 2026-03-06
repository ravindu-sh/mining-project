import pandas as pd


def get_related_cells(
    df: pd.DataFrame, row_idx: int, col_idx: int
) -> tuple[list, list]:
    row_cells = []
    col_cells = []

    for i in range(row_idx - 1, -1, -1):
        row_cells.append(df.iloc[i, col_idx])

    for i in range(row_idx + 1, df.shape[0] - 1):
        row_cells.append(df.iloc[i, col_idx])

    for j in range(col_idx - 1, -1, -1):
        col_cells.append(df.iloc[row_idx, j])

    for j in range(col_idx + 1, df.shape[1] - 1):
        col_cells.append(df.iloc[row_idx, j])

    return row_cells, col_cells
