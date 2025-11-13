import pandas as pd
import numpy as np
from scipy.stats import zscore

# DEPTH2 score calculation
# def depth2_calculation(expr_matrix, normal=None):
#     # z_scores = (expr_matrix - expr_matrix.mean()) / expr_matrix.std()
#     z_scores = zscore(expr_matrix, axis=0)
#     abs_z_scores = np.abs(z_scores)
#     depth2_scores = abs_z_scores.std(axis=0)
#     print(depth2_scores.head())
#     return pd.Series(depth2_scores, index=expr_matrix.columns)
def depth2_calculation(expr_matrix, normal=None):
    # z-score per row (gene expression across samples)
    # print(expr_matrix.head())
    z_scores = zscore(expr_matrix, axis=1, nan_policy='omit')
    abs_z_scores = np.abs(z_scores)
    depth2_scores = abs_z_scores.std(axis=0)   # std deviation per sample
    # print(depth2_scores.head())
    return pd.Series(depth2_scores, index=expr_matrix.columns)
