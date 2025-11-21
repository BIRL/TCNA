import pandas as pd
import numpy as np
from scipy.stats import zscore

def depth2_calculation(expr_matrix, normal=None):
    # z-score per row (gene expression across samples)
    z_scores = zscore(expr_matrix, axis=1, nan_policy='omit')
    abs_z_scores = np.abs(z_scores)
    depth2_scores = abs_z_scores.std(axis=0)   # std deviation per sample

    return pd.Series(depth2_scores, index=expr_matrix.columns)
