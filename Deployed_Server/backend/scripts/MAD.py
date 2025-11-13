import pandas as pd
import numpy as np


def mad_calculation(df):
    try:
        if df.shape[1] <= 1:  # Single sample or no samples
            return pd.Series(0, index=df.index)
        expr_array = df.values.astype(np.float32)
        mean_per_gene = np.mean(expr_array, axis=1)
        abs_dev = np.abs(expr_array - mean_per_gene[:, np.newaxis])
        mad_per_gene = np.mean(abs_dev, axis=1)
        mad_per_gene = np.where(np.isnan(mad_per_gene), 0, mad_per_gene)
        # mad_per_gene = mad_per_gene.fillna(0).replace([np.inf, -np.inf], 0)  # Replace NaN, inf with 0
        return pd.Series(mad_per_gene, index=df.index)
        # return mad_per_gene 
    except Exception as e:
        print(f"[ERROR] mad_calculation failed: {e}")
        return pd.Series(0, index=df.index)
    # mean = values.mean()
    # return (values - mean).abs().mean()