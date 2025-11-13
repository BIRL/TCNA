import pandas as pd
import numpy as np 

def log_calculation(tumor_df, normal_df):
    try:
        if tumor_df.empty or normal_df.empty or tumor_df.shape[1] <= 1 or normal_df.shape[1] <= 1:
            return pd.Series(0, index=tumor_df.index if not tumor_df.empty else normal_df.index)
        tumor_mean = tumor_df.mean(axis=1)
        normal_mean = normal_df.mean(axis=1)
        logfc = tumor_mean - normal_mean  # Using difference as per your code
        # logfc = logfc.fillna(0).replace([np.inf, -np.inf], 0)
        return logfc
    except Exception as e:
        print(f"[ERROR] log_calculation failed: {e}")
        return pd.Series(0, index=tumor_df.index if not tumor_df.empty else normal_df.index)