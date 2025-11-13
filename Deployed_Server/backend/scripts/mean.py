import pandas as pd
import numpy as np 

def mean_calculation(df):
    try:
        if df.shape[1] <= 1:  # Single sample or no samples
            return pd.Series(0, index=df.index)
        mean = df.mean(axis=1)
        mean = mean.fillna(0).replace([np.inf, -np.inf], 0)  # Replace NaN, inf with 0
        return mean
    except Exception as e:
        print(f"[ERROR] mean_calculation failed: {e}")
        return pd.Series(0, index=df.index)
    # return df.mean()