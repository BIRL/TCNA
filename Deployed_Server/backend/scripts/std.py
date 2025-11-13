import pandas as pd
import numpy as np 

def std_calculation(df):
    try:
        if df.shape[1] <= 1:  # Single sample or no samples
            return pd.Series(0, index=df.index)
        std = df.std(axis=1, ddof=1)  # ddof=1 for sample standard deviation
        std = std.fillna(0).replace([np.inf, -np.inf], 0)  # Replace NaN, inf with 0
        return std
    except Exception as e:
        print(f"[ERROR] std_calculation failed: {e}")
        return pd.Series(0, index=df.index)
    # return df.std(ddof=1)
