import pandas as pd
import numpy as np

def cv_calculation(df):
    try:
        if df.shape[1] <= 1:  # Single sample or no samples
            return pd.Series(0, index=df.index)
        mean = df.mean(axis=1)
        std = df.std(axis=1, ddof=1)  # ddof=1 for sample standard deviation
        cv = std / mean
        cv = cv.fillna(0).replace([np.inf, -np.inf], 0)  # Replace NaN, inf with 0
        return cv*100
    except Exception as e:
        print(f"[ERROR] cv_calculation failed: {e}")
        return pd.Series(0, index=df.index)
    # std = df.std(ddof1=1)
    # mean = df.mean()
    # return (std / mean) * 100 if mean != 0 else np.nan
