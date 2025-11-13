import pandas as pd
import numpy as np

def cv2_calculation(df):
    try:
        if df.shape[1] <= 1:  # Single sample or no samples
            return pd.Series(0, index=df.index)
        mean = df.mean(axis=1)
        std = df.std(axis=1, ddof=1)  # ddof=1 for sample standard deviation
        cv = std / mean
        cv = cv.fillna(0).replace([np.inf, -np.inf], 0)  # Replace NaN, inf with 0
        return (cv*100)**2
    except Exception as e:
        print(f"[ERROR] cv2_calculation failed: {e}")
        return pd.Series(0, index=df.index)