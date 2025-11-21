import pandas as pd

def depth_calculation(tumor_df, normal_df=None) -> pd.Series:
    """
    DEPTH heterogeneity score.
    Arguments:
        tumor_df: genes x tumor samples
        normal_df: genes x normal samples (can be None or empty)

    Returns:
        pd.Series: DEPTH score per tumor sample
    """
    if tumor_df is None or tumor_df.empty:
        return pd.Series(dtype=float)

    # Reference mean: use normal if present, else tumor mean
    if normal_df is not None and not normal_df.empty:
        mean_ref = normal_df.mean(axis=1, skipna=True)
    else:
        mean_ref = tumor_df.mean(axis=1, skipna=True)

    # Squared deviation of each tumor sample from reference mean
    score = (tumor_df.sub(mean_ref, axis=0) ** 2)

    # DEPTH score = std across genes for each tumor sample
    depth_scores = score.std(axis=0, skipna=True)
    # print(depth_scores.head())

    return depth_scores
