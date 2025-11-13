import pandas as pd
# 5. DEPTH-ith score calculation
# def depth_ith_calculation(expr_matrix):
#     score_matrix = (expr_matrix - expr_matrix.mean(axis=0)) ** 2
#     heterogeneity_score = score_matrix.std(axis=0)

#     return heterogeneity_score
# def depth_calculation(tumor_df: pd.DataFrame, normal_df: pd.DataFrame) -> pd.Series:
#     """
#     Compute DEPTH heterogeneity score for each tumor sample.
#     - tumor_df: genes x tumor samples
#     - normal_df: genes x normal samples (can be None)
    
#     Returns:
#         pd.Series indexed by tumor sample names with DEPTH scores
#     """
#     if tumor_df is None or tumor_df.empty:
#             return pd.Series(dtype=float)

#         # Calculate reference mean
#     if normal_df is not None and not normal_df.empty:
#             mean_ref = normal_df.mean(axis=0, skipna=True)
#     else:
#             mean_ref = tumor_df.mean(axis=0, skipna=True)

#         # Score matrix: squared deviation of each tumor sample from mean_ref
#     score = (tumor_df.sub(mean_ref, axis=1) ** 2)

#         # Heterogeneity score = SD across genes for each tumor sample
#     heterogeneity_score = score.std(axis=1, skipna=True)
#     # depth_ith_df = pd.DataFrame({'Sample': heterogeneity_score.index, 'depth_ith': heterogeneity_score.values})
#     # depth_ith_df.to_csv("/Users/rubabshah/Desktop/lums research/metrics_csv/Liver/depth_per_sample.csv", index = False)

#     return heterogeneity_score


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