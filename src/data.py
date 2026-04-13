import numpy as np
import pandas as pd


def transform_application_table(df):
    """
    Function only designed for lightgbm algorithm that accepts null values
    """
    print(f"Cols Count before: {len(df.columns)}")
    
    # Missing goods price flag
    df['GOODS_PRICE_MISSING'] = df['AMT_GOODS_PRICE'].isna().astype(int)
    
    # Adding the missing flag for EXT_SOOURCES
    df['EXT_SOURCE_1_MISSING_FLAG'] =  df['EXT_SOURCE_1'].isna().astype(int)
    df['EXT_SOURCE_2_MISSING_FLAG'] =  df['EXT_SOURCE_2'].isna().astype(int)
    df['EXT_SOURCE_3_MISSING_FLAG'] =  df['EXT_SOURCE_3'].isna().astype(int)
    
    # Imputing with median
    df['EXT_SOURCE_1'] = df['EXT_SOURCE_1'].fillna(df['EXT_SOURCE_1'].median(skipna=True))
    df['EXT_SOURCE_2'] = df['EXT_SOURCE_2'].fillna(df['EXT_SOURCE_2'].median(skipna=True))
    df['EXT_SOURCE_3'] = df['EXT_SOURCE_3'].fillna(df['EXT_SOURCE_3'].median(skipna=True))
    
    # Stat methods per record
    df['EXT_SOURCE_MEAN'] = df[['EXT_SOURCE_1', 'EXT_SOURCE_2', 'EXT_SOURCE_3']].mean(axis=1)
    df['EXT_SOURCE_MEDI'] = df[['EXT_SOURCE_1', 'EXT_SOURCE_2', 'EXT_SOURCE_3']].median(axis=1)
    df['EXT_SOURCE_MIN'] = df[['EXT_SOURCE_1', 'EXT_SOURCE_2', 'EXT_SOURCE_3']].min(axis=1)
    df['EXT_SOURCE_MAX'] = df[['EXT_SOURCE_1', 'EXT_SOURCE_2', 'EXT_SOURCE_3']].max(axis=1)
    
    
    ### Housing related cols
    housing_related_cols = ['APARTMENTS_AVG','BASEMENTAREA_AVG','YEARS_BEGINEXPLUATATION_AVG','YEARS_BUILD_AVG','COMMONAREA_AVG','ELEVATORS_AVG','ENTRANCES_AVG','FLOORSMAX_AVG','FLOORSMIN_AVG','LANDAREA_AVG','LIVINGAPARTMENTS_AVG','LIVINGAREA_AVG','NONLIVINGAPARTMENTS_AVG','NONLIVINGAREA_AVG','APARTMENTS_MODE','BASEMENTAREA_MODE','YEARS_BEGINEXPLUATATION_MODE','YEARS_BUILD_MODE','COMMONAREA_MODE','ELEVATORS_MODE','ENTRANCES_MODE','FLOORSMAX_MODE','FLOORSMIN_MODE','LANDAREA_MODE','LIVINGAPARTMENTS_MODE','LIVINGAREA_MODE','NONLIVINGAPARTMENTS_MODE','NONLIVINGAREA_MODE','APARTMENTS_MEDI','BASEMENTAREA_MEDI','YEARS_BEGINEXPLUATATION_MEDI','YEARS_BUILD_MEDI','COMMONAREA_MEDI','ELEVATORS_MEDI','ENTRANCES_MEDI','FLOORSMAX_MEDI','FLOORSMIN_MEDI','LANDAREA_MEDI','LIVINGAPARTMENTS_MEDI','LIVINGAREA_MEDI','NONLIVINGAPARTMENTS_MEDI','NONLIVINGAREA_MEDI','FONDKAPREMONT_MODE','HOUSETYPE_MODE','TOTALAREA_MODE','WALLSMATERIAL_MODE','EMERGENCYSTATE_MODE']
    for col in housing_related_cols:
        df[f'{col}_MISSING_FLAG'] = df[col].isna().astype(int)
    
    ### Social Circle cols
    # Separating the cat (product) 
    df['REVOLVING_LOAN'] = (df['NAME_CONTRACT_TYPE'] == 'Revolving loans').astype(int)
    
    
    ### Bureau related cols
    cols_bureau = ['AMT_REQ_CREDIT_BUREAU_HOUR','AMT_REQ_CREDIT_BUREAU_DAY','AMT_REQ_CREDIT_BUREAU_WEEK','AMT_REQ_CREDIT_BUREAU_MON','AMT_REQ_CREDIT_BUREAU_QRT','AMT_REQ_CREDIT_BUREAU_YEAR']
    for col in cols_bureau:
        df[f'{col}_MISSING_FLAG'] = df[col].isna().astype(int)

    ## Transforming dtypes en cats pour les objs
    categorical_cols = df.select_dtypes(include="object").columns

    for col in categorical_cols:
        df[col] = df[col].astype("category")
    
    
    print(f"Cols Count before: {len(df.columns)}")
    
    return df



def transform_bureau_tables(bureau, bb):
    bureau = bureau.copy()
    bb = bb.copy()

    # -----------------------------
    # bureau flags
    # -----------------------------
    bureau['DAYS_CREDIT_ENDDATE_MISSING_FLAG'] = bureau['DAYS_CREDIT_ENDDATE'].isna().astype(int)
    bureau['DAYS_ENDDATE_FACT_MISSING_FLAG'] = bureau['DAYS_ENDDATE_FACT'].isna().astype(int)
    bureau['AMT_CREDIT_MAX_OVERDUE_MISSING_FLAG'] = bureau['AMT_CREDIT_MAX_OVERDUE'].isna().astype(int)
    bureau['AMT_CREDIT_SUM_LIMIT_MISSING_FLAG'] = bureau['AMT_CREDIT_SUM_LIMIT'].isna().astype(int)
    bureau['AMT_ANNUITY_MISSING_FLAG'] = bureau['AMT_ANNUITY'].isna().astype(int)
    bureau['HAS_DEBT_FLAG'] = bureau['AMT_CREDIT_SUM_DEBT'].notna().astype(int)

    bureau['IS_ACTIVE'] = (bureau['CREDIT_ACTIVE'] == 'Active').astype(int)
    bureau['IS_CLOSED'] = (bureau['CREDIT_ACTIVE'] == 'Closed').astype(int)
    bureau['HAS_OVERDUE'] = (bureau['AMT_CREDIT_SUM_OVERDUE'] > 0).astype(int)

    # -----------------------------
    # bureau_balance encoding
    # -----------------------------
    status_map = {
        'X': np.nan,
        'C': -1,
        '0': 0,
        '1': 1,
        '2': 2,
        '3': 3,
        '4': 4,
        '5': 5
    }

    bb['STATUS_NUM'] = bb['STATUS'].map(status_map)

    bb = bb.sort_values(['SK_ID_BUREAU', 'MONTHS_BALANCE'])

    # -----------------------------
    # loan-level features from bureau_balance
    # -----------------------------
    bb_agg = bb.groupby('SK_ID_BUREAU').agg({
        'STATUS_NUM': [
            'max',
            'mean',
            'std',
            lambda x: (x > 0).sum(),
            lambda x: (x >= 3).sum(),
            lambda x: (x == 5).sum(),
        ],
        'MONTHS_BALANCE': [
            'count',
            'min'
        ]
    })

    bb_agg.columns = [
        'BB_MAX_DPD',
        'BB_MEAN_DPD',
        'BB_STD_DPD',
        'BB_COUNT_DPD',
        'BB_COUNT_SEVERE_DPD',
        'BB_COUNT_DEFAULT',
        'BB_MONTHS_COUNT',
        'BB_OLDEST_MONTH'
    ]
    bb_agg = bb_agg.reset_index()

    bb_agg['BB_HAS_DPD'] = (bb_agg['BB_COUNT_DPD'] > 0).astype(int)
    bb_agg['BB_HAS_SEVERE_DPD'] = (bb_agg['BB_COUNT_SEVERE_DPD'] > 0).astype(int)
    bb_agg['BB_HAS_DEFAULT'] = (bb_agg['BB_COUNT_DEFAULT'] > 0).astype(int)

    # -----------------------------
    # aggregate bureau at client level
    # -----------------------------
    bureau_client = bureau.groupby('SK_ID_CURR').agg({
        'AMT_CREDIT_SUM_DEBT': 'sum',
        'AMT_CREDIT_SUM': 'sum',
        'IS_ACTIVE': 'sum',
        'IS_CLOSED': 'sum',
        'SK_ID_BUREAU': 'count',
        'HAS_OVERDUE': 'max',
        'DAYS_CREDIT_ENDDATE_MISSING_FLAG': 'mean',
        'DAYS_ENDDATE_FACT_MISSING_FLAG': 'mean',
        'AMT_CREDIT_MAX_OVERDUE_MISSING_FLAG': 'mean',
        'AMT_CREDIT_SUM_LIMIT_MISSING_FLAG': 'mean',
        'AMT_ANNUITY_MISSING_FLAG': 'mean',
        'HAS_DEBT_FLAG': 'mean'
    }).rename(columns={
        'AMT_CREDIT_SUM_DEBT': 'TOTAL_DEBT',
        'AMT_CREDIT_SUM': 'TOTAL_CREDIT',
        'IS_ACTIVE': 'ACTIVE_LOANS_COUNT',
        'SK_ID_BUREAU': 'TOTAL_LOANS'
    })

    bureau_client['DEBT_RATIO'] = bureau_client['TOTAL_DEBT'] / bureau_client['TOTAL_CREDIT']
    bureau_client['CLOSED_RATIO'] = bureau_client['IS_CLOSED'] / bureau_client['TOTAL_LOANS']
    bureau_client = bureau_client.reset_index()

    # -----------------------------
    # bring SK_ID_CURR into bb_agg
    # -----------------------------
    bb_agg = bb_agg.merge(
        bureau[['SK_ID_BUREAU', 'SK_ID_CURR']],
        on='SK_ID_BUREAU',
        how='left'
    )

    # -----------------------------
    # aggregate bb at client level
    # -----------------------------
    bb_client = bb_agg.groupby('SK_ID_CURR').agg({
        'BB_MAX_DPD': ['mean', 'max'],
        'BB_MEAN_DPD': ['mean'],
        'BB_STD_DPD': ['mean'],
        'BB_COUNT_DPD': 'sum',
        'BB_COUNT_SEVERE_DPD': 'sum',
        'BB_COUNT_DEFAULT': 'sum',
        'BB_HAS_DPD': 'mean',
        'BB_HAS_SEVERE_DPD': 'mean',
        'BB_HAS_DEFAULT': 'mean',
        'BB_MONTHS_COUNT': ['mean', 'sum']
    })

    bb_client.columns = ['_'.join(col) for col in bb_client.columns]
    bb_client = bb_client.reset_index()

    # -----------------------------
    # final client-level merge
    # -----------------------------
    final = bureau_client.merge(bb_client, on='SK_ID_CURR', how='left')

    return final




def transform_previous_and_pos_cash(df_previous_applications, df_pos_cash):
    df_prev = df_previous_applications.copy()
    df_pos = df_pos_cash.copy()

    # ==================================================
    # 1) PREVIOUS_APPLICATION
    # ==================================================

    # Convert object -> category
    prev_obj_cols = df_prev.select_dtypes(include=["object"]).columns.tolist()
    for col in prev_obj_cols:
        df_prev[col] = df_prev[col].astype("category")

    # Missing flags
    prev_cols_with_missing = df_prev.columns[df_prev.isna().any()].tolist()
    for col in prev_cols_with_missing:
        df_prev[f"{col}_MISSING_FLAG"] = df_prev[col].isna().astype(int)

    # Ratios
    amt_application = df_prev["AMT_APPLICATION"].replace(0, np.nan)
    amt_credit = df_prev["AMT_CREDIT"].replace(0, np.nan)

    df_prev["AMT_ANNUITY_AMT_APPLICATION_RATIO"] = df_prev["AMT_ANNUITY"] / amt_application
    df_prev["AMT_ANNUITY_AMT_CREDIT_RATIO"] = df_prev["AMT_ANNUITY"] / amt_credit
    df_prev["AMT_GOODS_PRICE_AMT_CREDIT_RATIO"] = df_prev["AMT_GOODS_PRICE"] / amt_credit

    # Clean inf
    df_prev.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Numeric aggregations
    prev_num_cols = df_prev.select_dtypes(include=["int64", "float64"]).columns.tolist()
    prev_num_cols = [col for col in prev_num_cols if col not in ["SK_ID_CURR", "SK_ID_PREV"]]

    prev_agg_dict = {col: ["mean", "max", "min", "sum"] for col in prev_num_cols}
    prev_agg_dict["SK_ID_PREV"] = ["count", "nunique"]

    df_prev_num_agg = df_prev.groupby("SK_ID_CURR").agg(prev_agg_dict)
    df_prev_num_agg.columns = [
        f"PREV_{col}_{stat}".upper() for col, stat in df_prev_num_agg.columns
    ]
    df_prev_num_agg.reset_index(inplace=True)

    # Categorical aggregations for previous_application
    prev_cat_cols = df_prev.select_dtypes(include=["category"]).columns.tolist()
    prev_cat_agg_list = []

    for col in prev_cat_cols:
        tmp = pd.crosstab(df_prev["SK_ID_CURR"], df_prev[col], dropna=False)
        tmp.columns = [
            f"PREV_{col}_{str(val)}_COUNT".upper().replace(" ", "_")
            for val in tmp.columns
        ]
        tmp.reset_index(inplace=True)
        prev_cat_agg_list.append(tmp)

    if prev_cat_agg_list:
        df_prev_cat_agg = prev_cat_agg_list[0]
        for tmp in prev_cat_agg_list[1:]:
            df_prev_cat_agg = df_prev_cat_agg.merge(tmp, on="SK_ID_CURR", how="left")
        df_prev_agg = df_prev_num_agg.merge(df_prev_cat_agg, on="SK_ID_CURR", how="left")
    else:
        df_prev_agg = df_prev_num_agg

    # ==================================================
    # 2) POS_CASH_BALANCE
    # ==================================================

    # Convert object -> category
    pos_obj_cols = df_pos.select_dtypes(include=["object"]).columns.tolist()
    for col in pos_obj_cols:
        df_pos[col] = df_pos[col].astype("category")

    # Missing flags
    pos_cols_with_missing = df_pos.columns[df_pos.isna().any()].tolist()
    for col in pos_cols_with_missing:
        df_pos[f"{col}_MISSING_FLAG"] = df_pos[col].isna().astype(int)

    # Business features
    df_pos["HAS_DPD"] = (df_pos["SK_DPD"] > 0).astype(int)
    df_pos["HAS_DPD_DEF"] = (df_pos["SK_DPD_DEF"] > 0).astype(int)
    df_pos["HAS_SEVERE_DPD"] = (df_pos["SK_DPD"] >= 30).astype(int)
    df_pos["HAS_SEVERE_DPD_DEF"] = (df_pos["SK_DPD_DEF"] >= 30).astype(int)

    cnt_inst = df_pos["CNT_INSTALMENT"].replace(0, np.nan)
    df_pos["LOAN_COMPLETION_RATIO"] = 1 - (df_pos["CNT_INSTALMENT_FUTURE"] / cnt_inst)
    df_pos["IS_COMPLETED"] = (df_pos["CNT_INSTALMENT_FUTURE"] == 0).astype(int)
    df_pos["MONTHS_BALANCE_ABS"] = df_pos["MONTHS_BALANCE"].abs()

    # Clean inf
    df_pos.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Numeric aggregations
    pos_num_cols = df_pos.select_dtypes(include=["int64", "float64"]).columns.tolist()
    pos_num_cols = [col for col in pos_num_cols if col not in ["SK_ID_CURR", "SK_ID_PREV"]]

    pos_agg_dict = {col: ["mean", "max", "min", "sum"] for col in pos_num_cols}
    pos_agg_dict["SK_ID_PREV"] = ["count", "nunique"]

    df_pos_num_agg = df_pos.groupby("SK_ID_CURR").agg(pos_agg_dict)
    df_pos_num_agg.columns = [
        f"POS_{col}_{stat}".upper() for col, stat in df_pos_num_agg.columns
    ]
    df_pos_num_agg.reset_index(inplace=True)

    # Categorical aggregations for POS_CASH_balance
    pos_cat_cols = df_pos.select_dtypes(include=["category"]).columns.tolist()
    pos_cat_agg_list = []

    for col in pos_cat_cols:
        tmp = pd.crosstab(df_pos["SK_ID_CURR"], df_pos[col], dropna=False)
        tmp.columns = [
            f"POS_{col}_{str(val)}_COUNT".upper().replace(" ", "_")
            for val in tmp.columns
        ]
        tmp.reset_index(inplace=True)
        pos_cat_agg_list.append(tmp)

    if pos_cat_agg_list:
        df_pos_cat_agg = pos_cat_agg_list[0]
        for tmp in pos_cat_agg_list[1:]:
            df_pos_cat_agg = df_pos_cat_agg.merge(tmp, on="SK_ID_CURR", how="left")
        df_pos_agg = df_pos_num_agg.merge(df_pos_cat_agg, on="SK_ID_CURR", how="left")
    else:
        df_pos_agg = df_pos_num_agg

    # ==================================================
    # 3) FINAL MERGE
    # ==================================================
    final = df_prev_agg.merge(df_pos_agg, on="SK_ID_CURR", how="left")
    final.replace([np.inf, -np.inf], np.nan, inplace=True)

    return final