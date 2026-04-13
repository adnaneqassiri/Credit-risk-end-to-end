import sys
import os
import re
sys.path.append(os.path.abspath(".."))
from model import train_model
from data import clean_feature_names
import pandas as pd
pd.set_option('display.max_columns', None)

df_train = pd.read_parquet('../data/train_data/df_train_final.parquet')
df_test = pd.read_parquet('../data/train_data/df_test_final.parquet')

print(df_train.shape)

result = train_model(clean_feature_names(df_train), clean_feature_names(df_test))

print(df_train["TARGET"].isna().sum())
print(df_train["TARGET"].value_counts(dropna=False))
