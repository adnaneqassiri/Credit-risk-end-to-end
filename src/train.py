import sys
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

sys.path.append(os.path.abspath(".."))
from data import transform_application_table, transform_bureau_tables, transform_previous_and_pos_cash, transform_credit_card_balance, transform_installments_payments
from model import train_model
import pandas as pd
pd.set_option('display.max_columns', None)

####  ---- Paths
table_applications_train_path = "../data/raw/application_train.csv"
table_applications_test_path = "../data/raw/application_test.csv"
bureau_path = "../data/raw/bureau.csv"
bureau_balance_path = "../data/raw/bureau_balance.csv"
previous_path = "../data/raw/previous_application.csv"
pos_path = "../data/raw/POS_CASH_balance.csv"
credit_card_path = "../data/raw/credit_card_balance.csv"
installments_path = "../data/raw/installments_payments.csv"

logging.info("Paths initialized")

####  ---- Reading Dataframes
logging.info("Reading datasets...")
df_train = pd.read_csv(table_applications_train_path)
df_test = pd.read_csv(table_applications_test_path)
bureau = pd.read_csv(bureau_path, header=0)
bureau_balance = pd.read_csv(bureau_balance_path, header=0)
previous_app = pd.read_csv(previous_path, header=0)
pos_cash = pd.read_csv(pos_path, header=0)
credit_card = pd.read_csv(credit_card_path, header=0)
installments = pd.read_csv(installments_path, header=0)

logging.info("Datasets loaded")

####  ---- Transforming Datframes
logging.info("Transforming application tables...")
df_train_tf = transform_application_table(df_train)
df_test_tf = transform_application_table(df_test)

logging.info("Transforming bureau tables...")
final_bureau_table = transform_bureau_tables(bureau, bureau_balance)

logging.info("Transforming previous applications and POS cash...")
previous_app_tf = transform_previous_and_pos_cash(previous_app, pos_cash)

logging.info("Transforming credit card balance...")
credit_card_tf = transform_credit_card_balance(credit_card)

logging.info("Transforming Instalement...")
installments_tf = transform_installments_payments(installments)

print(final_bureau_table.shape)
print(df_train_tf.shape)
print(previous_app_tf.shape)
print(credit_card_tf.shape)

logging.info("Merging datasets...")
train_df = df_train_tf.merge(final_bureau_table, on='SK_ID_CURR', how='left').merge(previous_app_tf, on='SK_ID_CURR', how='left').merge(credit_card_tf, on='SK_ID_CURR', how='left').merge(installments_tf, on='SK_ID_CURR', how='left')
test_df = df_test_tf.merge(final_bureau_table, on='SK_ID_CURR', how='left').merge(previous_app_tf, on='SK_ID_CURR', how='left').merge(credit_card_tf, on='SK_ID_CURR', how='left').merge(installments_tf, on='SK_ID_CURR', how='left')

logging.info("Saving final datasets...")
os.makedirs("../data/train_data/", exist_ok=True)
train_df.to_parquet('../data/train_data/df_train_final.parquet')
test_df.to_parquet('../data/train_data/df_test_final.parquet')

logging.info("The datadframes are saved to: '../data/train_data/'")
