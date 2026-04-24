import os
import logging
import pandas  as pd
from src.db.queries import insert_data
from src.data.transform import build_final_datasets
from src.config import TRAIN_DATA_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def main():
    logging.info("Building final gold datasets...")
    train_df, test_df = build_final_datasets()

    logging.info("Train dataset shape: %s", train_df.shape)
    logging.info("Test dataset shape: %s", test_df.shape)

    os.makedirs(TRAIN_DATA_DIR, exist_ok=True)

    train_path = os.path.join(TRAIN_DATA_DIR, "df_train_final.parquet")
    test_path = os.path.join(TRAIN_DATA_DIR, "df_test_final.parquet")

    train_df.to_parquet(train_path, index=False)
    test_df.to_parquet(test_path, index=False)

    logging.info("Train dataset saved to %s", train_path)
    logging.info("Test dataset saved to %s", test_path)
    logging.info("Gold datasets built successfully")

    
    # Stack verticale
    test_df["TARGET"] = pd.NA
    final_df = pd.concat([train_df, test_df], axis=0, ignore_index=True)
    logging.info("Creating/replacing PostgreSQL table 'gold' with final features...")
    insert_data(final_df, table_name="gold")
    logging.info("PostgreSQL table 'gold' refreshed successfully")
    
if __name__ == "__main__":
    main()  
