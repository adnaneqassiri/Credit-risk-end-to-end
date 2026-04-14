import sys
import os
sys.path.append(os.path.abspath(".."))

import pandas as pd
pd.set_option('display.max_columns', None)

from sklearn.metrics import roc_curve, roc_auc_score
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
import lightgbm as lgb
from collections import Counter


def train_model(df_train, df_test):
    results = {
        "auc_train": 0,
        "auc_val": 0,
        "model": ''
    }
    if df_train is not None:
        # Variables explicatives et cible
        y = df_train["TARGET"]
        X = df_train.drop(columns=["TARGET", "SK_ID_CURR"])

        # Découpage train / test
        X_train, X_valid, y_train, y_valid = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        counter = Counter(y_train)
        scale_pos_weight = counter[0] / counter[1]

        # Modèle de classification
        model = lgb.LGBMClassifier(
            n_estimators=7000,
            learning_rate=0.0005,
            num_leaves=256,
            max_depth=-1,
            min_data_in_leaf=150,
            subsample=0.6,
            subsample_freq=1,
            colsample_bytree=0.6,
            reg_alpha=0.1,
            reg_lambda=1.0,
            random_state=42,
            n_jobs=-1,
            scale_pos_weight=scale_pos_weight
        )

        model.fit(
            X_train, y_train,
            eval_set=[(X_valid, y_valid)],
            eval_metric="auc",
            callbacks=[lgb.early_stopping(600, verbose=True)]
        )
        results['auc_train'] = eval(model, X_train, y_train)
        results['auc_val'] = eval(model, X_valid, y_valid)
        results['model'] = model
        
    if df_test is not None:
        ids = df_test["SK_ID_CURR"]
        X = df_test.drop(columns=["SK_ID_CURR"])
        results['submit_target'] = df = model.predict_proba(X)[:, 1]
        
        submission = pd.DataFrame(
            {
                'SK_ID_CURR': ids.values,
                'TARGET': df
            }
        )
        submission.to_csv("../../data/submissions/submission.csv", index=False)

    return results
    
        
        
def eval(model, X, y):
    # Probabilités de la classe positive
    y_pred = model.predict_proba(X)[:, 1]

    # Calcul de l'AUC
    auc_score = roc_auc_score(y, y_pred)
    print(f"AUC = {auc_score:.4f}")

    # Calcul des points de la courbe ROC
    fpr, tpr, thresholds = roc_curve(y, y_pred)

    # Tracé de la courbe ROC
    plt.figure(figsize=(8, 6))
    plt.plot(fpr, tpr, label=f"ROC curve (AUC = {auc_score:.4f})")
    plt.plot([0, 1], [0, 1], linestyle="--", label="Random classifier")
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title("Courbe ROC")
    plt.legend()
    plt.grid(True)
    plt.show()
    
    return auc_score
