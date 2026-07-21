# scripts/evaluate.py

import pandas as pd
from sklearn.model_selection import KFold, cross_validate
import joblib
import json
import yaml
import os


def evaluate_model():
    with open("params.yaml", "rb") as fd:
        params = yaml.safe_load(fd)

    with open("models/fitted_model.pkl", "rb") as fd:
        pipeline = joblib.load(fd)

    data = pd.read_csv("data/initial_data.csv")
    X = data.drop(columns = ['price', 'building_id'])
    y = data[params['target_col']]
    cv_strategy = KFold(n_splits=params["n_splits"])
    cv_res = cross_validate(
        pipeline,
        X,
        y,
        cv=cv_strategy,
        n_jobs=params["n_jobs"],
        scoring=params['metrics']
    )

    for key, value in cv_res.items():
        cv_res[key] = round(value.mean(), 3) 
    os.makedirs('cv_results', exist_ok=True) 
    with open("cv_results/cv_res.json", "w") as fd:
        json.dump(cv_res, fd)
    


if __name__ == '__main__':
    evaluate_model()