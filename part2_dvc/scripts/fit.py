import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing  import OneHotEncoder
from sklearn.ensemble import RandomForestRegressor
from category_encoders import CatBoostEncoder
import yaml
import os
import joblib


def fit_model():
    with open("params.yaml", "rb") as fd:
        params = yaml.safe_load(fd)
    data = pd.read_csv("data/initial_data.csv")
    X = data.drop(columns = ['price', 'building_id'])
    y = data[params['target_col']]

    cat_features = data.select_dtypes(['object'])
    is_binary = cat_features.nunique() == 2
    binary_cols = is_binary[is_binary].index
    non_binary_cat_cols = is_binary[~is_binary].index
    preprocessor = ColumnTransformer([
        ("binary", OneHotEncoder(drop=params['one_hot_drop'], sparse_output=False), binary_cols),
        ("CatboostEncoder", CatBoostEncoder(), non_binary_cat_cols)],
        remainder=params['preproc_remainder'],
        verbose_feature_names_out=False
    )
    model = RandomForestRegressor(
        n_estimators=params["n_estimators"],
        max_depth=params["max_depth"],
        min_samples_leaf=params["min_samples_leaf"],
        max_features=params["max_features"],
        random_state=params["random_state"],
        n_jobs=params["n_jobs"],
    )
    pipeline = Pipeline(
        [
            ('preprocessor', preprocessor),
            ('model', model)
        ]
    )
    pipeline.fit(X, y) 
    os.makedirs("models", exist_ok=True)
    with open("models/fitted_model.pkl", "wb") as fd:
        joblib.dump(pipeline, fd)

if __name__ == '__main__':
    fit_model()