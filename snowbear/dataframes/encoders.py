import json
from typing import Optional, Union, Dict

from snowbear.dataframes import DataFrame, col, functions
from snowbear.dataframes.terms import Case


def _get_categories(df: DataFrame, categories, cat_col):
    if categories == "auto":
        categories_ = df.groupby(col(cat_col)).aggregate(count=functions.Count(col(cat_col))).to_pandas()[
            cat_col].to_list()
    else:
        categories_ = categories
    return categories_


class OneHotEncoder:
    def __init__(
            self,
            *,
            input_col: Optional[str] = None,
            output_cols: Optional[Union[Dict, str]] = None,
            categories="auto",
    ):

        self.input_col = input_col
        self.output_cols = output_cols
        self.categories = categories

    def fit(self, df: DataFrame) -> "OneHotEncoder":
        self._fitted_values = _get_categories(df, self.categories, self.input_col)

        return self

    def transform(self, df: DataFrame) -> DataFrame:

        terms = {}
        orig_columns = df.columns()
        for column in orig_columns:
            if column != self.input_col:
                terms[column] = df[column]
            else:
                for category in self._fitted_values:
                    terms[f"{self.input_col}_{category}"] = Case().when(df[self.input_col] == category, 1).else_(0)

        return df.select(**terms)

    def fit_transform(self, df: DataFrame) -> DataFrame:
        return self.fit(df).transform(df)
