import json
from typing import Optional, Union, List, Dict

from snowbear.dataframes import DataFrame, col, functions


def _check_input_columns(df: DataFrame, input_columns):
    if not input_columns:
        input_columns = df.columns()
    else:
        # Check if list
        if not isinstance(input_columns, list):
            input_columns = [input_columns]

    return input_columns


def _get_categories(df: DataFrame, categories, cat_cols):
    # {"COL1": ["cat1", "cat2", ...], "COL2": [...]}
    categories_ = []
    if categories == "auto":
        object_const = []
        for column in cat_cols:
            categories_ = df.groupby(col(column)).aggregate(count=functions.Count(col(column))).to_pandas()[column]

    else:  # If categories has already been defined...
        # if self.handle_unknown == "error":
        # Check so we does not have new
        categories_ = categories

    return categories_


class OneHotEncoder:
    def __init__(
            self,
            *,
            input_cols: Optional[Union[List[str], str]] = None,
            output_cols: Optional[Union[Dict, str]] = None,
            categories="auto",
            handle_unknown='ignore',
            drop_input_cols=True
    ):

        self.input_cols = input_cols
        self.output_cols = output_cols
        self.categories = categories
        self.handle_unknown = handle_unknown

    def _check_output_columns(self):
        #  {"COL1": [cat1, cat2, ...], "COL2": [cat1, cat2, ...]}
        # output_columns ...
        cat_cols = {}
        needed_cols = 0
        output_cols = self.output_cols
        categories = self.fitted_values_
        input_cols = self.input_cols

        if output_cols:
            # Check so we have the same number of output columns as input columns
            if not len(output_cols) == len(input_cols):
                raise ValueError(f"Too few output columns provided. Have {len(output_cols)} need {len(input_cols)}")
            # Check so the total numer of columns per input column is equal to the number of categories
            tot_output_cols = sum(len(output_cols[feat]) for feat in output_cols)
            needed_cols = sum(len(output_cols[feat]) for feat in categories)
            if not needed_cols == tot_output_cols:
                raise ValueError(
                    f"Need the same number of output category columns as categories. Have {needed_cols} categories "
                    f"and {tot_output_cols} output category columns"
                )
            cat_cols = output_cols
        else:
            for column in input_cols:
                uniq_vals = categories[column]
                col_names = [column + '_' + val for val in uniq_vals]
                cat_cols[column] = col_names
                needed_cols += len(uniq_vals)

        # Snowflake can handle more columns,but it is depended on data type so let's keep it safe and limit to  3k
        if needed_cols > 3000:
            raise ValueError(
                "To many categories, maximum 3000 is allowed")

        return cat_cols

    def fit(self, df: DataFrame) -> object:
        """
        Fit the OneHotEncoder using df.
        :param df: Snowpark DataFrame used for getting the categories for each input column
        :return: Fitted encoder
        """
        #

        encode_cols = _check_input_columns(df, self.input_cols)
        self.input_cols = encode_cols

        self.fitted_values_ = _get_categories(df, self.categories, encode_cols)

        return self

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform df using one-hot encoding, it will create one new column for each category found with fit.
        If drop_input_cols is True then the input columns are dropped from the returned DataFrame.
        :param df: Snowpark DataFrame to transform
        :return: Encoded Snowpark DataFrame
        """

        encode_cols = self.input_cols

        output_cols = self._check_output_columns()
        self.output_cols = output_cols

        # Check for new categories?

        for col in encode_cols:
            uniq_vals = self.fitted_values_[col]
            col_names = output_cols[col]
            df = df.with_columns(col_names, [F.iff(F.col(col) == val, F.lit(1), F.lit(0)) for val in uniq_vals])
            if self.handle_unknown == 'keep':
                df = df.with_column(col + '__unknown', F.iff(~ F.col(col).in_(uniq_vals), F.lit(1), F.lit(0)))

            if self.drop_input_cols:
                df = df.drop(col)

        return df

    def fit_transform(self, df: DataFrame) -> DataFrame:
        """
        Fit OneHotEncoder to df and transform the df, it will create one new column for each category found with fit.
        If drop_input_cols is True then the input columns are dropped from the returned DataFrame.
        :param df: Snowpark DataFrame to encode
        :return: Encoded Snowpark DataFrame
        """
        return self.fit(df).transform(df)
