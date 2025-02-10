import pandas as pd
from sklearn.base import BaseEstimator


class Naive(BaseEstimator):
    """A naive forecasting model that uses lagged values for prediction."""

    def fit(self, X: pd.DataFrame, y: pd.Series) -> "Naive":
        """
        Fits the Naive model. This model does not require fitting.

        Parameters:
            X (pd.DataFrame): The input features.
            y (pd.Series): The target values.

        Returns:
            Naive: The fitted model.
        """
        return self

    def predict(self, X: pd.DataFrame) -> pd.Series:
        """
        Makes predictions using the Naive model.

        Parameters:
            X (pd.DataFrame): The input features containing 'lag1' and 'lag2' columns.

        Returns:
            pd.Series: The predicted values.
        """
        return X["lag1"] + (X["lag1"] - X["lag2"])


class LagFeatureAdder:
    """
    A class to add lag features to a DataFrame.
    """

    def __init__(self, df_ind, ids):
        """
        Initialize the LagFeatureAdder.

        Parameters:
        df_ind (pd.DataFrame): The DataFrame with columns 'unique_id' and 'h3_index'.
        ids (pd.Series): Series containing the unique IDs in order.
        """
        self.df_ind = df_ind
        self.ids = ids

    def add_lag_features(self, df_values):
        """
        Merges df_ind with df_values and adds lag features to the DataFrame.

        Parameters:
        df_values (pd.DataFrame): The input DataFrame with columns 'ds' and 'lag1'.

        Returns:
        pd.DataFrame: The DataFrame with added 'lag1_mean' column.
        """
        # Add unique_id column
        df_values["unique_id"] = self.ids

        # Merge with df_ind
        df_values = df_values.merge(self.df_ind, on="unique_id", how="left")

        # Calculate lag1 mean by h3_index
        df_values["lag1_mean"] = df_values.groupby(["h3_index"])["lag1"].transform(
            "mean"
        )
        # print numbre of nan in lag1_mean
        print(df_values["lag1_mean"].isna().sum())
        # Remove temporary columns
        df_values = df_values.drop(columns=["h3_index", "unique_id"])

        return df_values
