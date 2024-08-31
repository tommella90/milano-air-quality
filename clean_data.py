import pandas as pd
import numpy as np

class PollutionDataProcessor:
    def __init__(self, df: pd.DataFrame):
        self.df = df
    
    def filter_stations(self, n=5):
        """Filter the DataFrame to keep only the first `n` unique stations."""
        station_to_keep = self.df["Station"].unique()[:n]
        self.df = self.df[self.df["Station"].isin(station_to_keep)]
    
    def replace_strings_with_floats(self):
        """Replace string representations of values with floats and handle missing values."""
        for col in self.df.columns: 
            if col != "Station" and col != "Date":
                print(f"Processing column: {col}")
                self.df[col] = self.df[col].replace({
                    "< 0.5": 0.,
                    "< 5": 5.,
                    "< 2": 2.,
                    "< 4": 0.4,
                    "< 1": 1,
                    "-": np.nan,
                    "N.D.": np.nan
                })
                if self.df[col].dtype == "object":
                    # Replace commas with dots and convert to float, using errors='coerce'
                    self.df[col] = self.df[col].str.replace(',', '.')
                    self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                    
        # Specific case for 'CO'
        self.df['CO'] = self.df['CO'].replace({"< 0.5": 0.5}).astype(float)
    
    def convert_dates(self):
        """Convert the 'Date' column to datetime format."""
        self.df['Date'] = pd.to_datetime(self.df['Date'])
    
    def group_and_mean(self):
        """Group by 'Date' and compute mean for specified columns."""
        self.df = self.df.groupby("Date").mean(numeric_only=True).round(3)
    
    def process(self):
        """Run all processing steps."""
        self.filter_stations()
        self.replace_strings_with_floats()
        self.convert_dates()
        self.group_and_mean()
        return self.df

# Usage 