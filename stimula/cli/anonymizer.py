import random
from io import TextIOBase

import pandas as pd


class Anonymizer:
    def anonymize(self, input, output):
        # read dataframe from stdin, read all as str to avoid conversion errors
        df = pd.read_csv(input, dtype=str)

        # get columns to anonymize
        columns = self.get_sensitive_columns(df)

        # anonymize columns
        df = self.anonymize_columns(df, columns)

        # write to stdout
        df.to_csv(output, index=False)

        return df

    def get_sensitive_columns(self, df):
        # filter sensitive column names
        return [col for col in df.columns if self.is_sensitive(col, df[col])]

    def is_sensitive(self, name, values):
        # list non-empty values
        non_empty_value_count = len(values.dropna())

        # if none left, return false
        if non_empty_value_count == 0:
            return False

        # if all non-empty values are numeric, then return false
        if pd.to_numeric(values.dropna(), errors='coerce').notnull().all():
            return False

        # lower case the column name
        name = name.lower()

        # if the column contains the word "name", "address" or "email", then return true
        if 'name' in name or 'address' in name or 'email' in name:
            return True

        if 'zip' in name or 'city' in name or 'date' in name:
            return False

        # if too few values provided, then return true
        if non_empty_value_count < 4:
            return True

        # count unique values
        unique_count = len(values.unique())

        # if unique values are less than or equal to 25% of the total values, then return false
        if unique_count <= 0.25 * non_empty_value_count:
            return False

        # else don't know, return true
        return True

    def anonymize_columns(self, df, columns):
        for col in columns:
            df[col] = df[col].apply(lambda x: self.anonymize_value(x))
        return df

    def anonymize_value(self, x):
        # if nan, return as is
        if pd.isnull(x):
            return x

        # if x is not a string, return as is
        if not isinstance(x, str):
            return x

        # split words by space
        words = x.split()

        # anonymize each word
        return ' '.join(self.anonymize_word(word) for word in words)

    def anonymize_word(self, x):
        # is the word capitalized?
        is_capitalized = len(x) > 1 and x[0].isupper() and x[1:].islower()
        # n is the number of times to swap characters. Minimum must be an odd number to minimize the chance of the word left unchanged
        n = max(3, len(x) // 8)
        # iterate n times
        for i in range(n):
            # create two random numbers, smaller than the length of the string
            i = random.randint(0, len(x) - 1)
            j = random.randint(0, len(x) - 1)
            # swap two random characters
            x = self.swap(x, i, j)

        # if the word was capitalized, capitalize it again
        if is_capitalized:
            x = x.capitalize()
        return x

    def swap(self, s, i, j):
        l = list(s)
        l[i], l[j] = l[j], l[i]
        return ''.join(l)
