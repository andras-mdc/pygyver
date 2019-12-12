import pandas as pd
import numpy as np
import pickle as pkl


class Transformer(object):
    """
    Transforms raw data into modelling data using both declared and trained configurations.
    """
    def __init__(self,
                 min_samples=1000,
                 max_binaries=10,
                 catch_all='other'):

        self.fit_params = dict()

        self.config_params = dict()
        self.config_params['catch_all'] = catch_all
        self.config_params['min_samples'] = min_samples
        self.config_params['max_binaries'] = max_binaries

    def fit(self, df, target=None, binarize=[]):
        """
        :param df: Pandas Dataframe to train on
        :param target: target variable column name
        :param binarize: columns to binarize
        :return: Transformer
        """
        self.fit_params['target_col'] = target
        self.fit_params['columns_in'] = df.columns.tolist()
        self.fit_params['binarize'] = binarize

        binaries = {}
        for col in self.fit_params['binarize']:
            group = df.groupby([col]).agg({col: ['count']})
            group = group[group.iloc[:, 0] >= self.config_params['min_samples']]
            group = group.sort_values(by=group.columns[0], ascending=False)
            binaries[col] = group.iloc[:self.config_params['max_binaries']].index.tolist()
        self.fit_params['binaries'] = binaries

        new_cols = []
        existing_cols = df.columns.tolist()
        for column in self.fit_params['binarize']:
            categories = sorted(self.fit_params['binaries'][column])
            categories.append(self.config_params['catch_all'])
            strcat = [str(int(cat)) if isinstance(cat,(int,np.float)) else cat for cat in categories]
            new_cols.append([column + '_' + cat for cat in strcat])
            existing_cols.remove(column)

        self.fit_params['columns_out'] = existing_cols.append(new_cols)

        return self

    def transform(self, df):
        """
        :param df: Pandas Dataframe to transform
        :return: Transformed Dataframe
        """
        df = df.copy(deep=True)
        for column in self.fit_params['binarize']:
            categories = sorted(self.fit_params['binaries'][column])
            catch_all = self.config_params['catch_all']

            # introduce binary columns, preserving the same order in fit().
            for cat in categories:
                if isinstance(cat, (int, np.float)):
                    catstr = str(int(cat))
                else:
                    catstr = cat
                df['_'.join([column, catstr])] = 0

            df['_'.join([column, catch_all])] = 1

            # update binary columns iteratively
            for cat in categories:
                if isinstance(cat, (int, np.float)):
                    catstr = str(int(cat))
                else:
                    catstr = cat
                df.loc[df[column] == cat, '_'.join([column, catstr])] = 1
                df.loc[df[column] == cat, '_'.join([column, catch_all])] = 0

            # drop categorical column:
            df.drop(column, axis=1, inplace=True)

        return df

    def save(self, pkl_path):
        """
        :param pkl_path: path to save transformer (as pickled dict)
        :return:
        """
        saved_params = dict()
        saved_params['fit_params'] = self.fit_params
        saved_params['config_params'] = self.config_params

        with open(pkl_path, 'wb') as f:
            pkl.dump(saved_params, f)

    @classmethod
    def load(cls, pkl_path):
        """
        :param pkl_path: path to load from (a pickled dict)
        :return: loaded Transformer
        """

        with open(pkl_path, 'rb') as f:
            loaded_params = pkl.load(f)

        temp_cls = Transformer()
        temp_cls.fit_params = loaded_params['fit_params']
        temp_cls.config_params = loaded_params['config_params']

        return temp_cls


