import pickle as pkl
import numpy as np

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

    def fit(self, df, binarize=[], scale=[], labels=[], targets=[]):
        """
        :param df: Pandas Dataframe to train on
        :param target: target variable column name
        :param binarize: columns to binarize
        :param scale: columns to scale
        :return: Transformer
        """
        self.fit_params['columns_in'] = df.columns.tolist()
        self.fit_params['binarize'] = binarize
        self.fit_params['scale'] = scale
        self.fit_params['labels'] = labels
        self.fit_params['targets'] = targets

        binaries = {}
        for col in self.fit_params['binarize']:
            group = df.groupby([col]).agg({col: ['count']})
            group = group[group.iloc[:, 0] >= self.config_params['min_samples']]
            group = group.sort_values(by=group.columns[0], ascending=False)
            binaries[col] = group.iloc[:self.config_params['max_binaries']].index.tolist()
        self.fit_params['binaries'] = binaries

        binary_cols = []
        for column in self.fit_params['binarize']:
            categories = sorted(self.fit_params['binaries'][column])
            categories.append(self.config_params['catch_all'])
            strcat = [str(int(cat)) if isinstance(cat, (int, np.float)) else cat for cat in categories]
            binary_cols.extend([column + '_' + cat for cat in strcat])

        self.fit_params['columns_out'] = self.fit_params['labels'] + \
                                         self.fit_params['scale'] + \
                                         binary_cols + \
                                         self.fit_params['targets']

        scales = {}
        for col in self.fit_params['scale']:
            scales[col] = {}
            scales[col]['mu'] = df[col].mean()
            scales[col]['sigma'] = df[col].std()
        self.fit_params['scales'] = scales

        return self

    def transform(self, df, partial_data=False, impute_nulls=True):
        """
        :param df: Pandas Dataframe to transform
        :param partial_data: boolean, if true allows a partial transformation
        :return: Transformed Dataframe
        """
        df = df.copy(deep=True)

        if partial_data:
            cols = list(set(self.fit_params['binarize']) & set(df.columns.tolist()))
        else:
            cols = self.fit_params['binarize']

        for column in cols:
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

        for column in self.fit_params['scale']:
            mu = self.fit_params['scales'][column]['mu']
            sigma = self.fit_params['scales'][column]['sigma']
            df[column] = (df[column] - mu)/sigma
            if impute_nulls:
                df[column].fillna(0, inplace=True)

        return df[self.fit_params['columns_out']]

    def reverse_transform(self, df):
        """
        Reverses a transformation.
        Note: some categories will have been lost, and some nulls might have been imputed
        Those operations cannot be undone
        :param df: dataframe, already transformed
        :return: dataframe, original format
        """
        for cat in self.fit_params['binarize']:
            df[cat] = self.config_params['catch_all']
            for level in self.fit_params['binaries'][cat]:
                for col in self.fit_params['columns_out']:
                    if col == '_'.join([cat, level]):
                        df.loc[df[col] == 1, cat] = level
                        df.drop(columns=col, inplace=True)

        for col in self.fit_params['scale']:
            mu = self.fit_params['scales'][col]['mu']
            sigma = self.fit_params['scales'][col]['sigma']
            df[col] = df[col]*sigma + mu

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
