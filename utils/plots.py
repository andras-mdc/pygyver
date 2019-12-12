from sklearn import metrics
from sklearn.utils import shuffle
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import itertools


def j_score(observed,softprob):
    fpr, tpr, thresholds = metrics.roc_curve(observed,softprob)
    j_scores = tpr - fpr
    return sorted(zip(j_scores,thresholds))[-1][1]


def pr_curve(observed, softprob, title='Precision-Recall Curve', max_precision=1, axes=[0]):
    precision, recall, thresholds = metrics.precision_recall_curve(observed, softprob)
    auprc = metrics.average_precision_score(observed, softprob)
    prev = observed.sum() / observed.count()

    axes.plot([0, 1], [prev, prev], 'r--')
    axes.step(recall, precision, color='b', alpha=0.2, where='post')
    axes.fill_between(recall, precision, color='b', alpha=0.2, step='post')
    axes.set_xlim([0, 1])
    axes.set_ylim([0, max_precision])
    axes.set_xlabel('Recall')
    axes.set_ylabel('Precision')
    axes.set_title(title)
    axes.text(0.01, max_precision, 'AUPRC = %0.2f' % auprc,
              verticalalignment='top',
              horizontalalignment='left',
              fontsize=12)


def roc_curve(observed, softprob, title='Precision-Recall Curve', axes=[0]):
    fpr, tpr, thresholds = metrics.roc_curve(observed, softprob)
    auc = metrics.auc(fpr, tpr)

    axes.plot([0, 1], [0, 1], 'r--')
    axes.plot(fpr, tpr, 'b', label='AUC = %0.2f' % auc)
    axes.set_xlim([0, 1])
    axes.set_ylim([0, 1])
    axes.set_xlabel('False Positive Rate')
    axes.set_ylabel('True Positive Rate')
    axes.set_title(title)
    axes.text(0.01, 1, 'AUC = %0.2f' % auc,
              verticalalignment='top',
              horizontalalignment='left',
              fontsize=12)


def conf_matrix(observed, predicted, axes,
                title=None, class_names=None, normalize=False):

    if class_names is None:
        class_names = list({val for val in observed + predicted})

    if title is None:
        if normalize:
            title = 'Confusion Matrix %ages'
        else:
            title = 'Confusion Matrix'

    observed = [class_names[i] for i in observed]
    predicted = [class_names[i] for i in predicted]
    cm = metrics.confusion_matrix(observed, predicted, class_names)

    if normalize:
        cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]

    plt.sca(axes)
    plt.title(title)
    plt.imshow(cm, interpolation='nearest', cmap=plt.get_cmap('Blues'), aspect='auto')
    plt.colorbar(fraction=0.046, pad=0.04, aspect='auto')

    tick_marks = np.arange(len(class_names))
    plt.xticks(tick_marks, class_names)
    plt.yticks(tick_marks, class_names)
    plt.ylabel('True Class')
    plt.xlabel('Predicted Class')

    thresh = cm.max() / 1.5 if normalize else cm.max() / 2
    for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
        if normalize:
            plt.text(j, 0.25+i*0.5, "{:0.4f}".format(cm[i, j]),
                     horizontalalignment="center",
                     color="white" if cm[i, j] > thresh else "black")
        else:
            plt.text(j, 0.25+i*0.5, "{:,}".format(cm[i, j]),
                     horizontalalignment="center",
                     color="white" if cm[i, j] > thresh else "black")


def regr_mae(df,
             prediction_col,
             observation_col):

    df_copy = df.copy(deep=True)
    df_copy[observation_col] = df_copy[observation_col].fillna(0)
    df_copy[prediction_col] = df_copy[prediction_col].fillna(0)

    df_copy['abs_err'] = abs(df_copy[prediction_col]-df_copy[observation_col])
    return df_copy['abs_err'].sum() / df_copy['abs_err'].count()


def regr_rmse(df,
              prediction_col,
              observation_col):

    df_copy = df.copy(deep=True)
    df_copy[observation_col] = df_copy[observation_col].fillna(0)
    df_copy[prediction_col] = df_copy[prediction_col].fillna(0)

    df_copy['sq_err'] = (df_copy[prediction_col] - df_copy[observation_col])**2
    return np.sqrt(df_copy['sq_err'].sum() / df_copy['sq_err'].count())


def regr_error_within_samples(sample_size,
                              dataset,
                              prediction_col,
                              observation_col,
                              versions_col):
    samples_list = {}
    counter = 0

    for version, chunk in dataset[[versions_col,
                                   prediction_col,
                                   observation_col]].groupby(versions_col):

        chunk.fillna(0, inplace=True)
        chunk = shuffle(chunk)

        for i in range(int(np.floor(chunk.shape[0] / sample_size))):
            sample = chunk[i * sample_size:(i + 1) * sample_size]

            samples_list[counter] = [version,
                                     sample_size,
                                     i,
                                     chunk[observation_col].mean(),
                                     chunk[prediction_col].mean(),
                                     regr_mae(sample, prediction_col, observation_col),
                                     regr_rmse(sample, prediction_col, observation_col)]
            counter = counter + 1

    output = pd.DataFrame.from_dict(samples_list, orient='index')
    output.columns = ['version', 'sample_size', 'sample', 'avg_observed', 'avg_predicted', 'mae', 'rmse']

    grouped = output.groupby(['version', 'avg_observed', 'avg_predicted']).agg({'sample': ['count'],
                                                                                'mae': ['mean', 'max'],
                                                                                'rmse': ['mean', 'max']})

    grouped.columns = ['_'.join(col).strip() for col in grouped.columns.values]
    grouped.reset_index(inplace=True)

    return grouped


def regr_error_by_sample(sample_size,
                         dataset,
                         prediction_col,
                         observation_col,
                         versions_col):
    samples_list = {}
    counter = 0

    for version, chunk in dataset[[versions_col,
                                   prediction_col,
                                   observation_col]].groupby(versions_col):

        chunk.fillna(0, inplace=True)
        chunk = shuffle(chunk)
        n_samples = int(np.floor(chunk.shape[0] / sample_size))

        for i in range(int(np.floor(chunk.shape[0] / sample_size))):
            sample = chunk[i * sample_size:(i + 1) * sample_size]

            samples_list[counter] = [version,
                                     sample_size,
                                     n_samples,
                                     i,
                                     sample[observation_col].sum(),
                                     sample[prediction_col].sum(),
                                     abs(sample[observation_col].sum() - sample[prediction_col].sum())]
            counter = counter + 1

    output = pd.DataFrame.from_dict(samples_list, orient='index')

    output.columns = ['version', 'sample_size', 'num_samples', 'sample_idx', 'sample', 'prediction', 'error']

    grouped = output.groupby(['version', 'num_samples', 'sample_size']).agg({'sample': ['mean'],
                                                                             'error': ['mean', 'max']})

    grouped.columns = ['_'.join(col).strip() for col in grouped.columns.values]
    grouped.reset_index(inplace=True)

    grouped['percent_error_mean'] = grouped['error_mean'] / grouped['sample_mean']
    grouped['percent_error_max'] = grouped['error_max'] / grouped['sample_mean']

    return grouped

def mae_by_date(df,
                versions_col,
                date_col,
                prediction_col,
                observation_col):

    df_copy = df.copy(deep=True)
    df_copy[observation_col] = df_copy[observation_col].fillna(0)
    df_copy[prediction_col] = df_copy[prediction_col].fillna(0)

    df_copy['abs_err'] = abs(df_copy[observation_col] - df_copy[prediction_col])

    accuracy_by_date = df_copy[[versions_col, date_col, 'abs_err']].pivot_table(index=[date_col],
                                                                                values=['abs_err'],
                                                                                columns=[versions_col],
                                                                                aggfunc=np.mean)

    accuracy_by_date.columns = [col[1] for col in accuracy_by_date.columns.tolist()]
    accuracy_by_date.reset_index(inplace=True)

    return accuracy_by_date
