from google.cloud import bigquery

def set_write_disposition(write_disposition):
    """ Sets bigquery.WriteDisposition based on write_disposition """
    if write_disposition == 'WRITE_APPEND':
        return bigquery.WriteDisposition.WRITE_APPEND
    elif write_disposition == 'WRITE_EMPTY':
        return bigquery.WriteDisposition.WRITE_EMPTY
    elif write_disposition == 'WRITE_TRUNCATE':
        return bigquery.WriteDisposition.WRITE_TRUNCATE
    else:
        raise KeyError("{} is not a valid write_disposition key".format(write_disposition))