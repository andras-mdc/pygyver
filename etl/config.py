""" Logging handler """
import logging
import logging.config

def configure_logging(env=None):
    """ Sets logging"""
    logging.info("Environment: %s", env)

    logging.config.dictConfig(
        {
            'version': 1,
            'disable_existing_loggers': False,
            'root': {
                'level': 'INFO',
                'handlers': ['default']
            },
            'formatters': {
                'plain_text': {
                    'format': '%(asctime)s %(levelname)s %(message)s',
                    'datefmt': '%H:%M:%S'
                }
            },
            'handlers': {
                'default': {
                    'class': 'logging.StreamHandler',
                    'stream': 'ext://sys.stdout',
                    'formatter': 'plain_text',
                    'level': 'NOTSET'
                }
            }
        }
    )
