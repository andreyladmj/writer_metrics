from abc import ABCMeta, abstractmethod
import pandas as pd
from numpy.testing import assert_approx_equal
from sqlalchemy.util import NoneType


class DSWriterMetricsInterface:
    __metaclass__ = ABCMeta

    ID = None
    COLUMNS = {'writer_id', 'metrics_id', 'metrics_value', 'top_quantile',
               'observation_count', 'std_error', 'date_observation'}

    FIELDS_FOR_COMPARING = ['metrics_value', 'observation_count', 'std_error']

    @abstractmethod
    def get_id(self):
        pass

    @abstractmethod
    def extract_writers_metrics(self, df: pd.DataFrame):
        """ Extract Writers Metrics ID """

    @abstractmethod
    def get_writers_metrics(self) -> list:
        """ Get Writers Metrics """
