from datetime import timedelta

from writer_portrait.db import DBNULL
from writer_portrait.interfaces.metric_interface import DSMetricsInterface

import pandas as pd
import numpy as np

from writer_portrait.interfaces.writer_metric_interface import DSWriterMetricsInterface


class Metric(DSMetricsInterface, DSWriterMetricsInterface):
    ID = None
    threshold = None

    def __init__(self):
        self.df_raw = pd.DataFrame()
        self.median = None
        self.q20 = None
        self.q80 = None
        self.mean_std = None
        self.std_mean = None
        self.snr = None
        self.writer_metrics = None

    def load_raw_data(self):
        pass

    def calculate_metric_score(self, min_obs_count: int = 20):
        if self.df_raw.empty:
            raise Exception("raw dataframe is empty")

        assert 'metrics_value' in self.df_raw.columns
        assert 'order_id' in self.df_raw.columns

        grouped = self.df_raw.groupby('writer_id')

        df_wr = grouped.metrics_value.mean().to_frame('metrics_value')
        df_wr['metrics_id'] = self.ID
        df_wr['top_quantile'] = self.calculate_writer_top_quantile()
        df_wr['observation_count'] = grouped.order_id.count()
        df_wr['metrics_value_std'] = grouped.metrics_value.std()
        df_wr['std_error'] = df_wr.metrics_value_std / np.sqrt(df_wr.observation_count)
        df_wr['date_observation'] = grouped.date_observation.max()

        # metric values
        self.median = df_wr.metrics_value.median()
        self.q20 = df_wr.metrics_value.quantile(.2)
        self.q80 = df_wr.metrics_value.quantile(.8)

        mask = df_wr.observation_count >= min_obs_count
        min_obs_df_wr = df_wr[mask]

        self.std_mean = min_obs_df_wr.metrics_value.std()  # signal
        self.mean_std = min_obs_df_wr.metrics_value_std.mean()  # noise
        snr = np.divide(self.std_mean, self.mean_std)

        if np.isinf(snr):
            snr = DBNULL()

        self.snr = snr

        self.extract_writers_metrics(df_wr.reset_index())

    def extract_writers_metrics(self, df: pd.DataFrame):
        for col in DSWriterMetricsInterface.COLUMNS:
            assert col in df.columns, f"{col} not in DSWriterMetricsInterface columns"

        self.writer_metrics = df[DSWriterMetricsInterface.COLUMNS].fillna(DBNULL()).to_dict('rows')

    def iterate_by_period(self, period: timedelta):
        start_date = self.df_raw.date_observation.min()
        end_date = start_date + period

        while end_date < self.df_raw.date_observation.max():
            sub_metric = self.__class__()
            sub_metric.df_raw = self.df_raw[self.df_raw.date_observation < end_date]
            yield sub_metric
            end_date = end_date + period

    def get_id(self) -> int:
        return self.ID

    def get_writers_metrics(self) -> list:
        return self.writer_metrics

    def get_median(self) -> float:
        return self.median

    def get_q20(self) -> float:
        return self.q20

    def get_q80(self) -> float:
        return self.q80

    def get_ban_threshold(self) -> float:
        return self.threshold

    def get_mean_std(self) -> float:
        return self.mean_std

    def get_std_mean(self) -> float:
        return self.std_mean

    def get_signal2noise_ratio(self) -> float:
        return self.snr

    def calculate_writer_top_quantile(self):
        return .8  # will be provided later
