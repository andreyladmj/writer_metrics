import logging
from datetime import datetime, timedelta
from time import time

logging.basicConfig(level=logging.INFO)

from writer_portrait.db import DB
from writer_portrait.metrics.cancel_rate import CancelRateMetric
from writer_portrait.metrics.customer_rating import CustomerRatingMetric
from writer_portrait.metrics.fine_rate import FineRateMetric
from writer_portrait.metrics.plag_rate import PlagRateMetric
from writer_portrait.metrics.reassign_rate import ReassignRateMetric


def update_metric(metric, start_date, dtime, min_obs_count=50):
    stime = time()
    print("\n\nUpdating", metric.__class__, "\n\n")
    metric.load_raw_data(date_start=start_date)

    for partition in metric.iterate_by_period(dtime):
        s2time = time()
        print("From:", partition.df_raw.date_observation.min(), "To:", partition.df_raw.date_observation.max(), end=' ')
        if not partition.df_raw.empty:
            partition.calculate_metric_score(min_obs_count=min_obs_count)
            db.update_metric(partition)
            db.update_writer_metric(partition)
        print("Time:", time() - s2time)

    print("Updated!", time() - stime)


if __name__ == '__main__':
    dtime = timedelta(hours=1)
    i = 0
    db = DB()

    metrics_for_update = [
        (datetime.strptime("2018-01-01", "%Y-%m-%d"), CancelRateMetric()),
        (datetime.strptime("2018-01-01", "%Y-%m-%d"), CustomerRatingMetric()),
        (datetime.strptime("2018-01-01", "%Y-%m-%d"), FineRateMetric()),
        (datetime.strptime("2018-01-01", "%Y-%m-%d"), PlagRateMetric()),
        (datetime.strptime("2018-01-01", "%Y-%m-%d"), ReassignRateMetric()),
    ]

    for start_date, metric in metrics_for_update:
        update_metric(metric, start_date, dtime)
