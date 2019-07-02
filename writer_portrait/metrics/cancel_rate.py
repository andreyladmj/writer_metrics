from datetime import datetime
import pandas as pd
from db.connections import DBConnectionsFacade

from writer_portrait.db import DB
from writer_portrait.metric import Metric

db_edusson_engine_read = DBConnectionsFacade.get_replica()


class CancelRateMetric(Metric):
    ID = 3
    threshold = 1

    def get_name(self) -> str:
        return 'cancel rating metric'

    """ set start ate as metric property """

    def load_raw_data(self, date_start: datetime = datetime(2018, 1, 1, 0, 0), end_date: datetime = None):
        sub_query = ""

        if end_date:
            sub_query += f"AND order_date < '{end_date.strftime('%Y-%m-%d')}'"

        sql_query = f'''
            SELECT 
                order_id,
                writer_id,
                date_state_change as date_observation,
                IF(state_id=3, 0, 1) AS is_canceled
            FROM es_orders
            WHERE is_paid_order = 1
            AND order_date > '{date_start.strftime("%Y-%m-%d")}'
            {sub_query}
            AND state_id IN (3,4,5,6)
            AND writer_id IS NOT NULL
            AND site_id != 31
            AND test_order = 0;
            '''

        self.df_raw = pd.read_sql(sql=sql_query, con=db_edusson_engine_read)
        self.df_raw = self.df_raw.rename(columns={"is_canceled": "metrics_value"})
        return self


if __name__ == '__main__':
    metric = CancelRateMetric()

    start_date = datetime.strptime("2017-01-01", "%Y-%m-%d")

    metric.load_raw_data(date_start=start_date)
    metric.calculate_metric_score(min_obs_count=10)

    print('Median:', metric.get_median())
    print('Q 20:', metric.get_q20())
    print('Q 80:', metric.get_q80())
    print('Ban Threshold:', metric.get_ban_threshold())
    print('Mean STD:', metric.get_mean_std())
    print('STD Mean:', metric.get_std_mean())
    print('Signal2noise Ratio:', metric.get_signal2noise_ratio())

    db = DB()
    db.update_metric(metric)
    db.update_writer_metric(metric)
