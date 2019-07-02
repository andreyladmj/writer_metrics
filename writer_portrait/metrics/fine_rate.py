from datetime import datetime
import pandas as pd
from db.connections import DBConnectionsFacade

from writer_portrait.db import DB
from writer_portrait.metric import Metric

db_edusson_engine_read = DBConnectionsFacade.get_replica()


class FineRateMetric(Metric):
    ID = 5
    threshold = 1

    def get_name(self) -> str:
        return 'fine rate metric'

    def load_raw_data(self, date_start: datetime = datetime(2018, 1, 1, 0, 0), end_date: datetime = None):
        sub_query = ""

        if end_date:
            sub_query += f"AND t1.order_date < '{end_date.strftime('%Y-%m-%d')}'"

        sql_query = f'''
            SELECT 
                t1.order_id,
                IF(t2.date_state_change IS NULL, t1.order_date, t2.date_state_change) AS date_observation,
                IF(t2.user_id IS NULL, t1.writer_id, t2.user_id) AS writer_id,
                t1.writer_total,
                SUM(t2.value_usd) AS total_fine_usd,
                COUNT(t2.value_usd) AS total_fine_count
            FROM es_orders t1
            LEFT JOIN es_balance_transactions t2 ON t2.order_id=t1.order_id AND t2.state_id=2 AND t2.type=9
            WHERE t1.is_paid_order = 1
            AND t1.order_date > '{date_start.strftime('%Y-%m-%d')}'
            {sub_query}
            AND t1.state_id IN (3,4,5,6)
            AND NOT (t2.user_id IS NULL AND t1.writer_id IS NULL)
            AND t1.site_id != 31
            AND t1.test_order = 0
            GROUP BY t1.order_id, writer_id;
            '''

        self.df_raw = pd.read_sql(sql=sql_query, con=db_edusson_engine_read)
        self.df_raw = self.df_raw.rename(columns={"total_fine_count": "metrics_value"})
        self.df_raw.total_fine_usd = self.df_raw.total_fine_usd.fillna(0)

        return self


if __name__ == '__main__':
    metric = FineRateMetric()

    start_date = datetime.strptime("2017-01-01", "%Y-%m-%d")

    metric.load_raw_data(date_start=start_date)
    metric.calculate_metric_score(min_obs_count=50)

    print("From:", metric.df_raw.date_observation.min(), "To:", metric.df_raw.date_observation.max(), end=' ')

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
