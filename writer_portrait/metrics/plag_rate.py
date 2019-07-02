from datetime import datetime
import pandas as pd
from db.connections import DBConnectionsFacade

from writer_portrait.db import DB
from writer_portrait.metric import Metric

db_edusson_engine_read = DBConnectionsFacade.get_replica()


class PlagRateMetric(Metric):
    ID = 4
    threshold = 1

    def get_name(self) -> str:
        return 'plag rate metric'

    def load_raw_data(self, date_start: datetime = datetime(2018, 1, 1, 0, 0), end_date: datetime = None):
        sub_query = ""

        if end_date:
            sub_query += f"AND t3.order_date < '{end_date.strftime('%Y-%m-%d')}'"

        sql_query = f'''
            SELECT
                t0.order_id,
                t2.updated_at as date_observation,
                t1.upload_user_id AS writer_id,
                IF(t2.external_percent >= 10, 1, 0) AS is_plagiarized
            FROM es_file_revision t0
            LEFT JOIN es_file t1 ON t1.id = t0.id
            LEFT JOIN bp_report_revision t2 ON t2.revision_id=t0.id
            LEFT JOIN es_orders t3 ON t3.order_id = t0.order_id
            LEFT JOIN es_product p1 ON p1.order_id = t0.order_id
            LEFT JOIN es_product_type_essay pe1 ON p1.product_id = pe1.product_id
            WHERE t0.words_count/(pe1.pages*255) >= 0.95
            AND t2.external_percent IS NOT NULL
            AND t3.order_date >= '{date_start.strftime('%Y-%m-%d')}'
            AND t3.test_order = 0
            AND t3.site_id != 31
            {sub_query}
            GROUP BY t0.order_id, t1.upload_user_id;
            '''

        self.df_raw = pd.read_sql(sql=sql_query, con=db_edusson_engine_read)
        self.df_raw = self.df_raw.rename(columns={"is_plagiarized": "metrics_value"})
        return self


if __name__ == '__main__':
    metric = PlagRateMetric()

    start_date = datetime.strptime("2017-01-01", "%Y-%m-%d")

    metric.load_raw_data(date_start=start_date)
    metric.calculate_metric_score(min_obs_count=50)

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
