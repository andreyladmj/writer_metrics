from datetime import datetime
import pandas as pd
import numpy as np
from db.connections import DBConnectionsFacade

from writer_portrait.db import DB
from writer_portrait.metric import Metric

db_edusson_engine_read = DBConnectionsFacade.get_replica()


class CustomerRatingMetric(Metric):
    ID = 1
    threshold = 1

    def get_name(self) -> str:
        return 'customer rating metric'

    def load_raw_data(self, writer_ids: list = None, date_start: datetime = datetime(2018, 1, 1, 0, 0),
                      end_date: datetime = None):
        if writer_ids and isinstance(writer_ids, list) and len(writer_ids) > 0:
            writer_ids = np.array(writer_ids)
            ids_str = ','.join(writer_ids.astype(str))
            sub_query = "AND receiver_id IN ({ids})".format(ids=ids_str)
        else:
            sub_query = 'AND is_active=1'

        if end_date:
            sub_query += f" AND date_upd < '{end_date.strftime('%Y-%m-%d')}'"

        sql_query = f'''
            SELECT
                t1.order_id,
                receiver_id AS writer_id,
                date_upd AS date_observation,
                value_id as customer_rating
            FROM es_ratings t0
            LEFT JOIN es_rating_order t1 ON t1.rating_id = t0.rating_id
            LEFT JOIN es_users t3 ON t3.user_id = t0.receiver_id
            WHERE t3.user_type_id = 2
            AND t3.user_id NOT IN (
                SELECT tmp.user_id 
                FROM es_users tmp
                JOIN es_user_roles tmp1 ON tmp1.user_id = tmp.user_id AND tmp1.role_id = 20
                WHERE tmp.user_type_id = 2 
            )
            AND t3.is_test_user = 0
            AND date_upd > '{date_start.strftime("%Y-%m-%d")}'
            {sub_query}
            '''

        self.df_raw = pd.read_sql(sql=sql_query, con=db_edusson_engine_read)
        self.df_raw = self.df_raw.drop_duplicates(["order_id", "writer_id"], keep="last")
        self.df_raw = self.df_raw.rename(columns={"customer_rating": "metrics_value"})
        return self


if __name__ == '__main__':
    metric = CustomerRatingMetric()

    start_date = datetime.strptime("2017-01-01", "%Y-%m-%d")

    metric.load_raw_data(date_start=start_date).calculate_metric_score(min_obs_count=50)

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
