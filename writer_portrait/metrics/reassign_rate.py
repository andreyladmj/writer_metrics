from datetime import datetime
import pandas as pd
import numpy as np
from db.connections import DBConnectionsFacade

from writer_portrait.db import DB
from writer_portrait.metric import Metric

db_edusson_engine_read = DBConnectionsFacade.get_replica()


class ReassignRateMetric(Metric):
    ID = 2
    threshold = 1

    def get_name(self) -> str:
        return 'reassign rate metric'

    """ Update sql query, check status"""

    def load_raw_data(self, date_start: datetime = datetime(2018, 1, 1, 0, 0), end_date: datetime = None):
        # reassign comments/reasons
        sub_query = ''

        if end_date:
            sub_query = f" AND t0.date_create < '{end_date.strftime('%Y-%m-%d')}'"

        sql_query = f'''
        SELECT 
            t0.order_id, 
            t0.writer_id,
            t1.name as reason,
            t0.date_create AS date_observation
        FROM es_order_reassign_history t0
        LEFT JOIN es_reasons t1 ON t1.id = t0.reason_id
        LEFT JOIN es_orders t4 ON t4.order_id = t0.order_id
        WHERE t0.date_create > '{date_start}'
        {sub_query}
        AND t4.order_total_usd > 0
        AND t4.state_id != 2
        AND t4.test_order = 0
        AND t4.site_id != 31;
        '''

        df_reassigned = pd.read_sql(sql=sql_query, con=db_edusson_engine_read)

        # paid order without reassign
        sql_query = f'''
        SELECT 
            t0.order_id, 
            t0.writer_id,
            t0.date_state_change AS date_observation
        FROM es_orders t0
        WHERE t0.order_date > '{date_start}'
        AND t0.state_id IN (3,4,5,6)
        AND t0.order_total_usd > 0
        AND t0.state_id != 2
        AND t0.writer_id IS NOT NULL
        AND t0.test_order = 0
        AND t0.site_id != 31;
        '''
        df_ords = pd.read_sql(sql=sql_query, con=db_edusson_engine_read)

        # investigate writers reassign rate vs finished orders coount
        df_reassigned['is_reassigned'] = 1
        df_ords['is_reassigned'] = 0
        df_obs = pd.concat([df_reassigned, df_ords], axis=0, sort=False).reset_index(drop=True)

        df_obs.reason = df_obs.reason.fillna(np.nan)

        self.df_raw = df_obs
        self.df_raw = self.df_raw.rename(columns={"is_reassigned": "metrics_value"})

        return self


if __name__ == '__main__':
    metric = ReassignRateMetric()

    start_date = datetime.strptime("2017-01-01", "%Y-%m-%d")
    metric.load_raw_data(date_start=start_date)
    metric.calculate_metric_score()

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
