import logging
from time import time

from sqlalchemy import select, MetaData, event, alias, func
from db.connections import DBConnectionsFacade
import numpy as np
import pandas as pd
from writer_portrait.interfaces.metric_interface import DSMetricsInterface
from writer_portrait.interfaces.writer_metric_interface import DSWriterMetricsInterface

engine = DBConnectionsFacade.get_ds()

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 2000)


class DBNULL: pass


def add_own_encoders(conn, cursor, query, *args):
    cursor.connection.encoders[np.int64] = lambda value, encoders: int(value)
    cursor.connection.encoders[np.float64] = lambda value, encoders: float(value)
    cursor.connection.encoders[pd.Timestamp] = lambda value, encoders: encoders[str](str(value.to_pydatetime()))
    cursor.connection.encoders[pd.Timedelta] = lambda value, encoders: value.total_seconds()
    cursor.connection.encoders[DBNULL] = lambda value, encoders: "NULL"
    cursor.connection.encoders[np.nan] = lambda value, encoders: "NULL"


event.listen(engine, "before_cursor_execute", add_own_encoders)

meta = MetaData()
meta.reflect(bind=engine, only=['ds_metrics', 'ds_writer_metrics'])

DSMetrics = meta.tables['ds_metrics']
DSWriterMetrics = meta.tables['ds_writer_metrics']

logger = logging.getLogger('DB')


class DB:
    def update_metric(self, metric: DSMetricsInterface):
        assert isinstance(metric, DSMetricsInterface)

        with engine.connect() as connection:
            transaction = connection.begin()
            try:
                q = DSMetrics.select().where(DSMetrics.c.metrics_id == metric.get_id())
                exist_metrics = connection.execute(q).fetchone()

                if exist_metrics:
                    query = DSMetrics.update().where(DSMetrics.c.metrics_id == exist_metrics.metrics_id)
                else:
                    query = DSMetrics.insert()

                vals = dict(
                    metrics_id=metric.get_id(),
                    metrics_name=metric.get_name(),
                    median=metric.get_median(),
                    q20=metric.get_q20(),
                    q80=metric.get_q80(),
                    ban_threshold=metric.get_ban_threshold(),
                    mean_std=metric.get_mean_std(),
                    std_mean=metric.get_std_mean(),
                    signal2noise_ratio=metric.get_signal2noise_ratio(),
                )

                for k, v in vals.items():
                    if pd.isnull(v):
                        vals[k] = DBNULL()

                query = query.values(vals)

                connection.execute(query)
                transaction.commit()
            except Exception as e:
                transaction.rollback()
                logger.error("There are an error in update_metric. Transaction rollback...")
                logger.error(e)
                return

    def update_writer_metric(self, metric: DSWriterMetricsInterface):
        assert isinstance(metric, DSWriterMetricsInterface)

        data = metric.get_writers_metrics()

        if not len(data):
            return

        n = np.ceil(len(data) / 5000) + 1  # approx 5k in batch

        metric_id = metric.get_id()

        with engine.connect() as connection:
            transaction = connection.begin()

            try:
                for i, batch in enumerate(np.array_split(data, n)):
                    stime = time()
                    affected_rows = 0
                    new_df_data = pd.DataFrame.from_records(batch).set_index('writer_id')
                    ids = new_df_data.index.unique()

                    subq = alias(select([func.max(DSWriterMetrics.c.id)])
                                 .where(DSWriterMetrics.c.writer_id.in_(ids))
                                 .where(DSWriterMetrics.c.metrics_id == metric_id)
                                 .group_by(DSWriterMetrics.c.writer_id))

                    q = DSWriterMetrics.select().where(DSWriterMetrics.c.id.in_(subq))
                    df_db_rows = pd.read_sql(q, connection, index_col='writer_id')

                    exists_ids = df_db_rows.index.unique()

                    new_df_data = pd.DataFrame.from_records(batch).set_index('writer_id')

                    df_db_rows['new_observation_count'] = new_df_data.observation_count
                    df_db_rows['new_metrics_value'] = new_df_data.metrics_value
                    df_db_rows['new_std_error'] = new_df_data.std_error
                    df_db_rows['new_top_quantile'] = new_df_data.top_quantile

                    df_db_rows.loc[
                        df_db_rows.new_std_error.apply(lambda x: not isinstance(x, float)), 'new_std_error'] = np.nan
                    df_db_rows.loc[
                        df_db_rows.metrics_value.apply(lambda x: not isinstance(x, float)), 'metrics_value'] = np.nan
                    df_db_rows = df_db_rows.fillna(0)

                    eq1 = (df_db_rows.observation_count != df_db_rows.new_observation_count)
                    eq2 = ~(np.isclose(df_db_rows.metrics_value, df_db_rows.new_metrics_value, 1.e-5))
                    eq3 = ~(np.isclose(df_db_rows.std_error, df_db_rows.new_std_error, 1.e-5))
                    eq4 = ~(np.isclose(df_db_rows.top_quantile, df_db_rows.new_top_quantile, 1.e-5))

                    update_ids = df_db_rows[eq1 | eq2 | eq3 | eq4].index

                    rows_for_insert = new_df_data[~new_df_data.index.isin(exists_ids)].reset_index().to_dict('rows')
                    rows_for_insert += new_df_data[new_df_data.index.isin(update_ids)].reset_index().to_dict('rows')

                    if rows_for_insert:
                        query = DSWriterMetrics.insert().values(rows_for_insert)
                        affected_rows = connection.execute(query).rowcount

                    logger.info(f"Batch: {i}, Time: {time()-stime}, Affected rows: {affected_rows} from {len(batch)}")

                transaction.commit()
            except Exception as e:
                transaction.rollback()
                logger.error("There are an error in update_writer_metric. Transaction rollback...")
                logger.error(e)
                return
