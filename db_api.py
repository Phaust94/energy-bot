from sqlite3 import connect, Connection
from dataclasses import dataclass, field
import typing
import os
import datetime
import pytz

import pandas as pd
import matplotlib.pyplot as plt

from helpers import counter, HOUR_COEFFICIENTS

__all__ = [
    "ElectricityDB",
]

ADMIN_ID = 476001386

TIME_TO_SECONDS_FORMAT = "%Y-%m-%d %H:%M:%S"


@dataclass
class ElectricityDB:
    db_location: str
    _db_conn: Connection = field(init=False, default=None)

    def __post_init__(self):
        db_exists = os.path.exists(self.db_location)
        self._db_conn = connect(self.db_location)
        if not db_exists:
            self.create_tables()

    def create_tables(self) -> None:
        self.query("""
        CREATE TABLE RAW_RECORDS
        (
        USER_ID int,
        TS TIMESTAMP_NTZ,
        VALUE float,
        PRIMARY KEY (USER_ID, TS)
        )
        """)

        self.query("""
        CREATE TABLE HOURLY_DELTAS
        (
        USER_ID int,
        TS TIMESTAMP_NTZ,
        DELTA float,
        PRIMARY KEY (USER_ID, TS)
        )
        """)

        return None

    def query(
            self,
            query_text: str,
            params: typing.Iterable[typing.Any] = None,
            safe: bool = False,
            raise_on_error: bool = True
    ) -> typing.Optional[pd.DataFrame]:
        try:
            if not safe:
                res = pd.read_sql(query_text, self._db_conn, params=params)
            else:
                res = self._db_conn.execute(query_text, params)
        except Exception as e:
            if raise_on_error:
                raise e
            exc_txt = str(e)
            if exc_txt == "'NoneType' object is not iterable":
                return None
            res = pd.DataFrame([{"Exception_text": exc_txt}])
        return res

    def _add_record_hourly(self, tg_id: int, value: float, ts: datetime.datetime) -> None:
        query = """
        SELECT TS, VALUE
        FROM RAW_RECORDS
        WHERE 1=1
        AND USER_ID = :tg_id
        AND TS < :ts
        ORDER BY TS DESC
        LIMIT 1
        """

        prev_row = self.query(query, {"tg_id": tg_id, 'ts': ts})
        if prev_row is None or prev_row.empty:
            return None

        prev_row = prev_row.iloc[0]
        prev_ts = prev_row["TS"]
        prev_ts = datetime.datetime.strptime(prev_ts, TIME_TO_SECONDS_FORMAT)
        delta_value = value - prev_row["VALUE"]
        current_ts = prev_ts + datetime.timedelta(hours=1)
        hour_start = current_ts.replace(minute=0, second=0, microsecond=0)
        hourly_coefficients = []
        hours = []
        while hour_start < ts:
            hours.append(
                hour_start
            )
            hour_str = hour_start.strftime("%H")
            hourly_coefficients.append(
                HOUR_COEFFICIENTS[hour_str]
            )
            current_ts += datetime.timedelta(hours=1)
            hour_start = current_ts.replace(minute=0, second=0, microsecond=0)

        tot = sum(hourly_coefficients)
        deltas = [
            {
                "USER_ID": tg_id,
                "TS": hour,
                "DELTA": delta_value * coefficient / tot
            }
            for hour, coefficient in zip(hours, hourly_coefficients)
        ]
        deltas = pd.DataFrame(deltas)
        deltas.to_sql("HOURLY_DELTAS", self._db_conn, if_exists="append", index=False)

        return None

    def _add_record_raw(self, tg_id: int, value: float, ts: datetime.datetime) -> None:
        row = pd.DataFrame([{
            "USER_ID": tg_id,
            "TS": ts,
            "VALUE": value,
        }])
        row.to_sql("RAW_RECORDS", self._db_conn, if_exists="append", index=False)
        return None

    @staticmethod
    def _prepare_date_time(time: str = None, date: str = None) -> datetime.datetime:
        now = datetime.datetime.now(tz=pytz.timezone('Europe/Kiev'))
        if not date:
            date = now.strftime("%Y-%m-%d")
        if not time:
            time = now.strftime("%H:%M:00")
        if len(time.split(":")) < 3:
            time += ":00"
        ts = datetime.datetime.strptime(f"{date} {time}", TIME_TO_SECONDS_FORMAT)
        return ts

    def add_record(self, tg_id: int, value: float, time: str = None, date: str = None) -> None:
        ts = self._prepare_date_time(time, date)
        self._add_record_raw(tg_id, value, ts)
        self._add_record_hourly(tg_id, value, ts)
        return None

    def list_records(self, tg_id: int) -> str:
        records = self.query(
            """
            SELECT * 
            FROM RAW_RECORDS 
            WHERE USER_ID = :tg_id
            """,
            {"tg_id": tg_id}
        )

        res = self.df_to_str(records, ["USER_ID"])
        return res

    def list_hourly_records(self, tg_id: int) -> str:
        records = self.query(
            """
            SELECT * 
            FROM HOURLY_DELTAS 
            WHERE USER_ID = :tg_id
            """,
            {"tg_id": tg_id}
        )

        res = self.df_to_str(records, ["USER_ID"])

        return res

    @staticmethod
    def df_to_str(df: pd.DataFrame, excluded_columns: typing.List[str] = None) -> str:
        excluded_columns = excluded_columns or []
        records = df.drop(columns=excluded_columns)
        res = [", ".join(records.columns)]
        for _, row in records.iterrows():
            res.append(", ".join(map(str, row)))
        res = "\n".join(res)
        return res

    def delete_records(self, tg_id: int) -> None:
        self.query(
            "DELETE FROM RAW_RECORDS WHERE USER_ID = :tg_id",
            {"tg_id": tg_id},
            safe=True
        )
        self.query(
            "DELETE FROM HOURLY_DELTAS WHERE USER_ID = :tg_id",
            {"tg_id": tg_id},
            safe=True
        )
        return None

    def drop_tables(self, tg_id: int) -> None:
        if tg_id == ADMIN_ID:
            self.query(
                "DROP TABLE RAW_RECORDS",
                safe=True,
                raise_on_error=False
            )
            self.query(
                "DROP TABLE HOURLY",
                safe=True,
                raise_on_error=False
            )
        return None

    def execute_command(self, tg_id: int, command: str) -> typing.Optional[str]:
        res = None
        if tg_id == ADMIN_ID:
            res = self.query(
                command,
                {"tg_id": tg_id},
                safe=False,
                raise_on_error=False,
            )

        if res is not None:
            res = self.df_to_str(res)
        else:
            res = "Executed successfully"

        return res

    def _month_so_far(
            self, tg_id: int,
            now: datetime.datetime, month_start: datetime.datetime
    ) -> str:
        query = """
                with prev_month_max as (
                    select 
                    MAX(value) as prev_month_max
                    from raw_records
                    where 1=1
                    and user_id = :tg_id
                    and ts < :month_start
                    GROUP BY 1=1
                ),
                curr_month as (
                    SELECT
                    MIN(value) as THIS_MONTH_MIN
                    from raw_records
                    where 1=1
                    and user_id = :tg_id
                    and ts <= :now
                    and ts >= :month_start
                    GROUP BY 1=1
                ),
                all_prev_raw as (
                    SELECT prev_month_max, 1 as prio
                    FROM prev_month_max
                    UNION ALL 
                    SELECT THIS_MONTH_MIN as prev_month_max, 2 as prio
                    FROM curr_month
                ),
                all_prev as (
                    SELECT prev_month_max
                    FROM all_prev_raw
                    ORDER BY prio
                    LIMIT 1
                )
                select 
                MAX(value) - prev_month_max as MONTH_TOTAL
                from raw_records
                CROSS JOIN all_prev
                where 1=1
                and user_id = :tg_id
                and ts <= :now
                and ts >= :month_start
                """
        month_so_far = self.query(
            query,
            {
                "tg_id": tg_id,
                "now": now,
                "month_start": month_start,
            }
        )
        total = month_so_far['MONTH_TOTAL'].iloc[0]
        total = total or 0
        msg = f"Month so far: {total:.1f} kWT"
        return msg

    def _hourly_month_to_day(
            self,
            tg_id: int,
            now: datetime.datetime, month_start: datetime.datetime
    ) -> str:
        now_week = now - datetime.timedelta(days=7)
        hourly_deltas_q = """
                select 
                TS, DELTA
                from hourly_deltas
                where 1=1
                and user_id = :tg_id
                and ts <= :now
                and ts >= :now_week
                """
        hourly_deltas = self.query(
            hourly_deltas_q,
            {
                "tg_id": tg_id,
                "now": now,
                "now_week": now_week,
            }
        )
        hourly_deltas["TS_STR"] = hourly_deltas["TS"].apply(
            lambda x: datetime.datetime.strptime(x, TIME_TO_SECONDS_FORMAT).strftime("%m-%d %H:%M")
        )
        hourly_deltas.plot(
            x='TS_STR', y='DELTA', kind='bar',
            figsize=(30, 30)
        )
        plt.xticks(rotation=90)
        fn = self.save_pic(tg_id, now)
        return fn

    @staticmethod
    @counter
    def save_pic(tg_id: int, now: datetime.datetime, __postfix: int = None) -> str:
        __postfix = __postfix or 0
        now_str = str(now).replace(":", "_").replace(" ", "_")
        fn = f"pictures/stats_{tg_id}_{now_str}_{__postfix}.png"
        fn = os.path.join(
            os.getcwd(),
            fn
        )
        with open(fn, 'wb') as f:
            plt.savefig(f)
        plt.close()
        return fn

    def _daily_usage(
            self,
            tg_id: int,
            now: datetime.datetime, month_start: datetime.datetime
    ) -> pd.DataFrame:
        hourly_deltas_q = """
                        select 
                        strftime('%Y-%m-%d', TS) as DAY,
                        SUM(DELTA) as ENERGY_SPENT
                        from hourly_deltas
                        where 1=1
                        and user_id = :tg_id
                        and ts <= :now
                        and ts >= :month_start
                        GROUP BY 1
                        ORDER BY 1
                        """
        hourly_deltas = self.query(
            hourly_deltas_q,
            {
                "tg_id": tg_id,
                "now": now,
                "month_start": month_start,
            }
        )
        return hourly_deltas

    @staticmethod
    def _daily_usage_str(daily_usage: pd.DataFrame) -> str:
        avg = daily_usage["ENERGY_SPENT"].mean()
        std = daily_usage["ENERGY_SPENT"].std()
        msg1 = f"Daily average for this month so far: {avg:.1f} kWT"
        msg2 = f"STD for this month so far: {std:.1f} kWT"
        msg = f"{msg1}\n{msg2}"
        return msg

    def _daily_usage_chart(self, tg_id: int, now: datetime.datetime, daily_usage: pd.DataFrame) -> str:
        daily_usage["DAY_STR"] = daily_usage["DAY"].apply(
            lambda x: x[5:]
        )
        avg = daily_usage["ENERGY_SPENT"].mean()
        daily_usage["ENERGY_SPENT_AVG"] = avg
        std = daily_usage["ENERGY_SPENT"].std()
        fig, ax = plt.subplots()
        ax = daily_usage.plot(
            x='DAY_STR', y='ENERGY_SPENT', kind='bar',
            figsize=(10, 10),
            color='b',
            ax=ax,
        )
        ax = daily_usage.plot(
            x="DAY_STR", y="ENERGY_SPENT_AVG", kind="line",
            color='r',
            label='_',
            ax=ax,
        )
        if not pd.isna(std):
            daily_usage["ENERGY_SPENT_LOWER_BOUND"] = avg - 3 * std
            daily_usage["ENERGY_SPENT_UPPER_BOUND"] = avg + 3 * std
            ax = daily_usage.plot(
                x="DAY_STR", y="ENERGY_SPENT_LOWER_BOUND", kind="line",
                linestyle='--', color='m',
                label='_',
                ax=ax,
            )
            ax = daily_usage.plot(
                x="DAY_STR", y="ENERGY_SPENT_UPPER_BOUND", kind="line",
                linestyle='--', color='m',
                label='_',
                ax=ax,
            )
            ax.fill_between(
                daily_usage["DAY_STR"],
                daily_usage['ENERGY_SPENT_UPPER_BOUND'],
                daily_usage['ENERGY_SPENT_LOWER_BOUND'],
                color='tab:pink'
            )
        plt.xticks(rotation=90)
        plt.legend(loc='top right')
        fn = self.save_pic(tg_id, now)
        return fn

    def diff_from_prev(self, tg_id: int, now: datetime.datetime):
        query = """
        select 
                        value as PREV_RECORD
                        from raw_records
                        where 1=1
                        and user_id = :tg_id
                        and ts <= :now
                        ORDER BY TS desc
                        LIMIT 2
        """
        df = self.query(
            query,
            {
                "tg_id": tg_id,
                "now": now,
            }
        )
        delta = df.max() - df.min()
        delta = delta["PREV_RECORD"]
        msg = f"Delta from the previous time is {delta:.1f} kWT"
        return msg

    def get_stats(self, tg_id: int) -> typing.Tuple[str, typing.List[str]]:
        txt = []
        charts = []
        now = self._prepare_date_time()
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        txt.append(self._month_so_far(tg_id, now, month_start))
        diff_prv = self.diff_from_prev(tg_id, now)
        txt.append(diff_prv)
        # charts.append(self._hourly_month_to_day(tg_id, now, month_start))

        daily_usage = self._daily_usage(tg_id, now, month_start)
        txt.append(self._daily_usage_str(daily_usage))
        charts.append(self._daily_usage_chart(tg_id, now, daily_usage))

        txt = "\n".join(txt)
        return txt, charts

    def close_connection(self) -> None:
        # noinspection PyBroadException
        try:
            self.query("COMMIT", safe=False)
        except Exception:
            pass
        self._db_conn.close()
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close_connection()
        return None
