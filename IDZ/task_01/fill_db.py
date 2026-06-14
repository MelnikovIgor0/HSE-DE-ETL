import subprocess
import pandas as pd
import ydb
import concurrent.futures
import time
import random
from datetime import datetime

df = pd.read_csv('~/Downloads/dataset_1/archive (4)/wfp_food_prices_global_2021.csv')
df.columns = df.columns.str.strip()
df = df.where(pd.notnull(df), None)

df['row_id'] = range(len(df))
df['market_id'] = df['market_id'].fillna(0).astype(int)
df['commodity_id'] = df['commodity_id'].fillna(0).astype(int)
df['row_id'] = df['row_id'].astype(int)

for col in ['latitude', 'longitude', 'price', 'usdprice']:
    df[col] = df[col].astype(object).where(df[col].notna(), None)

print(f"Rows: {len(df)}")

iam_token = subprocess.check_output(['yc', 'iam', 'create-token']).decode().strip()

driver_config = ydb.DriverConfig(
    'grpcs://ydb.serverless.yandexcloud.net:2135',
    '/ru-central1/b1gb7sun0bo8phi56avr/etn0rkad770qqs8qq4q7',
    credentials=ydb.AccessTokenCredentials(iam_token),
    root_certificates=ydb.load_ydb_root_certificate(),
)

COLUMN_TYPES = ydb.BulkUpsertColumns() \
    .add_column('row_id', ydb.OptionalType(ydb.PrimitiveType.Int64)) \
    .add_column('countryiso3', ydb.OptionalType(ydb.PrimitiveType.Utf8)) \
    .add_column('date', ydb.OptionalType(ydb.PrimitiveType.Utf8)) \
    .add_column('admin1', ydb.OptionalType(ydb.PrimitiveType.Utf8)) \
    .add_column('admin2', ydb.OptionalType(ydb.PrimitiveType.Utf8)) \
    .add_column('market', ydb.OptionalType(ydb.PrimitiveType.Utf8)) \
    .add_column('market_id', ydb.OptionalType(ydb.PrimitiveType.Int64)) \
    .add_column('latitude', ydb.OptionalType(ydb.PrimitiveType.Double)) \
    .add_column('longitude', ydb.OptionalType(ydb.PrimitiveType.Double)) \
    .add_column('category', ydb.OptionalType(ydb.PrimitiveType.Utf8)) \
    .add_column('commodity', ydb.OptionalType(ydb.PrimitiveType.Utf8)) \
    .add_column('commodity_id', ydb.OptionalType(ydb.PrimitiveType.Int64)) \
    .add_column('unit', ydb.OptionalType(ydb.PrimitiveType.Utf8)) \
    .add_column('priceflag', ydb.OptionalType(ydb.PrimitiveType.Utf8)) \
    .add_column('pricetype', ydb.OptionalType(ydb.PrimitiveType.Utf8)) \
    .add_column('currency', ydb.OptionalType(ydb.PrimitiveType.Utf8)) \
    .add_column('price', ydb.OptionalType(ydb.PrimitiveType.Double)) \
    .add_column('usdprice', ydb.OptionalType(ydb.PrimitiveType.Double))


def create_table(session):
    session.execute_scheme("""
        CREATE TABLE IF NOT EXISTS food_prices (
            row_id Int64 NOT NULL,
            countryiso3 Text,
            date Text,
            admin1 Text,
            admin2 Text,
            market Text,
            market_id Int64,
            latitude Double,
            longitude Double,
            category Text,
            commodity Text,
            commodity_id Int64,
            unit Text,
            priceflag Text,
            pricetype Text,
            currency Text,
            price Double,
            usdprice Double,
            PRIMARY KEY (row_id)
        );
    """)
    print("TABLE CREATED")


def df_to_rows(batch_df: pd.DataFrame) -> list[dict]:
    rows = []
    for row in batch_df.itertuples(index=False):
        rows.append({
            'row_id': int(row.row_id),
            'countryiso3': str(row.countryiso3) if row.countryiso3 is not None else None,
            'date': str(row.date) if row.date is not None else None,
            'admin1': str(row.admin1) if row.admin1 is not None else None,
            'admin2': str(row.admin2) if row.admin2 is not None else None,
            'market': str(row.market) if row.market is not None else None,
            'market_id': int(row.market_id),
            'latitude': float(row.latitude) if row.latitude is not None else None,
            'longitude': float(row.longitude) if row.longitude is not None else None,
            'category': str(row.category) if row.category is not None else None,
            'commodity': str(row.commodity) if row.commodity is not None else None,
            'commodity_id': int(row.commodity_id),
            'unit': str(row.unit) if row.unit is not None else None,
            'priceflag': str(row.priceflag) if row.priceflag is not None else None,
            'pricetype': str(row.pricetype) if row.pricetype is not None else None,
            'currency': str(row.currency) if row.currency is not None else None,
            'price': float(row.price) if row.price is not None else None,
            'usdprice': float(row.usdprice) if row.usdprice is not None else None,
        })
    return rows


BATCH_SIZE = 500
MAX_WORKERS = 3
MAX_RETRIES = 7
BASE_DELAY = 2.0


def upload_batch(driver, batch_df, batch_idx, total_batches):
    rows = df_to_rows(batch_df)
    last_error = None

    for attempt in range(MAX_RETRIES):
        try:
            driver.table_client.bulk_upsert(
                '/ru-central1/b1gb7sun0bo8phi56avr/etn0rkad770qqs8qq4q7/food_prices',
                rows,
                COLUMN_TYPES,
            )
            if attempt > 0:
                print(f"OK  {batch_idx + 1:>4}/{total_batches}  (retried {attempt}x)")
            else:
                print(f"OK  {batch_idx + 1:>4}/{total_batches}")
            return
        except Exception as e:
            last_error = e
            delay = BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
            print(f"RETRY {attempt + 1}/{MAX_RETRIES}  batch {batch_idx}  "
                  f"wait {delay:.1f}s  reason: {type(e).__name__}")
            time.sleep(delay)

    print(f"[FAILED] batch {batch_idx} after {MAX_RETRIES} attempts: {last_error}")


def load_data_parallel(driver, df):
    total = len(df)
    total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    batches = [
        (df.iloc[i * BATCH_SIZE : (i + 1) * BATCH_SIZE], i)
        for i in range(total_batches)
    ]
    t0 = datetime.now()
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for b, idx in batches:
            time.sleep(0.05)
            futures[executor.submit(upload_batch, driver, b, idx, total_batches)] = idx
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"[UNHANDLED] batch {futures[future]}: {e}")
    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\nLOAD COMPLETED — {total} rows in {elapsed:.1f}s "
          f"({total / elapsed:.0f} rows/sec)")


with ydb.Driver(driver_config) as driver:
    driver.wait(fail_fast=True, timeout=15)
    with ydb.SessionPool(driver) as pool:
        pool.retry_operation_sync(create_table)
    load_data_parallel(driver, df)
    with ydb.SessionPool(driver) as pool:
        def check(session):
            result = session.transaction().execute(
                "SELECT COUNT(*) AS cnt FROM food_prices;",
                commit_tx=True,
            )
            print(f"ROWS IN DB: {result[0].rows[0].cnt}")
        pool.retry_operation_sync(check)
