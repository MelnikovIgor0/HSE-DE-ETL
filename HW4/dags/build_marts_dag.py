from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import logging

logger = logging.getLogger(__name__)

POSTGRES_CONFIG = {
    'host': 'postgres',
    'database': 'airflow',
    'user': 'admin',
    'password': 'admin'
}

def get_postgres_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)

def build_user_activity_mart(**context):
    logger.info("Building activity mart...")
    conn = get_postgres_connection()
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE mart_user_activity;")
    query = """
        INSERT INTO mart_user_activity (
            user_id,
            total_sessions,
            total_session_duration_minutes,
            avg_session_duration_minutes,
            total_pages_visited,
            avg_pages_per_session,
            total_actions,
            avg_actions_per_session,
            most_used_device,
            first_session_date,
            last_session_date,
            days_active
        )
        SELECT 
            user_id AS user_id,
            COUNT(*) as total_sessions,
            SUM(EXTRACT(EPOCH FROM (end_time - start_time)) / 60) AS total_session_duration_minutes,
            AVG(EXTRACT(EPOCH FROM (end_time - start_time)) / 60) AS avg_session_duration_minutes,
            SUM(CARDINALITY(pages_visited)) AS total_pages_visited,
            AVG(CARDINALITY(pages_visited)) AS avg_pages_per_session,
            SUM(CARDINALITY(actions)) AS total_actions,
            AVG(CARDINALITY(actions)) AS avg_actions_per_session,
            MODE() WITHIN GROUP (ORDER BY device) AS most_used_device,
            MIN(start_time) AS first_session_date,
            MAX(start_time) AS last_session_date,
            (MAX(start_time)::DATE - MIN(start_time)::DATE + 1) AS days_active
        FROM user_sessions
        GROUP BY user_id;
    """
    cursor.execute(query)
    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM mart_user_activity;")
    count = cursor.fetchone()[0]
    logger.info(f"Created {count} items")
    cursor.close()
    conn.close()

def build_support_efficiency_mart(**context):
    logger.info("Building efficiency mart...")
    conn = get_postgres_connection()
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE mart_support_efficiency;")
    query = """
        INSERT INTO mart_support_efficiency (
            ticket_id,
            user_id,
            issue_type,
            status,
            created_at,
            updated_at,
            resolution_time_hours,
            messages_count,
            is_resolved
        )
        SELECT 
            ticket_id AS ticket_id,
            user_id AS user_id,
            issue_type AS issue_type,
            status AS status,
            created_at AS created_at,
            updated_at AS updated_ar,
            EXTRACT(EPOCH FROM (updated_at - created_at)) / 3600 AS resolution_time_hours,
            messages_count AS messages_count,
            CASE WHEN status = 'closed' THEN TRUE ELSE FALSE END AS is_resolved
        FROM support_tickets;
    """
    cursor.execute(query)
    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM mart_support_efficiency;")
    count = cursor.fetchone()[0]
    logger.info(f"Created {count} items")
    cursor.close()
    conn.close()

with DAG(
    'build_analytical_marts',
    description='Building marts DAG',
    start_date=datetime(year=2026, month=3, day=7)
) as dag:
    [
        PythonOperator(task_id='build_user_activity_mart', python_callable=build_user_activity_mart, dag=dag),
        PythonOperator(task_id='build_support_efficiency_mart', python_callable=build_support_efficiency_mart, dag=dag)
    ]
