from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_batch
import logging

logger = logging.getLogger(__name__)

POSTGRES_CONFIG = {
    'host': 'postgres',
    'database': 'airflow',
    'user': 'admin',
    'password': 'admin'
}

MONGO_CONFIG = {
    'host': 'mongodb',
    'port': 27017,
    'username': 'admin',
    'password': 'admin',
    'database': 'db'
}

def get_mongo_connection():
    client = MongoClient(
        f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
    )
    return client[MONGO_CONFIG['database']]

def get_postgres_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)

def extract_user_sessions(**context):
    mongo_db = get_mongo_connection()
    sessions = list(mongo_db.user_sessions.find({}))
    for session in sessions:
        if '_id' in session:
            session['_id'] = str(session['_id'])
    context['ti'].xcom_push(key='raw_users', value=sessions)

def transform_user_sessions(**context):
    sessions = context['ti'].xcom_pull(key='raw_users')
    data_to_insert = []
    for session in sessions:
        data_to_insert.append((
            session['session_id'],
            session['user_id'],
            session['start_time'],
            session['end_time'],
            session['device'],
            session['pages_visited'],
            session['actions']
        ))
    context['ti'].xcom_push(key='processed_users', value=data_to_insert)

def load_user_sessions(**context):
    data_to_insert = context['ti'].xcom_pull(key='processed_users')
    pg_conn = get_postgres_connection()
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("TRUNCATE TABLE user_sessions CASCADE;")
    insert_query = """
        INSERT INTO user_sessions (
            session_id, user_id, start_time, end_time, device, 
            pages_visited, actions
        ) VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    execute_batch(pg_cursor, insert_query, data_to_insert)
    pg_conn.commit()
    logger.info(f"Replicated {len(data_to_insert)} items")
    pg_cursor.close()
    pg_conn.close()

def extract_event_logs(**context):
    mongo_db = get_mongo_connection()
    events = list(mongo_db.event_logs.find({}))
    for event in events:
        if '_id' in event:
            event['_id'] = str(event['_id'])
    context['ti'].xcom_push(key='raw_events', value=events)

def transform_event_logs(**context):
    events = context['ti'].xcom_pull(key='raw_events')
    data_to_insert = []
    for event in events:
        details = event.get('details', {})
        data_to_insert.append((
            event['event_id'],
            event['timestamp'],
            event['event_type'],
            details.get('page', ''),
            details.get('element', ''),
            details.get('user_id', '')
        ))
    context['ti'].xcom_push(key='processed_events', value=data_to_insert)

def load_event_logs(**context):
    data_to_insert = context['ti'].xcom_pull(key='processed_events')
    pg_conn = get_postgres_connection()
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("TRUNCATE TABLE event_logs CASCADE;")
    insert_query = """
        INSERT INTO event_logs (
            event_id, timestamp, event_type, page, element, user_id
        ) VALUES (%s, %s, %s, %s, %s, %s);
    """
    execute_batch(pg_cursor, insert_query, data_to_insert)
    pg_conn.commit()
    logger.info(f"Replecated {len(data_to_insert)} items")
    pg_cursor.close()
    pg_conn.close()

def extract_support_tickets(**context):
    mongo_db = get_mongo_connection()
    tickets = list(mongo_db.support_tickets.find({}))
    for ticket in tickets:
        if '_id' in ticket:
            ticket['_id'] = str(ticket['_id'])
    context['ti'].xcom_push(key='raw_tickets', value=tickets)

def transform_support_tickets(**context):
    tickets = context['ti'].xcom_pull(key='raw_tickets')
    tickets_data = []
    messages_data = []
    for ticket in tickets:
        messages = ticket.get('messages', [])
        tickets_data.append((
            ticket['ticket_id'],
            ticket['user_id'],
            ticket['issue_type'],
            ticket['status'],
            ticket['created_at'],
            ticket['updated_at'],
            len(messages)
        ))
        for msg in messages:
            messages_data.append((
                ticket['ticket_id'],
                msg.get('sender', ''),
                msg.get('message', ''),
                msg.get('timestamp')
            ))
    context['ti'].xcom_push(key='processed_tickets', value=tickets_data)
    context['ti'].xcom_push(key='processed_messages', value=messages_data)

def load_support_tickets(**context):
    tickets_data = context['ti'].xcom_pull(key='processed_tickets')
    messages_data = context['ti'].xcom_pull(key='processed_messages')
    pg_conn = get_postgres_connection()
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("TRUNCATE TABLE support_tickets CASCADE;")
    ticket_query = """
        INSERT INTO support_tickets (
            ticket_id, user_id, issue_type, status, created_at, 
            updated_at, messages_count
        ) VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    execute_batch(pg_cursor, ticket_query, tickets_data)
    message_query = """
        INSERT INTO support_messages (
            ticket_id, sender, message, timestamp
        ) VALUES (%s, %s, %s, %s);
    """
    execute_batch(pg_cursor, message_query, messages_data)
    pg_conn.commit()
    logger.info(f"Replicated {len(tickets_data)} & {len(messages_data)} items")
    pg_cursor.close()
    pg_conn.close()

def extract_user_recommendations(**context):
    mongo_db = get_mongo_connection()
    recommendations = list(mongo_db.user_recommendations.find({}))
    for rec in recommendations:
        if '_id' in rec:
            rec['_id'] = str(rec['_id'])
    context['ti'].xcom_push(key='raw_recomendations', value=recommendations)

def transform_user_recommendations(**context):
    recommendations = context['ti'].xcom_pull(key='raw_recomendations')
    data_to_insert = []
    for rec in recommendations:
        data_to_insert.append((
            rec['user_id'],
            rec['recommended_products'],
            rec['last_updated']
        ))
    context['ti'].xcom_push(key='processed_recommendations', value=data_to_insert)

def load_user_recommendations(**context):
    data_to_insert = context['ti'].xcom_pull(key='processed_recommendations')
    pg_conn = get_postgres_connection()
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("TRUNCATE TABLE user_recommendations CASCADE;")
    insert_query = """
        INSERT INTO user_recommendations (
            user_id, recommended_products, last_updated
        ) VALUES (%s, %s, %s);
    """
    execute_batch(pg_cursor, insert_query, data_to_insert)
    pg_conn.commit()
    logger.info(f"Replicated {len(data_to_insert)} items")
    pg_cursor.close()
    pg_conn.close()

def extract_moderation_queue(**context):
    mongo_db = get_mongo_connection()
    reviews = list(mongo_db.moderation_queue.find({}))
    for review in reviews:
        if '_id' in review:
            review['_id'] = str(review['_id'])
    context['ti'].xcom_push(key='raw_queue', value=reviews)

def transform_moderation_queue(**context):
    reviews = context['ti'].xcom_pull(key='raw_queue')
    data_to_insert = []
    for review in reviews:
        data_to_insert.append((
            review['review_id'],
            review['product_id'],
            review['user_id'],
            review['review_text'],
            review['rating'],
            review['flags'],
            review['moderation_status'],
            review['submitted_at']
        ))
    context['ti'].xcom_push(key='processed_queue', value=data_to_insert)

def load_moderation_queue(**context):
    data_to_insert = context['ti'].xcom_pull(key='processed_queue')
    pg_conn = get_postgres_connection()
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("TRUNCATE TABLE moderation_queue CASCADE;")
    insert_query = """
        INSERT INTO moderation_queue (
            review_id, product_id, user_id, review_text, rating, 
            flags, moderation_status, submitted_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    execute_batch(pg_cursor, insert_query, data_to_insert)
    pg_conn.commit()
    logger.info(f"Replicated {len(data_to_insert)} items")
    pg_cursor.close()
    pg_conn.close()

with DAG(
    'mongodb_to_postgres_replication',
    description='Replication DAG',
    start_date=datetime(year=2026, month=3, day=7)
) as dag:
    [
        PythonOperator(task_id='extract_user_sessions', python_callable=extract_user_sessions, dag=dag) >> PythonOperator(task_id='transform_user_sessions', python_callable=transform_user_sessions, dag=dag) >> PythonOperator(task_id='load_user_sessions', python_callable=load_user_sessions, dag=dag),
        PythonOperator(task_id='extract_event_logs', python_callable=extract_event_logs, dag=dag) >> PythonOperator(task_id='transform_event_logs', python_callable=transform_event_logs, dag=dag) >> PythonOperator(task_id='load_event_logs', python_callable=load_event_logs, dag=dag),
        PythonOperator(task_id='extract_support_tickets', python_callable=extract_support_tickets, dag=dag) >> PythonOperator(task_id='transform_support_tickets', python_callable=transform_support_tickets, dag=dag) >> PythonOperator(task_id='load_support_tickets', python_callable=load_support_tickets, dag=dag),
        PythonOperator(task_id='extract_user_recommendations', python_callable=extract_user_recommendations, dag=dag) >> PythonOperator(task_id='transform_user_recommendations', python_callable=transform_user_recommendations, dag=dag) >> PythonOperator(task_id='load_user_recommendations', python_callable=load_user_recommendations, dag=dag),
        PythonOperator(task_id='extract_moderation_queue', python_callable=extract_moderation_queue, dag=dag) >> PythonOperator(task_id='transform_moderation_queue', python_callable=transform_moderation_queue, dag=dag) >> PythonOperator(task_id='load_moderation_queue', python_callable=load_moderation_queue, dag=dag)
    ]
