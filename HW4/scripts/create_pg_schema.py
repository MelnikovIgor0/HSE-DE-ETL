import psycopg2

def main():
    conn = psycopg2.connect(
        host='postgres',
        database='airflow',
        user='admin',
        password='admin'
    )
    conn.autocommit = True
    cursor = conn.cursor()
    with open('/opt/airflow/scripts/init_db.sql', 'r') as file:
        cursor.execute(file.read())
    cursor.close()
    conn.close()
    print("SUCCESS")

if __name__ == '__main__':
    main()
