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
    with open('/opt/airflow/scripts/init_marts.sql', 'r') as file:
        cursor.execute(file.read())
    print("SUCCESS")
    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()
