# Домашнее задание №6 по дисциплине ETL

# Задание 1

1) Созданю managed postgres кластер:
![](screenshots/01_created_postgres.png)

2) Создаю и заполняю табличку:
![](screenshots/02_create_and_fill_table.png)

3) Создаю хранилище S3 и бакет в нем:
![](screenshots/03_create_object_storage_and_bucket.png)

4) Создаю data_transfer:
![](screenshots/04_create_data_transfer_1.png)
![](screenshots/04_create_data_transfer_2.png)

5) Запускаю data_transfer и убеждаюсь что все работает:
![](screenshots/05_create_and_run_data_transfer_success.png)

6) Пример результата работы data_transfer:
![](screenshots/06_data_transfer_success_result.png)

7) Проверяю, что при повторном прогоне дататрансфер ничего не ломается, не затирается, и появляется вторая реплика данных:
![](screenshots/07_repeated_data_transfer_success.png)