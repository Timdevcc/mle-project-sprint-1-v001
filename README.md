Airflow

Код DAG первого этапа: part1_airflow/dags/flats_price.py

Функция DAG первого этапа: prepare_flats_price_dataset

Код DAG второго этапа: part1_airflow/dags/clean_flats_dataset.py

Функция DAG второго этапа: clean_flats_price_dataset

DVC
Python-код этапов DVC-пайплайна: part2_dvc/scripts/

Файлы конфигурации DVC-пайплайна:
part2_dvc/dvc.yaml
part2_dvc/params.yaml
part2_dvc/dvc.lock

Имя бакета: s3-student-mle-20260702-1d35328110-freetrack

Для корректной работы с тг ботом вставьте TOKEN и CHAT_ID в part1_airflow/.env
