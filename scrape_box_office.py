import airflow
import requests
import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3
import os

current_week = datetime.now().isocalendar()
year, week_number = current_week[0], current_week[1]


dag = DAG(
    dag_id="scrape_box_office",
    description="Download weekly box office data and save to database.",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@weekly",
)


def _scrape_data():
    response = requests.get(
        f"https://www.boxofficemojo.com/weekly/{year}W{week_number}/"
    )
    soup = BeautifulSoup(response.text, "html.parser")

    rows = []
    for move in soup.find_all("tr")[1:]:
        rank = move.find_all("td")[0].text
        title = move.find_all("td")[2].text.replace(",", "")
        gross = move.find_all("td")[3].text
        gross = gross.replace("$", "").replace(",", "")
        rows.append((rank, title, gross))
    with open("/tmp/boxoffice.txt", "w") as f:
        f.writelines([",".join(row) + "\n" for row in rows])


def _append_to_db():
    conn = sqlite3.connect("/tmp/boxoffice.db")
    # drop table for testing
    # conn.execute(" DROP TABLE IF EXISTS box_office")

    SQL = """
        CREATE TABLE IF NOT EXISTS box_office
        (id INTEGER PRIMARY KEY AUTOINCREMENT, year INT, week INT, rank INT, title TEXT, gross INT)
    """
    conn.execute(SQL)

    with open("/tmp/boxoffice.txt", "r") as f:
        for row in f.readlines():
            row = row.split(",")
            SQL = """INSERT INTO box_office
                     (year, week, rank, title, gross)
                     VALUES (?, ?, ?, ?, ?)
            """
            print(SQL)
            conn.execute(SQL, (year, week_number, row[0], row[1], row[2]))
    conn.commit()
    conn.close()


def _validate_db_inserts():
    # Get amount of rows inserted into db
    conn = sqlite3.connect("/tmp/boxoffice.db")
    SQL = "SELECT COUNT(*) FROM box_office WHERE YEAR = (?) AND WEEK = (?)"
    insert_count = conn.execute(SQL, (year, week_number)).fetchone()[0]
    print(f"inserted rows: {insert_count}")

    # Get amount of rows scraped
    with open("/tmp/boxoffice.txt", "r") as f:
        scraped_count = len(f.readlines())

    print(f"DB INSERTS: {insert_count} SCRAPED: {scraped_count}")
    if insert_count != scraped_count:
        raise Exception("Error: The amount inserted does not match the amount scraped")
    else:
        print("Data validation complete.")


def _cleanup_tmp_files():
    os.remove("/tmp/boxoffice.txt")


scrape_data = PythonOperator(
    task_id="scrape_data", python_callable=_scrape_data, dag=dag
)

append_to_db = PythonOperator(
    task_id="append_to_db", python_callable=_append_to_db, dag=dag
)

validate_db_inserts = PythonOperator(
    task_id="valiadte_db_inserts", python_callable=_validate_db_inserts, dag=dag
)

clean_up_tmp_files = PythonOperator(
    task_id="clean_up_tmp_files", python_callable=_cleanup_tmp_files, dag=dag
)

scrape_data >> append_to_db >> validate_db_inserts >> clean_up_tmp_files