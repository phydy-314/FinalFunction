import datetime
import logging
import os
import json
import pymssql
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

app = func.FunctionApp()

STORAGE_CONNECTION_STRING = os.getenv("AzureWebJobsStorage")
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
BLOB_CONTAINER = "bidssfinal"
BLOB_NAME = "data.json"
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
EMAIL_TO = "vynnp22416c@st.uel.edu.vn" 
EMAIL_FROM = "phuongvynguyenngoc97@gmail.com" 


def download_blob_in_memory():
    try:
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER, blob=BLOB_NAME)
        blob_data = blob_client.download_blob().readall()  
        logging.info(f"Downloaded {BLOB_NAME} from Blob Storage")
        return json.loads(blob_data.decode('utf-8'))  
    except Exception as e:
        error_message = f"Blob download failed: {e}"
        logging.error(error_message)
        send_error_email(error_message)
        return None


def send_error_email(error_message):
    if not SENDGRID_API_KEY or not EMAIL_TO:
        logging.error("SendGrid API Key hoặc email nhận không được cấu hình!")
        return
    
    message = Mail(
        from_email=EMAIL_FROM,
        to_emails=EMAIL_TO,
        subject="Azure Function Error - File Load",
        html_content=f"<strong>Error:</strong> {error_message}")
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        logging.info(f"SendGrid Response: {response.status_code}")
    except Exception as email_error:
        logging.error(f"Failed to send email: {email_error}")

def update_database(data):
    try:
        conn = pymssql.connect(server=SQL_SERVER, user=SQL_USERNAME, password=SQL_PASSWORD, database=SQL_DATABASE)
        cursor = conn.cursor()
        for row in data:
            logging.info(f"🔹 SQL Data: ID={row['id']}, Name={row['name']}, Age={row['age']}")

        cursor.execute("SELECT 1 FROM sys.tables WHERE name = 'Users'")
        if cursor.fetchone() is None:
            logging.info(" Table 'Users' chưa tồn tại!")
        else:
            logging.info(" Table 'Users' tồn tại!")

        values = [(row["id"], row["name"], row["age"]) for row in data]
        cursor.executemany("""
        MERGE INTO Users AS target
        USING (SELECT %s AS ID, %s AS Name, %s AS Age) AS source
        ON target.ID = source.ID
        WHEN MATCHED THEN 
            UPDATE SET Name = source.Name, Age = source.Age
        WHEN NOT MATCHED THEN
            INSERT (ID, Name, Age) VALUES (source.ID, source.Name, source.Age);
        """, values)

        conn.commit()
        logging.info("Database updated successfully!")
        conn.close()
    except Exception as e:
        logging.error(f"Database update failed: {e}")
        send_error_email(str(e))


@app.timer_trigger(schedule="0 */30 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=False)
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().isoformat()
    logging.info(f"Azure Function triggered at {utc_timestamp}")
    data = download_blob_in_memory()
    if data is None:
        return  

    update_database(data)


@app.event_grid_trigger(arg_name="azeventgrid")
def EventGridTrigger(azeventgrid: func.EventGridEvent):
    logging.info("Python EventGrid trigger processed an event")

    data = download_blob_in_memory()
    if data is None:
        return  

    update_database(data)
