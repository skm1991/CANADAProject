import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
from time import sleep

# Configuration
BIGQUERY_PROJECT_ID = 'soundarounddatondw'
BIGQUERY_DATASET_ID = 'Power_BI_KPI'
DBCANA_SPREADSHEET_ID = '1beyQsjx8NgcuuEVf_t9VfmYB1Cet_HaH5vE5MkO-9W8'  # Spreadsheet for DBCANA
VC_SPREADSHEET_ID = '1IeQJi-E8u6yHiSdyK0utr4gt-ARE6uR296C3lcbhw_Q'      # Spreadsheet for VC
SERVICE_ACCOUNT_FILE = 'service-account.json'
SENDER_EMAIL = "powerbidev@pyleusa.com"
RECEIVER_EMAIL = "powerbidev@pyleusa.com"
EMAIL_PASSWORD = "Mart@#67#5"

# Logging configuration
logging.basicConfig(filename='../main.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Email notification function
def send_email(subject, body):
    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECEIVER_EMAIL
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP('smtp.office365.com', 587) as server:
            server.starttls()
            server.login(SENDER_EMAIL, EMAIL_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())
            logger.info("Email sent successfully")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")

# Queries and corresponding Google Sheets ranges for DBCANA and VC
dbcana_queries = {
    'DBCANA-SP-PPCSpend': f"SELECT * FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.SC_DBCANA_PPC_SP_SPEND_CDF`;",
    'DBCANA-SP-PPCSales': f"SELECT * FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.SC_DBCANA_PPC_SP_SALES_CDF`;",
    'DBCANA-SP-PPCUnitsSold': f"SELECT * FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.SC_DBCANA_PPC_SP_UNITS_CDF`;",
    'DBCANA-SP-PPCOrders': f"SELECT * FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.SC_DBCANA_PPC_SP_ORDERS_CDF`;",
    'DBCANA-SP-Clicks': f"SELECT * FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.SC_DBCANA_PPC_SP_CLICKS_CDF`;",
    'DBCANA-SP-Impressions': f"SELECT * FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.SC_DBCANA_PPC_SP_IMPRESSIONS_CDF`;",
    'DBCANA-Orders': f"SELECT * FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.SC_CA_DBCANA_ORDEREDUNITS_CDF`;",
    'DBCANA-OrderedRevenue': f"SELECT * FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.SC_CA_DBCANA_ORDEREDREVENUE_CDF`;"
}

vc_queries = {
    'VC-CA-SP-PPCSpend': f"SELECT * FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.VC_CA_PPC_SP_SPEND_CDF`;",
    'VC-CA-SP-PPCSales': f"SELECT * FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.VC_CA_PPC_SP_SALES_CDF`;",
    'VC-CA-SP-PPCUnitsSold': f"SELECT * FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.VC_CA_PPC_SP_UNITS_CDF`;",
    'VC-CA-SP-PPCOrders': f"SELECT * FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.VC_CA_PPC_SP_ORDERS_CDF`;",
    'VC-CA-SP-Clicks': f"SELECT * FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.VC_CA_PPC_SP_CLICKS_CDF`;",
    'VC-CA-SP-Impressions': f"SELECT * FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.VC_CA_PPC_SP_IMPRESSIONS_CDF`;",
    'VC-Orders': f"SELECT * FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.VC_CA_ORDEREDUNITS_CDF`;",
    'VC-OrderedRevenue': f"SELECT * FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.VC_CA_ORDEREDREVENUE_CDF`;"
}

# Function to update SyncLOG sheet
def update_sync_log(spreadsheet_id, updated_sheets_log):
    try:
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        service = build('sheets', 'v4', credentials=credentials)
        sheet = service.spreadsheets()

        # Prepare data for SyncLOG
        sync_data = [["Sheet Name", "Rows Updated", "Last Update Timestamp"]]
        sync_data.extend(updated_sheets_log)

        logger.info(f"Updating SyncLOG sheet in spreadsheet {spreadsheet_id}")
        sheet.values().update(
            spreadsheetId=spreadsheet_id,
            range="SyncLOG!A1",
            valueInputOption='USER_ENTERED',
            body={'values': sync_data}
        ).execute()

        logger.info(f"SyncLOG sheet updated successfully in spreadsheet {spreadsheet_id}")
    except Exception as e:
        logger.error(f"Failed to update SyncLOG sheet in spreadsheet {spreadsheet_id}: {e}")
        raise

# Function to run query with retry logic
def run_query_with_retry(client, query, retries=3):
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Running query (Attempt {attempt}): {query}")
            query_job = client.query(query)
            results = query_job.result()
            return results
        except Exception as e:
            logger.error(f"Attempt {attempt} failed: {e}")
            if attempt < retries:
                sleep(5)  # wait for 5 seconds before retrying
                logger.info(f"Retrying query (Attempt {attempt + 1})")
            else:
                logger.error(f"All {retries} attempts failed for query: {query}")
                raise e

# Function to update data in Google Sheets
def update_google_sheets(queries_and_sheets, spreadsheet_id, client):
    try:
        # Credentials for Google Sheets API
        sheets_credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        service = build('sheets', 'v4', credentials=sheets_credentials)
        sheet = service.spreadsheets()

        # List to store log data for SyncLOG
        updated_sheets_log = []

        for sheet_name, query in queries_and_sheets.items():
            logger.info(f"Running query for sheet: {sheet_name}")
            # Run query with retry logic
            results = run_query_with_retry(client, query)

            logger.info(f"Query completed for sheet: {sheet_name}. Converting results to DataFrame")
            # Convert the query results to a DataFrame
            df = results.to_dataframe().fillna(0)

            logger.info(f"Converted results to DataFrame for sheet: {sheet_name}. Preparing data for Google Sheets")
            # Convert DataFrame to list of lists for Google Sheets
            data = [df.columns.tolist()] + df.values.tolist()

            logger.info(f"Clearing existing data in Google Sheets range: {sheet_name}")
            # Clear the existing data in the Google Sheets range
            sheet.values().clear(spreadsheetId=spreadsheet_id, range=sheet_name, body={}).execute()

            logger.info(f"Writing new data to Google Sheets range: {sheet_name}")
            # Write the data to Google Sheets
            sheet.values().update(
                spreadsheetId=spreadsheet_id,
                range=sheet_name,
                valueInputOption='USER_ENTERED',
                body={'values': data}
            ).execute()

            rows_updated = len(data) - 1
            logger.info(f"{rows_updated} rows updated in sheet '{sheet_name}'.")

            # Log data for SyncLOG
            update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            updated_sheets_log.append([sheet_name, rows_updated, update_time])

        # Update SyncLOG sheet with update details
        update_sync_log(spreadsheet_id, updated_sheets_log)

    except Exception as e:
        logger.error(f"An error occurred while updating Google Sheets for spreadsheet {spreadsheet_id}: {e}")
        raise

def run_script():
    try:
        logger.info("Authenticating to BigQuery and Google Sheets API")
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
        bigquery_client = bigquery.Client(credentials=credentials, project=BIGQUERY_PROJECT_ID)

        # Update DBCANA sheets
        logger.info("Updating DBCANA sheets")
        update_google_sheets(dbcana_queries, DBCANA_SPREADSHEET_ID, bigquery_client)

        # Update VC sheets
        logger.info("Updating VC sheets")
        update_google_sheets(vc_queries, VC_SPREADSHEET_ID, bigquery_client)

        logger.info("main.py ran successfully")
        send_email("main.py Success", "CANADA Google Sheets Updated.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        send_email("main.py Failed", f"CANADA Google Sheets sync encountered an error:\n\n{e}")

if __name__ == "__main__":
    run_script()
