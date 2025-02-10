import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Configuration
SERVICE_ACCOUNT_FILE = 'service-account.json'
DBCANA_SHEET_ID = '1beyQsjx8NgcuuEVf_t9VfmYB1Cet_HaH5vE5MkO-9W8'
VC_SHEET_ID = '1IeQJi-E8u6yHiSdyK0utr4gt-ARE6uR296C3lcbhw_Q'

DBCANA_SHEET_NAMES = [
    'DBCANA-SP-PPCSpend', 'DBCANA-SP-PPCSales', 'DBCANA-SP-PPCUnitsSold',
    'DBCANA-SP-PPCOrders', 'DBCANA-SP-Clicks', 'DBCANA-SP-Impressions',
    'DBCANA-Orders', 'DBCANA-OrderedRevenue'
]

VC_SHEET_NAMES = [
    'VC-CA-SP-PPCSpend', 'VC-CA-SP-PPCSales', 'VC-CA-SP-PPCUnitsSold',
    'VC-CA-SP-PPCOrders', 'VC-CA-SP-Clicks', 'VC-CA-SP-Impressions',
    'VC-Orders', 'VC-OrderedRevenue'
]

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Logging configuration
logging.basicConfig(filename='create_sheet_tabs.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
logger = logging.getLogger()

# Function to get the existing sheets (tabs) in the spreadsheet
def get_existing_sheets(sheet_id):
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=credentials)
    spreadsheet = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheets = spreadsheet.get('sheets', [])
    return [sheet['properties']['title'] for sheet in sheets]

# Function to add a sheet (tab) to a spreadsheet
def add_sheet_to_spreadsheet(sheet_id, sheet_name):
    try:
        # Authenticate and build the Google Sheets service
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=credentials)
        sheet = service.spreadsheets()

        # Add the new sheet (tab) to the spreadsheet
        request_body = {
            "requests": [
                {
                    "addSheet": {
                        "properties": {
                            "title": sheet_name
                        }
                    }
                }
            ]
        }

        logger.info(f"Adding sheet '{sheet_name}' to spreadsheet with ID {sheet_id}")
        sheet.batchUpdate(spreadsheetId=sheet_id, body=request_body).execute()
        logger.info(f"Successfully added sheet '{sheet_name}' to spreadsheet ID {sheet_id}")

    except Exception as e:
        logger.error(f"An error occurred while adding the sheet {sheet_name} to {sheet_id}: {e}")
        raise

# Function to check and create sheets for a spreadsheet
def create_sheets_for_spreadsheet(sheet_id, sheet_names):
    existing_sheets = get_existing_sheets(sheet_id)
    logger.info(f"Existing sheets in the spreadsheet {sheet_id}: {existing_sheets}")

    for name in sheet_names:
        if name not in existing_sheets:
            add_sheet_to_spreadsheet(sheet_id, name)
        else:
            logger.info(f"Sheet '{name}' already exists in spreadsheet {sheet_id}. Skipping creation.")

def main():
    # Create sheets for DBCANA
    logger.info("Creating sheets for DBCANA")
    create_sheets_for_spreadsheet(DBCANA_SHEET_ID, DBCANA_SHEET_NAMES)

    # Create sheets for VC
    logger.info("Creating sheets for VC")
    create_sheets_for_spreadsheet(VC_SHEET_ID, VC_SHEET_NAMES)

if __name__ == "__main__":
    main()
