import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Define the scope
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Add credentials to the account
creds = ServiceAccountCredentials.from_json_keyfile_name("service-account.json", scope)

# Authorize the clientsheet
client = gspread.authorize(creds)

# Open the Google Sheet (by name or by URL)
sheet = client.open("Weekly Data Sheet-CANADA")

# List of sheet names to be created
sheet_names = [
    "Net PPM | CA", "VC-CA-SP-PPCSpend", "VC-CA-SP-PPCSales", "VC-CA-SP-PPCUnitsSold",
    "VC-CA-SP-PPCOrders", "VC-CA-SP-Clicks", "VC-CA-SP-Impressions", "Conversion Rate | AMZ All",
    "Click Through Rate | PPC-VC-CA-SP", "Conversion Rate | PPC ALL", "ACOS | PPC-CA All",
    "TACoS | PPC-CA All", "TACoS COGS | PPC-VC-CA-SP", "DBCANA-SP-PPCSpend", "DBCANA-SP-PPCSales",
    "DBCANA-SP-PPCUnitsSold", "DBCANA-SP-PPCOrders", "DBCANA-SP-Clicks", "DBCANA-SP-Impressions",
    "AMZ-ALL-CA-PPCSpend", "AMZ-ALL-CA-PPCSales", "AMZ-ALL-CA-PPCUnitsSold", "AMZ-ALL-CA-PPCOrders",
    "AMZ-ALL-CA-Clicks", "AMZ-ALL-CA-Impressions", "Glance Views | VC-CA"
]

# Create the sheets if they do not exist
existing_sheets = [sh.title for sh in sheet.worksheets()]

for sheet_name in sheet_names:
    if sheet_name not in existing_sheets:
        sheet.add_worksheet(title=sheet_name, rows="1000", cols="20")
        print(f"Created sheet: {sheet_name}")
    else:
        print(f"Sheet already exists: {sheet_name}")

print("All sheets are created or already existed.")