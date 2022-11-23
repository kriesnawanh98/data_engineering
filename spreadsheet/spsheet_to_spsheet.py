def copy_sheet_to_another_file(
    source_spreadsheet_id,
    source_sheet_id: int,
    sheet_name,
    destination_spreadsheet_id,
):
    from google.auth import default
    from googleapiclient import discovery
    from pprint import pprint
    import gspread

    # delete a sheet in destination file
    creds, _ = default()
    gc = gspread.authorize(creds)

    try:
        spreadsheet = gc.open_by_key(destination_spreadsheet_id)
        worksheet = spreadsheet.worksheet(f"{sheet_name}")
        spreadsheet.del_worksheet(worksheet)
    except Exception as e:
        print(f"No need to delete sheet {sheet_name} in destination file")

    # Copy the selected sheet to another file spreadsheet
    credentials = None
    service = discovery.build('sheets', 'v4', credentials=credentials)

    copy_spreadsheet_request_body = {
        'destination_spreadsheet_id': destination_spreadsheet_id,
    }

    request = service.spreadsheets().sheets().copyTo(
        spreadsheetId=source_spreadsheet_id,
        sheetId=source_sheet_id,
        body=copy_spreadsheet_request_body)
    response = request.execute()

    pprint(response)

    # rename the sheet in detination file
    rename_worksheet = gc.open_by_key(destination_spreadsheet_id).worksheet(
        f"Copy of {sheet_name}")

    rename_worksheet.update_title(f"{sheet_name}")


source_spreadsheet_id = "1EEYBU8g0WOEHy3WqWpcH731sTg5xbT6LBPK9Yc2SopY"
source_sheet_id = 1229142366
sheet_name = "index_category_weight"
destination_spreadsheet_id = "1YgKvnecsNpQ-e5y1uXj2BBtHo3vKbEAHGh8vuwhAOuc"

copy_sheet_to_another_file(
    source_spreadsheet_id=source_spreadsheet_id,
    source_sheet_id=source_sheet_id,
    sheet_name=sheet_name,
    destination_spreadsheet_id=destination_spreadsheet_id)
