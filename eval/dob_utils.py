from datetime import datetime

def classify_dob(dob):
    if dob == "XX-XX-XXXX":
        return 3
    elif "XX" in dob:
        return 2
    else:
        return 1

def validate_date_format(dob):
    parts = dob.split('-')
    month, day, year = 'XX', 'XX', 'XXXX'
    if len(parts) == 3:
        month = parts[0] if parts[0].isdigit() else 'XX'
        day = parts[1] if parts[1].isdigit() else 'XX'
        year = parts[2] if parts[2].isdigit() else 'XXXX'
        try:
            date_str = f"{int(month):02d}-{int(day):02d}-{year}"
            datetime.strptime(date_str, "%m-%d-%Y")
            return date_str
        except ValueError:
            return f"{month}-{day}-{year}"
    else:
        return "XX-XX-XXXX"
