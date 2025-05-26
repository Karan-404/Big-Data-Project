import re
import csv

input_file = "sample_weather.txt"
output_file = "weather_cleaned.csv"

def clean_line(line):
    parts = re.split(r'\s+', line.strip())
    if len(parts) < 17:
        return None
    record = {
        "station_id": parts[0],
        "wmo_id": parts[1],
        "datetime": parts[2],
        "temperature": parts[3],
        "humidity": parts[4],
        "pressure": parts[6],
        "alt_pressure": parts[8],
        "wind_speed": parts[10],
        "wind_dir": parts[12],
        "max_temp": parts[13],
        "min_temp": parts[14],
        "rainfall": parts[15].replace("I", "")
    }
    return record

with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
    fieldnames = ["station_id","wmo_id","datetime","temperature","humidity","pressure","alt_pressure","wind_speed","wind_dir","max_temp","min_temp","rainfall"]
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    for line in infile:
        rec = clean_line(line)
        if rec:
            writer.writerow(rec)

print(f"Data cleaned and saved to {output_file}")
