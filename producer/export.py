import os
import csv
import json
from datetime import datetime
    
    
data_folder = "исходные данные"
data = []
for file_name in sorted(os.listdir(data_folder)):
    file_path_name = os.path.join(data_folder, file_name)
    with open(file_path_name, 'r', encoding="UTF-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
            data[-1]["id"] = len(data)
    print(len(data))

    with open("data.json", "w", encoding="UTF-8") as file_out:
        json.dump(data, file_out, ensure_ascii=False, indent=2)