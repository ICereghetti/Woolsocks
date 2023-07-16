import csv
import json

input_file = 'woolsocks.bankaccounts.csv'
output_file = 'bankaccounts.csv'

with open(input_file, 'r') as f_in, open(output_file, 'w', newline='') as f_out:
    reader = csv.reader(f_in)
    writer = csv.writer(f_out)
    
    # Write the header row
    writer.writerow(next(reader))
    
    for row in reader:
        # Remove double quotes from JSON objects
        for i in range(len(row)):
            try:
                row[i] = json.loads(row[i])
            except (json.JSONDecodeError, TypeError):
                pass
        
        # Write the modified row to the output file
        writer.writerow(row)
        
