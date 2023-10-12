# format df_preprocessed.json, so that its line by line

# csv to line by line json
import pandas as pd
import json

# # Replace 'input.csv' with the path to your CSV file
# output_json = 'streaming.json'

# # Read the CSV file into a Pandas DataFrame
# data = pd.read_csv("streaming.csv")

# # Convert the data to JSON
# data_json = data.to_dict(orient='records')

# # Read the JSON objects back into each line
# with open(output_json, 'w') as f:
#     for entry in data_json:
#         json.dump(entry, f)
#         f.write('\n')  # Add a newline after each JSON object

# take first 2.1k rows of streaming.json into demo.json
with open('streaming.json') as f:
    with open('demo.json', 'w') as f2:
        for i in range(2100):
            f2.write(f.readline())