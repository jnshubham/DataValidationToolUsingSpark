# Sort Files
import os, json
path = r'C:\Users\yfcb\Downloads\adf-spmintg-credit\adf-spmintg-credit\dataflow'
for folder,_,files in os.walk(path):
    for file in files:
        if '.json' in file:
            with open(folder+'\\'+file) as f:
                data = json.load(f)
            try:
                print(file,data['properties']['folder']['name'])
            except Exception as e:
                pass
            
        