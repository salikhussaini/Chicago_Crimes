import os 
#Get Current File Path
file_path = os.path.dirname(os.path.abspath(__file__))
new_dir = file_path+r'\Crime_Year'

for year in range(2001,2023):
    os.mkdir(f'{new_dir}\{year}')