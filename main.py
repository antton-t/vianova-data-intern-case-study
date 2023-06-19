import requests
from requests.exceptions import HTTPError
import json
import pandas as pd
import os

#URL from the website we want to scrap
base_url = "https://public.opendatasoft.com/explore/dataset/geonames-all-cities-with-a-population-1000/export/?disjunctive.cou_name_en"



def get_info_from_url(base_url) :

#Getting the response from the HTTP request
#arg url
#return string

  try :
    info = requests.get(base_url)
 
  except HTTPError as err :
    sys.exit('Http error occurred: ' + err)
  except Exception as err :
    sys.exit('Other error occurred: ' + err)
  return info.text

def get_csv(add) :

  #Getting csv link

  found = 0
  csv_find = add.find('distribution')
  sub = add[csv_find - 1 :]
  csv_find = sub.find('[')
  csv_find1 = sub.find(']')
  sub = sub[csv_find - 1 : csv_find1 + 1]
  data_list = json.loads(sub)
  for index, item in enumerate(data_list) :
    if ('CSV' in item.values()) :
      found = index
      break
  csv_data = data_list[found]
  value = [value for key, value in csv_data.items() if key == 'contentUrl'][0]
  print(value)
  return value

def read_csv() :

  #read csv and keep the 3 variables need
  #
  data_file = pd.read_csv('data.csv', on_bad_lines='skip', sep=';')
  data_to_keep = ['country_code', 'cou_name_en', 'population']
  filtered_data = data_file[data_to_keep]
  print(type(filtered_data))
  filtered_csv(filtered_data)

def filtered_csv(data) :

  #filter raw data by kepping only under 10000000 country

  data_filtered_by_pop_high = data[data['population'] > 10000000]
  data_unique = data_filtered_by_pop_high['cou_name_en'].unique()
  data = data.loc[data['cou_name_en'].isin(data_unique)==False]
  data_clean = data.drop_duplicates(subset='cou_name_en')
  del data_clean['population']
  print(data_clean)
  export_txt(data_clean)

def export_txt(data) :

  #answer export result in a txt
  data.to_csv('result.csv', sep='\t', index=False)

def main() ->int:

  add = get_info_from_url(base_url)
  value_url = get_csv(add)
  info = requests.get(value_url)
  with open('data.csv', 'wb') as file :
    file.write(info.content)
  read_csv()
  #remove csv download
  os.remove("data.csv")

  return 0

if __name__ == "__main__" :
  SystemExit(main())
