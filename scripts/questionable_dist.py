import xlrd
import json

def open_file(excel_file_path,json_file_path):
    excel_file = xlrd.open_workbook(excel_file_path)
    jsonFile = open(json_file_path,'w')
    names = excel_file.sheet_names()
    jsonArray=[]
    for name in names:
        sheet = excel_file.sheet_by_name(name)
        if  not 'USA' in name:
            cell_values = sheet._cell_values
            for cell_value in cell_values:
                if cell_value[0] != 'Distributor':
                    jsonObj={}
                    jsonObj['distributor']=cell_value[0]
                    jsonObj['state']=cell_value[1]
                    jsonObj['country']=cell_value[2]
                    jsonArray.append(jsonObj)
    jsonFile.write(json.dumps(jsonArray,indent=4))


open_file('Questionable Distributor Listing - 150331.xlsx','distributors_keywords.json')