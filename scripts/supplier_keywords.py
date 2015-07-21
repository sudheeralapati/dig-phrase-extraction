import xlrd
import json

def open_file(excel_file_path,json_file_path):
    excel_file = xlrd.open_workbook(excel_file_path)
    jsonFile = open(json_file_path,'w')
    names = excel_file.sheet_names()

    jsonArray=[]

    for name in names:
        if name != 'Overview':
            sheet = excel_file.sheet_by_name(name)
            cell_values = sheet._cell_values
            for cell_value in cell_values:
                if cell_value[0] != 'From':
                    jsonObj={}
                    jsonObj['company']=cell_value[0]
                    jsonObj['address']=cell_value[1]
                    jsonObj['district']=cell_value[2]
                    jsonObj['city']=cell_value[3]
                    jsonArray.append(jsonObj)
    jsonFile.write(json.dumps(jsonArray,indent=4))


open_file('China Microelectronics Suppliers by bbuilding.xlsx','suppliers_keywords.json')