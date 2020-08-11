import json
import os
import io
import xlrd
import boto3
import csv


def lambda_handler(event, context):
    bucketname = [INPUTBCKTNAME]
    
    ###### CSV
    csv_files_list = ['corn-prices-historical-chart-data.csv','Macrotrends-crude-oil-prices-daily.csv','rice-futures.csv','soybean-prices-historical-chart-data.csv']


    for csv_file in csv_files_list:
        #print(csv_file)
        key = csv_file
        s3_resource = boto3.resource('s3')
        s3_object = s3_resource.Object(bucketname, key)
        data = s3_object.get()['Body'].read().decode('utf-8').splitlines()
        lines = csv.reader(data, delimiter=',')
        bucket_name_output = [OUTPUT_BCKTNAME]
        file_name = "output.csv"
        lambda_path = "/tmp/" + file_name
        s3_path = "output/" + file_name
        os.system('echo testing... >'+lambda_path)
        with open(lambda_path, 'w+') as file:
            i=0
            for line in lines:
                i = i+1
                if i == 3:
                    namefile = line[0] + '.csv'
                if len(line) == 2 and len(line[1])>0:
                    file.write(",".join(line)+ '\n')
            file.close()
        s3_resource.meta.client.upload_file(lambda_path, bucket_name_output, namefile)
    
    
    
    ###### EXCEL 
    key = 'SlaughterCountsFull.xlsx'
    s3_resource = boto3.resource('s3')
    s3_object = s3_resource.Object(bucketname, key)
    content = s3_object.get()['Body'].read()
    wb = xlrd.open_workbook(file_contents=content)
    sheet = wb.sheet_by_index(0)
    bucket_name_output = [OUTPUT_BCKTNAME]
    file_name = "output.csv"
    lambda_path = "/tmp/" + file_name
    s3_path = "output/" + file_name
    os.system('echo testing... >'+lambda_path)
    for j in range(31,34):
        with open(lambda_path, 'w+') as file:
            file.write('date' + ',' + 'value' + '\n')
            for i in range(sheet.nrows):
                val = sheet.cell_value(i, j)
                if isinstance(val, str):
                    if len(val) > 0:
                        namefile = 'SlaughterCounts-' + val + '.csv'
                elif isinstance(val, float):
                    file.write(str(sheet.cell_value(i,0)) + ',' + str(sheet.cell_value(i,j)) + '\n')
                    #print(sheet.cell_value(i,0),sheet.cell_value(i,31))
            file.close()
        s3_resource.meta.client.upload_file(lambda_path, bucket_name_output, namefile)
        
