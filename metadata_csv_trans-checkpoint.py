from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import csv
import sys

spark = SparkSession.builder.getOrCreate()


a = {}


def error_handling(error_message, operation_type):
    print('Error while processing ' + operation_type+'.Error Message received ' + error_message)
    sys.exit()


def read_src_file(sno, df_name='df_read', input_type='CSV'):
    print(sno, df_name)
    try:
        with open('//root//Amitkafka//Demo//read_path.csv') as path_csv:
            csv_read = csv.reader(path_csv, delimiter=',')
            count = 0
            for r in csv_read:
                if count == int(sno):
                    if input_type == 'CSV':
                        a[df_name] = spark.read.csv(r[1], header=True)
                        count += 1
                    elif input_type == 'MYSQL':
                        count += 1
                    elif input_type == 'MYSQL':
                        count += 1
                    elif input_type == 'MSSQL':
                        count += 1
                    elif input_type == 'ORACLE':
                        count += 1
                    elif input_type == 'JSON':
                        count += 1
                    elif input_type == 'PARQUET':
                        count += 1
    except ValueError:
        error_handling(ValueError, 'Source')


def read_trans_file(sno, df_name='df_trans', input1=None, input2=None):
    with open('//root//Amitkafka//Demo//Trans_metadata.csv') as trans_csv:
        trans_csv = csv.reader(trans_csv, delimiter=',')
        count = 0
        for r in trans_csv:
            if count != int(sno):
                count += 1
            elif r[1].upper() == 'JOIN' and input1 is not None and input2 is not None:
                print('Performing Join Transformation.')
                cond = [col(r[3].split('=')[0]) == col(r[3].split('=')[-1])]
                if r[4] is not '':
                    cond.append(col(r[4].split('=')[0]) == col(r[4].split('=')[-1]))
                if r[5] is not '':
                    cond.append(col(r[5].split('=')[0]) == col(r[5].split('=')[-1]))
                if r[6] is not '':
                    cond.append(col(r[6].split('=')[0]) == col(r[6].split('=')[-1]))
                a[df_name] = (a[input1].alias(input1).join(a[input2].alias(input2), cond, r[2])).select([col('%s'%(input1)+'.name'),col('%s'%(input1)+'.class')]+[col('%s'%(input2)+'.scale')])
                a[df_name].show()
                break
            elif r[1].upper() == 'FILTER':
                print('Performing Filter Transformation.')
                a[df_name] = a[input1].alias(input1).filter(r[3])
                a[df_name].show()
                break


def write_dst_file(sno, df_name='df_write'):
    with open('//root//Amitkafka//Demo//dest_file.csv') as dest_file:
        dest_csv = csv.reader(dest_file, delimiter=',')
        line_count = 0
        for r in dest_csv:
            if line_count == int(sno):
                print('Writing to HDFS FIle.')
                a[df_name].write.csv(r[1])
                break
            line_count += 1


with open('//root//Amitkafka//Demo//metadata.csv') as csv_file:
    print("Reading Metadata File")
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            line_count += 1
        elif row[2] == 'SRC':
            print("Processing Source Read Operation.")
            read_src_file(row[1], row[3])
            print("Processing Transformation Operation.")
        elif row[2] == 'TRS':
            print("Reading  File.")
            read_trans_file(row[1], row[3], row[4], row[5])
        elif row[2] == 'DST':
            print("Processing Destination Write Operation.")
            write_dst_file(row[1], row[4])




