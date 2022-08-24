from msilib.schema import Error
import subprocess, os, sys, glob, pathlib
import pandas as pd

def initializeValidation(kwargs):
    kwargs['filePath'] = os.path.abspath(os.getcwd())
    memory = '60g'
    
    if(sys.platform=='win32'):
        kwargs['driverPath'] = f'''{kwargs['filePath']}\\bin\\mssql-jdbc-11.2.0.jre8.jar;{kwargs['filePath']}\\bin\\mssql-jdbc_auth-11.2.0.x64.dll;'''
        kwargs['conf'] = f'''--conf spark.driver.extraClassPath="{kwargs["driverPath"]}" --conf spark.driver.memory='{memory}' '''
        
        if(kwargs['filterCondition']==''):
            #cmd = '''spark-submit "{filePath}\\spark\\compareData.py" --sourceURL '{sourceURL}' --targetURL '{targetURL}' --sourceUser '{sourceUser}' --targetUser '{targetUser}' --sourcePassword '{sourcePassword}' --targetPassword '{targetPassword}' --sourceDatabase '{sourceDatabase}' --targetDatabase '{targetDatabase}' --sourceTable '{sourceTable}' --targetTable '{targetTable}' --keyColumns '{keyColumns}' --excludedColumns '{excludedColumns}' --filterCondition '{filterCondition}' '''.format(**kwargs)
            cmd = '''python "{filePath}\\spark\\compareData.py" --sourceURL '{sourceURL}' --targetURL '{targetURL}' --sourceUser '{sourceUser}' --targetUser '{targetUser}' --sourcePassword '{sourcePassword}' --targetPassword '{targetPassword}' --sourceDatabase '{sourceDatabase}' --targetDatabase '{targetDatabase}' --sourceTable '{sourceTable}' --targetTable '{targetTable}' --keyColumns '{keyColumns}' --excludedColumns '{excludedColumns}' '''.format(**kwargs)
        else:
            cmd = '''python "{filePath}\\spark\\compareData.py" --sourceURL '{sourceURL}' --targetURL '{targetURL}' --sourceUser '{sourceUser}' --targetUser '{targetUser}' --sourcePassword '{sourcePassword}' --targetPassword '{targetPassword}' --sourceDatabase '{sourceDatabase}' --targetDatabase '{targetDatabase}' --sourceTable '{sourceTable}' --targetTable '{targetTable}' --keyColumns '{keyColumns}' --excludedColumns '{excludedColumns}' --filterCondition '{filterCondition}' '''.format(**kwargs)

        
    else:
        kwargs['driverPath'] = f'''{kwargs['filePath']}/bin/mssql-jdbc-11.2.0.jre8.jar:{kwargs['filePath']}/bin/mssql-jdbc_auth-11.2.0.x64.dll'''
        kwargs['conf'] = f'''--conf spark.driver.extraClassPath={kwargs['driverPath']} --conf spark.driver.memory={memory} '''
        
        if(kwargs['filterCondition']==''):
            cmd = '''spark-submit {conf} '{filePath}/spark/compareData.py' --sourceURL '{sourceURL}' --targetURL '{targetURL}' --sourceUser '{sourceUser}' --targetUser '{targetUser}' --sourcePassword '{sourcePassword}' --targetPassword '{targetPassword}' --sourceDatabase '{sourceDatabase}' --targetDatabase '{targetDatabase}' --sourceTable '{sourceTable}' --targetTable '{targetTable}' --keyColumns '{keyColumns}' --excludedColumns '{excludedColumns}' '''.format(**kwargs)
            #cmd = '''python "{filePath}\\spark\\compareData.py" --sourceURL '{sourceURL}' --targetURL '{targetURL}' --sourceUser '{sourceUser}' --targetUser '{targetUser}' --sourcePassword '{sourcePassword}' --targetPassword '{targetPassword}' --sourceDatabase '{sourceDatabase}' --targetDatabase '{targetDatabase}' --sourceTable '{sourceTable}' --targetTable '{targetTable}' --keyColumns '{keyColumns}' --excludedColumns '{excludedColumns}' --filterCondition '{filterCondition}' '''.format(**kwargs)
        else:
            cmd = '''spark-submit {conf} '{filePath}/spark/compareData.py' --sourceURL '{sourceURL}' --targetURL '{targetURL}' --sourceUser '{sourceUser}' --targetUser '{targetUser}' --sourcePassword '{sourcePassword}' --targetPassword '{targetPassword}' --sourceDatabase '{sourceDatabase}' --targetDatabase '{targetDatabase}' --sourceTable '{sourceTable}' --targetTable '{targetTable}' --keyColumns '{keyColumns}' --excludedColumns '{excludedColumns}' --filterCondition '{filterCondition}' '''.format(**kwargs)        
        
    print(cmd)
    #os.system(cmd)
    op = subprocess.run(cmd)
    if(op.returncode!=0):
        raise Error
    else:
        print('Success')
        return fetchData(kwargs)
        
def fetchData(kwargs):
    if(sys.platform=='win32'):
        baseDirectory = "{kwargs['filepath']}\\output\\{kwargs['sourceTable']}\\"
        latestDirectory = max(glob.glob(os.path.join(baseDirectory, '*/')), key=os.path.getmtime)
    else:
        baseDirectory = "{kwargs['filepath']}/output/{kwargs['sourceTable']}/"
        latestDirectory = max(glob.glob(os.path.join(baseDirectory, '*/')), key=os.path.getmtime)
    
    # files = pathlib.Path(latestDirectory).glob('*')
    # for file in files:
    #     processFile(file)
    countResult = ''
    countFlag = 'FAILED'
    countfilePath = ''
    dataResult = ''
    dataFlag = 'FAILED'
    datafilePath = ''
    datatypeResult = ''
    datatypeFlag = 'FAILED'
    datatypefilePath = ''
    
    for filename in os.listdir(latestDirectory):
        if('datatypecomparision' in filename.lower()):
            datatypefilePath =  os.path.join(latestDirectory, filename)
            if('_success' in datatypefilePath):
                datatypeFlag= 'SUCCESS'
            df = pd.read_csv(datatypefilePath)
            htmldf = df.to_html(index=False)
            datatypeResult =  htmldf.replace('<table border="1" class="dataframe">','<table class="table table-bordered table-hover table-sm">').replace('<thead>','<thead class="thead-dark">').replace('<th>','<th scope="col">').replace('<tr style="text-align: right;">','')
            
        elif('datacomparision' in filename.lower()):
            datafilePath =  os.path.join(latestDirectory, filename)
            if('_success' in datafilePath):
                dataFlag= 'SUCCESS'
            df = pd.read_csv(datafilePath)
            htmldf = df.head(200).to_html(index=False)
            dataResult =  htmldf.replace('<table border="1" class="dataframe">','<table class="table table-bordered table-hover table-sm">').replace('<thead>','<thead class="thead-dark">').replace('<th>','<th scope="col">').replace('<tr style="text-align: right;">','')
            
        elif('countcomparision' in filename.lower()):
            countfilePath =  os.path.join(latestDirectory, filename)
            if('_success' in countfilePath):
                countFlag= 'SUCCESS'
            with open(countfilePath,'r') as t:
                countResult = t.readlines()
        
    return countfilePath, countFlag, countResult, \
            datatypefilePath, datatypeFlag, datatypeResult, \
                datafilePath, dataFlag, dataResult
     