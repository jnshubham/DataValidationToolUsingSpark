import configparser
import subprocess, os, sys, glob, pathlib
import pandas as pd
import psutil

class SparkCommandSyntaxError(Exception):
    '''This is invoked when there is syntax error in spark command'''
    pass

def getAvailableMemory():
    mem = psutil.virtual_memory()
    availableMemory = mem.available/1024.0/1024.0/1024.0
    memForSpark = availableMemory - 1
    return str(int(memForSpark))+'g'
    
    
    
def getURLFromConfig(kw):
    configDriver = configparser.ConfigParser()
    configDriver.read('config/driver.config.ini')
    
    #Generating Source URL
    sourceDriverClassName = configDriver[kw['sdriver']]['driver']
    print(sourceDriverClassName)
    url = configDriver[kw['sdriver']]['url']
    print(url)
    sourceURL = url.format(**{'uri': kw['sourceURL'],
                            'db': kw['sourceDatabase'],
                            'usr': kw['sourceUser'],
                            'pwd': kw['sourcePassword']})
    
    #Generating Target URL
    targetDriverClassName = configDriver[kw['tdriver']]['driver']
    url = configDriver[kw['tdriver']]['url']
    targetURL = url.format(**{'uri': kw['targetURL'],
                            'db': kw['targetDatabase'],
                            'usr': kw['targetUser'],
                            'pwd': kw['targetPassword']})
    configDriver.clear()
    return sourceDriverClassName, sourceURL, targetDriverClassName, targetURL
    
    

def initializeValidation(kwargs):
    kwargs['filePath'] = os.path.abspath(os.getcwd())
    memory = getAvailableMemory()
    kwargs['driverPath'] = os.path.join(kwargs['filePath'],'bin','*;')
    kwargs['conf'] = f'''--conf spark.driver.extraClassPath="{kwargs["driverPath"]}" --conf spark.driver.memory='{memory}' '''
    kwargs['scriptPath'] = os.path.join(kwargs['filePath'],'spark','compareData.py')

    kwargs['sourceDriver'], kwargs['sourceURL'], kwargs['targetDriver'], kwargs['targetURL'] = getURLFromConfig(kwargs)
    if('steps' not in kwargs.keys()):
        kwargs['steps'] = 1
    if(kwargs['filterCondition']==''):
        #cmd = '''spark-submit "{filePath}\\spark\\compareData.py" --sourceURL '{sourceURL}' --targetURL '{targetURL}' --sourceUser '{sourceUser}' --targetUser '{targetUser}' --sourcePassword '{sourcePassword}' --targetPassword '{targetPassword}' --sourceDatabase '{sourceDatabase}' --targetDatabase '{targetDatabase}' --sourceTable '{sourceTable}' --targetTable '{targetTable}' --keyColumns '{keyColumns}' --excludedColumns '{excludedColumns}' --filterCondition '{filterCondition}' '''.format(**kwargs)
        #cmd = '''python "{scriptPath}" --sourceURL {sourceURL} --targetURL {targetURL} --sourceUser {sourceUser} --targetUser {targetUser} --sourcePassword {sourcePassword} --targetPassword {targetPassword} --sourceDatabase {sourceDatabase} --targetDatabase {targetDatabase} --sourceTable {sourceTable} --targetTable {targetTable} --keyColumns {keyColumns} --driver {driver} --excludedColumns {excludedColumns} '''.format(**kwargs)
        cmd = '''python "{scriptPath}" --sourceURL {sourceURL} --targetURL {targetURL} --sourceDriver {sourceDriver} --targetDriver {targetDriver} --sourceTable {sourceTable} --targetTable {targetTable} --steps {steps} --keyColumns "{keyColumns}" --excludedColumns "{excludedColumns}" '''.format(**kwargs)
    else:
        #cmd = '''python "{scriptPath}" --sourceURL {sourceURL} --targetURL {targetURL} --sourceUser {sourceUser} --targetUser {targetUser} --sourcePassword {sourcePassword} --targetPassword {targetPassword} --sourceDatabase {sourceDatabase} --targetDatabase {targetDatabase} --sourceTable {sourceTable} --targetTable {targetTable} --keyColumns {keyColumns} --driver {driver} --excludedColumns {excludedColumns} --filterCondition "{filterCondition}" '''.format(**kwargs)
        cmd = '''python "{scriptPath}" --sourceURL {sourceURL} --targetURL {targetURL} --sourceDriver {sourceDriver} --targetDriver {targetDriver} --sourceTable {sourceTable} --targetTable {targetTable} --steps {steps} --keyColumns "{keyColumns}" --excludedColumns "{excludedColumns}" --filterCondition "{filterCondition}" '''.format(**kwargs)
        
    print(cmd)
    op = subprocess.run(cmd)

    if(op.returncode!=0):
        raise SparkCommandSyntaxError(cmd)
    else:
        print('Success')
        return fetchData(kwargs)
        
def fetchData(kwargs):
    baseDirectory = os.path.join(kwargs['filePath'], 'output',kwargs['sourceTable'])
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
            if('_success' in datatypefilePath.lower()):
                datatypeFlag= 'SUCCESS'
            df = pd.read_csv(datatypefilePath)
            htmldf = df.to_html(index=False)
            datatypeResult =  htmldf.replace('<table border="1" class="dataframe">','<table id="tempdt1" class="table table-bordered table-hover table-sm">').replace('<thead>','<thead class="thead-light">').replace('<th>','<th scope="col">').replace('<tr style="text-align: right;">','')
            
        elif('datacomparision' in filename.lower()):
            datafilePath =  os.path.join(latestDirectory, filename)
            if('_success' in datafilePath.lower()):
                dataFlag= 'SUCCESS'
            df = pd.read_csv(datafilePath)
            if(len(df)>0):
                htmldf = df.groupby('ColumnName').head(int(2000/df.ColumnName.nunique(dropna = True)))
                htmldf = htmldf.to_html(index=False)
                dataResult =  htmldf.replace('<table border="1" class="dataframe">','<table id="tempdt" class="table table-bordered table-hover table-sm">').replace('<thead>','<thead class="thead-light">').replace('<tr style="text-align: right;">','')
                dataResult = f'''<div align='left'><p><b>Executed By: </b>{os.getlogin()}</p><p><b>Filter: </b>{kwargs['filterCondition']}</p><p><b>Columns having differences: </b>{','.join(df.ColumnName.unique())}</p> {dataResult}</div> '''
            
        elif('countcomparision' in filename.lower()):
            countfilePath =  os.path.join(latestDirectory, filename)
            if('_success' in countfilePath.lower()):
                countFlag= 'SUCCESS'
            with open(countfilePath,'r') as t:
                countResult = '\\n'.join(t.readlines())
        
    return countfilePath, countFlag, countResult, \
            datatypefilePath, datatypeFlag, datatypeResult, \
                datafilePath, dataFlag, dataResult
     

def getConfigs():
    configs = configparser.ConfigParser()
    configs.read('config/db.config.ini')
    surl, suser, spwd, sdb = configs['sourceDatabase']['url'], configs['sourceDatabase']['username'], configs['sourceDatabase']['password'], configs['sourceDatabase']['database']
    turl, tuser, tpwd, tdb = configs['targetDatabase']['url'], configs['targetDatabase']['username'], configs['targetDatabase']['password'], configs['targetDatabase']['database']
    configDriver = configparser.ConfigParser()
    configDriver.read('config/driver.config.ini')
    dbs = configDriver.sections()
    configs.clear()
    configDriver.clear()
    return surl, suser, spwd, sdb, turl, tuser, tpwd, tdb, dbs

def addDriver(request):
    configDriver = configparser.ConfigParser()
    configDriver.read('config/driver.config.ini')
    if(request['dbName'] in configDriver.sections()):
        return f"Driver already exist for {request['dbName']}"
    else:
        configDriver.add_section(request['dbName'])
        configDriver.set(request['dbName'], 'driver', request['dbDriver'])
        configDriver.set(request['dbName'], 'url', request['dbURL'])
        
        with open('config/driver.config.ini', 'w') as configfile:
            configDriver.write(configfile)

        configDriver.clear()
        return 'Success'

