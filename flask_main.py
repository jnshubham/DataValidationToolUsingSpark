from flask import Flask, render_template, url_for, request, make_response, session, send_file
import os, sys
import datetime
from validator import initializeValidation, getConfigs, addDriver

app = Flask(__name__)
app.config['SECRET_KEY']='089bcc9bf633533dd60b6f19402d1634'


@app.route('/')
def validationInput():
    surl, suser, spwd, sdb, turl, tuser, tpwd, tdb, dbs = getConfigs()
    
    return render_template('dataValidation.html', surl = surl, suser = suser, spwd = spwd, sdb = sdb, 
                           turl = turl, tuser = tuser, tpwd = tpwd, tdb = tdb, dbs = dbs)

@app.route('/ValidationOP', methods=['GET', 'POST'])
def validationOutput():
    print(type(request.form))
    countfilePath, countFlag, countResult, \
    datatypefilePath, datatypeFlag, datatypeResult, \
    datafilePath, dataFlag, dataResult = initializeValidation((request.form).to_dict())
    
    countResult = countResult.replace('\\n', "<br>")
    if(dataFlag=='SUCCESS'):
        dataResult = '<p stype="color:green">Data Matched Successfully </p>'
    
    countFlag = "<p style='color:green'>SUCCESS</p>" if('success' in countFlag.lower()) else "<p style='color:red'>FAILED</p>"
    datatypeFlag = "<p style='color:green'>SUCCESS</p>" if('success' in datatypeFlag.lower()) else "<p style='color:red'>FAILED</p>"
    dataFlag = "<p style='color:green'>SUCCESS</p>" if('success' in dataFlag.lower()) else "<p style='color:red'>FAILED</p>"

    return render_template('comparisionResult.html', 
                           countResult=countResult, countFlag=countFlag, countfilePath=countfilePath, 
                           datatypeResult=datatypeResult, datatypeFlag=datatypeFlag, datatypefilePath=datatypefilePath, 
                           dataResult=dataResult, dataFlag=dataFlag, datafilePath=datafilePath, 
                           )


@app.route('/download/<filePath>', methods=['GET', 'POST'])
def download_results(filePath):
    print(filePath)
    return send_file(
        filePath,
        mimetype='text/csv',
        as_attachment=True
    )
    
@app.route('/addDB')
def addDBPage():
    return render_template('addNewDatabase.html')
    
@app.route('/addDriver', methods = ['GET', 'POST'])
def addDatabaseDriver():
    op = addDriver(request.form.to_dict())
    if(op=='Success'):
        f = request.files['dbJar']
        path = os.path.join('lib',f.filename)
        f.save(os.path.join('lib',f.filename))
        return validationInput()
    else:
        return render_template('addNewDatabase.html', error=op)
    
if __name__=='__main__':
    import pandas as pd
    app.run(debug=True, host='0.0.0.0', port=80)