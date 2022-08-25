from flask import Flask, render_template, url_for, request, make_response, session
import os
import datetime
from validator import initializeValidation

app = Flask(__name__)
app.config['SECRET_KEY']='089bcc9bf633533dd60b6f19402d1634'


@app.route('/')
def validationInput():
    return render_template('dataValidation.html')

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

if __name__=='__main__':
    import pandas as pd
    app.run(debug=True, host='127.0.0.1', port=5000)