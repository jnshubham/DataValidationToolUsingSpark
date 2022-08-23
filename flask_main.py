from flask import Flask, render_template, url_for, request, make_response, session
import os
import datetime

app = Flask(__name__)
app.config['SECRET_KEY']='089bcc9bf633533dd60b6f19402d1634'


@app.route('/')
def validationInput():
    return render_template('dataValidation.html')

@app.route('/ValidationOP', methods=['GET', 'POST'])
def validationOutput():
    print(request.form)
    return render_template('dataValidation.html')

if __name__=='__main__':
    import pandas as pd
    app.run(host='0.0.0.0', port=5000)
