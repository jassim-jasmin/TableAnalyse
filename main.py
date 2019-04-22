from flask import Flask, redirect, url_for, request, render_template
from MyTool.Calculate_percent.stndcode import pgsqlStncCalc as sqlTable
import os

data_path = os.path.join('static', 'templates')

#data_path = os.path.join('root','PycharmProjects','test','TestTable','Framework')
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = data_path

argList = dict()

@app.route('/')
def home():
   return render_template('index.html')

@app.route('/home',methods = ['POST', 'GET'])
def index():
    if request.method == 'POST':
        argList["pSQLHostName"] = "192.168.15.10"#(request.form['pSQLHostName'])
        argList["pSQLDBUserName"] = "postgres"#(request.form['pSQLDBUserName'])
        argList["pSQLDBPassword"] = "softinc"#(request.form['pSQLDBPassword'])
        argList["pSQLDBName"] = "zillow_2016"
        argList["pSQLSchemaName"] = (request.form['pSQLSchemaName'])
        #argList["pSQLTableName"] = (request.form['pSQLTableName'])
        argList["SQLHostName"] = "192.168.15.10"
        argList["SQLUserName"] = "root"
        argList["SQLPassword"] = "softinc"
        argList["SQLSchemaName"] = (request.form['SQLSchemaName'])

        ob = sqlTable()
        ob.compareTable(argList["pSQLHostName"],argList["pSQLDBUserName"],argList["pSQLDBPassword"],argList["pSQLDBName"],argList["pSQLSchemaName"],argList["SQLHostName"],argList["SQLUserName"],argList["SQLPassword"],argList["SQLSchemaName"])

        data =  os.path.join(app.config['UPLOAD_FOLDER'], 'data.png')
        print(data)
        return render_template('data.html', data_image = data)


if __name__ == "__main__":
        app.run(debug=True, host='192.168.15.63', port=9090, threaded=True)