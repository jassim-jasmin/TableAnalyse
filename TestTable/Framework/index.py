from flask import Flask, redirect, url_for, request, render_template, make_response
from MyTool.Calculate_percent.stndcode import pgsqlStncCalc as sqlTable
import os
from flask_cors import CORS, cross_origin
from flask.json import jsonify
import json

data_path = os.path.join('static', 'templates')

#data_path = os.path.join('root','PycharmProjects','test','TestTable','Framework')
app = Flask(__name__)
cors= CORS(app, resources={r"/login":{"origin":"*"}})
app.config['CORS_HEADERS'] = 'Content-Type'
app.config['UPLOAD_FOLDER'] = data_path

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error':'Not found'}),404)

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


def setAttribute(psqlSchemaName,SQLSchemaName,fieldName):
        print(psqlSchemaName)
        print(SQLSchemaName)

        argList["pSQLHostName"] = "192.168.15.10"#(request.form['pSQLHostName'])
        argList["pSQLDBUserName"] = "postgres"#(request.form['pSQLDBUserName'])
        argList["pSQLDBPassword"] = "softinc"#(request.form['pSQLDBPassword'])
        argList["pSQLDBName"] = "zillow_2016"
        argList["pSQLSchemaName"] = psqlSchemaName
        #argList["pSQLTableName"] = (request.form['pSQLTableName'])
        argList["SQLHostName"] = "192.168.15.10"
        argList["SQLUserName"] = "root"
        argList["SQLPassword"] = "softinc"
        argList["SQLSchemaName"] = SQLSchemaName
        argList["fieldName"] = fieldName

        ob = sqlTable()
        table = ob.compareTableCountForAngular(argList["pSQLHostName"],argList["pSQLDBUserName"],argList["pSQLDBPassword"],argList["pSQLDBName"],argList["pSQLSchemaName"],argList["SQLHostName"],argList["SQLUserName"],argList["SQLPassword"],argList["SQLSchemaName"],fieldName)

        jsonTableData = json.dumps(table)
        #for name, value in table.items():
            #print(name,value)
        print(jsonTableData)
        return (jsonTableData)

@app.route('/main', methods=['POST', 'OPTIONS','PUT'])
@cross_origin(origin='*', headers=['access-control-allow-origin', 'Content-Type'])
def main():
    print("Initial invoke")
    try :
        if request:
            dataFromclient = request.json
            print(dataFromclient)
            OutData = setAttribute(dataFromclient.get('psqlSchemaName'),dataFromclient.get('SQLSchemaName'),dataFromclient.get('fieldName'))
            return OutData
    except Exception as e:
        print(e)
    #return jsonify({'data':'hai from server'})

if __name__ == "__main__":
        app.run(debug=True, host='192.168.15.63', port=4201, threaded=True)