import json, time

from flask import Flask, render_template, request

from com.google.jobmanager import JobManager
from com.sundaytoz.chart import Chart
from com.sundaytoz.logger import Logger
from com.sundaytoz.chartmanager import ChartManager
from gmailauth import gmail_auth, oauth2check

app = Flask(__name__)
app.register_blueprint(gmail_auth)
app.secret_key = 'vjwmfdkxm!@#'
app.config['SESSION_TYPE'] = 'filesystem'

#========================================================================
# API
#========================================================================

@app.before_request
def before():
    Logger.setLevel(Logger.DEBUG)
    Logger.debug("before url={url}".format(url=request.endpoint))
    if not request.endpoint or  'gmail_auth' not in request.endpoint:
        redirection = oauth2check()
        if redirection:
            return redirection

@app.route('/', methods=['GET'])
def index():
    return render_template('layout.html', app_name="PuzzleArt")

@app.route('/chart', methods=['GET'])
def chart():
    charts = Chart().get_all()
    for chart in charts:
        chart.pop('query',None)
    return render_template('chart.html', app_name="PuzzleArt", charts=charts)

@app.route('/chart', methods=['DELETE'])
def chart_delete():
    chart_id = request.json['chart_id']
    Chart().delete(chart_id)
    return json.dumps({'success':True})

@app.route('/chart/', methods=['PUT'])
def chart_update():
    chart = request.json['chart']
    Chart().update(chart)
    return json.dumps({'success':True})

@app.route('/chart/<int:chart_id>', methods=['GET'])
def chart_data(chart_id):
    return json.dumps(ChartManager().get_result(chart_id))

@app.route('/charts/<string:chart_ids>', methods=['GET'])
def chart_datas(chart_ids):
    result = {}
    for chart_id in chart_ids.split(','):
        result[chart_id] = ChartManager().get_result(chart_id)
    return json.dumps(result)

@app.route('/chart/builder', methods=['GET'])
def chart_builder():
    return render_template('chartbuilder.html', app_name="PuzzleArt", charts=Chart().get_all())

@app.route('/chart/builder', methods=['POST'])
def chart_builder_add():
    Chart().add(request.json['chart'])
    return json.dumps({'success':True})

@app.route('/chart/builder/query', methods=['POST'])
def chart_builder_query():
    query = request.json['query']
    job_name = 'chart-tmp-{time}'.format(time=int(time.time()))
    results = JobManager().query(query=query, job_name=job_name)
    return json.dumps(list(map(lambda x: list(x), results)))

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=9002)