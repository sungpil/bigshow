import json, time, datetime

from flask import Flask, render_template, request, redirect, url_for

from com.google.gmailauth import gmail_auth, oauth2check
from com.google.jobmanager import JobManager
from com.sundaytoz.chart import Chart
from com.sundaytoz.chartmanager import ChartManager
from com.sundaytoz.customquery import CustomQuery
from com.sundaytoz.logger import Logger
from config.dev import config

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
    if not request.endpoint or 'gmail_auth' not in request.endpoint:
        redirection = oauth2check()
        if redirection:
            return redirection


@app.route('/', methods=['GET'])
@app.route('/charts', methods=['GET'])
def charts():
    charts = Chart().get_all()
    for chart in charts:
        chart.pop('query',None)
    return render_template('charts.html', app_name="PuzzleArt", charts=charts)


@app.route('/charts', methods=['POST'])
def charts_add():
    return json.dumps({'success':Chart().add(request.json['chart'])})


@app.route('/charts/<int:chart_id>', methods=['DELETE'])
def charts_delete(chart_id):
    return json.dumps({'success':Chart().delete(chart_id)})


@app.route('/charts/<int:chart_id>', methods=['PUT'])
def charts_update(chart_id):
    chart = request.json['chart']
    if 'query' in chart.keys():
        ChartManager().del_cache(chart_id)
    success = Chart().update(chart_id, request.json['chart'])
    return json.dumps({'success':success})


@app.route('/charts/<int:chart_id>', methods=['GET'])
def charts_get(chart_id):
    return json.dumps(ChartManager().get_result(chart_id))


@app.route('/charts/<string:chart_ids>', methods=['GET'])
def charts_mget(chart_ids):
    result = {}
    for chart_id in chart_ids.split(','):
        result[chart_id] = ChartManager().get_result(chart_id)
    return json.dumps(result)


@app.route('/chartbuilder', methods=['GET'], defaults={'chart_id':None})
@app.route('/chartbuilder/<int:chart_id>', methods=['GET'])
def chartbuilder(chart_id):
    return render_template('chartbuilder.html', app_name="PuzzleArt", charts=Chart().get_all(), selected_chart_id=chart_id)


@app.route('/chartbuilder', methods=['POST'])
def chartbuilder_query():
    chart_type = request.json['chart_type']
    query = request.json['query']
    job_name = 'chart-tmp-{time}'.format(time=int(time.time()))
    results = JobManager().query(query=ChartManager().get_query(chart_type, query), job_name=job_name)
    return json.dumps(list(map(lambda x: list(x), results)))


@app.route('/custom', methods=['GET'])
def custom():
    query = CustomQuery().retention(datetime.datetime.strptime('2017-10-10', '%Y-%m-%d'),datetime.datetime.strptime('2017-10-15', '%Y-%m-%d'), 7)
    Logger().debug("query={0}".format(query))
    return json.dumps(query)


if __name__ == '__main__':
    port = config['server']['port']
    app.run(host='0.0.0.0',port=port)