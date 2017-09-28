from flask import Flask, render_template, request, url_for
from com.sundaytoz.logger import Logger
from com.sundaytoz.jobmanager import JobManager
from com.sundaytoz.cache import Cache
from com.sundaytoz.chart import Chart
from gmailauth import gmail_auth, oauth2check
import json, ast, time

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
    return json.dumps(_get_chart_result(chart_id))

@app.route('/charts/<string:chart_ids>', methods=['GET'])
def chart_datas(chart_ids):
    result = {}
    for chart_id in chart_ids.split(','):
        result[chart_id] = _get_chart_result(chart_id)
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

#========================================================================
# chart
#========================================================================

def _get_chart_result(chart_id, from_cache=True):
    status_key = "last_job:{chart_id}".format(chart_id=chart_id)
    last_job_id = Cache().get(status_key)
    Logger.debug("_get_chart_result: status_key={status_key}, last_job_id={last_job_id}".format(status_key=status_key, last_job_id=last_job_id))
    if not last_job_id:
        last_job_id = "chart-{chart_id}-{time}".format(chart_id=chart_id, time=int(time.time()))
    if from_cache:
        results = Cache().get(last_job_id)
    else:
        results = None
    if not results:
        Logger.debug("_get_chart_result: BIGQUERY")
        if JobManager().exists(last_job_id):
            results = JobManager().get_result(last_job_id)
        else:
            query = Chart().get_query(chart_id)
            JobManager().query_async(query, last_job_id)
            Cache().set(status_key, last_job_id, 3600)
        if results and from_cache:
            results = list(map(lambda x: list(x), results))
            Cache().set(last_job_id, results)
        return results
    else:
        Logger.debug("_get_chart_result: CACHE")
        return ast.literal_eval(results)

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=9002)