import json

from flask import Flask, render_template, request

from com.google.gmailauth import gmail_auth, oauth2check
from com.sundaytoz.bigshow.chartmanager import ChartManager
from com.sundaytoz.bigshow.chart import Chart
from com.sundaytoz.bigshow.note import Note
from com.sundaytoz.logger import Logger
from config.dev import config

app = Flask(__name__)
app.register_blueprint(gmail_auth)
app.secret_key = 'vjwmfdkxm!@#'
app.config['SESSION_TYPE'] = 'filesystem'


@app.before_request
def before():
    Logger.set_level(Logger.DEBUG)
    Logger.debug("before url={url}".format(url=request.endpoint))
    if not request.endpoint or 'gmail_auth' not in request.endpoint:
        redirection = oauth2check()
        if redirection:
            return redirection

#########################################################################
#   View
#########################################################################


@app.route('/', methods=['GET'], defaults={'note_id':None})
@app.route('/dashboard', methods=['GET'], defaults={'note_id':None})
@app.route('/dashboard/<int:note_id>', methods=['GET'])
def chart_view(note_id):

    notes = Note().get_all()
    selected_note = None
    if notes:
        selected_note = notes[0]
        for note in notes:
            if note_id == note['id']:
                selected_note = note
                break
    if selected_note:
        charts = Chart().get_all(selected_note['id'])
    else:
        charts = Chart().get_all()
    for chart in charts:
        chart.pop('query', None)
    return render_template('charts.html', app_name=config['app']['name'], charts=charts, notes=notes, selected_note=selected_note)


@app.route('/chartbuilder', methods=['GET'], defaults={'chart_id': None})
@app.route('/chartbuilder/<int:chart_id>', methods=['GET'])
def chart_builder(chart_id):
    return render_template('chartbuilder.html',
                           app_name=config['app']['name'],
                           charts=Chart().get_all(),
                           notes=Note().get_all(),
                           selected_chart_id=chart_id)


#########################################################################
#   Restful
#########################################################################


@app.route('/charts', methods=['GET'])
def charts_get_all():
    chart_ids = request.args.get('chart_ids', default=None, type=str)
    charts = Chart().get_all(chart_ids=chart_ids.split(',') if chart_ids else None)
    return __result_json(charts)


@app.route('/charts', methods=['POST'])
def charts_add():
    return json.dumps({'id': Chart().add(request.json['chart'])}), 201


@app.route('/charts/<int:chart_id>', methods=['GET'])
def charts_get(chart_id):
    chart = Chart().get(chart_id)
    return __result_json(chart)


@app.route('/charts/<int:chart_id>/result', methods=['GET'])
def charts_result(chart_id):
    force = request.args.get('force', default='false', type=str)
    status, results, error = ChartManager.get_result(chart_id=chart_id, from_cache=(force.lower() == 'false'))
    return json.dumps({'status': status, 'results': results, 'error': error})


@app.route('/charts/results', methods=['GET'])
def chart_results():
    chart_ids = request.args.get('chart_ids', default=None, type=str)
    result = {}
    if chart_ids:
        for chart_id in chart_ids.split(','):
            status, results, error = ChartManager.get_result(chart_id)
            result[chart_id] = {'status': status, 'results': results, 'error': error}
    return __result_json(result)


@app.route('/charts/<int:chart_id>', methods=['DELETE'])
def charts_delete(chart_id):
    return json.dumps({'success': Chart().delete(chart_id)})


@app.route('/charts/<int:chart_id>', methods=['PUT'])
def charts_update(chart_id):
    chart = request.json['chart']
    if 'query' in chart.keys():
        ChartManager.del_cache(chart_id=chart_id)
    return json.dumps({'success': Chart().update(chart_id=chart_id, chart=request.json['chart'])})


@app.route('/charts/queries', methods=['POST'])
def charts_queries():
    chart_id = request.json['chart_id']
    chart_type = request.json['chart_type']
    query_type = request.json['query_type']
    query = request.json['query']
    Logger.debug('chart_type={0}, query_type={1}'.format(chart_type, query_type))
    results, error = ChartManager.query(chart_id=chart_id, chart_type=chart_type, query_type=query_type, query=query)
    return json.dumps({'results': results, 'error': error})


@app.route('/notes', methods=['GET'])
def notes_get_all():
    return __result_json(Note().get_all())


@app.route('/notes', methods=['POST'])
def notes_add():
    return json.dumps(Note().get(note_id=Note().add(title=request.json['title'])))


@app.route('/notes/<int:note_id>', methods=['GET'])
def notes_get(note_id):
    return __result_json(Note().get(note_id=note_id))


def __result_json(data):
    if not data:
        return '', 204
    else:
        return json.dumps(data), 200


if __name__ == '__main__':
    port = config['server']['port']
    app.run(host='0.0.0.0', port=port)
