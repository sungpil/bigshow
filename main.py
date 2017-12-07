import json

from flask import Flask, render_template, request

from com.google.gmailauth import gmail_auth, oauth2check
from com.sundaytoz import bigshow
from com.sundaytoz.bigshow import models
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


@app.route('/', methods=['GET'], defaults={'note_id': None})
@app.route('/dashboard', methods=['GET'], defaults={'note_id': None})
@app.route('/dashboard/<int:note_id>', methods=['GET'])
def chart_view(note_id):
    notes = models.Note.get_all()
    selected_note = None
    if notes:
        selected_note = notes[0]
        for note in notes:
            if note_id == note['id']:
                selected_note = note
                break
    if selected_note:
        charts = models.Chart.get_all(selected_note['id'])
    else:
        charts = models.Chart.get_all()
    for chart in charts:
        chart.pop('query', None)
    return render_template('charts.html', app_name=config['app']['name'], charts=charts,
                           notes=notes, selected_note=selected_note)


@app.route('/charts/manager', methods=['GET'], defaults={'chart_id': None})
@app.route('/charts/manager/<int:chart_id>', methods=['GET'])
def chart_manager(chart_id):
    return render_template('chartmanager.html',
                           app_name=config['app']['name'],
                           charts=models.Chart.get_all(),
                           notes=models.Note.get_all(),
                           selected_chart_id=chart_id)

@app.route('/schedules/manager', methods=['GET'])
def schedules():
    return render_template('schedule.html', app_name=config['app']['name'],
                           schedules=models.Schedule.get_all())


#########################################################################
#   Restful
#########################################################################


@app.route('/charts', methods=['GET'])
def charts_get_all():
    chart_ids = request.args.get('chart_ids', default=None, type=str)
    charts = models.Chart.get_all(chart_ids=chart_ids.split(',') if chart_ids else None)
    return __result_json(charts)


@app.route('/charts', methods=['POST'])
def charts_add():
    return json.dumps({'id': models.Chart.add(request.json['chart'])}), 201


@app.route('/charts/<int:chart_id>', methods=['GET'])
def charts_get(chart_id):
    chart = models.Chart.get(chart_id)
    return __result_json(chart)


@app.route('/charts/<int:chart_id>/result', methods=['GET'])
def charts_result(chart_id):
    force = request.args.get('force', default='false', type=str)
    status, results, error = bigshow.Chart.get_result(chart_id=chart_id, from_cache=(force.lower() == 'false'))
    return json.dumps({'status': status, 'results': results, 'error': error})


@app.route('/charts/results', methods=['GET'])
def chart_results():
    chart_ids = request.args.get('chart_ids', default=None, type=str)
    result = {}
    if chart_ids:
        for chart_id in chart_ids.split(','):
            status, results, error = bigshow.Chart.get_result(chart_id)
            result[chart_id] = {'status': status, 'results': results, 'error': error}
    return __result_json(result)


@app.route('/charts/<int:chart_id>', methods=['DELETE'])
def charts_delete(chart_id):
    return json.dumps({'success': models.Chart.delete(chart_id)})


@app.route('/charts/<int:chart_id>', methods=['PUT'])
def charts_update(chart_id):
    chart = request.json['chart']
    if 'query' in chart.keys():
        bigshow.Chart.del_cache(chart_id=chart_id)
    return json.dumps({'success': models.Chart.update(chart_id=chart_id, chart=request.json['chart'])})


@app.route('/charts/queries', methods=['POST'])
def charts_queries():
    chart_id = request.json['chart_id']
    resource = request.json['resource']
    query_type = request.json['query_type']
    query_params = request.json['query_params']
    query = request.json['query']
    Logger.debug('resource={0}, query_type={1}'.format(resource, query_type))
    results, error = bigshow.Chart.query_sync(chart_id=chart_id, resource=resource,
                                              query_type=query_type, query=query, query_params=query_params)
    return json.dumps({'results': results, 'error': error})


@app.route('/notes', methods=['GET'])
def notes_get_all():
    return __result_json(models.Note.get_all())


@app.route('/notes', methods=['POST'])
def notes_add():
    return json.dumps(models.Note.get(note_id=models.Note.add(title=request.json['title']))), 201


@app.route('/notes/<int:note_id>', methods=['GET'])
def notes_get(note_id):
    return __result_json(models.Note.get(note_id=note_id))


@app.route('/schedules/<int:schedule_id>', methods=['GET'])
def schedules_get(schedule_id):
    return __result_json(models.Schedule.get(schedule_id=schedule_id))


@app.route('/schedules/<int:schedule_id>', methods=['DELETE'])
def schedules_delete(schedule_id):
    return __result_json(models.Schedule.delete(schedule_id=schedule_id))


@app.route('/schedules', methods=['GET'])
def schedules_gat_all():
    return __result_json(models.Schedule.get_all())


@app.route('/schedules', methods=['POST'])
def schedules_add():
    return json.dumps(models.Schedule.add(schedule=request.json)), 201


@app.route('/schedules', methods=['PUT'])
def schedules_update():
    return json.dumps(models.Schedule.update(schedule=request.json)), 201


def __result_json(data):
    if not data:
        return '', 204
    else:
        return json.dumps(data), 200


if __name__ == '__main__':
    port = config['server']['port']
    app.run(host='0.0.0.0', port=port)
