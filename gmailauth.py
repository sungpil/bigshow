from flask import Blueprint, redirect, url_for, session, request, abort
from com.sundaytoz.logger import Logger
from com.sundaytoz.admin import Admin
from oauth2client.client import OAuth2WebServerFlow
import time

gmail_auth = Blueprint('gmail_auth', __name__, template_folder='templates')

def oauth2check():
    Logger.debug("oauth2check")
    if 'user' not in session:
        Logger.debug("not credentials")
        return redirect(__getFlow().step1_get_authorize_url())
    else:
        Logger.debug("oauth2check: success")
    return None

@gmail_auth.route('/gmail/oauth2callback')
def oauth2callback():
    code = request.args['code']
    Logger.debug("oauth2callback code={code}".format(code=code))
    credentials = __getFlow().step2_exchange(code)
    email = credentials.id_token[u'email']
    user = Admin().get(email)
    Logger.debug("code = {code}, user_id={user_id}, email={email}".format(code=code, user_id=user['idx'], email=email))
    if user:
        session['user'] = {'id':user['idx'],'time':time.time()}
        return redirect(url_for('chart'))
    else:
        abort(401)

def __getFlow():
    from config.dev import config
    return OAuth2WebServerFlow(client_id=config['google']['client_id'],
                               client_secret=config['google']['client_secret'],
                               scope='https://www.googleapis.com/auth/userinfo.email https://www.googleapis.com/auth/userinfo.profile',
                               redirect_uri=url_for('gmail_auth.oauth2callback', _external=True))