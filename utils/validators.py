from flask import session, current_app as app
from datetime import datetime
import pytz
import re


def is_authenticated():
    #if not session or 'user_id' not in session:
    #    return False
    #
    #user = db.users.find_one(
    #    {'_id': ObjectId(session['user_id']), 'is_deleted': False})
    #
    #if not user:
    #    session.clear()
    #    return False
    #
    #session_expiry = datetime.strptime(
    #    session['expiry'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.utc)
    #
    #if datetime.now(pytz.utc) > session_expiry:
    #    session.clear()
    #    return False

    return True


def is_onboarded():
    #user = db.users.find_one({'_id': ObjectId(session['user_id'])})
    #
    #if not user.get("is_onboarded", False):
    #    return False

    return True


def has_good_standing():
    #user = db.users.find_one({'_id': ObjectId(session['user_id'])})
    #
    ## Check if user is not verified yet
    #if user.get("is_verified", False) is False:
    #    return False
    #
    ## Check if user is suspended
    #if user.get("is_suspended", False) is True:
    #    return False
    #
    ## Check if user is deleted
    #if user.get("is_deleted", False) is True:
    #    return False

    return True


def is_valid_email(email):
    return re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email)