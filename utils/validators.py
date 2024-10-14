from flask import session, current_app as app
from profanity_check import predict_prob
from datetime import datetime
from bson import ObjectId
from utils.database import db
import pytz
import re


def is_authenticated():
    if not session or 'user_id' not in session:
        return False

    user = db.users.find_one(
        {'_id': ObjectId(session['user_id']), 'is_deleted': False})

    if not user:
        session.clear()
        return False

    session_expiry = datetime.strptime(
        session['expiry'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.utc)

    if datetime.now(pytz.utc) > session_expiry:
        session.clear()
        return False

    return True


def is_onboarded():
    user = db.users.find_one({'_id': ObjectId(session['user_id'])})

    if not user.get("is_onboarded", False):
        return False

    return True


def has_good_standing():
    user = db.users.find_one({'_id': ObjectId(session['user_id'])})

    # Check if user is not verified yet
    if user.get("is_verified", False) is False:
        return False

    # Check if user is suspended
    if user.get("is_suspended", False) is True:
        return False

    # Check if user is deleted
    if user.get("is_deleted", False) is True:
        return False

    return True


def is_valid_email(email):
    return re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email)


def has_group_access(group_id):
    user_id = ObjectId(session['user_id'])
    group_id = ObjectId(group_id)
    profile = db.users.find_one({'_id': user_id})
    group = db.groups.find_one({'_id': group_id})

    # Check if we try to load a group but not matching its user_type
    if group.get('is_default', False) and group.get('user_type', None) != profile.get('user_type', None) and group.get('user_type', None) is not None:
        return False

    # Check if we try to load a private non-default group but are not part of it
    if group.get('is_private', False) and not group.get('is_default', False) and group.get('_id') not in profile.get('groups', []):
        return False

    # Check if we try to load a private group but not verified yet
    if group.get('is_private', False) and not profile.get('is_verified'):
        return False

    return True


def has_group_ownership(group_id):
    user_id = ObjectId(session['user_id'])
    group_id = ObjectId(group_id)
    group = db.groups.find_one({'_id': group_id})

    if group.get('owner_id') is not user_id:
        return False

    return True


def is_offensive_text(text):
    # Probability text is offensive
    probability = predict_prob([text])

    if probability > 0.85:
        # Text is very likely offensive
        return True
    else:
        # Text is likely not offensive
        return False
