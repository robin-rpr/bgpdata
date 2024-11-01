"""
BGPDATA - BGP Data Collection and Analytics Service

This software is part of the BGPDATA project, which is designed to collect, process, and analyze BGP data from various sources.
It helps researchers and network operators get insights into their network by providing a scalable and reliable way to analyze and inspect historical and live BGP data from Route Collectors around the world.

Author: Robin Röper

© 2024 BGPDATA. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions, and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions, and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of BGPDATA nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
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