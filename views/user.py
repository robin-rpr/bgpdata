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
from flask import Blueprint, render_template, redirect, url_for, request, session, abort, current_app as app
from utils.validators import is_authenticated, is_onboarded
from utils.postmark import postmark
from datetime import datetime
import pytz

# Create Blueprint
user_blueprint = Blueprint('user', __name__)


#@user_blueprint.route("/<user_id>")
#def user(user_id):
#    try:
#        if not is_authenticated():
#            return redirect(url_for('index'))
#
#        if not is_onboarded():
#            return redirect(url_for('onboarding'))
#
#        profile = get_user(session['user_id'])
#        user = get_user(user_id)
#
#        if not user:
#            abort(404, description="User not found")
#
#        my_groups, ids = get_user_groups(session['user_id'])
#
#        if user_id != session['user_id']:
#            # Retrieve other persons groups
#            _, ids = get_user_groups(user_id, False)
#
#        posts = render_template(
#            'paginated.html', profile=profile, posts=get_posts(ids, user.get('_id'), 0, 10))
#        popular = get_popular_posts(ids)
#        studies = list(db.studies.find({}))
#
#    except Exception as e:
#        app.logger.error("Failed to load user: %s", str(e))
#        return abort(500, description="An error occurred")
#
#    return render_template('user.html', profile=profile, user=user, posts=posts, studies=studies, my_groups=my_groups, popular=popular)
#
#
#@user_blueprint.route('/update', methods=["POST"])
#def update_user():
#    try:
#        if not is_authenticated():
#            return abort(403, description="Not logged in")
#
#        # List of fields that can be updated
#        fields = ['first_name', 'last_name',
#                  'avatar', 'study_id', 'company']
#
#        # Collect the fields to update
#        update_fields = {field: request.form.get(field)
#                         for field in fields if request.form.get(field)}
#
#        if 'first_name' in update_fields:
#            length = len(update_fields.get('first_name'))
#            if length > 30 or length < 2:
#                return abort(400, description="Field 'first_name' must be between 2 and 30 characters long")
#
#        if 'last_name' in update_fields:
#            length = len(update_fields.get('last_name'))
#            if length > 30 or length < 2:
#                return abort(400, description="Field 'last_name' must be between 2 and 30 characters long")
#
#        if 'avatar' in update_fields:
#            length = len(update_fields.get('avatar'))
#            if length > 500000 or length < 30:
#                return abort(400, description="Field 'avatar' must be between 30 and 500000 characters long")
#
#        if 'study_id' in update_fields:
#            study_id = ObjectId(update_fields.get('study_id'))
#            study = db.studies.find_one({'_id': study_id})
#            if not study:
#                return abort(400, description="Study not found")
#            else:
#                update_fields['study_id'] = study_id
#
#        if 'company' in update_fields:
#            length = len(update_fields.get('company'))
#            if length > 50 or length < 2:
#                return abort(400, description="Field 'company' must be between 2 and 50 characters long")
#
#        if not update_fields:
#            return abort(400, description="No fields provided to update")
#
#        # Update email
#        update_fields['email'] = session['email']
#
#        # Update date
#        update_fields['updated_at'] = datetime.now(pytz.utc)
#
#        # Update the user document in the database
#        db.users.update_one({'_id': ObjectId(session['user_id'])}, {
#            '$set': update_fields})
#
#        updated = get_user(session['user_id'])
#
#        # Update the is_onboarded state if the user matches the criteria
#        if not updated.get('is_onboarded', False) and updated.get('first_name', None) and updated.get('last_name', None) and (updated.get('study_id', None) or updated.get('company', None)):
#            db.users.update_one({'_id': ObjectId(session['user_id'])}, {
#                '$set': {'is_onboarded': True, "updated_at": datetime.now(pytz.utc)}})
#
#            # If the user is an unverified user we want to send him the verification email
#            if not updated.get('is_verified', False):
#                subject = "Your FOMBook Account Verification"
#                message = f"""<p>Hey { updated.get('first_name', 'there')},</p>
#                            <p>Thank you for registering with FOMBook. To verify your account, we require some details from you.</p>
#                            <p>Please provide proof that you have studied at FOM University in the past. Acceptable forms of proof include</p>
#                            <p>a certificate, transcript, or any official document that confirms your affiliation with FOM University.</p>
#                            <p>Once we receive and verify your documents, we will activate your account.</p>
#                            <p>Thank you for your cooperation.</p>
#                            <p>Best regards,</p>
#                            <p>The FOMBook Team</p>"""
#
#                try:
#                    app.logger.debug("Sending Email for Account Verification")
#                    postmark.emails.send(
#                        From='help@fombook.com',
#                        To=updated.get('email'),
#                        ReplyTo="help+verification@fombook.com",
#                        Subject=subject,
#                        HtmlBody=message,
#                        TextBody=message
#                    )
#                except Exception as e:
#                    app.logger.error("Failed to send email: %s", str(e))
#
#    except Exception as e:
#        app.logger.error("Failed to update user: %s", str(e))
#        return abort(500, description="An error occurred")
#
#    return redirect(url_for('feed'))
#