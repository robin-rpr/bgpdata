from flask import Blueprint, render_template, redirect, url_for, request, session, abort, current_app as app
from utils.queries import get_user, get_posts, get_group, get_user_groups, get_most_viewed_posts
from utils.validators import is_authenticated, is_onboarded, has_group_access, has_group_ownership, has_good_standing, is_offensive_text
from utils.database import db
from datetime import datetime
from bson import ObjectId
import pytz

# Create Blueprint
group_blueprint = Blueprint('group', __name__)


@group_blueprint.route("/<group_id>")
def group(group_id):
    try:
        if not is_authenticated():
            return redirect(url_for('index'))

        if not is_onboarded():
            return redirect(url_for('onboarding'))

        group_id = str(group_id)

        if not has_group_access(group_id):
            abort(403, description="No group access")

        group = get_group(group_id)

        if not group:
            return abort(404, description="Group not found")

        profile = get_user(session['user_id'])
        posts = render_template('paginated.html', profile=profile, posts=get_posts(
            list([ObjectId(group_id)]), None, 0, 10))
        my_groups, ids = get_user_groups(session['user_id'])
        most_viewed = get_most_viewed_posts(list([ObjectId(group_id)]))

        # Add user to group if not member of it already
        if ObjectId(session['user_id']) not in ids and not group.get('is_default', False):
            db.users.update_one(
                {"_id": ObjectId(session['user_id'])},
                {"$push": {"groups": {
                    "group_id": ObjectId(group_id),
                    "last_read": datetime.now(pytz.utc)
                }}, "$set": {
                    "updated_at": datetime.now(pytz.utc)}}
            )
            # Retrieve groups again
            my_groups, ids = get_user_groups(session['user_id'])

        # Update last read date for group
        db.users.update_one(
            {'_id': ObjectId(session['user_id']),
             'groups.group_id': ObjectId(group_id)},
            {'$set': {'groups.$.last_read': datetime.utcnow(
            ), "updated_at": datetime.now(pytz.utc)}}
        )

    except Exception as e:
        app.logger.error("Failed to load group: %s", str(e))
        return abort(500, description="An error occurred")

    return render_template('group.html', profile=profile, group=group, posts=posts, most_viewed=most_viewed, my_groups=my_groups)


@group_blueprint.route("/create", methods=["POST"])
def create_group():
    try:
        if not is_authenticated():
            return abort(403, description="Not logged in")

        if not has_good_standing():
            return abort(403, description="Account is restricted")

        name = str(request.form.get("name"))
        description = str(request.form.get('description'))

        # Check whether group details violate profanity rules, if so suspend user
        if is_offensive_text(name) or is_offensive_text(description):
            db.users.update_one({"_id": ObjectId(session['user_id'])}, {
                                "$set": {"is_suspended": True, "updated_at": datetime.now(pytz.utc)}})
            return redirect(url_for('feed'))

        if not name:
            return abort(400, description="Name is required")

        if len(name) > 30:
            return abort(400, description="Name too long")

        if len(description) > 100:
            return abort(400, description="Description too long")

        result = db.groups.insert_one(
            {
                "name": str(name),
                "description": str(description),
                "owner_id": ObjectId(session['user_id']),
                "user_type": None,
                "is_default": False,
                "is_private": False,
                "created_at": datetime.now(pytz.utc),
                "updated_at": datetime.now(pytz.utc),
            },
        )

        group = {
            "group_id": result.inserted_id,
            "last_read": datetime.now(pytz.utc)
        }

        db.users.update_one(
            {"_id": ObjectId(session['user_id'])},
            {"$push": {"groups": group}, "$set": {
                "updated_at": datetime.now(pytz.utc)}}
        )

    except Exception as e:
        app.logger.error("Failed to create group: %s", str(e))
        return abort(500, description="An error occurred")

    return redirect(url_for('groups'))


@group_blueprint.route("/join", methods=["POST"])
def group_join():
    try:
        if not is_authenticated():
            return abort(403, description="Not logged in")

        group_id = str(request.form.get("group_id"))

        if not has_group_access(group_id):
            return abort(403, description="No group access")

        group = get_group(group_id)

        if not group:
            return abort(400, description="Group not found")

        # Check if the user is already part of the group
        user_id = ObjectId(session['user_id'])
        user = db.users.find_one(
            {"_id": user_id, "groups.group_id": ObjectId(group_id)})
        if user:
            return abort(400, description="Already a member of this group")

        # Update last read date for group
        db.users.update_one(
            {"_id": user_id},
            {"$push": {"groups": {
                "group_id": ObjectId(group_id),
                "last_read": datetime.now(pytz.utc),
            }}, "$set": {"updated_at": datetime.now(pytz.utc)}}
        )

    except Exception as e:
        app.logger.error("Failed to join group: %s", str(e))
        return abort(500, description="An error occurred")

    return redirect('/group/' + str(group_id))
