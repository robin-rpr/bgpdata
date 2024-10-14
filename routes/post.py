from flask import Blueprint, render_template, redirect, url_for, request, session, abort, current_app as app
from utils.queries import get_user, get_user_groups, get_post, get_group, get_most_viewed_posts
from utils.validators import is_authenticated, is_onboarded, has_group_access, has_good_standing, is_offensive_text
from utils.database import db
from datetime import datetime
from bson import ObjectId
import pytz

# Create Blueprint
post_blueprint = Blueprint('post', __name__)


@post_blueprint.route("/<post_id>")
def post(post_id):
    try:
        if not is_authenticated():
            return redirect(url_for('index'))

        if not is_onboarded():
            return redirect(url_for('onboarding'))

        post_id = str(post_id)

        profile = get_user(session['user_id'])
        my_groups, ids = get_user_groups(session['user_id'])
        post = get_post(post_id)

        if not post:
            return abort(404, description="Post not found")

        group_id = str(post.get('group_id'))
        group = get_group(group_id)

        if not group:
            return abort(404, description="Group not found")

        if not has_group_access(group_id):
            abort(403, description="No group access")

        # Increase post's views count
        db.posts.update_one({'_id': ObjectId(post_id)}, {
                            '$inc': {'views': 1}, "$set": {"updated_at": datetime.now(pytz.utc)}})

        most_viewed = get_most_viewed_posts(list([ObjectId(group_id)]))

    except Exception as e:
        app.logger.error("Failed to retrieve post: %s", str(e))
        return abort(500, description="An error occurred")

    return render_template('post.html', profile=profile, post=post, group=group, most_viewed=most_viewed, my_groups=my_groups)


@post_blueprint.route('/create', methods=["POST"])
def create_post():
    try:
        if not is_authenticated():
            return abort(403, description="Not logged in")

        if not has_good_standing():
            return abort(403, description="Account is restricted")

        title = str(request.form.get('title'))
        content = str(request.form.get('content'))
        group_id = str(request.form.get('group_id'))
        _, ids = get_user_groups(session['user_id'])

        # Check whether post violates profanity rules, if so suspend user
        if is_offensive_text(title) or is_offensive_text(content):
            db.users.update_one({"_id": ObjectId(session['user_id'])}, {
                                "$set": {"is_suspended": True, "updated_at": datetime.now(pytz.utc)}})
            return redirect(url_for('feed'))

        if ObjectId(group_id) not in ids:
            return abort(400, description="Group not found")

        if not request.form.get('title'):
            return abort(400, description="Title is required")

        if not request.form.get('content'):
            return abort(400, description="Content is required")

        if len(title) > 120:
            return abort(400, description="Title too long")

        if len(content) > 500:
            return abort(400, description="Content too long")

        result = db.posts.insert_one({
            "title": title,
            "content": content,
            "author_id": ObjectId(session['user_id']),
            "group_id": ObjectId(group_id),
            "media": [],
            "comments": [],
            "likes": [],
            "views": 0,
            "reports": [],
            "is_hidden": False,
            "created_at": datetime.now(pytz.utc),
            "updated_at": datetime.now(pytz.utc)
        })

    except Exception as e:
        app.logger.error("Failed to create post: %s", str(e))
        return abort(500, description="An error occurred")

    return redirect('/post/' + str(result.inserted_id))


@post_blueprint.route("/like", methods=["POST"])
def like_post():
    if not is_authenticated():
        return abort(403, description="Not logged in")

    try:
        post_id = ObjectId(request.form.get("post_id"))
        user_id = ObjectId(session['user_id'])
        post = db.posts.find_one({"_id": post_id})

        if not post:
            return abort(404, description="Post not found")

        if ObjectId(session['user_id']) in post.get("likes", []):
            # User already liked the post, so we remove the like
            db.posts.update_one(
                {"_id": post_id},
                {"$pull": {"likes": user_id}, "$set": {
                    "updated_at": datetime.now(pytz.utc)}}
            )
        else:
            # User hasn't liked the post yet, so we add the like
            db.posts.update_one(
                {"_id": post_id},
                {"$addToSet": {"likes": user_id}, "$set": {
                    "updated_at": datetime.now(pytz.utc)}}
            )

    except Exception as e:
        app.logger.error("Failed to like post: %s", str(e))
        return abort(500, description="An error occurred")

    return "OK", 200


@post_blueprint.route("/delete", methods=["POST"])
def delete_post():
    try:
        if not is_authenticated():
            return abort(403, description="Not logged in")

        post_id = ObjectId(request.form.get("post_id"))

        post = get_post(post_id)

        if not post:
            return abort(404, description="Post not found")

        if post.get('author_id') != ObjectId(session['user_id']):
            return abort(400, description="Not author of post")

        db.posts.delete_one({"_id": post_id})

    except Exception as e:
        app.logger.error("Failed to delete post: %s", str(e))
        return abort(500, description="An error occurred")

    return redirect("/feed")


@post_blueprint.route("/report", methods=["POST"])
def report_post():
    try:
        if not is_authenticated():
            return abort(403, description="Not logged in")

        if not has_good_standing():
            return abort(403, description="Account is restricted")

        post_id = str(request.form.get("post_id"))
        user_id = ObjectId(session['user_id'])

        post = get_post(post_id)

        if not post:
            return abort(404, description="Post not found")

        if post.get('author_id') == user_id:
            return abort(400, description="Author of post")

        if not has_group_access(str(post.get('group_id'))):
            abort(403, description="No group access")

        if user_id not in post.get('reports', []):
            db.posts.update_one(
                {"_id": ObjectId(post_id)},
                {"$push": {"reports": user_id}, "$set": {
                    "updated_at": datetime.now(pytz.utc)}}
            )

            # Hide post and suspend author after 10 reports
            if len(post.get('reports', [])) > 9:
                db.posts.update_one(
                    {"_id": post.get('_id')},
                    {"$set": {
                        "is_hidden": True,
                        "updated_at": datetime.now(pytz.utc),
                    }}
                )

                db.users.update_one(
                    {"_id": post.get('author_id')},
                    {"$set": {
                        "is_suspended": True,
                        "updated_at": datetime.now(pytz.utc),
                    }}
                )

    except Exception as e:
        app.logger.error("Failed to report post: %s", str(e))
        return abort(500, description="An error occurred")

    return "OK", 200
