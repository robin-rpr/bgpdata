from flask import Blueprint, redirect, request, session, abort, url_for, current_app as app
from utils.queries import get_user, get_post, get_comment
from utils.validators import is_authenticated, has_group_access, has_good_standing, is_offensive_text
from utils.database import db
from datetime import datetime
from bson import ObjectId
import pytz

# Create Blueprint
comment_blueprint = Blueprint('comment', __name__)


@comment_blueprint.route("/create", methods=["POST"])
def create_comment():
    try:
        if not is_authenticated():
            return abort(403, description="Not logged in")

        post_id = str(request.form.get("post_id"))
        text = str(request.form.get('text'))

        # Check whether comment violates profanity rules, if so suspend user
        if is_offensive_text(text):
            db.users.update_one({"_id": ObjectId(session['user_id'])}, {
                                "$set": {"is_suspended": True, "updated_at": datetime.now(pytz.utc)}})
            return redirect(url_for('feed'))

        if len(text) > 300:
            return abort(300, description="Comment too long")

        post = get_post(post_id)

        if not post:
            return abort(400, description="Post not found")

        if not has_group_access(str(post.get("group_id"))):
            abort(403, description="No group access")

        result = db.comments.insert_one({
            "post_id": ObjectId(post_id),
            "author_id": ObjectId(session['user_id']),
            "text": text,
            "reports": [],
            "is_hidden": False,
            "created_at": datetime.now(pytz.utc),
            "updated_at": datetime.now(pytz.utc)
        })

        db.posts.update_one(
            {"_id": ObjectId(post_id)},
            {"$push": {"comments": result.inserted_id},
                "$set": {"updated_at": datetime.now(pytz.utc)}}
        )

    except Exception as e:
        app.logger.error("Failed to comment on post: %s", str(e))
        return abort(500, description="An error occurred")

    return redirect('/post/' + str(post_id))


@comment_blueprint.route("/delete", methods=["POST"])
def delete_comment():
    if not is_authenticated():
        return abort(403, description="Not logged in")

    try:
        comment_id = str(request.form.get("comment_id"))
        comment = get_comment(comment_id)

        if not comment:
            return abort(404, description="Comment not found")

        if comment.get("author_id") != ObjectId(session['user_id']):
            abort(403, description="Not author of comment")

        result = db.posts.update_one(
            {"_id": comment.get("post_id")},
            {"$pull": {"comments": ObjectId(comment_id)},
             "$set": {"updated_at": datetime.now(pytz.utc)}}
        )

        if result.modified_count == 0:
            return abort(400, description="Failed removing comment from post")

        db.comments.delete_one({"_id": ObjectId(comment_id)})

    except Exception as e:
        app.logger.error("Failed to delete comment: %s", str(e))
        return abort(500, description="An error occurred")

    return redirect("/post/" + str(comment.get("post_id")))


@ comment_blueprint.route("/report", methods=["POST"])
def report_comment():
    try:
        if not is_authenticated():
            return abort(403, description="Not logged in")

        if not has_good_standing():
            return abort(403, description="Account is restricted")

        comment_id = str(request.form.get("comment_id"))
        user_id = ObjectId(session['user_id'])

        comment = get_comment(comment_id)

        if not comment:
            return abort(404, description="Post not found")

        if comment.get('author_id') == user_id:
            return abort(400, description="Author of post")

        if user_id not in comment.get('reports', []):
            db.comments.update_one(
                {"_id": ObjectId(comment_id)},
                {"$push": {"reports": user_id}, "$set": {
                    "updated_at": datetime.now(pytz.utc)}}
            )

            # Hide comment and suspend author after 10 reports
            if len(comment.get('reports', [])) > 9:
                db.comments.update_one(
                    {"_id": comment.get('_id')},
                    {"$set": {
                        "is_hidden": True,
                        "updated_at": datetime.now(pytz.utc),
                    }}
                )

                db.users.update_one(
                    {"_id": comment.get('author_id')},
                    {"$set": {
                        "is_suspended": True,
                        "updated_at": datetime.now(pytz.utc),
                    }}
                )

    except Exception as e:
        app.logger.error("Failed to report post: %s", str(e))
        return abort(500, description="An error occurred")

    return "OK", 200
