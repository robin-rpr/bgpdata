from datetime import datetime, timedelta
from flask import session, current_app as app
from bson import ObjectId
from utils.database import db
import pytz


"""
User
"""


def get_user(user_id):
    user = list(db.users.aggregate([
        {"$match": {"_id": ObjectId(user_id)}},
        {"$lookup": {
            "from": "studies",
            "localField": "study_id",
            "foreignField": "_id",
            "as": "study"
        }},
        {"$unwind": {"path": "$study", "preserveNullAndEmptyArrays": True}}
    ]))
    return user[0] if user else None


def get_users(skip=0, limit=25, query=None):
    users = list(db.users.aggregate([
        {"$match": {
            "is_deleted": False,
            "is_suspended": False,
            "$or": [
                {"first_name": {"$regex": query, "$options": "i"}},
                {"last_name": {"$regex": query, "$options": "i"}}
            ]
        }} if query else {"$match": {}},
        {"$lookup": {
            "from": "studies",
            "localField": "study_id",
            "foreignField": "_id",
            "as": "study"
        }},
        {"$unwind": {"path": "$study", "preserveNullAndEmptyArrays": True}},
        {"$sort": {"_id": -1}},
        {"$skip": skip},
        {"$limit": limit}
    ]))

    return users


"""
Groups
"""


def get_group(group_id):
    return db.groups.find_one({'_id': ObjectId(group_id)})


def get_groups(user_id, limit=25, query=None):
    user = db.users.find_one({'_id': ObjectId(user_id)})

    # Retrieve user groups
    user_group_ids = [group['group_id'] for group in user.get('groups', [])]

    # Retrieve default groups
    default_groups = list(db.groups.find({
        "default": True,
        "$or": [
            {"user_type": user.get('user_type')},
            {"user_type": None}
        ]
    }, {"_id": 1}))
    default_group_ids = [group["_id"] for group in default_groups]

    # Combine user groups with default groups
    combined_group_ids = list(user_group_ids + default_group_ids)

    groups = list(db.groups.aggregate([
        {"$match": {
            "is_private": False,
            "is_default": False,
            "name": {"$regex": query, "$options": "i"}
        }} if query else {"$match": {"is_private": False, "is_default": False}},
        {
            "$lookup": {
                "from": "users",
                "let": {"group_id": "$_id"},
                "pipeline": [
                    {"$match": {
                        "$expr": {"$in": ["$$group_id", "$groups.group_id"]}}},
                    {"$count": "member_count"}
                ],
                "as": "user_info"
            }
        },
        {
            "$lookup": {
                "from": "users",
                "let": {"group_id": "$_id"},
                "pipeline": [
                    {"$match": {
                        "$expr": {"$in": ["$$group_id", "$groups.group_id"]}}},
                    {"$limit": 2},
                    {"$project": {"_id": 1}}
                ],
                "as": "member_ids"
            }
        },
        {
            "$lookup": {
                "from": "users",
                "localField": "member_ids._id",
                "foreignField": "_id",
                "as": "members"
            }
        },
        {
            "$addFields": {
                "member_count": {"$ifNull": [{"$arrayElemAt": ["$user_info.member_count", 0]}, 0]},
                "is_member": {"$in": ["$_id", combined_group_ids]}
            }
        },
        {
            "$project": {
                "user_info": 0,
                "member_ids": 0
            }
        },
        {
            "$sort": {
                "member_count": -1
            }
        },
        {"$limit": limit},
    ]))

    ids = [group["_id"] for group in groups]

    return groups, ids


def get_owned_groups(user_id, limit=25, query=None):
    groups = list(db.groups.aggregate([
        {"$match": {
            "owner_id": ObjectId(user_id),
            "name": {"$regex": query, "$options": "i"}
        }} if query else {"$match": {"owner_id": ObjectId(user_id)}},
        {
            "$lookup": {
                "from": "users",
                "let": {"group_id": "$_id"},
                "pipeline": [
                    {"$match": {
                        "$expr": {"$in": ["$$group_id", "$groups.group_id"]}}},
                    {"$count": "member_count"}
                ],
                "as": "user_info"
            }
        },
        {
            "$lookup": {
                "from": "users",
                "let": {"group_id": "$_id"},
                "pipeline": [
                    {"$match": {
                        "$expr": {"$in": ["$$group_id", "$groups.group_id"]}}},
                    {"$limit": 2},
                    {"$project": {"_id": 1}}
                ],
                "as": "member_ids"
            }
        },
        {
            "$lookup": {
                "from": "users",
                "localField": "member_ids._id",
                "foreignField": "_id",
                "as": "members"
            }
        },
        {
            "$addFields": {
                "member_count": {"$ifNull": [{"$arrayElemAt": ["$user_info.member_count", 0]}, 0]},
                "is_member": True
            }
        },
        {
            "$project": {
                "user_info": 0,
                "member_ids": 0
            }
        },
        {
            "$sort": {
                "member_count": -1
            }
        },
        {"$limit": limit},
    ]))

    ids = [group["_id"] for group in groups]

    return groups, ids


def get_user_groups(user_id, include_private=True, include_default=True):
    user = db.users.find_one({'_id': ObjectId(user_id)})

    # Retrieve user groups
    user_group_ids = [group['group_id'] for group in user.get('groups', [])]

    match_section = {
        "is_default": True,
        "$or": [
            {"user_type": user.get('user_type')},
            {"user_type": None}
        ]
    }

    # Do not show private groups to unverified users
    if user.get('is_verified') is False:
        match_section['is_private'] = False

    # Retrieve default groups
    default_groups = list(db.groups.find(
        match_section, {"_id": 1})) if include_default else []

    default_group_ids = [group["_id"] for group in default_groups]

    # Combine user groups with default groups
    combined_group_ids = list(user_group_ids + default_group_ids)

    # Retrieve last read dates for _only_ user groups.
    # Justification: We don't want to overwhelm the user with notifications from default groups.
    last_read_dates = {group['group_id']: group['last_read']
                       for group in user.get('groups', [])}

    groups = list(db.groups.aggregate([
        {"$match": {"_id": {"$in": combined_group_ids}}} if include_private else {
            "$match": {"_id": {"$in": combined_group_ids}, "is_private": False}},
        {"$addFields": {
            "last_read": {
                "$arrayElemAt": [
                    {
                        "$map": {
                            "input": {
                                "$filter": {
                                    "input": [{"group_id": k, "last_read": v} for k, v in last_read_dates.items()],
                                    "as": "grp",
                                    "cond": {"$eq": ["$$grp.group_id", "$_id"]}
                                }
                            },
                            "as": "grp",
                            "in": "$$grp.last_read"
                        }
                    },
                    0
                ]
            }
        }},
        {"$lookup": {
            "from": "posts",
            "let": {"groupId": "$_id", "lastRead": "$last_read"},
            "pipeline": [
                {"$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$group_id", "$$groupId"]},
                            {"$gt": ["$created_at", "$$lastRead"]}
                        ]
                    }
                }},
                {"$count": "unread_count"}
            ],
            "as": "unread_count"
        }},
        {"$addFields": {
            "unread_count": {"$arrayElemAt": ["$unread_count.unread_count", 0]},
        }},
    ]))

    ids = [group["_id"] for group in groups]

    return groups, ids


"""
Posts
"""


def get_post(post_id):
    post = list(db.posts.aggregate([
        {"$match": {"_id": ObjectId(post_id)}},
        {"$addFields": {
            "likes_count": {"$size": "$likes"},
            "comments_count": {"$size": "$comments"},
            "is_liked": {"$in": [ObjectId(session['user_id']), "$likes"]}
        }},
        {"$lookup": {
            "from": "users",
            "localField": "author_id",
            "foreignField": "_id",
            "as": "author"
        }},
        {"$unwind": "$author"},
        {"$lookup": {
            "from": "groups",
            "localField": "group_id",
            "foreignField": "_id",
            "as": "group"
        }},
        {"$unwind": "$group"},
        {"$lookup": {
            "from": "studies",
            "localField": "author.study_id",
            "foreignField": "_id",
            "as": "author_study"
        }},
        {"$unwind": {"path": "$author_study", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "comments",
            "pipeline": [
                {"$match": {
                    "$or": [
                        # Hide comments that have been flagged
                        {"is_hidden": False},
                        # Except for the author itself
                        {"author_id": ObjectId(session['user_id'])}
                    ]
                }},
            ],
            "localField": "comments",
            "foreignField": "_id",
            "as": "comments_data"
        }},
        {"$lookup": {
            "from": "users",
            "localField": "comments_data.author_id",
            "foreignField": "_id",
            "as": "comments_authors"
        }},
        {"$addFields": {
            "comments_data": {
                "$map": {
                    "input": "$comments_data",
                    "as": "comment",
                    "in": {
                        "$mergeObjects": [
                            "$$comment",
                            {"author": {
                                "$arrayElemAt": [
                                    {"$filter": {
                                        "input": "$comments_authors",
                                        "as": "author",
                                        "cond": {"$eq": ["$$author._id", "$$comment.author_id"]}
                                    }},
                                    0
                                ]}
                             }
                        ]
                    }
                }
            }
        }},
        {"$set": {"views": {"$add": ["$views", 1]}}}
    ]))

    return post[0] if post else None


def get_posts(group_ids, author_id=None, skip=0, limit=10, query=None):
    match_stage = {
        "group_id": {"$in": group_ids},
        "$or": [
            # Hide posts that have been flagged
            {"is_hidden": False},
            # Except for the author itself
            {"author_id": ObjectId(session['user_id'])}
        ]
    }

    if author_id:
        match_stage["author_id"] = author_id

    if query:
        match_stage["$or"] = [
            {"title": {"$regex": query, "$options": "i"}},
            {"content": {"$regex": query, "$options": "i"}}
        ]

    return list(db.posts.aggregate([
        {"$match": match_stage},
        {"$sort": {"_id": -1}},
        {"$skip": skip},
        {"$limit": limit},
        {"$addFields": {
            "likes_count": {"$size": "$likes"},
            "comments_count": {"$size": "$comments"},
            "is_liked": {"$in": [ObjectId(session['user_id']), "$likes"]}
        }},
        {"$lookup": {
            "from": "users",
            "localField": "author_id",
            "foreignField": "_id",
            "as": "author"
        }},
        {"$unwind": "$author"},
        {"$lookup": {
            "from": "groups",
            "localField": "group_id",
            "foreignField": "_id",
            "as": "group"
        }},
        {"$unwind": "$group"},
        {"$lookup": {
            "from": "studies",
            "localField": "author.study_id",
            "foreignField": "_id",
            "as": "author_study"
        }},
        {"$unwind": {"path": "$author_study", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "comments",
            "let": {"postId": "$_id"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$eq": ["$post_id", "$$postId"]},
                    "$or": [
                        # Hide comments that have been flagged
                        {"is_hidden": False},
                        # Except for the author itself
                        {"author_id": ObjectId(session['user_id'])}
                    ]
                }},
                {"$sort": {"created_at": -1}},
                {"$limit": 1}
            ],
            "as": "latest_comment"
        }},
        {"$unwind": {"path": "$latest_comment", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "users",
            "localField": "latest_comment.author_id",
            "foreignField": "_id",
            "as": "latest_comment_author"
        }},
        {"$unwind": {"path": "$latest_comment_author",
                     "preserveNullAndEmptyArrays": True}},
        {"$addFields": {
            "latest_comment.author": "$latest_comment_author"
        }},
        {"$project": {
            "latest_comment_author": 0
        }}
    ]))


def get_popular_posts(group_ids, days=3, limit=5, query=None):
    days_ago = datetime.now(pytz.utc) - timedelta(days=days)
    return list(db.posts.aggregate([
        {"$match": {
            "group_id": {"$in": group_ids},
            "created_at": {"$gte": days_ago},
            "$or": [
                # Hide posts that have been flagged
                {"is_hidden": False},
                # Except for the author itself
                {"author_id": ObjectId(session['user_id'])},
                {"title": {"$regex": query, "$options": "i"}},
                {"content": {"$regex": query, "$options": "i"}}
            ]
        }} if query else {"$match": {
            "group_id": {"$in": group_ids},
            "created_at": {"$gte": days_ago},
            "$or": [
                # Hide posts that have been flagged
                {"is_hidden": False},
                # Except for the author itself
                {"author_id": ObjectId(session['user_id'])}
            ]
        }},
        {"$addFields": {
            "likes_count": {"$size": "$likes"},
            "comments_count": {"$size": "$comments"},
        }},
        {"$addFields": {
            "trending_score": {
                "$add": [
                    "$views",
                    {"$multiply": ["$likes_count", 2]},
                    {"$multiply": ["$comments_count", 3]}
                ]
            }
        }},
        {"$sort": {"trending_score": -1}},
        {"$limit": limit},
        {"$lookup": {
            "from": "groups",
            "localField": "group_id",
            "foreignField": "_id",
            "as": "group"
        }},
        {"$unwind": "$group"}
    ]))


def get_most_viewed_posts(group_ids, days=7, limit=5):
    days_ago = datetime.now(pytz.utc) - timedelta(days=days)
    return list(db.posts.aggregate([
        {'$match': {
            'group_id': {"$in": group_ids},
            'created_at': {'$gte': days_ago},
            '$or': [
                # Hide posts that have been flagged
                {'is_hidden': False},
                # Except for the author itself
                {'author_id': ObjectId(session['user_id'])}
            ]
        }},
        {'$sort': {'views': -1}},
        {'$limit': 5}
    ]))


"""
Comments
"""


def get_comment(comment_id):
    return db.comments.find_one({'_id': ObjectId(comment_id)})
