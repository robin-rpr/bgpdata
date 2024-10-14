def find_author_by_id(authors, author_id):
    return next((author for author in authors if author['_id'] == author_id), None)
