from datetime import datetime
import hashlib
import bleach
import re


def time_ago(dt):
    now = datetime.utcnow()
    diff = now - dt

    seconds = diff.total_seconds()
    minutes = seconds // 60
    hours = minutes // 60
    days = hours // 24
    years = days // 365

    if seconds < 60:
        return f"{int(seconds)}s"
    elif minutes < 60:
        return f"{int(minutes)}m"
    elif hours < 24:
        return f"{int(hours)}h"
    elif days < 30:
        return f"{int(days)}d"
    elif years < 1:
        months = days // 30
        return f"{int(months)}m"
    else:
        return f"{int(years)}y"


def hash_text(text):
    return hashlib.sha256(text.encode()).hexdigest()


def format_text(text):
    # Normalize whitespace around newlines
    text = re.sub(r'[ \t]*\n[ \t]*', '\n', text)

    # Replace lines containing only whitespaces with a single newline
    text = re.sub(r'\n\s*\n', '\n\n', text)

    # Replace multiple newlines with a maximum of two
    text = re.sub(r'\n{2,}', '\n\n', text)

    # Convert http links to https
    text = re.sub(r'http://', 'https://', text)

    # Wrap links with <a> tags
    text = re.sub(r'(https?://\S+)', r'<a href="\1">\1</a>', text)

    # Replace single-newlines with <br/>
    text = re.sub(r'\n', '<br/>', text)

    # Reduce multiple consecutive empty newlines to a single empty newline
    text = re.sub(r'\n\s*\n', '\n\n', text)

    return text


def sanitize_text(text):
    if text:
        return bleach.clean(text)
    else:
        return text
