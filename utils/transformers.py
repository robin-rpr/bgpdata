"""
BGPDATA - BGP Data Collection and Analytics Service

This software is part of the BGPDATA project, which is designed to collect, process, and analyze BGP data from various sources.
It helps researchers and network operators get insights into their network by providing a scalable and reliable way to analyze and inspect historical and live BGP data from RIPE NCC RIS.

Author: Robin Röper

© 2024 BGPDATA. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions, and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions, and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of BGPDATA nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
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
