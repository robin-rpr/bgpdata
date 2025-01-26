import os
import socket

# Log level
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()

# Route Views Kafka Consumer configuration
routeviews_consumer_conf = {
    'bootstrap.servers': 'stream.routeviews.org:9092',
    'group.id': f"bgpdata-{socket.gethostname()}",
    'partition.assignment.strategy': 'roundrobin',
    'enable.auto.commit': False,
    'security.protocol': 'PLAINTEXT',
    'fetch.max.bytes': 50 * 1024 * 1024, # 50 MB
    'session.timeout.ms': 30000,  # For stable group membership
}

# RIS Kafka Consumer configuration
ris_consumer_conf = {
    'bootstrap.servers': 'node01.kafka-pub.ris.ripe.net:9094,node02.kafka-pub.ris.ripe.net:9094,node03.kafka-pub.ris.ripe.net:9094',
    'group.id': f"bgpdata-{socket.gethostname()}",
    'partition.assignment.strategy': 'roundrobin',
    'enable.auto.commit': False,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'public',
    'sasl.password': 'public',
    'fetch.max.bytes': 50 * 1024 * 1024, # 50 MB
    'session.timeout.ms': 30000,  # For stable group membership
}

# RIS Avro Encoding schema
ris_avro_schema = {
    "type": "record",
    "name": "RisLiveBinary",
    "namespace": "net.ripe.ris.live",
    "fields": [
        {
            "name": "type",
            "type": {
                "type": "enum",
                "name": "MessageType",
                "symbols": ["STATE", "OPEN", "UPDATE", "NOTIFICATION", "KEEPALIVE"],
            },
        },
        {"name": "timestamp", "type": "long"},
        {"name": "host", "type": "string"},
        {"name": "peer", "type": "bytes"},
        {
            "name": "attributes",
            "type": {"type": "array", "items": "int"},
            "default": [],
        },
        {
            "name": "prefixes",
            "type": {"type": "array", "items": "bytes"},
            "default": [],
        },
        {"name": "path", "type": {"type": "array", "items": "long"}, "default": []},
        {"name": "ris_live", "type": "string"},
        {"name": "raw", "type": "string"},
    ],
}
