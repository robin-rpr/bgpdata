import pybgpstream
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime, UTC

Base = declarative_base()

class BGPUpdate(Base):
    __tablename__ = 'bgp_updates'

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    peer_asn = Column(Integer)
    prefix = Column(String)
    ip_version = Column(String)
    update_type = Column(String)
    resource = Column(String)

# Set up connection to the database
def get_db_session():
    engine = create_engine('postgresql://user:password@localhost/bgp')
    Session = sessionmaker(bind=engine)
    return Session()

# Background task to collect real-time data and store it in the database
def collect_real_time_data():
    session = get_db_session()

    stream = pybgpstream.BGPStream(
        proj_name="ris-live",
        filter="resource AS3333"  # Filter by the resource, can be modified as needed
    )

    for elem in stream:
        update = BGPUpdate(
            peer_asn=elem.peer_asn,
            prefix=elem.fields["prefix"],
            update_type="announcement" if elem.type == "A" else "withdrawal",
            ip_version="ipv6" if ":" in elem.fields["prefix"] else "ipv4",
            timestamp=datetime.fromtimestamp(elem.time, UTC),
            resource=elem.fields["prefix"]
        )
        session.add(update)
        session.commit()

    session.close()

if __name__ == '__main__':
    collect_real_time_data()
