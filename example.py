# -*- coding: utf-8 -*-

import time
from sqlalchemy import create_engine

from sqlalchemy_cache import SQLAlchemy

engine = create_engine('mysql://root@localhost/test', isolation_level="SERIALIZABLE")
db = SQLAlchemy(engine)


def _get_timestamp(self):
    return time.time()


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50))
    desc = db.Column(db.String(200))
    count = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=_get_timestamp)
