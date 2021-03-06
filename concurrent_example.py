# -*- coding: utf-8 -*-

from __future__ import division
import traceback
import time
import random
from multiprocessing import Pool

from test_sqlalchemy_cache import session, User
from sqlalchemy_cache import FromCache, Lock


def update_user(d):
    try:
        s = session.query(User).filter_by(id=d['id']).with_lockmode('update')
        s_q = s.options(FromCache(User.cache))
        user_q = s_q.one()
        if user_q.count + d['count'] >= 0:
            l = Lock(cache, s_q.key_from_query())
            kl = l.acquire()
            if kl is None:
                print user_q.count, d['count']
                user = s.one()
                user.count += d['count']
                session.commit()
                l.unlock()
                time.sleep(0.1)
            elif kl >= 0:
                print d['count']
                s = kl/1000.0
                time.sleep(s)
                update_user(d)
        else:
            session.commit()
    except:
        print traceback.format_exc()


def main():
    infos = []
    for i in xrange(1, 100):
        infos.append(
            {
            'id': 1,
            'count': random.randrange(-100, -1)
            }
        )
    pool = Pool(2)
    pool.map(update_user, infos)
    pool.close()
    pool.join()


if __name__ == '__main__':
    main()
