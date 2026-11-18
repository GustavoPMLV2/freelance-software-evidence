"""ReplaySafe: reproducible integration reliability demonstration, synthetic data only."""
from __future__ import annotations
import hashlib,json,sqlite3,time
from pathlib import Path
from contextlib import contextmanager,closing

MAX_SQLITE_INTEGER=2**63-1

class Conflict(ValueError): pass
class RetryLater(Exception): pass

class Inbox:
    def __init__(self,path):
        self.path=str(path)
        with self.db() as c:
            c.executescript('''
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY,digest TEXT NOT NULL,payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS effects(id TEXT PRIMARY KEY,amount INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox(id TEXT PRIMARY KEY,attempts INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL DEFAULT 'pending',error TEXT,lease_until REAL);
            CREATE TABLE IF NOT EXISTS sent(id TEXT PRIMARY KEY,payload TEXT NOT NULL);
            ''')
            # Databases created before row claims existed gain the lease column on open.
            if 'lease_until' not in {r[1] for r in c.execute('PRAGMA table_info(outbox)')}:
                c.execute('ALTER TABLE outbox ADD COLUMN lease_until REAL')
    @contextmanager
    def db(self):
        c=sqlite3.connect(self.path,timeout=20)
        try:
            c.execute('PRAGMA busy_timeout=20000')
            with c:
                yield c
        finally:
            c.close()
    def receive(self,event, fail_before_commit=False):
        if set(event)!={'id','amount'} or not isinstance(event['id'],str) or not event['id'] or type(event['amount']) is not int or not 0<=event['amount']<=MAX_SQLITE_INTEGER:
            raise ValueError('Invalid event schema')
        p=json.dumps(event,sort_keys=True,separators=(',',':'))
        digest=hashlib.sha256(p.encode()).hexdigest()
        with self.db() as c:
            c.execute('BEGIN IMMEDIATE')
            old=c.execute('SELECT digest FROM events WHERE id=?',(event['id'],)).fetchone()
            if old:
                if old[0]!=digest: raise Conflict('Event identity reused with different payload')
                return 'duplicate'
            c.execute('INSERT INTO events VALUES(?,?,?)',(event['id'],digest,p))
            c.execute('INSERT INTO effects VALUES(?,?)',(event['id'],event['amount']))
            c.execute('INSERT INTO outbox(id) VALUES(?)',(event['id'],))
            if fail_before_commit: raise RuntimeError('Synthetic failure before commit')
        return 'accepted'
    def dispatch(self,provider,max_attempts=3,lease_seconds=30.0):
        # Dispatchers may overlap (for example, two scheduled runs). Each attempt first
        # claims its row with a time-bounded lease, so only one dispatcher sends it.
        # A claim left by an interrupted dispatcher becomes available when its lease
        # expires. A send that outlives its lease can be repeated; the provider's
        # idempotency key absorbs that repeat.
        if not lease_seconds>=0: raise ValueError('lease_seconds must be zero or positive')
        with self.db() as c:
            ids=c.execute("SELECT id FROM outbox WHERE status='pending' ORDER BY id").fetchall()
        for (eid,) in ids:
            with self.db() as c:
                now=time.time()
                claimed=c.execute("UPDATE outbox SET attempts=attempts+1,lease_until=? WHERE id=? AND status='pending' AND (lease_until IS NULL OR lease_until<=?)",(now+lease_seconds,eid,now)).rowcount
                if not claimed: continue
                n=c.execute('SELECT attempts FROM outbox WHERE id=?',(eid,)).fetchone()[0]
                payload=json.loads(c.execute('SELECT payload FROM events WHERE id=?',(eid,)).fetchone()[0])
            try:
                provider.send(eid,payload)
            except Exception as e:
                # Only the latest claim records a failure, so a late failure cannot
                # reopen or dead-letter an event that another attempt delivered.
                with self.db() as c:
                    c.execute("UPDATE outbox SET status=?,error=?,lease_until=NULL WHERE id=? AND status='pending' AND attempts=?",('dead' if n>=max_attempts else 'pending',type(e).__name__,eid,n))
            else:
                with self.db() as c:
                    c.execute("UPDATE outbox SET status='done',error=NULL,lease_until=NULL WHERE id=?",(eid,))
    def counts(self):
        with self.db() as c:
            return {name:c.execute('SELECT COUNT(*) FROM '+name).fetchone()[0] for name in ['events','effects','outbox','sent']} | {'dead':c.execute("SELECT COUNT(*) FROM outbox WHERE status='dead'").fetchone()[0], 'pending':c.execute("SELECT COUNT(*) FROM outbox WHERE status='pending'").fetchone()[0]}

class SyntheticProvider:
    """Supports an idempotency key. No external network or real money."""
    def __init__(self,path,fail_once=(),fail_always=(),ambiguous_once=()):
        self.path=str(path); self.fail_once=set(fail_once); self.fail_always=set(fail_always); self.ambiguous=set(ambiguous_once)
    def send(self,key,payload):
        if key in self.fail_always: raise RetryLater('provider unavailable')
        if key in self.fail_once:
            self.fail_once.remove(key); raise RetryLater('transient failure')
        with closing(sqlite3.connect(self.path,timeout=20)) as c, c:
            c.execute('INSERT OR IGNORE INTO sent VALUES(?,?)',(key,json.dumps(payload,sort_keys=True)))
        if key in self.ambiguous:
            self.ambiguous.remove(key); raise RetryLater('provider committed; acknowledgement lost')
