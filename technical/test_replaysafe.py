import concurrent.futures,json,tempfile,threading,time,unittest,gc
from pathlib import Path
from replaysafe import Inbox,SyntheticProvider,Conflict,RetryLater

class Reliability(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'demo.sqlite';self.box=Inbox(self.path)
    def tearDown(self):self.tmp.cleanup()
    def test_duplicate_delivery(self):
        e={'id':'a','amount':120};self.assertEqual(self.box.receive(e),'accepted');self.assertEqual(self.box.receive(e),'duplicate');self.assertEqual(self.box.counts()['effects'],1)
    def test_identity_conflict(self):
        self.box.receive({'id':'a','amount':120})
        with self.assertRaises(Conflict):self.box.receive({'id':'a','amount':121})
        self.assertEqual(self.box.counts()['effects'],1)
    def test_invalid_payload_has_no_effect(self):
        for e in [{'id':'a','amount':True},{'id':'a','amount':-1},{'id':'a','amount':2**63},{'id':'a','amount':1,'unexpected':'x'},{'id':'','amount':1}]:
            with self.assertRaises(ValueError):self.box.receive(e)
        self.assertEqual(self.box.counts()['events'],0)
    def test_rollback_before_commit(self):
        e={'id':'a','amount':1}
        with self.assertRaises(RuntimeError):self.box.receive(e,True)
        self.assertEqual(self.box.counts()['effects'],0);self.box.receive(e);self.assertEqual(self.box.counts()['outbox'],1)
    def test_concurrent_replays(self):
        deliveries=[{'id':str(i%100),'amount':i%100} for i in range(500)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as p:r=list(p.map(self.box.receive,deliveries))
        self.assertEqual(r.count('accepted'),100);self.assertEqual(r.count('duplicate'),400);self.assertEqual(self.box.counts()['effects'],100)
    def test_restart_retains_inbox(self):
        self.box.receive({'id':'a','amount':1});other=Inbox(self.path);self.assertEqual(other.receive({'id':'a','amount':1}),'duplicate')
    def test_transient_retry(self):
        self.box.receive({'id':'a','amount':1});p=SyntheticProvider(self.path,fail_once=['a']);self.box.dispatch(p);self.assertEqual(self.box.counts()['pending'],1);self.box.dispatch(p);self.assertEqual(self.box.counts()['sent'],1)
    def test_lost_ack_does_not_duplicate_remote_effect(self):
        self.box.receive({'id':'a','amount':1});p=SyntheticProvider(self.path,ambiguous_once=['a']);self.box.dispatch(p);self.box.dispatch(p);self.assertEqual(self.box.counts()['sent'],1);self.assertEqual(self.box.counts()['pending'],0)
    def test_permanent_failure_reaches_dead_letter(self):
        self.box.receive({'id':'a','amount':1});p=SyntheticProvider(self.path,fail_always=['a'])
        for _ in range(4):self.box.dispatch(p)
        self.assertEqual(self.box.counts()['dead'],1);self.assertEqual(self.box.counts()['sent'],0)
    def test_persisted_outbox_recovers_after_restart(self):
        self.box.receive({'id':'a','amount':1});other=Inbox(self.path);other.dispatch(SyntheticProvider(self.path));self.assertEqual(other.counts()['sent'],1)
    def test_concurrent_identity_conflict_keeps_one_effect(self):
        def receive(amount):
            try:return self.box.receive({'id':'same','amount':amount})
            except Conflict:return 'conflict'
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            statuses=list(pool.map(receive,[10,20]))
        self.assertCountEqual(statuses,['accepted','conflict'])
        self.assertEqual(self.box.counts()['effects'],1)
        self.assertEqual(self.box.counts()['outbox'],1)
    def test_dead_letter_is_not_requeued_after_restart(self):
        self.box.receive({'id':'a','amount':1})
        provider=SyntheticProvider(self.path,fail_always=['a'])
        for _ in range(3):self.box.dispatch(provider)
        fresh=Inbox(self.path);fresh.dispatch(SyntheticProvider(self.path))
        self.assertEqual(fresh.counts()['dead'],1)
        self.assertEqual(fresh.counts()['sent'],0)
    def test_overlapping_dispatchers_send_each_event_once(self):
        for i in range(60):self.box.receive({'id':f'e{i:02}','amount':i})
        calls=[];lock=threading.Lock();start=threading.Barrier(4)
        class Counting(SyntheticProvider):
            def send(self,key,payload):
                with lock:calls.append(key)
                super().send(key,payload)
        provider=Counting(self.path)
        def run():
            start.wait();Inbox(self.path).dispatch(provider)
        workers=[threading.Thread(target=run) for _ in range(4)]
        for w in workers:w.start()
        for w in workers:w.join()
        self.assertEqual(len(calls),60,'Each event must reach the provider once, not once per dispatcher')
        self.assertEqual(self.box.counts()['sent'],60)
        self.assertEqual(self.box.counts()['pending'],0)
    def test_late_failure_cannot_reopen_a_delivered_event(self):
        self.box.receive({'id':'a','amount':1});path=self.path
        class LateTimeout(SyntheticProvider):
            def send(self,key,payload):
                # The lease of this attempt has expired; another dispatcher delivers meanwhile.
                Inbox(path).dispatch(SyntheticProvider(path))
                raise RetryLater('timeout reported after the other delivery')
        self.box.dispatch(LateTimeout(path),lease_seconds=0)
        counts=self.box.counts()
        self.assertEqual(counts['sent'],1);self.assertEqual(counts['pending'],0);self.assertEqual(counts['dead'],0)
    def test_interrupted_send_is_retried_after_its_lease_expires(self):
        self.box.receive({'id':'a','amount':1})
        class Crash(BaseException):pass
        class Crashing(SyntheticProvider):
            def send(self,key,payload):raise Crash('process stopped during send')
        with self.assertRaises(Crash):self.box.dispatch(Crashing(self.path),lease_seconds=0.5)
        restarted=Inbox(self.path);restarted.dispatch(SyntheticProvider(self.path))
        self.assertEqual(restarted.counts()['sent'],0,'The interrupted claim is still leased')
        time.sleep(0.6);restarted.dispatch(SyntheticProvider(self.path))
        self.assertEqual(restarted.counts()['sent'],1);self.assertEqual(restarted.counts()['pending'],0)
    @unittest.skipUnless(Path('/proc/self/fd').is_dir(),'Requires Linux procfs')
    def test_connections_do_not_accumulate_without_garbage_collection(self):
        gc.collect();before=len(list(Path('/proc/self/fd').iterdir()))
        enabled=gc.isenabled();gc.disable()
        try:
            for i in range(40):self.box.receive({'id':str(i),'amount':i})
            self.box.dispatch(SyntheticProvider(self.path))
            after=len(list(Path('/proc/self/fd').iterdir()))
            self.assertLessEqual(after,before+4,'Connections must close without relying on GC')
        finally:
            if enabled:gc.enable()
            gc.collect()

if __name__=='__main__':unittest.main()
