# PoolQueue is a simple module for running tasks on a Queue in
# a pool of different processes.
#
# Copyright Jeremy Sanders 2020
# Source control: https://github.com/jeremysanders/poolqueue
#
# MIT Licensed (see LICENSE.txt)

import atexit
import os
import time
import sys
import pickle
import socket
import struct
import select
import traceback

# make sure we clean up processes before exiting
# maps id(list) -> list for socket and pid lists to allow easy cleanup
_all_socks = {}
_all_pids = {}
@atexit.register
def _cleanup():
    for socks in _all_socks.values():
        for sock in socks:
            sendobj(sock, None)
        socks.clear()
    for pids in _all_pids.values():
        for pid in pids:
            os.waitpid(pid, 0)
        pids.clear()

def socksend(sock, data):
    while len(data)>0:
        size = socksend(sock, data)
        if size==len(data):
            return
        data = data[size:]

def sendobj(sock, obj):
    msg = pickle.dumps(obj)
    hdr = struct.pack('@Q', len(msg))
    comb = hdr+msg
    sock.send(comb)

_hdrsize = struct.calcsize('@Q')
def recvobj(sock):
    """Receive object from socket."""
    # this is slightly more complex then sending to avoid calling recv() often
    read = b''
    while len(read)<_hdrsize:
        read += sock.recv(4096)

    size = struct.unpack('@Q', read[:_hdrsize])[0]
    read = read[_hdrsize:]
    while len(read) < size:
        read += sock.recv(size-len(read))

    obj = pickle.loads(read)
    return obj

def worker(sock, initfunc, env):
    """Repeat, processing new requests."""

    if initfunc is not None:
        initfunc()

    while True:
        obj = recvobj(sock)
        if obj is None:
            # time to finish
            return

        jobid, func, args, argsv = obj
        if func in env:
            # is this a function name we know about?
            func = env[func]

        try:
            retn = func(*args, **argsv)
        except Exception as e:
            e.backtrace = traceback.format_exc()
            retn = e

        sendobj(sock, (jobid, retn))

class PoolQueueException(RuntimeError):
    pass

class PoolQueue:
    """Queue to process tasks.

    :par numforks: number of processes to launch to process tasks
    :par initfunc: function to call when task is started
    :par reraise: if there is an exception in the task, raise an exception. Otherwise return it as a task result.
    :par ordered: results are returned in the order that tasks are added. If False, they are returned as soon as they are available.
    :par retn_ids: the task result is (task_id, result), rather than result. task_ids are automatically assigned if not provided.
    :par env: special optional environment. This is a dict (e.g. locals()) which is used to run functions without pickling them. Useful for closures/nested functions.
    """

    def __init__(self, numforks=16, initfunc=None, reraise=True,
                 ordered=True, retn_ids=False, env=None):
        self.reraise = reraise
        self.ordered = ordered
        self.retn_ids = retn_ids

        self.socks = []
        self.freesocks = set() # sockets to waiting processes
        self.busysocks = set() # sockets to processing processes
        self.pids = [] # PIDs of forks

        self.jobs = [] # items to process
        self.jobids = [] # ids of added jobs (in ordered mode)
        self.jobctr = 0 # automatic job counter
        self.jobresults = {}  # processed data go here

        # environment containing functions not to pickle
        env = {} if env is None else {
            k:v for k,v in env.items() if callable(v)}
        # map function to name
        self.env_inv = {v:k for k,v in env.items()}

        for i in range(numforks):
            s1, s2 = socket.socketpair()
            s1.setblocking(True)
            s2.setblocking(True)
            s1.set_inheritable(True)
            pid = os.fork()
            if pid == 0:
                # client
                del self.socks
                del self.pids
                # make sure we don't try cleanup up from other process
                _all_socks.clear()
                _all_pids.clear()
                # run worker function and exit
                worker(s1, initfunc, env)
                sys.exit(0)
            else:
                # server
                s1.close()
                self.socks.append(s2)
                self.pids.append(pid)

        _all_socks[id(self.socks)] = self.socks
        _all_pids[id(self.pids)] = self.pids

        self.freesocks = set(self.socks)

    def _sendjobs(self):
        """Send any remaining jobs if there are free workers."""
        while self.freesocks and self.jobs:
            job = self.jobs.pop(0)
            sock = self.freesocks.pop()
            sendobj(sock, job)
            self.busysocks.add(sock)

    def add(self, func, args, argsv=None, jobid=None):
        """Adds a job to the queue.
        """
        if jobid is None:
            jobid = self.jobctr
            self.jobctr += 1

        if argsv is None:
            argsv = {}

        if func in self.env_inv:
            # pass name instead of function to avoid pickling
            func = self.env_inv[func]

        self.jobs.append((jobid, func, args, argsv))
        if self.ordered:
            self.jobids.append(jobid)

        self._sendjobs()

    def poll(self, timeout=0):
        """Poll for results.
        Return True if available
        """

        readable, writable, errors = select.select(
            self.busysocks, [], [], timeout)

        if readable:
            for sock in readable:
                jobid, result = recvobj(sock)
                if self.reraise and isinstance(result, Exception):
                    raise PoolQueueException(
                        'Exception running task:\n%s' % (
                            result.backtrace))

                self.jobresults[jobid] = result
                self.busysocks.remove(sock)
                self.freesocks.add(sock)
            return True
        return False

    def wait(self):
        """Wait until all jobs are processed."""

        while self.jobs or self.busysocks:
            self._sendjobs()
            self.poll(timeout=1)

    def iter_results(self):
        """Iterate over any returned results."""
        if self.ordered:
            # must return items in order
            while self.jobids:
                jobid = self.jobids[0]
                if jobid in self.jobresults:
                    result = self.jobresults.pop(jobid)
                    if self.retn_ids:
                        result = jobid, result
                    yield result
                    self.jobids.pop(0)
                else:
                    break
        else:
            while self.jobresults:
                jobid = self.jobresults.__iter__().__next__()
                result = self.jobresults.pop(jobid)
                if self.retn_ids:
                    result = jobid, result
                yield result

    def process(self, func, iterable, argsv=None, interval=None):
        """Process jobs from an iterable, returning results.

        func(args, **argsv) is called where args iterates over iterable
        """
        if argsv is None:
            argsv = {}

        for args in iterable:
            while not self.freesocks:
                self.poll(timeout=interval)
            self.add(func, args, argsv=argsv)
            yield from self.iter_results()

        yield from self.results(interval=interval)

    def results(self, poll=False, interval=None):
        """Process all jobs, return results as an iterable.

        poll: exit when no more results are available

        """

        if poll:
            self._sendjobs()
            self.poll()
            yield from self.iter_results()
        else:
            while self.jobs or self.busysocks or self.jobresults:
                self._sendjobs()
                yield from self.iter_results()
                if self.busysocks:
                    self.poll(timeout=interval)

    def finish(self):
        """Finish processing current jobs. Exit subproceses."""

        self.wait()

        # signal to finish up
        for sock in self.socks:
            sendobj(sock, None)

        for pid in self.pids:
            res = os.waitpid(pid, 0)

        # empty out anything
        for v in self.pids, self.socks, self.busysocks, self.freesocks:
            v.clear()

        # cleanup
        if id(self.socks) in _all_socks:
            del _all_socks[id(self.socks)]
        if id(self.pids) in _all_pids:
            del _all_pids[id(self.pids)]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()

###################################################################################

def _testfunc(a):
    import time
    time.sleep((a+1)*0.02)
    return boo+a

def _testfunc2(b, addon=0):
    import time
    time.sleep(0.1)
    return str(66+b+addon)

def _testinit():
    global boo
    boo = 42

def _testiter():
    for i in range(5):
        yield (i,)

def test():
    with PoolQueue(numforks=2, initfunc=_testinit, retn_ids=False, ordered=True) as q:
        res = tuple(q.process(_testfunc, _testiter()))
        assert res == (42,43,44,45,46)
    with PoolQueue(numforks=3, initfunc=_testinit, retn_ids=False, ordered=False) as q:
        res = sorted(list(q.process(_testfunc, _testiter())))
        assert res == [42,43,44,45,46]
    with PoolQueue(numforks=4, initfunc=_testinit, retn_ids=True, ordered=True) as q:
        res = tuple(q.process(_testfunc, _testiter()))
        assert res == ((0,42),(1,43),(2,44),(3,45),(4,46))

    with PoolQueue(numforks=1, initfunc=_testinit) as q:
        q.add(_testfunc, (2,))
        q.add(_testfunc2, (5,), {'addon':10})
        q.add(_testfunc2, (-1,), {'addon':3})
        q.wait()
        res = tuple(q.iter_results())
        assert res == (44, '81', '68')

    # test using a nested function and env
    bigdata = [1,2,3,4,5,6]
    def nestfunc(a):
        return a+sum(bigdata)
    with PoolQueue(numforks=4, env=locals()) as q:
        for i, res in enumerate(q.process(nestfunc, ((j+1,) for j in range(128)))):
            assert res == 21+i+1

    q = PoolQueue(numforks=1, initfunc=_testinit)
    q.finish()

    assert(_all_socks=={} and _all_pids=={})

    print('Tests succeeded')

if __name__ == '__main__':
    test()
