# ForkQueue is a simple module for running tasks on a Queue in
# a pool of different processes.
#
# Copyright Jeremy Sanders 2020
# Source control: https://github.com/jeremysanders/forkqueue
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
            _sendobj(sock, None)
        socks.clear()
    for pids in _all_pids.values():
        for pid in pids:
            os.waitpid(pid, 0)
        pids.clear()

def _sendobj(sock, obj):
    msg = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    hdr = struct.pack('@Q', len(msg))
    comb = hdr+msg
    sock.send(comb)

_hdrsize = struct.calcsize('@Q')
def _recvobj(sock):
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

def _worker(sock, initfunc, env):
    """Repeat, processing new requests."""

    if initfunc is not None:
        initfunc()

    while True:
        obj = _recvobj(sock)
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

        _sendobj(sock, (jobid, retn))

class ForkQueueException(RuntimeError):
    pass

class ForkQueue:
    """Queue to process tasks in separate threads.

    Main class of the forkqueue module.
    """

    def __init__(self, numforks=16, initfunc=None, reraise=True,
                 ordered=True, retn_ids=False, env=None):

        """
        Initialise the class

        Args:
          numforks (int): Number of processes to launch to process tasks.
          initfunc (callable): this optional function is called in forked processes when starting
          reraise (bool): If an exception is raised in the forked process, reraise it in the main
            process. If this is not set, return the exception instead.
          ordered (bool): If True, yield results in the order tasks are added. Otherwise return
            them in any order.
          retn_ids (bool): If True, instead of yielding only the result, a tuple of
            (job_id, result) is yielded instead.
          env (dict): A dictionary of callables (e.g. from locals()). If a task uses a function
            in this dict, it is passed to the forked process by name, rather than being
            pickled. This allows the forked process to run, for example, nested functions.
        """

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
                _worker(s1, initfunc, env)
                # this is a bit of a nasty way to exit, but ipython breaks
                # otherwise
                os._exit(0)
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
            _sendobj(sock, job)
            self.busysocks.add(sock)

    def add(self, func, args, argsv=None, jobid=None):
        """Adds a job to the queue.

        Args:
          func (callable): Function to call to execute task.
          args (tuple): Arguments to give to the callable.
          argsv (dict): Optional named arguments to pass to the callable.
          jobid (int/str): Unique ID to be assigned the job. If not given, these are generated
            to be incrementing integers starting from 0.

        Returns:
          None
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
        """Poll the forked processes for results.

        Args:
          timeout (float/None): Wait for up to timeout seconds until there is a result.
            0 means do not wait at all. None will wait forever.

        Returns:
          bool: Whether a result is available.
        """

        readable, writable, errors = select.select(
            self.busysocks, [], [], timeout)

        if readable:
            for sock in readable:
                jobid, result = _recvobj(sock)
                self.busysocks.remove(sock)
                self.freesocks.add(sock)

                if self.reraise and isinstance(result, Exception):
                    self.jobresults[jobid] = None
                    raise ForkQueueException(
                        'Exception running task:\n%s' % (
                            result.backtrace))
                else:
                    self.jobresults[jobid] = result
            return True
        return False

    def wait(self):
        """Wait until all jobs are processed.

        Returns:
          None
        """

        while self.jobs or self.busysocks:
            self._sendjobs()
            self.poll(timeout=1)

    def yield_results(self):
        """Yield any results which are currently available.

        If retn_ids is True, then each result is returned as (job_id, result).
        If ordered is True, then results from jobs are yielded in order. Otherwise
        they are returned in any order.
        """
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
        """Process jobs generated from an iterable, yielding results.

        Jobs are added for each item of the iterable. func(args, **argsv) is called
        in the subprocess where args is an item in the iterable.

        If retn_ids is True, then each result is returned as (job_id, result).
        If ordered is True, then results from jobs are yielded in order. Otherwise
        they are returned in any order.

        Args:
          func (callable): Function to call.
          iterable (iterable): An iterable yielding sets of tuples which act as the
            arguments to fhe function being called.
          argsv (dict): If given, these named arguments are given to all calls to the
            function.
          interval (float): How often to check for results (seconds). If None, then
            we wait until a result is ready.
        """
        if argsv is None:
            argsv = {}

        for args in iterable:
            while not self.freesocks:
                self.poll(timeout=interval)
            self.add(func, args, argsv=argsv)
            yield from self.yield_results()

        yield from self.results(interval=interval)

    def map(self, func, *args):
        """A map like function for the queue.

        This makes ForkQueue suitable for feeding into emcee or zeus,
        as a replacement for a multiprocessing.Pool() object.
        """

        yield from self.process(func, zip(*args))

    def results(self, poll=False, interval=None):
        """Process remaining jobs, yielding results.

        If retn_ids is True, then each result is returned as (job_id, result).
        If ordered is True, then results from jobs are yielded in order. Otherwise
        they are returned in any order.

        Args:
          poll (bool): Yield results which are available, then return. Otherwise, wait
            until all jobs have finished.
        """

        if poll:
            self._sendjobs()
            self.poll()
            yield from self.yield_results()
        else:
            while self.jobs or self.busysocks or self.jobresults:
                self._sendjobs()
                yield from self.yield_results()
                if self.busysocks:
                    self.poll(timeout=interval)

    def finish(self):
        """Finish processing current jobs. Exit subproceses.

        Returns:
          None
        """

        self.wait()

        # signal to finish up
        for sock in self.socks:
            _sendobj(sock, None)

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
        """Return context manager for queue."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager for queue."""
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
    with ForkQueue(numforks=2, initfunc=_testinit, retn_ids=False, ordered=True) as q:
        res = tuple(q.process(_testfunc, _testiter()))
        assert res == (42,43,44,45,46)
    with ForkQueue(numforks=3, initfunc=_testinit, retn_ids=False, ordered=False) as q:
        res = sorted(list(q.process(_testfunc, _testiter())))
        assert res == [42,43,44,45,46]
    with ForkQueue(numforks=4, initfunc=_testinit, retn_ids=True, ordered=True) as q:
        res = tuple(q.process(_testfunc, _testiter()))
        assert res == ((0,42),(1,43),(2,44),(3,45),(4,46))

    with ForkQueue(numforks=1, initfunc=_testinit) as q:
        q.add(_testfunc, (2,))
        q.add(_testfunc2, (5,), {'addon':10})
        q.add(_testfunc2, (-1,), {'addon':3})
        q.wait()
        res = tuple(q.yield_results())
        assert res == (44, '81', '68')

    # test using a nested function and env
    bigdata = [1,2,3,4,5,6]
    def nestfunc(a):
        return a+sum(bigdata)
    with ForkQueue(numforks=4, env=locals()) as q:
        for i, res in enumerate(q.process(nestfunc, ((j+1,) for j in range(128)))):
            assert res == 21+i+1

    q = ForkQueue(numforks=1, initfunc=_testinit)
    q.finish()

    assert(_all_socks=={} and _all_pids=={})

    print('Tests succeeded')

if __name__ == '__main__':
    test()
