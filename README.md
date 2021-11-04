# forkqueue

forkqueue is a Python 3 module to support running tasks in separate forked proceses. It differs from other similar modules, like multiprocessing, as it relies on the presence of a `fork()` operating system call. This allows it to run tasks, for example, calling closures or nested functions. The separate processes can easily share input data as they inherit the same environment from when the queue is constructed, saving memory usage. There is very little overhead in creating a new pool of forked processes. Because the module uses separate processes, rather than threads, it is not affected by the Python global-interpreter-lock (GIL).

As the module relies on a working `fork()` system call, it is only designed to work on Unix-like operating systems, such as Linux, Mac OS or WSL under Windows.

## Examples
This is a minimal example which prints 1, 2, 3, 4.
```python
from forkqueue import ForkQueue

def myfunc(x):
    return x+1

with ForkQueue() as queue:
     args = [(0,), (1,), (2,), (3,)]
     for result in queue.process(myfunc, args):
         print(result)
```

This example prints out the sequence of values 1+(1+1)+(1+2+3), 2+(2+1)+(1+2+3), ... . It demonstrates calling the nested function `myfunc` in four different processes with a set of input parameters (here `args`). 

```python
from forkqueue import ForkQueue

def main()
    c = [1,2,3]
    def myfunc(a, b):
        return a+b+sum(c)

    with ForkQueue(numforks=4, env=locals()) as queue:
        args = ((a, a+1) for a in range(100))
        for result in queue.process(myfunc, args):
            print(result)

if __name__ == '__main__':
    main()
```

Here is an example which uses the add interface to add jobs, printing 0, 1, 4.

```python
from forkqueue import ForkQueue

def myfunc1(x):
    return x**2

def myfunc2(x):
    return x

with ForkQueue() as queue:
    queue.add(myfunc1, (0,))
    queue.add(myfunc2, (1,))
    queue.add(myfunc1, (2,))

    for result in queue.results():
        print(result)
```

Using `ordered=False` the results can be returned in any order. This makes returning results quicker and uses less memory for storing results until ready. The `retn_ids=True` option also returns the job ID, allowing results to be identified. Job IDs can either be given, or are automatically created from an integer incrementing from 0. This example returns results in any order, populating an output array, and writing `[1, 4, 9, 16]`.

```python
from forkqueue import ForkQueue

def myfunc(x,y):
    return x*y

out = [None]*4
with ForkQueue(ordered=False, retn_ids=True) as queue:
    args = ((i+1,i+1) for i in range(4))
    for jobid, result in queue.process(myfunc, args):
        out[jobid] = result
print(out)
```

## API

The interface to the module is through a class called ForkQueue. This is usually used as a context manager (with statement) so that the forked processes are properly ended. Alternatively, the class can also be constructed, used, then `finish()` can be used to clean up, although the context manager is recommended.


```python
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

    def poll(self, timeout=0):
        """Poll the forked processes for results.

        Args:
          timeout (float/None): Wait for up to timeout seconds until there is a result.
            0 means do not wait at all. None will wait forever.

        Returns:
          bool: Whether a result is available.
        """

    def wait(self):
        """Wait until all jobs are processed.

        Returns:
          None
        """

    def yield_results(self):
        """Yield any results which are currently available.

        If retn_ids is True, then each result is returned as (job_id, result).
        If ordered is True, then results from jobs are yielded in order. Otherwise
        they are returned in any order.
	"""

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

    def map(self, func, *args):
        """A map like function for the queue.

        This makes ForkQueue suitable as a replacement for a multiprocessing.Pool() object.
        """

    def results(self, poll=False, interval=None):
        """Process remaining jobs, yielding results.

        If retn_ids is True, then each result is returned as (job_id, result).
        If ordered is True, then results from jobs are yielded in order. Otherwise
        they are returned in any order.

        Args:
          poll (bool): Yield results which are available, then return. Otherwise, wait
            until all jobs have finished.
        """

    def finish(self):
        """Finish processing current jobs. Exit subproceses.

        Returns:
          None
        """

    def __enter__(self):
        """Return context manager for queue."""

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager for queue."""
```

## How it works

When a ForkQueue is created, the `fork()` call is used to create `numforks` processes. These processes listen on a Unix domain socket for input. Tasks are added onto an input queue. If any of the processes are available for running tasks, then the function to run and its arguments are sent over the socket after being pickled. The remote process runs this task and returns the result back over the socket. The ForkQueue monitors the remote processes  using the `select` system call on the list of sockets. The queue keeps tracks which processes are free and which are busy. When the queue is finished, the remote processes are sent a special command causing them to exit.

If the function called by the task is passed in the environment `env` passed to the `__init__` function, then the name of the task is sent to the remote process is sent to the remote process, rather than the pickled function. This allows nested or otherwise unpicklable callables to be used in the tasks.

## Caveats

* As mentioned above, this module will not work on native Windows Python installations. It should work on WSL, however.

* There are problems when mixing threads and forked processes.

* There may be some problems if your function uses existing shared resources, for example open files. By default Python does not allow file descriptors to be inherited. See https://www.python.org/dev/peps/pep-0446/ . You may wish to use `os.set_inheritable` to avoid this problem.
