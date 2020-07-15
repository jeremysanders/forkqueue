# poolqueue

poolqueue is a Python 3 module to support running tasks in separate forked proceses. It differs from other similar modules, like multiprocessing, as it relies on the presence of a `fork()` operating system call. This allows it to share tasks, for example, calling closures or nested functions. This allows the separate processes to easily share input data as they inherit the same environment from before the queue is constructed, saving memory usage. There is very little overhead in creating a new pool of forked processes compared to separate processes. Because the code runs in separate processes, rather than threads, it is not affected by the Python global-interpreter-lock (GIL).

As the module relies on a working `fork()` system call, it is only designed to work on Unix-like operating systems, such as Linux, Mac OS or WSL under Windows.

poolqueue

## Examples

This example prints out the sequence of values 1+(1+1)+(1+2+3), 2+(2+1)+(1+2+3), ... . It demonstrates calling the nested function `myfunc` in four different processes with a set of input parameters (here `args`). 

```python
from poolqueue import PoolQueue

def main()
  c = [1,2,3]
  def myfunc(a, b):
    return a+b+sum(c)

  with PoolQueue(numforks=4, env=locals()) as queue:
    args = ((a, a+1) for a in range(100))
    for result in queue.process(myfunc, args):
      print(result)

if __name__ == '__main__':
  main()
```

TODO: More examples and documentation
