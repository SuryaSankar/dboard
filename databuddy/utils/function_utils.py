import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool


def run_function_and_store_return_value(
        function=None, args=None, kwargs=None, return_queue=None):
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}

    return_value = function(*args, **kwargs)
    if return_queue:
        return_queue.put((function.__name__, return_value))


def wrap_function_in_process_with_return_queue(
        function=None, args=None, kwargs=None, return_queue=None):
    return multiprocessing.Process(
        target=run_function_and_store_return_value,
        kwargs={
            "function": function,
            "args": args,
            "kwargs": kwargs,
            "return_queue": return_queue
        })


def function_runner(function=None, args=None, kwargs=None):
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}

    return function(*args, **kwargs)


def run_in_threads(func_args_kwargs_list):
    pool = ThreadPool()
    return pool.starmap(function_runner, func_args_kwargs_list)


def run_in_processes(func_args_kwargs_list):
    pool = multiprocessing.Pool()
    return pool.starmap(function_runner, func_args_kwargs_list)
