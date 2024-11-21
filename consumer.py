import os
from functools import wraps

from werkzeug.utils import find_modules, import_string


def is_package(package_path):
    init_file = os.path.join(package_path, '__init__.py')
    return os.path.isfile(init_file)


class Consumer:
    def __init__(self):
        pass

    def auto_import(self, import_name):
        "consumer.subscribe"
        if not is_package(import_name):
            return

        for name in find_modules(import_name, recursive=True, include_packages=False):
            import_string(name)


call_map = {}


def subscribe(w):
    def dec(f):
        call_map[w] = f

        @wraps(f)
        def decorated_function(*args, **kwargs):
            print(type(args))
            print(kwargs)
            _a = ('qq',)
            r = f(*_a, **kwargs)
            print(2)
            return r

        return decorated_function

    return dec


@subscribe("w")
def say_hello(msg):
    """Greet someone."""
    print(f"Hello ! %s" % msg)


if __name__ == '__main__':
    print(call_map)
