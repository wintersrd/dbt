from contextlib import contextmanager

STRICT_MODE = False
FULL_REFRESH = False
USE_CACHE = True
WARN_ERROR = False
TEST_NEW_PARSER = False


PARSE_MODE = False


@contextmanager
def parse_context():
    global PARSE_MODE
    initial = PARSE_MODE
    PARSE_MODE = True
    yield
    PARSE_MODE = initial


def reset():
    global STRICT_MODE, FULL_REFRESH, USE_CACHE, WARN_ERROR, TEST_NEW_PARSER, \
        PARSE_MODE

    STRICT_MODE = False
    FULL_REFRESH = False
    USE_CACHE = True
    WARN_ERROR = False
    TEST_NEW_PARSER = False


def set_from_args(args):
    global STRICT_MODE, FULL_REFRESH, USE_CACHE, WARN_ERROR, TEST_NEW_PARSER, \
        PARSE_MODE
    USE_CACHE = getattr(args, 'use_cache', True)

    FULL_REFRESH = getattr(args, 'full_refresh', False)
    STRICT_MODE = getattr(args, 'strict', False)
    WARN_ERROR = (
        STRICT_MODE or
        getattr(args, 'warn_error', False)
    )

    TEST_NEW_PARSER = getattr(args, 'test_new_parser', False)
    PARSE_MODE = False
