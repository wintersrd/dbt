import collections
import copy
import datetime
import decimal
import functools
import hashlib
import itertools
import json
import os
from enum import Enum
from typing import Tuple, Type, Any, Optional, TypeVar, Dict, Iterable, Set, List
from typing_extensions import Protocol

import dbt.exceptions

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_types import NodeType
from dbt.clients import yaml_helper

DECIMALS: Tuple[Type[Any], ...]
try:
    import cdecimal  # typing: ignore
except ImportError:
    DECIMALS = (decimal.Decimal,)
else:
    DECIMALS = (decimal.Decimal, cdecimal.Decimal)


class ExitCodes(int, Enum):
    Success = 0
    ModelError = 1
    UnhandledError = 2


def to_bytes(s):
    return s.encode("latin-1")


def coalesce(*args):
    for arg in args:
        if arg is not None:
            return arg
    return None


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i : i + n]


def get_profile_from_project(project):
    target_name = project.get("target", {})
    profile = project.get("outputs", {}).get(target_name, {})
    return profile


def get_model_name_or_none(model):
    if model is None:
        name = "<None>"

    elif isinstance(model, str):
        name = model
    elif isinstance(model, dict):
        name = model.get("alias", model.get("name"))
    elif hasattr(model, "alias"):
        name = model.alias
    elif hasattr(model, "name"):
        name = model.name
    else:
        name = str(model)
    return name


def compiler_warning(model, msg, resource_type="model"):
    name = get_model_name_or_none(model)
    logger.info(
        "* Compilation warning while compiling {} {}:\n* {}\n".format(resource_type, name, msg)
    )


def id_matches(unique_id, target_name, target_package, nodetypes, model):
    """Return True if the unique ID matches the given name, package, and type.

    If package is None, any package is allowed.
    nodetypes should be a container of NodeTypes that implements the 'in'
    operator.
    """
    node_type = model.resource_type
    node_parts = unique_id.split(".", 2)
    if len(node_parts) != 3:
        msg = "unique_id {} is malformed".format(unique_id)
        dbt.exceptions.raise_compiler_error(msg, model)

    resource_type, package_name, node_name = node_parts
    if resource_type not in nodetypes:
        return False

    if node_type == NodeType.Source.value:
        if node_name.count(".") != 1:
            msg = "{} names must contain exactly 1 '.' character".format(node_type)
            dbt.exceptions.raise_compiler_error(msg, model)
    else:
        if "." in node_name:
            msg = "{} names cannot contain '.' characters".format(node_type)
            dbt.exceptions.raise_compiler_error(msg, model)

    if target_name != node_name:
        return False

    return target_package is None or target_package == package_name


def find_in_subgraph_by_name(subgraph, target_name, target_package, nodetype):
    """Find an entry in a subgraph by name. Any mapping that implements
    .items() and maps unique id -> something can be used as the subgraph.

    Names are like:
        '{nodetype}.{target_package}.{target_name}'

    You can use `None` for the package name as a wildcard.
    """
    for name, model in subgraph.items():
        if id_matches(name, target_name, target_package, nodetype, model):
            return model

    return None


def find_in_list_by_name(haystack, target_name, target_package, nodetype):
    """Find an entry in the given list by name."""
    for model in haystack:
        name = model.unique_id
        if id_matches(name, target_name, target_package, nodetype, model):
            return model

    return None


MACRO_PREFIX = "dbt_macro__"
DOCS_PREFIX = "dbt_docs__"


def get_dbt_macro_name(name):
    if name is None:
        raise dbt.exceptions.InternalException("Got None for a macro name!")
    return "{}{}".format(MACRO_PREFIX, name)


def get_dbt_docs_name(name):
    if name is None:
        raise dbt.exceptions.InternalException("Got None for a doc name!")
    return "{}{}".format(DOCS_PREFIX, name)


def get_materialization_macro_name(materialization_name, adapter_type=None, with_prefix=True):
    if adapter_type is None:
        adapter_type = "default"

    name = "materialization_{}_{}".format(materialization_name, adapter_type)

    if with_prefix:
        return get_dbt_macro_name(name)
    else:
        return name


def get_docs_macro_name(docs_name, with_prefix=True):
    if with_prefix:
        return get_dbt_docs_name(docs_name)
    else:
        return docs_name


def split_path(path):
    return path.split(os.sep)


def merge(*args):
    if len(args) == 0:
        return None

    if len(args) == 1:
        return args[0]

    lst = list(args)
    last = lst.pop(len(lst) - 1)

    return _merge(merge(*lst), last)


def _merge(a, b):
    to_return = a.copy()
    to_return.update(b)
    return to_return


# http://stackoverflow.com/questions/20656135/python-deep-merge-dictionary-data
def deep_merge(*args):
    """
    >>> dbt.utils.deep_merge({'a': 1, 'b': 2, 'c': 3}, {'a': 2}, {'a': 3, 'b': 1})  # noqa
    {'a': 3, 'b': 1, 'c': 3}
    """
    if len(args) == 0:
        return None

    if len(args) == 1:
        return copy.deepcopy(args[0])

    lst = list(args)
    last = copy.deepcopy(lst.pop(len(lst) - 1))

    return _deep_merge(deep_merge(*lst), last)


def _deep_merge(destination, source):
    if isinstance(source, dict):
        for key, value in source.items():
            deep_merge_item(destination, key, value)
        return destination


def deep_merge_item(destination, key, value):
    if isinstance(value, dict):
        node = destination.setdefault(key, {})
        destination[key] = deep_merge(node, value)
    elif isinstance(value, tuple) or isinstance(value, list):
        if key in destination:
            destination[key] = list(value) + list(destination[key])
        else:
            destination[key] = value
    else:
        destination[key] = value


def _deep_map(func, value, keypath):
    atomic_types = (int, float, str, type(None), bool)

    if isinstance(value, list):
        ret = [_deep_map(func, v, (keypath + (idx,))) for idx, v in enumerate(value)]
    elif isinstance(value, dict):
        ret = {k: _deep_map(func, v, (keypath + (k,))) for k, v in value.items()}
    elif isinstance(value, atomic_types):
        ret = func(value, keypath)
    else:
        ok_types = (list, dict) + atomic_types
        raise dbt.exceptions.DbtConfigError(
            "in _deep_map, expected one of {!r}, got {!r}".format(ok_types, type(value))
        )

    return ret


def deep_map(func, value):
    """map the function func() onto each non-container value in 'value'
    recursively, returning a new value. As long as func does not manipulate
    value, then deep_map will also not manipulate it.

    value should be a value returned by `yaml.safe_load` or `json.load` - the
    only expected types are list, dict, native python number, str, NoneType,
    and bool.

    func() will be called on numbers, strings, Nones, and booleans. Its first
    parameter will be the value, and the second will be its keypath, an
    iterable over the __getitem__ keys needed to get to it.

    :raises: If there are cycles in the value, raises a
        dbt.exceptions.RecursionException
    """
    try:
        return _deep_map(func, value, ())
    except RuntimeError as exc:
        if "maximum recursion depth exceeded" in str(exc):
            raise dbt.exceptions.RecursionException("Cycle detected in deep_map")
        raise


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self


def get_materialization(node):
    return node.config.materialized


def is_enabled(node):
    return node.config.enabled


def get_pseudo_test_path(node_name, source_path, test_type):
    "schema tests all come from schema.yml files. fake a source sql file"
    source_path_parts = split_path(source_path)
    source_path_parts.pop()  # ignore filename
    suffix = [test_type, "{}.sql".format(node_name)]
    pseudo_path_parts = source_path_parts + suffix
    return os.path.join(*pseudo_path_parts)


def get_pseudo_hook_path(hook_name):
    path_parts = ["hooks", "{}.sql".format(hook_name)]
    return os.path.join(*path_parts)


class _Tagged(Protocol):
    tags: Iterable[str]


Tagged = TypeVar("Tagged", bound=_Tagged)


def get_nodes_by_tags(
    nodes: Iterable[Tagged], match_tags: Set[str], resource_type: NodeType
) -> List[Tagged]:
    matched_nodes = []
    for node in nodes:
        node_tags = node.tags
        if len(set(node_tags) & match_tags):
            matched_nodes.append(node)
    return matched_nodes


def md5(string):
    return hashlib.md5(string.encode("utf-8")).hexdigest()


def get_hash(model):
    return hashlib.md5(model.unique_id.encode("utf-8")).hexdigest()


def get_hashed_contents(model):
    return hashlib.md5(model.raw_sql.encode("utf-8")).hexdigest()


def flatten_nodes(dep_list):
    return list(itertools.chain.from_iterable(dep_list))


class memoized:
    """Decorator. Caches a function's return value each time it is called. If
    called later with the same arguments, the cached value is returned (not
    reevaluated).

    Taken from https://wiki.python.org/moin/PythonDecoratorLibrary#Memoize"""

    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __call__(self, *args):
        if not isinstance(args, collections.Hashable):
            # uncacheable. a list, for instance.
            # better to not cache than blow up.
            return self.func(*args)
        if args in self.cache:
            return self.cache[args]
        value = self.func(*args)
        self.cache[args] = value
        return value

    def __repr__(self):
        """Return the function's docstring."""
        return self.func.__doc__

    def __get__(self, obj, objtype):
        """Support instance methods."""
        return functools.partial(self.__call__, obj)


def invalid_ref_test_message(node, target_model_name, target_model_package, disabled):
    if disabled:
        msg = dbt.exceptions.get_target_disabled_msg(node, target_model_name, target_model_package)
    else:
        msg = dbt.exceptions.get_target_not_found_msg(node, target_model_name, target_model_package)
    return "WARNING: {}".format(msg)


def invalid_ref_fail_unless_test(node, target_model_name, target_model_package, disabled):
    if node.resource_type == NodeType.Test:
        msg = invalid_ref_test_message(node, target_model_name, target_model_package, disabled)
        if disabled:
            logger.debug(msg)
        else:
            dbt.exceptions.warn_or_error(msg)

    else:
        dbt.exceptions.ref_target_not_found(node, target_model_name, target_model_package)


def invalid_source_fail_unless_test(node, target_name, target_table_name):
    if node.resource_type == NodeType.Test:
        msg = dbt.exceptions.source_disabled_message(node, target_name, target_table_name)
        dbt.exceptions.warn_or_error(msg, log_fmt="WARNING: {}")
    else:
        dbt.exceptions.source_target_not_found(node, target_name, target_table_name)


def parse_cli_vars(var_string):
    try:
        cli_vars = yaml_helper.load_yaml_text(var_string)
        var_type = type(cli_vars)
        if var_type == dict:
            return cli_vars
        else:
            type_name = var_type.__name__
            dbt.exceptions.raise_compiler_error(
                "The --vars argument must be a YAML dictionary, but was "
                "of type '{}'".format(type_name)
            )
    except dbt.exceptions.ValidationException:
        logger.error("The YAML provided in the --vars argument is not valid.\n")
        raise


K_T = TypeVar("K_T")
V_T = TypeVar("V_T")


def filter_null_values(input: Dict[K_T, Optional[V_T]]) -> Dict[K_T, V_T]:
    return {k: v for k, v in input.items() if v is not None}


def add_ephemeral_model_prefix(s: str) -> str:
    return "__dbt__CTE__{}".format(s)


def timestring() -> str:
    """Get the current datetime as an RFC 3339-compliant string"""
    # isoformat doesn't include the mandatory trailing 'Z' for UTC.
    return datetime.datetime.utcnow().isoformat() + "Z"


class JSONEncoder(json.JSONEncoder):
    """A 'custom' json encoder that does normal json encoder things, but also
    handles `Decimal`s. Naturally, this can lose precision because they get
    converted to floats.
    """

    def default(self, obj):
        if isinstance(obj, DECIMALS):
            return float(obj)
        if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return obj.isoformat()
        if hasattr(obj, "to_dict"):
            # if we have a to_dict we should try to serialize the result of
            # that!
            obj = obj.to_dict()
        return super().default(obj)


class ForgivingJSONEncoder(JSONEncoder):
    def default(self, obj):
        # let dbt's default JSON encoder handle it if possible, fallback to
        # str()
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)


def translate_aliases(kwargs, aliases):
    """Given a dict of keyword arguments and a dict mapping aliases to their
    canonical values, canonicalize the keys in the kwargs dict.

    :return: A dict continaing all the values in kwargs referenced by their
        canonical key.
    :raises: `AliasException`, if a canonical key is defined more than once.
    """
    result = {}

    for given_key, value in kwargs.items():
        canonical_key = aliases.get(given_key, given_key)
        if canonical_key in result:
            # dupe found: go through the dict so we can have a nice-ish error
            key_names = ", ".join("{}".format(k) for k in kwargs if aliases.get(k) == canonical_key)

            raise dbt.exceptions.AliasException(
                'Got duplicate keys: ({}) all map to "{}"'.format(key_names, canonical_key)
            )

        result[canonical_key] = value

    return result


def pluralize(count, string):
    if count == 1:
        return "{} {}".format(count, string)
    elif string == "analysis":
        return "{} {}".format(count, "analyses")
    else:
        return "{} {}s".format(count, string)


def restrict_to(*restrictions):
    """Create the metadata for a restricted dataclass field"""
    return {"restrict": list(restrictions)}


# some types need to make constants available to the jinja context as
# attributes, and regular properties only work with objects. maybe this should
# be handled by the RelationProxy?


class classproperty(object):
    def __init__(self, func):
        self.func = func

    def __get__(self, obj, objtype):
        return self.func(objtype)
