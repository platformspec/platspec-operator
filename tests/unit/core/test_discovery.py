"""Tests for core/discovery.py — _to_python() deep conversion."""

from platspec_operator.core.discovery import _to_python


# ---------------------------------------------------------------------------
# _to_python — plain Python passthrough
# ---------------------------------------------------------------------------


def test_to_python_plain_dict_unchanged():
    d = {"a": 1, "b": "hello"}
    assert _to_python(d) == d


def test_to_python_plain_list_unchanged():
    lst = [1, "two", 3]
    assert _to_python(lst) == lst


def test_to_python_scalar_unchanged():
    assert _to_python(42) == 42
    assert _to_python("string") == "string"
    assert _to_python(None) is None


def test_to_python_nested_dict():
    nested = {"outer": {"inner": {"leaf": "value"}}}
    result = _to_python(nested)
    assert result == nested
    assert isinstance(result["outer"], dict)


def test_to_python_list_of_dicts():
    lst = [{"a": 1}, {"b": 2}]
    result = _to_python(lst)
    assert result == lst


def test_to_python_dict_with_list_values():
    d = {"items": [{"k": "v"}, {"k2": "v2"}]}
    result = _to_python(d)
    assert result == d
    assert isinstance(result["items"], list)


# ---------------------------------------------------------------------------
# _to_python — dict-like object conversion (simulated ResourceField)
# ---------------------------------------------------------------------------


class _DictLike:
    """Mimics kopf ResourceField: not a dict, but has .items()."""

    def __init__(self, data: dict):
        self._data = data

    def items(self):
        return self._data.items()

    def __iter__(self):
        return iter(self._data)


def test_to_python_converts_dict_like_top_level():
    obj = _DictLike({"key": "value"})
    result = _to_python(obj)
    assert isinstance(result, dict)
    assert result == {"key": "value"}


def test_to_python_converts_nested_dict_like():
    inner = _DictLike({"inner_key": "inner_val"})
    outer = _DictLike({"outer_key": inner})
    result = _to_python(outer)
    assert isinstance(result, dict)
    assert isinstance(result["outer_key"], dict)
    assert result["outer_key"]["inner_key"] == "inner_val"


def test_to_python_converts_dict_like_inside_plain_dict():
    resource_field = _DictLike({"cidr": "10.0.0.0/16"})
    spec = {"config": resource_field, "name": "vpc"}
    result = _to_python(spec)
    assert isinstance(result["config"], dict)
    assert result["config"]["cidr"] == "10.0.0.0/16"
    assert result["name"] == "vpc"


def test_to_python_converts_dict_like_inside_list():
    items = [_DictLike({"k": "v1"}), _DictLike({"k": "v2"})]
    result = _to_python(items)
    assert all(isinstance(r, dict) for r in result)
    assert result == [{"k": "v1"}, {"k": "v2"}]


def test_to_python_deeply_nested_dict_like():
    deep = _DictLike({"level3": "leaf"})
    mid = _DictLike({"level2": deep})
    top = {"level1": mid}
    result = _to_python(top)
    assert result["level1"]["level2"]["level3"] == "leaf"
    assert isinstance(result["level1"], dict)
    assert isinstance(result["level1"]["level2"], dict)


def test_to_python_list_with_mixed_types():
    items = [_DictLike({"a": 1}), "plain", 42, {"plain_dict": True}]
    result = _to_python(items)
    assert result[0] == {"a": 1}
    assert result[1] == "plain"
    assert result[2] == 42
    assert result[3] == {"plain_dict": True}
