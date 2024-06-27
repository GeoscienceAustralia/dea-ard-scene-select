from lark import Lark, Transformer
import attr
from enum import Enum
from packaging import version
import pytest

from scene_select.collections import ArdDataset


class Operator(Enum):
    EQ = "="
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="


@attr.define
class Filter:
    key: str
    operator: Operator
    value: str

    def get_value(self, dataset: ArdDataset):
        raise NotImplementedError

    def matches(self, test_value: str) -> bool:
        return {
            Operator.EQ: test_value == self.value,
            Operator.LT: test_value < self.value,
            Operator.LE: test_value <= self.value,
            Operator.GT: test_value > self.value,
            Operator.GE: test_value >= self.value,
        }[self.operator]


@attr.define
class SoftwareVersionFilter(Filter):
    def get_value(self, dataset: ArdDataset):
        return dataset.proc_info_doc["software_versions"][self.key]

    def matches(self, test_value: str) -> bool:
        v1 = version.parse(self.value)
        v2 = version.parse(test_value)
        return {
            Operator.EQ: v1 == v2,
            Operator.LT: v2 < v1,
            Operator.LE: v2 <= v1,
            Operator.GT: v2 > v1,
            Operator.GE: v2 >= v1,
        }[self.operator]


# Define the grammar
grammar = """
    start: filter+
    filter: key "=" value
    key: /[a-zA-Z0-9_]+/
    value: comparison
    comparison: OPERATOR? simple_value
    simple_value: UNQUOTED_STRING | QUOTED_STRING
    OPERATOR: "<" | "<=" | ">" | ">="
    UNQUOTED_STRING: /[a-zA-Z0-9_.]+/
    QUOTED_STRING: /"[^"]*"/

    %import common.WS
    %ignore WS
"""

# Create the parser
parser = Lark(grammar, parser="lalr", transformer=None)


class FilterTransformer(Transformer):
    def start(self, filters):
        return filters

    def filter(self, items):
        software_names = [
            "modtran",
            "wagl",
            "eugl",
            "gverify",
            "fmask",
            "tesp",
            "eodatasets3",
        ]

        key = str(items[0])
        operator = Operator(
            items[2].children[0] if items[2].children[0] != "simple_value" else "="
        )
        value = str(items[2].children[-1])

        if items[0] in software_names:
            return SoftwareVersionFilter(key=key, operator=operator, value=value)
        # TODO: Non-software filters.
        else:
            raise NotImplementedError(
                f"Filter key {key} not supported. Expecting one of {software_names}"
            )

    def key(self, k):
        return k[0]

    def value(self, v):
        return v[0]

    def comparison(self, c):
        return c

    def simple_value(self, v):
        return v[0]

    def UNQUOTED_STRING(self, s):
        return str(s)

    def QUOTED_STRING(self, s):
        return s[1:-1]  # Remove quotes


# Parse and transform the input
def parse_filters(input_string):
    tree = parser.parse(input_string)
    transformer = FilterTransformer()
    return transformer.transform(tree)


# Pytest functions
def test_parse_filters():
    input_string = "wagl<1.2.3 fmask<=1.2.3"
    result = parse_filters(input_string)

    assert len(result) == 2
    assert result[1] == Filter(key="wagl", operator=Operator.LT, value="1.2.3")
    assert result[2] == Filter(key="fmask", operator=Operator.LE, value="1.2.3")


def test_filter_matches():
    f1 = Filter(key="collection", operator=Operator.EQ, value="ls7")
    f2 = Filter(key="ard", operator=Operator.LT, value="1.2.3")
    f3 = Filter(key="fmask", operator=Operator.LE, value="1.2.3")

    assert f1.matches("ls7")
    assert not f1.matches("ls8")

    assert f2.matches("1.2.2")
    assert not f2.matches("1.2.3")
    assert not f2.matches("1.2.4")

    assert f3.matches("1.2.2")
    assert f3.matches("1.2.3")
    assert not f3.matches("1.2.4")


def test_quoted_string():
    input_string = 'name="John Doe"'
    result = parse_filters(input_string)

    assert len(result) == 1
    assert result[0] == Filter(key="name", operator=Operator.EQ, value="John Doe")
    assert result[0].matches("John Doe")
    assert not result[0].matches("Jane Doe")


if __name__ == "__main__":
    pytest.main([__file__])
