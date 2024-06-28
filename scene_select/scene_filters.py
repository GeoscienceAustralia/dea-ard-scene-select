"""
This is derived from ODC's expressions, but those frustratingly are too limited to work
for our searches now — for example, we want to accept ranges of strings for software
version numbers.

The actual syntax is the same, we just support ranges and comparisons that are non-numeric.

Four types of expressions are available:

    FIELD = VALUE
    FIELD in DATE-RANGE
    FIELD in [START, END]
    TIME > DATE
    TIME < DATE

Where DATE or DATE-RANGE is one of YYYY, YYYY-MM or YYYY-MM-DD
and START, END are either numbers or dates.
"""
import datetime
import warnings
from typing import Any, NamedTuple

import pandas
from eodatasets3.utils import default_utc
from pandas import to_datetime as pandas_to_datetime
from lark import Lark, v_args, Transformer

from datacube.model import Range

search_grammar = r"""
    start: expression*
    ?expression: equals_expr
               | time_in_expr
               | field_in_expr
               | time_gt_expr
               | time_lt_expr
               | field_gt_expr
               | field_lt_expr

    equals_expr: field "=" value
    time_in_expr: time "in" date_range
    field_in_expr: field "in" "[" value "," value "]"
    time_gt_expr: time ">" date_gt
    time_lt_expr: time "<" date_lt
    field_gt_expr: field ">" value
    field_lt_expr: field "<" value

    field: FIELD
    time: TIME

    ?value: INT -> integer
          | SIGNED_NUMBER -> number
          | ESCAPED_STRING -> string
          | SIMPLE_STRING -> simple_string
          | URL_STRING -> url_string
          | UUID -> simple_string

    ?date_range: date -> single_date
               | "[" date "," date "]" -> date_pair

    date_gt: date -> range_lower_bound

    date_lt: date -> range_upper_bound

    date: YEAR ["-" MONTH ["-" DAY ]]

    TIME: "time"
    FIELD: /[a-zA-Z][\w\d_]*/
    YEAR: DIGIT ~ 4
    MONTH: DIGIT ~ 1..2
    DAY: DIGIT ~ 1..2
    SIMPLE_STRING: /[a-zA-Z][\w._-]*/ | /[0-9]+[\w_-][\w._-]*/
    URL_STRING: /[a-z0-9+.-]+:\/\/([:\/\w._-])*/
    UUID: HEXDIGIT~8 "-" HEXDIGIT~4 "-" HEXDIGIT~4 "-" HEXDIGIT~4 "-" HEXDIGIT~12


    %import common.ESCAPED_STRING
    %import common.SIGNED_NUMBER
    %import common.INT
    %import common.DIGIT
    %import common.HEXDIGIT
    %import common.CNAME
    %import common.WS
    %ignore WS
"""


def identity(x):
    return x

class GreaterThan(NamedTuple):
    value: Any
class LessThan(NamedTuple):
    value: Any

@v_args(inline=True)
class TreeToSearchExprs(Transformer):
    # Convert the expressions
    def equals_expr(self, field, value):
        return {str(field): value}

    def field_in_expr(self, field, lower, upper):
        return {str(field): Range(lower, upper)}

    def time_in_expr(self, time_field, date_range):
        return {str(time_field): date_range}

    def time_gt_expr(self, time_field, date_gt):
        return {str(time_field): date_gt}

    def time_lt_expr(self, time_field, date_lt):

        return {str(time_field): date_lt}

    def field_gt_expr(self, field, value):
        if not field.endswith('_version'):
            # Underlying ODC doesn't support this operator for fields.
            # You have to use "in" expressions for numbers.
            raise ValueError(f"Can only use > with software version "
                             f"fields (or dates) for now, sorry. Tried {field}")
        return {str(field): GreaterThan(value)}

    def field_lt_expr(self, field, value):
        if not field.endswith('_version'):
            # Underlying ODC doesn't support this operator for fields.
            # You have to use "in" expressions for numbers.
            raise ValueError(f"Can only use < with software version "
                             f"fields (or dates) for now, sorry. Tried {field}")
        return {str(field): LessThan(value)}

    # Convert the literals
    def string(self, val):
        return str(val[1:-1])

    simple_string = url_string = field = time = str
    number = float
    integer = int
    value = identity

    def single_date(self, date):
        return _time_to_search_dims(date)

    def date_pair(self, start, end):
        return _time_to_search_dims((start, end))

    def range_lower_bound(self, date):
        return _time_to_search_dims((date, None))

    def range_upper_bound(self, date):
        return _time_to_search_dims((None, date))

    def date(self, y, m=None, d=None):
        return "-".join(x for x in [y, m, d] if x is not None)

    # Merge everything into a single dict
    def start(self, *search_exprs):
        combined = {}
        for expr in search_exprs:
            combined.update(expr)
        return combined


def parse_expressions(*expression_text):
    expr_parser = Lark(search_grammar)
    tree = expr_parser.parse(' '.join(expression_text))
    return TreeToSearchExprs().transform(tree)


def test_parser():
    assert parse_expressions('platform = "LANDSAT_8"') == {'platform': 'LANDSAT_8'}

    # Wagl version tests
    assert parse_expressions('wagl_version in ["1.2.3", "3.4.5"]') == {'wagl_version': Range('1.2.3', '3.4.5')}
    assert parse_expressions('wagl_version < "1.2.3.dev4"') == {'wagl_version': Range(None, '1.2.3.dev4')}


def _time_to_search_dims(time_range):

    tr_start, tr_end = time_range, time_range

    if hasattr(time_range, '__iter__') and not isinstance(time_range, str):
        tmp = list(time_range)
        if len(tmp) > 2:
            raise ValueError("Please supply start and end date only for time query")

        tr_start, tr_end = tmp[0], tmp[-1]

    if isinstance(tr_start, (int, float)) or isinstance(tr_end, (int, float)):
        raise TypeError("Time dimension must be provided as a datetime or a string")

    if tr_start is None:
        start = datetime.datetime.fromtimestamp(0)
    elif not isinstance(tr_start, datetime.datetime):
        # convert to datetime.datetime
        if hasattr(tr_start, 'isoformat'):
            tr_start = tr_start.isoformat()
        start = pandas_to_datetime(tr_start).to_pydatetime()
    else:
        start = tr_start

    if tr_end is None:
        tr_end = datetime.datetime.now().strftime("%Y-%m-%d")
    # Attempt conversion to isoformat
    # allows pandas.Period to handle datetime objects
    if hasattr(tr_end, 'isoformat'):
        tr_end = tr_end.isoformat()
    # get end of period to ensure range is inclusive

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        end = pandas.Period(tr_end).end_time.to_pydatetime()

    tr = Range(default_utc(start), default_utc(end))
    if start == end:
        return tr[0]

    return tr
