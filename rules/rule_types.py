# Split out from rules/evaluator.py so workers/rule_index.py can depend on this
# without a circular import (evaluator needs RuleIndex's type; rule_index needs this
# constant). rules/evaluator.py re-exports IMPLEMENTED_RULE_TYPES for existing callers.
IMPLEMENTED_RULE_TYPES = ("absolute_below", "absolute_above", "zscore_move", "percent_change", "spread_widen")
