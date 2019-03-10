from models import TransformationRule

CHECK_CONDITIONS = [
    "equals",
    "does not equal",
    "contains",
    "does not contain",
    "starts with",
    "does not start with",
    "ends with",
]


class CalculatedColumn(TransformationRule):
    column_name = None
    expression = None
    insert_position = -1

    def _validate_expression(self):
        pass


class ConditionalFill(TransformationRule):
    fill_column = None
    fill_value = None
    where_columns = None
    where_condition = None


class ConstantColumn(TransformationRule):
    column_name = None
    column_value = None


class FilterBy(TransformationRule):
    column_name = None
    filter_conditions = None


class FormatColumns(TransformationRule):
    column_formats = None


class MapValues(TransformationRule):
    default_map_value = None
    map_column = None
    map_values = None


class PivotTable(TransformationRule):
    group_columns = None
    GROUP_FUNCTIONS = [
        "alpha max",
        "alpha min",
        "concatenate",
        "count",
        "first",
        "first filled",
        "last",
        "max",
        "min",
        "sum",
        "unique",
    ]
    group_functions = None
    preserve_order = True


class RemoveColumns(TransformationRule):
    column_names = None


class RemoveDuplicateRows(TransformationRule):
    columns_subset = None


class RenameColumns(TransformationRule):
    columns = None


class ReplaceText(TransformationRule):
    column = None
    replace_pattern = None
    replace_pattern_is_regex = False
    replace_value = None
    replace_column = None


class SelectColumns(TransformationRule):
    columns = None


class SortValuesBy(TransformationRule):
    sort_columns = None
    sort_ascending = None
