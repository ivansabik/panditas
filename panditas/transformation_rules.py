from .models import TransformationRule

CHECK_CONDITIONS = [
    "equals",
    "does not equal",
    "contains",
    "does not contain",
    "starts with",
    "does not start with",
    "ends with",
]
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


class CalculatedColumn(TransformationRule):
    column_name = None
    expression = None
    insert_position = -1

    def __init__(self):
        pass

    def _validate_expression(self):
        pass


class ConditionalFill(TransformationRule):
    fill_column = None
    fill_value = None
    where_columns = None
    where_condition = None
    where_condition_value = None

    def __init__(
        self,
        fill_column=None,
        fill_value=None,
        name=None,
        where_columns=None,
        where_condition=None,
        where_condition_value=None,
    ):
        self.fill_column = fill_column
        self.fill_value = fill_value
        self.name = name
        self.where_columns = where_columns
        self.where_condition = where_condition
        self.where_condition_value = where_condition_value


class ConstantColumn(TransformationRule):
    column_name = None
    column_value = None

    def __init__(self, column_name=None, column_value=None, name=None):
        self.column_name = column_name
        self.column_value = column_value
        self.name = name


class FilterBy(TransformationRule):
    column_name = None
    filter_conditions = None

    def __init__(self):
        pass


class FormatColumns(TransformationRule):
    column_formats = None

    def __init__(self):
        pass


class MapValues(TransformationRule):
    default_map_value = None
    map_column = None
    map_values = None

    def __init__(self):
        pass


class PivotTable(TransformationRule):
    group_columns = None
    group_functions = None
    group_values = None
    preserve_order = True

    def __init__(
        self,
        group_columns=None,
        group_functions=None,
        group_values=None,
        name=None,
        preserve_order=True,
    ):
        self.group_columns = group_columns
        self.group_functions = group_functions
        self.group_values = group_values
        self.name = name
        self.preserve_order = preserve_order


class RemoveColumns(TransformationRule):
    column_names = None

    def __init__(self):
        pass


class RemoveDuplicateRows(TransformationRule):
    columns_subset = None

    def __init__(self):
        pass


class RenameColumns(TransformationRule):
    columns = None

    def __init__(self):
        pass


class ReplaceText(TransformationRule):
    column = None
    replace_pattern = None
    replace_pattern_is_regex = False
    replace_value = None
    replace_column = None

    def __init__(self):
        pass


class SelectColumns(TransformationRule):
    columns = None

    def __init__(self):
        pass


class SortValuesBy(TransformationRule):
    sort_columns = None
    sort_ascending = None

    def __init__(self):
        pass
