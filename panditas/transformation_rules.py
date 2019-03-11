from .models import DataFlow, TransformationRule

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
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass

    def _validate_expression(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass

    def run(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
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
        """Short summary.

        Parameters
        ----------
        fill_column : type
            Description of parameter `fill_column`.
        fill_value : type
            Description of parameter `fill_value`.
        name : type
            Description of parameter `name`.
        where_columns : type
            Description of parameter `where_columns`.
        where_condition : type
            Description of parameter `where_condition`.
        where_condition_value : type
            Description of parameter `where_condition_value`.
         : type
            Description of parameter ``.

        Returns
        -------
        type
            Description of returned object.

        """
        self.fill_column = fill_column
        self.fill_value = fill_value
        self.name = name
        self.where_columns = where_columns
        self.where_condition = where_condition
        self.where_condition_value = where_condition_value

    def run(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass


class ConstantColumn(TransformationRule):
    column_name = None
    column_value = None

    def __init__(self, column_name=None, column_value=None, name=None):
        """Short summary.

        Parameters
        ----------
        column_name : type
            Description of parameter `column_name`.
        column_value : type
            Description of parameter `column_value`.
        name : type
            Description of parameter `name`.

        Returns
        -------
        type
            Description of returned object.

        """
        self.column_name = column_name
        self.column_value = column_value
        self.name = name

    def run(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass


class FilterBy(TransformationRule):
    column_name = None
    filter_conditions = None

    def __init__(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass

    def run(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass


class FormatColumns(TransformationRule):
    column_formats = None

    def __init__(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass

    def run(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass


class MapValues(TransformationRule):
    default_map_value = None
    map_column = None
    map_values = None

    def __init__(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass

    def run(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass


class PivotTable(TransformationRule):
    data_set = None
    group_columns = None
    group_functions = None
    group_values = None
    preserve_order = True

    def __init__(
        self,
        data_set=None,
        group_columns=None,
        group_functions=None,
        group_values=None,
        name=None,
        preserve_order=True,
    ):
        """Short summary.

        Parameters
        ----------
        data_set : type
            Description of parameter `data_set`.
        group_columns : type
            Description of parameter `group_columns`.
        group_functions : type
            Description of parameter `group_functions`.
        group_values : type
            Description of parameter `group_values`.
        name : type
            Description of parameter `name`.
        preserve_order : type
            Description of parameter `preserve_order`.

        Returns
        -------
        type
            Description of returned object.

        """
        self.data_set = data_set
        self.group_columns = group_columns
        self.group_functions = group_functions
        self.group_values = group_values
        self.name = name
        self.preserve_order = preserve_order

    def run(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        df = DataFlow.get_output_df(self.data_set)
        values = {}
        # First add the cols provided for the pivot
        for key, column in enumerate(self.group_values):
            group_function = self.group_functions[key]
            if group_function == "unique":
                group_function = lambda x: ', '.join(set(str(v) for v in x if v))
            values[column] = group_function
        # Add not specified ones with default (pandas uses mean)
        missing_columns = list(set(df.columns.tolist()) - set(self.group_values))
        for column in missing_columns:
            values[column] = "mean"
        pivot_df = df.pivot_table(
            index=self.group_columns,
            values=self.group_values,
            aggfunc=values
        ).reset_index()
        self.output_data_set = DataFlow.save_output_df(pivot_df, self.name)


class RemoveColumns(TransformationRule):
    column_names = None

    def __init__(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass

    def run(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass


class RemoveDuplicateRows(TransformationRule):
    columns_subset = None

    def __init__(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass

    def run(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass


class RenameColumns(TransformationRule):
    columns = None

    def __init__(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass

    def run(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass


class ReplaceText(TransformationRule):
    column = None
    replace_pattern = None
    replace_pattern_is_regex = False
    replace_value = None
    replace_column = None

    def __init__(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass

    def run(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass


class SelectColumns(TransformationRule):
    columns = None

    def __init__(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass

    def run(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass


class SortValuesBy(TransformationRule):
    sort_columns = None
    sort_ascending = None

    def __init__(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass

    def run(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        pass
