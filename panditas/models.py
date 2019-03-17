import datetime
import logging

import pandas as pd


logger = logging.getLogger(__name__)


class DataFlow:
    custom_params = {}
    datasets = []
    name = None
    output_data_set = None
    steps = []

    def __init__(self, name=None, steps=[]):
        """Short summary.

        Parameters
        ----------
        name : type
            Description of parameter `name`.
        steps : type
            Description of parameter `steps`.

        Returns
        -------
        type
            Description of returned object.

        """
        self.name = name
        self.steps = steps
        if not self.name:
            self.name = "data_flow_{0}".format(
                datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            )
        for key, step in enumerate(self.steps):
            self.steps[key].position = key
            self._set_dependencies(step)

    def _set_dependencies(self, step):
        """Short summary.

        Parameters
        ----------
        step : type
            Description of parameter `step`.

        Returns
        -------
        type
            Description of returned object.

        """
        step_type = type(step).__name__
        # Multiple Merge have dependencies on all the data sets
        if step_type == "MergeMultipleRule":
            self.steps[step.position].depends_on = step.data_sets
        # Single Merge have dependencies on the left and right data sets
        elif step_type == "MergeRule":
            self.steps[step.position].depends_on = [
                step.left_data_set,
                step.right_data_set,
            ]
        # Data Sets do not have dependencies (unless manually specified)
        elif step_type != "DataSet":
            previous_step = self.steps[step.position - 1]
            self.steps[step.position].depends_on = [previous_step.name]
        elif step_type == "DataSet" and not self.steps[step.position].depends_on:
            self.steps[step.position].depends_on = []

    @staticmethod
    def get_output_df(step_name):
        """Short summary.

        Parameters
        ----------
        step_name : type
            Description of parameter `step_name`.

        Returns
        -------
        type
            Description of returned object.

        """
        # TODO: Check from setting and save to s3
        df = pd.read_parquet("/tmp/{0}.parquet".format(step_name), engine="pyarrow")
        return df

    def run(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        for key, step in enumerate(self.steps):
            if not step.name:
                step.name = "{0}_{1}".format(type(step).__name__, step.position)
                self.steps[key] = step
            logger.info(
                "Running step {0} name {1}".format(type(step).__name__, step.name)
            )
            step.run()
            result = step.output_data_set
            if not result:
                raise Exception(
                    "Step {0} returned an empty or no Data Set".format(step.name)
                )
            input_data_sets = step.input_data_sets + [result]
            if key + 1 < len(self.steps):
                self.steps[key + 1].input_data_sets = input_data_sets
        self.output_data_set = result

    @staticmethod
    def save_output_df(df, name):
        """Short summary.

        Parameters
        ----------
        df : type
            Description of parameter `df`.
        name : type
            Description of parameter `name`.

        Returns
        -------
        type
            Description of returned object.

        """
        # TODO: Check from setting and save to s3
        df.to_parquet("/tmp/{0}.parquet".format(name), engine="pyarrow")
        return name


class DataFlowStep:
    depends_on = []
    job_id = None
    input_data_sets = []
    name = None
    output_data_set = None
    position = None

    def run(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        # Save output to CSV or SQL
        pass


class DataSet(DataFlowStep):
    col_count = 0
    columns = []
    data_types = {}
    db_host = None
    db_pass = None
    db_port = 3306
    db_provider = "mysql"
    db_user = None
    df_path = None
    preview_data_set = pd.DataFrame()
    row_count = 0
    sheet_index = 0
    sheet_name = None
    source = "csv"
    sql_query = None
    table_name = None

    def __init__(
        self, columns=None, depends_on=[], df_path=None, name=None, source=None
    ):
        """Short summary.

        Parameters
        ----------
        columns : type
            Description of parameter `columns`.
        depends_on : type
            Description of parameter `depends_on`.
        df_path : type
            Description of parameter `df_path`.
        name : type
            Description of parameter `name`.
        source : type
            Description of parameter `source`.

        Returns
        -------
        type
            Description of returned object.

        """
        self.columns = columns
        self.depends_on = depends_on
        self.df_path = df_path
        self.name = name
        self.source = source

    def _get_columns(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        self.columns = []

    def _get_data_types(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        self.data_types = {}

    def get(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        df = pd.DataFrame()
        if self.source == "csv":
            df = pd.read_csv(self.df_path)
            if self.columns:
                df = df[self.columns]
        elif self.source == "sql":
            raise Exception("TODO: Finish")
        else:
            raise StandardError(
                "{0} is an invalid source, needs to be one of csv or sql".format(
                    self.source
                )
            )
        return DataFlow.save_output_df(df, self.name)

    def run(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        self.output_data_set = self.get()


class MergeMultipleRule(DataFlowStep):
    data_sets = []
    merge_types = []
    merge_keys = []

    def __init__(self, data_sets=[], merge_types=[], merge_keys=None, name=None):
        """Short summary.

        Parameters
        ----------
        data_sets : list
            The Data Frame names on which the merges will be performed.
        merge_types : list
            Type of merge to make, possibilities: left, right, outer and inner.
        merge_keys : list of tuples containing one list
            If provided the keys on which the merges will be made

        name : type
            Description of parameter `name`.

        Returns
        -------
        Data Frame
            resulting DF from merges.

        """
        self.data_sets = data_sets
        self.merge_types = merge_types
        self.merge_keys = merge_keys
        self.name = name
        if not merge_keys:
            merge_keys = list()
            for idx, merge in enumerate(self.merge_types):
                merge_keys.insert(idx, tuple())
                merge_keys[idx][0] = list()
                merge_keys[idx][1] = list()
        else:
            assert len(merge_keys) == len(
                merge_types
            ), "If merge keys are provided their lenght must match merge types lenght"
            for keys in merge_keys:
                assert isinstance(keys, tuple), "merge_keys must be a list of tuples"
                assert isinstance(
                    keys[0], list
                ), "each tuple must contain two lists, object at [0] is not a list"
                assert isinstance(
                    keys[1], list
                ), "each tuple must contain two lists, object at [1] is not a list"
                assert (
                    len(keys) == 2
                ), "There can only be to lists in the tuple, one for right hand, one for left hand"
                assert len(keys[0]) == len(
                    keys[1]
                ), "The number of columns or index level on which the join is made must be equal"

    def __repr__(self):
        return "Merge Multiple Rule on Data Flow: {} for data sets: {}".format(
            self.name, self.data_sets
        )

    def run(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        df = DataFlow.get_output_df(self.data_sets.pop(0))
        for idx, data_set in enumerate(self.data_sets):
            right_df = DataFlow.get_output_df(data_set)
            df = df.merge(
                right_df,
                how=self.merge_types[idx],
                left_on=self.merge_keys[idx][0],
                right_on=self.merge_keys[idx][1],
            )
        self.output_data_set = DataFlow.save_output_df(df, self.name)


class MergeRule(DataFlowStep):
    left_data_set = None
    right_data_set = None
    merge_type = "inner"
    merge_columns = None
    merge_columns_left = None
    merge_columns_right = None

    def __init__(
        self,
        left_data_set=None,
        right_data_set=None,
        merge_type="inner",
        merge_columns=None,
        merge_columns_left=None,
        merge_columns_right=None,
        name=None,
    ):
        """Short summary.

        Parameters
        ----------
        left_data_set : type
            Description of parameter `left_data_set`.
        right_data_set : type
            Description of parameter `right_data_set`.
        merge_type : type
            Description of parameter `merge_type`.
        merge_columns : type
            Description of parameter `merge_columns`.
        merge_columns_left : type
            Description of parameter `merge_columns_left`.
        merge_columns_right : type
            Description of parameter `merge_columns_right`.
        name : type
            Description of parameter `name`.

        Returns
        -------
        type
            Description of returned object.

        """
        self.left_data_set = left_data_set
        self.right_data_set = right_data_set
        self.merge_type = merge_type
        self.merge_columns = merge_columns
        self.merge_columns_left = merge_columns_left
        self.merge_columns_right = merge_columns_right
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
        left_df = DataFlow.get_output_df(self.left_data_set)
        right_df = DataFlow.get_output_df(self.right_data_set)
        df = pd.merge(
            left_df,
            right_df,
            how=self.merge_type,
            on=self.merge_type,
            left_on=self.merge_columns_left,
            right_on=self.merge_columns_right,
        )
        self.output_data_set = DataFlow.save_output_df(df, self.name)


class TransformationRule(DataFlowStep):
    def run(self):
        """Short summary.

        Parameters
        ----------


        Returns
        -------
        type
            Description of returned object.

        """
        raise NotImplementedError("Needs to be implemented by inheriting class")
