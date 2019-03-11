import datetime
import logging

import pandas as pd


logger = logging.getLogger(__name__)


class DataFlow:
    custom_params = {}
    name = None
    output_data_set = pd.DataFrame()
    steps = []

    def __init__(self, name=None, steps=[]):
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
        step_type = type(step).__name__
        # Multiple Merge have dependencies on all the data sets
        if step_type == "MergeMultipleRule":
            self.steps[step.position].depends_on = step.data_sets
        # Single Merge have dependencies on the left and right data sets
        elif step_type == "MergeRule":
            self.steps[step.position].depends_on = [step.left_data_set, step.right_data_set]
        # DataSets do not have dependencies (unless manually specified)
        elif step_type != "DataSet":
            previous_step = self.steps[step.position - 1]
            self.steps[step.position].depends_on = [previous_step.name]
        elif step_type == "DataSet" and not self.steps[step.position].depends_on:
            self.steps[step.position].depends_on = []

    def run(self):
        for step in self.steps:
            logger.info(
                "Running step {0} with name {1}".format(type(step).__name__, step.name)
            )
            step.run()


class DataFlowStep:
    depends_on = []
    job_id = None
    input_data_set = None
    name = None
    output_data_set = pd.DataFrame()
    position = None

    def run(self):
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
    local_path = None
    preview_data_set = pd.DataFrame()
    row_count = 0
    s3_path = None
    sheet_index = 0
    sheet_name = None
    source = "csv"
    sql_query = None
    table_name = None

    def __init__(self, columns=None, depends_on=[], local_path=None, name=None, source=None):
        self.columns = columns
        self.depends_on = depends_on
        self.local_path = local_path
        self.name = name
        self.source = source

    def get(self):
        self.output_data_set = pd.DataFrame()

    def _get_columns(self):
        self.columns = []

    def _get_data_types(self):
        self.data_types = {}


class MergeMultipleRule(DataFlowStep):
    data_sets = []
    merge_types = []

    def __init__(self, data_sets=[], merge_types=[], name=None):
        self.data_sets = data_sets
        self.merge_types = merge_types
        self.name = name

    def run(self):
        base_df = self.data_sets.pop(0)
        for key, data_set in enumerate(self.data_sets):
            base_df = base_df.merge(data_set, how=self.merge_types[key])


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
        self.left_data_set = left_data_set
        self.right_data_set = right_data_set
        self.merge_type = merge_type
        self.merge_columns = merge_columns
        self.merge_columns_left = merge_columns_left
        self.merge_columns_right = merge_columns_right
        self.name = name

    def run(self):
        self.output_data_set = pd.merge(
            self.left_data_set,
            self.right_data_set,
            how=self.merge_type,
            on=self.merge_type,
            left_on=self.merge_columns_left,
            right_on=self.merge_columns_right,
        )


class TransformationRule(DataFlowStep):
    input_data_set = None
    output_data_set = None

    def run(self):
        raise NotImplementedError("Needs to be implemented by inheriting class")
