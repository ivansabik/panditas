import pandas as pd


class DataFlow:
    custom_params = {}
    name = None
    output_data_set = pd.DataFrame()
    steps = []

    def __init__(self, name=None, steps=[]):
        self.name = name
        self.steps = steps

    def run(self):
        pass


class DataFlowStep:
    depends_on = None
    job_id = None
    input_data_set = None
    name = None
    output_data_set = pd.DataFrame()

    def _set_dependencies():
        # DataSets do not have dependencies, they can be sourced in parallel
        # Single Merge have dependencies on the left and right data sets
        # Multiple Merge have dependencies on all the data sets
        pass

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

    def __init__(self, columns=None, local_path=None, name=None, source=None):
        self.columns = columns
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

    def merge(self):
        base_df = self.data_sets.pop(0)
        for key, data_set in enumerate(self.data_sets):
            base_df = base_df.merge(data_set, how=self.merge_types[key])


class MergeRule(DataFlowStep):
    left_data_set = pd.DataFrame()
    right_data_set = pd.DataFrame()
    merge_type = "inner"
    merge_columns = None
    merge_columns_left = None
    merge_columns_right = None

    def __init__(self):
        pass

    def merge(self):
        self.output_data_set = pd.merge(
            self.left_data_set,
            self.right_data_set,
            how=self.merge_type,
            on=self.merge_type,
            left_on=self.merge_columns_left,
            right_on=self.merge_columns_right,
        )


class TransformationRule(DataFlowStep):
    input_data_set = pd.DataFrame()
    output_data_set = pd.DataFrame()

    def run(self):
        raise NotImplementedError("Needs to be implemented by inheriting class")
