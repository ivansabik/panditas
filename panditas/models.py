import pandas as pd


class DataFlow:
    custom_params = {}
    steps = []

    def run(self):
        pass


class DataFlowStep:
    depends_on = None
    job_id = None
    input_data_set = None
    output_data_set = pd.DataFrame()

    def run(self):
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
    name = None
    preview_data_set = pd.DataFrame()
    row_count = 0
    s3_path = None
    sheet_index = 0
    sheet_name = None
    source = "csv"
    sql_query = None
    table_name = None

    def get(self):
        self.output_data_set = pd.DataFrame()


class MergeRule(DataFlowStep):
    left_data_set = pd.DataFrame()
    right_data_set = pd.DataFrame()
    merge_type = "inner"
    merge_columns = None
    merge_columns_left = None
    merge_columns_right = None

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
