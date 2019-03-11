from panditas.models import DataFlow, DataSet, MergeMultipleRule, MergeRule
from panditas.transformation_rules import ConstantColumn


def test_data_set_dependencies():
    data_flow = DataFlow(
        name="Test Dependent Data Sets",
        steps=[
            DataSet(df_path="claims.csv", name="claims", source="csv"),
            DataSet(df_path="policies.csv", name="policies", source="csv"),
        ],
    )
    assert data_flow.steps[0].name == "claims"
    assert data_flow.steps[1].name == "policies"
    assert data_flow.steps[0].position == 0
    assert data_flow.steps[1].position == 1
    assert data_flow.steps[0].depends_on == []
    assert data_flow.steps[1].depends_on == []


def test_data_set_get_columns():
    pass


def test_data_set_dependencies_manual():
    data_flow = DataFlow(
        name="Test Data Sets",
        steps=[
            DataSet(df_path="claims.csv", name="claims", source="csv"),
            DataSet(
                df_path="policies.csv",
                name="policies",
                source="csv",
                depends_on=["claims"],
            ),
        ],
    )
    assert data_flow.steps[0].name == "claims"
    assert data_flow.steps[1].name == "policies"
    assert data_flow.steps[0].position == 0
    assert data_flow.steps[1].position == 1
    assert data_flow.steps[0].depends_on == []
    assert data_flow.steps[1].depends_on == ["claims"]


def test_dependencies():
    data_flow = DataFlow(
        name="Test Dependencies with Constant",
        steps=[
            DataSet(df_path="claims.csv", name="claims", source="csv"),
            ConstantColumn(
                column_name="new",
                column_value="THIS IS A CONSTANT VALUE",
                name="add_constant",
            ),
        ],
    )
    assert data_flow.steps[0].name == "claims"
    assert data_flow.steps[1].name == "add_constant"
    assert data_flow.steps[0].position == 0
    assert data_flow.steps[1].position == 1
    assert data_flow.steps[0].depends_on == []
    assert data_flow.steps[1].depends_on == ["claims"]


def test_dependencies_merge():
    data_flow = DataFlow(
        name="Test Merge",
        steps=[
            DataSet(df_path="df_one.csv", name="df_one", source="csv"),
            DataSet(df_path="df_two.csv", name="df_two", source="csv"),
            MergeRule(
                left_data_set="df_one",
                right_data_set="df_two",
                merge_type="inner",
                name="merge_data_sets",
            ),
        ],
    )
    assert data_flow.steps[0].name == "df_one"
    assert data_flow.steps[1].name == "df_two"
    assert data_flow.steps[2].name == "merge_data_sets"
    assert data_flow.steps[0].position == 0
    assert data_flow.steps[1].position == 1
    assert data_flow.steps[2].position == 2
    assert data_flow.steps[0].depends_on == []
    assert data_flow.steps[1].depends_on == []
    assert data_flow.steps[2].depends_on == ["df_one", "df_two"]


def test_dependencies_merge_multiple():
    data_flow = DataFlow(
        name="Test Merge Multiple",
        steps=[
            DataSet(df_path="claims.csv", name="claims", source="csv"),
            DataSet(df_path="policies.csv", name="policies", source="csv"),
            DataSet(df_path="agencies.csv", name="agencies", source="csv"),
            MergeMultipleRule(
                data_sets=["claims", "policies", "agencies"],
                name="merge_facts_dims",
                merge_types=["inner", "inner", "inner"],
            ),
        ],
    )
    assert data_flow.steps[0].depends_on == []
    assert data_flow.steps[1].depends_on == []
    assert data_flow.steps[2].depends_on == []
    assert data_flow.steps[3].depends_on == ["claims", "policies", "agencies"]
