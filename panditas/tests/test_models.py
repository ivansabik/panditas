from panditas.models import DataFlow, DataSet, MergeMultipleRule, MergeRule
from panditas.transformation_rules import ConstantColumn


def test_data_set_dependencies():
    data_flow = DataFlow(
        name="Test Dependent Data Sets",
        steps=[
            DataSet(local_path="claims.csv", name="claims", source="csv"),
            DataSet(local_path="policies.csv", name="policies", source="csv")
        ],
    )
    assert data_flow.steps[0].name == "claims"
    assert data_flow.steps[1].name == "policies"
    assert data_flow.steps[0].position == 0
    assert data_flow.steps[1].position == 1
    assert data_flow.steps[0].depends_on == []
    assert data_flow.steps[1].depends_on == []

def test_data_set_dependencies_manual():
    data_flow = DataFlow(
        name="Test Data Sets",
        steps=[
            DataSet(local_path="claims.csv", name="claims", source="csv"),
            DataSet(local_path="policies.csv", name="policies", source="csv", depends_on=["claims"])
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
            DataSet(local_path="claims.csv", name="claims", source="csv"),
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
            DataSet(local_path="claims.csv", name="claims", source="csv"),
            DataSet(local_path="policies.csv", name="policies", source="csv"),
            MergeRule(
                left_data_set="claims",
                right_data_set="policies",
                merge_type="inner",
                name="merge_data_sets"
            ),
        ],
    )
    assert data_flow.steps[0].name == "claims"
    assert data_flow.steps[1].name == "policies"
    assert data_flow.steps[2].name == "merge_data_sets"
    assert data_flow.steps[0].position == 0
    assert data_flow.steps[1].position == 1
    assert data_flow.steps[2].position == 2
    assert data_flow.steps[0].depends_on == []
    assert data_flow.steps[1].depends_on == []
    assert data_flow.steps[2].depends_on == ["claims", "policies"]


def test_dependencies_merge_multiple():
    data_flow = DataFlow(
        name="Test Merge Multiple",
        steps=[
            DataSet(
                local_path="claims.csv",
                name="claims",
                source="csv",
            ),
            DataSet(
                local_path="policies.csv",
                name="policies",
                source="csv",
            ),
            DataSet(
                local_path="agencies.csv",
                name="agencies",
                source="csv",
            ),
            MergeMultipleRule(
                data_sets=[
                    "claims",
                    "policies",
                    "agencies",
                ],
                name="merge_facts_dims",
                merge_types=["inner", "inner", "inner"],
            ),
        ],
    )
    assert data_flow.steps[0].depends_on == []
    assert data_flow.steps[1].depends_on == []
    assert data_flow.steps[2].depends_on == []
    assert data_flow.steps[3].depends_on == [
        "claims",
        "policies",
        "agencies",
    ]
