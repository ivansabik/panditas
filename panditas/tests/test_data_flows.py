from panditas.models import DataFlow, DataSet, MergeMultipleRule, MergeRule
from panditas.transformation_rules import ConstantColumn


def test_dependencies():
    data_flow = DataFlow(
        name="Test Merge",
        steps=[
            DataSet(
                local_path="claims.csv",
                name="claims",
                source="csv",
            ),
            ConstantColumn(
                column_name="new",
                column_value="THIS IS A CONSTANT VALUE",
                name="add_constant"
            )
        ]
    )
    assert data_flow.steps[0].depends_on == []
    assert data_flow.steps[1].depends_on == []

def test_dependencies_merge():
    data_flow = DataFlow(
        name="Test Merge",
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
            MergeRule(
                left_data_set="claims",
                right_data_set="policies",
                merge_type="inner"
            )
        ]
    )
    assert data_flow.steps[0].depends_on == []
    assert data_flow.steps[1].depends_on == []
    assert data_flow.steps[2].depends_on == []


def test_dependencies_merge_multiple():
    data_flow = DataFlow(
        name="Agency Experience",
        steps=[
            DataSet(
                columns=["revisionId", "lossReserveBalance", "claimStatus"],
                local_path="claims.csv",
                name="claims",
                source="csv",
            ),
            DataSet(
                columns=["policyId", "policyNumber"],
                local_path="policies.csv",
                name="policies",
                source="csv",
            ),
            DataSet(
                columns=["revisionId", "agencyId"],
                local_path="agencies.csv",
                name="agencies",
                source="csv",
            ),
            MergeMultipleRule(
                data_sets=[
                    "claims",
                    "inforce",
                    "transactions",
                    "policies",
                    "agencies",
                    "lines",
                ],
                name="merge_facts_dims",
                merge_types=["outer", "outer", "outer", "left", "left"],
            )
        ]
    )
    assert data_flow.steps[0].depends_on == []
    assert data_flow.steps[1].depends_on == []
    assert data_flow.steps[2].depends_on == []
    assert data_flow.steps[3].depends_on == []
