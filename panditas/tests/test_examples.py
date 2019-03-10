from panditas.models import DataFlow, DataSet, MergeMultipleRule
from panditas.transformation_rules import ConditionalFill, ConstantColumn, PivotTable


def test_insurance_agency_experience():
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
                columns=["revisionId", "policyId", "policyInforcePremium"],
                local_path="policy_state.csv",
                name="inforce",
                source="csv",
            ),
            DataSet(
                columns=[
                    "revisionId",
                    "policyId",
                    "policyChangeTransactionType",
                    "policyChangeWrittenPremium",
                ],
                local_path="policy_changes.csv",
                name="transactions",
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
            DataSet(
                columns=["revisionId", "lineOfBusinessName"],
                local_path="lines.csv",
                name="lines",
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
            ),
            # Claim Count
            ConstantColumn(
                column_name="claimCount", column_value=0, name="add_claim_count_column"
            ),
            ConditionalFill(
                fill_column="claimCount",
                fill_value=1,
                name="calculate_claim_count",
                where_columns="claimStatus",
                where_condition="contains",
                where_condition_value="Open",
            ),
            # Policy New
            ConstantColumn(
                column_name="newCount", column_value=0, name="add_new_count_column"
            ),
            ConditionalFill(
                fill_column="newCount",
                fill_value=1,
                name="calculate_new_count",
                where_columns=None,
                where_condition="equals",
                where_condition_value="New",
            ),
            ConstantColumn(
                column_name="newPremium", column_value=0, name="add_new_premium"
            ),
            ConditionalFill(
                fill_column="newPremium",
                fill_value=1,
                name="calculate_new_premium",
                where_condition="equals",
                where_condition_value="New",
            ),
            # Policy cancel
            ConstantColumn(
                column_name="cancelCount",
                column_value=0,
                name="add_cancel_count_column",
            ),
            ConditionalFill(
                fill_column="cancelCount",
                fill_value=1,
                name="calculate_cancel_count",
                where_columns=None,
                where_condition="equals",
                where_condition_value="Canceled",
            ),
            ConstantColumn(
                column_name="cancelPremium", column_value=0, name="add_cancel_premium"
            ),
            ConditionalFill(
                fill_column="cancelPremium",
                fill_value=1,
                name="calculate_cancel_premium",
                where_condition="equals",
                where_condition_value="Canceled",
            ),
            PivotTable(
                group_columns=["agencyName", "lineOfBusinessName"],
                group_values=[
                    "claimCount",
                    "lossReserveBalance",
                    "newCount",
                    "newPremium" "cancelCount",
                    "cancelPremium",
                    "policyInforcePremium",
                ],
                group_functions=["sum", "last", "sum", "sum", "sum", "sum", "max"],
                name="group_by_agency_line",
            ),
        ]
    )
    data_flow.run()
    assert data_flow.output_data_set.columns.tolist() == [
        "agencyName", "lineOfBusinessName", "claimCount", "lossReserveBalance",
        "newCount", "newPremium" "cancelCount", "cancelPremium",
        "policyInforcePremium"
    ]
