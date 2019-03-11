import pathlib

from panditas.models import DataFlow, DataSet, MergeMultipleRule
from panditas.transformation_rules import ConditionalFill, ConstantColumn, PivotTable

fixtures_path = "{0}/fixtures".format(pathlib.Path(__file__).parent)


def test_insurance_agency_experience():
    data_flow = DataFlow(
        name="Agency Experience",
        steps=[
            DataSet(
                columns=["revisionId", "lossReserveBalance", "claimStatus"],
                df_path="{0}/claims.csv".format(fixtures_path),
                name="claims",
                source="csv",
            ),
            DataSet(
                columns=["revisionId", "policyId", "policyInforcePremium"],
                df_path="{0}/policy_state.csv".format(fixtures_path),
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
                df_path="{0}/policy_changes.csv".format(fixtures_path),
                name="transactions",
                source="csv",
            ),
            DataSet(
                columns=["policyId", "policyNumber"],
                df_path="{0}/policies.csv".format(fixtures_path),
                name="policies",
                source="csv",
            ),
            DataSet(
                columns=["revisionId", "agencyName"],
                df_path="{0}/agencies.csv".format(fixtures_path),
                name="agencies",
                source="csv",
            ),
            DataSet(
                columns=["revisionId", "lineOfBusinessName"],
                df_path="{0}/lines.csv".format(fixtures_path),
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
                where_column="claimStatus",
                where_condition="contains",
                where_condition_values=["Open"],
            ),
            # Policy New
            ConstantColumn(
                column_name="newCount", column_value=0, name="add_new_count_column"
            ),
            ConditionalFill(
                fill_column="newCount",
                fill_value=1,
                name="calculate_new_count",
                where_column="policyChangeTransactionType",
                where_condition="==",
                where_condition_values=["New"],
            ),
            ConstantColumn(
                column_name="newPremium", column_value=0, name="add_new_premium"
            ),
            ConditionalFill(
                fill_column="newPremium",
                fill_value=1,
                name="calculate_new_premium",
                where_column="policyChangeTransactionType",
                where_condition="==",
                where_condition_values=["New"],
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
                where_column="policyChangeTransactionType",
                where_condition="==",
                where_condition_values=["Canceled"],
            ),
            ConstantColumn(
                column_name="cancelPremium", column_value=0, name="add_cancel_premium"
            ),
            ConditionalFill(
                fill_column="cancelPremium",
                fill_value=1,
                name="calculate_cancel_premium",
                where_column="policyChangeTransactionType",
                where_condition="==",
                where_condition_values=["Canceled"],
            ),
            PivotTable(
                group_columns=["agencyName", "lineOfBusinessName"],
                group_values=[
                    "claimCount",
                    "lossReserveBalance",
                    "newCount",
                    "newPremium",
                    "cancelCount",
                    "cancelPremium",
                    "policyInforcePremium",
                ],
                group_functions=["sum", "last", "sum", "sum", "sum", "sum", "max"],
                name="group_by_agency_line",
            ),
        ],
    )
    data_flow.run()
    assert data_flow.output_data_set == "group_by_agency_line"
    df = DataFlow.get_output_df(data_flow.output_data_set)
    assert sorted(df.columns.tolist()) == [
        "agencyName",
        "cancelCount",
        "cancelPremium",
        "claimCount",
        "lineOfBusinessName",
        "lossReserveBalance",
        "newCount",
        "newPremium",
        "policyInforcePremium",
    ]
