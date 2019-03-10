from panditas.models import DataFlow, DataSet, MergeMultipleRule

DataFlow(steps=[
    MergeMultipleRule(
        data_sets=[
            DataSet(
                columns=[],
                local_path = "claims.csv",
                name = "Claims",
                source = "csv"
            ),
            DataSet(
                columns=[],
                local_path = "policy_state.csv",
                name = "In Force Policies",
                source = "csv"
            ),
            DataSet(
                columns=[],
                local_path = "policy_changes.csv",
                name = "Policy Transactions",
                source = "csv"
            ),
            DataSet(
                columns=[],
                local_path = "policies.csv",
                name = "Policies",
                source = "csv"
            ),
            DataSet(
                columns=[],
                local_path = "agencies.csv",
                name = "Agencies",
                source = "csv"
            ),
            DataSet(
                columns=[],
                local_path = "lines.csv",
                name = "Lines",
                source = "csv"
            )
        ],
        merge_types=["outer", "outer", "outer", "left", "left"]
    ),
    # Claim Count
    ConstantColumn(
        column_name="claimCount"
        column_value=0
    ),
    ConditionalFill(
        fill_column="claimCount",
        fill_value= 1
        where_columns= "claimStatus"
        where_condition="contains"
        where_condition_value="Open"
    ),
    # Policy Count
    ConstantColumn(
        column_name="newCount"
        column_value=0
    ),
    ConditionalFill(
        fill_column="newCount",
        fill_value= 1
        where_columns= None
        where_condition="equals"
        where_condition_value="New"
    ),
    ConstantColumn(
        column_name="newPremium"
        column_value=0
    ),
    ConditionalFill(
        fill_column="newPremium",
        fill_value= 1
        where_condition="equals"
        where_condition_value="New"
    ),
    ConstantColumn(
        column_name="cancelCount"
        column_value=0
    ),
    ConditionalFill(
        fill_column="cancelCount",
        fill_value= 1
        where_columns= None
        where_condition="equals"
        where_condition_value="Canceled"
    ),
    ConstantColumn(
        column_name="cancelPremium"
        column_value=0
    ),
    ConditionalFill(
        fill_column="cancelPremium",
        fill_value= 1
        where_condition="equals"
        where_condition_value="Canceled"
    ),
    PivotTable(
        group_columns=["agencyName", "lineOfBusinessName"]
        group_values=[
            "claimCount", "lossReserveBalance", "newCount", "newPremium"
            "cancelCount", "cancelPremium"
        ]
        group_functions=["sum", "last", "sum", "sum", "sum", "sum"]
    )
]).run()
