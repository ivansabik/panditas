# <img height="45" src="https://raw.githubusercontent.com/ivansabik/panditas/master/doc/icon.png" /> panditas

Build Data Pipelines using Pandas and S3

### Models

#### Setting

- S3 Host
- AWS Secret Key
- AWS Access Key

#### Input DataFrame

- Date Columns
- Data Types by Column
- Format (CSV, Excel, SQL)
- Local path
- Preview Rows
- Row count
- S3 URL
- Sheet Index
- Sheet Name (In case of an Excel file, sheets are handled as single DF)
- Table Name
- Database provider (mysql)
- Database host
- Database user
- Database pass
- Database port

#### Data Transformation

They all have:
- DF Inputs
- DF Input resulting from the output of other transformation
- One DF output

Only 1 or 2 inputs are allowed for DF to Transformation and Only 1 input is allowed for Transformation to Transformation

Available transformations include:
- Calculated Column
- Columns Subset
- Conditional Fill
- Constant Column
- Filter
- Format Columns (Currency, Date, etc)
- Merge
- Pivot Table (for grouping by)
- Remove Duplicates
- Rename Column
- Replace Text
- Sort by Columns
- Value Mapper

#### Output DataFrame



#### Operators

These have access to an Input Data Frame and the graph parameters.
- Email
- Slack
- FTP
- ZIP

#### Data Pipeline Graph

- Start Date
- End Date
- Initial Run
- Running Interval
- Parameters (Start Date, End Date, Year, Agency Number, Claim Number, Policy Number, etc)

These can be run:
- As a python bin script
- As a docker command

### Example

<img src="https://raw.githubusercontent.com/ivansabik/panditas/master/doc/sample_data_pipeline.png" />

```python
import panditas

pipeline = panditas.Graph()

claims = panditas.InputDataFrame()
policies = panditas.InputDataFrame()
inforce_premium = panditas.InputDataFrame()
pipeline.add_input_df(claims)
pipeline.add_input_df(policies)
pipeline.add_input_df(inforce_premium)

merge_1 =
pipeline.add_transformation(merge_1, left_df=claims, right_df=policies)
pipeline.add_transformation(merge_2, left_df=claims, right_df=policies)
```

### Credits

"gummy bear" icon by emilegraphics from the Noun Project.
