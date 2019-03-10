# <img height="45" src="https://raw.githubusercontent.com/ivansabik/panditas/master/doc/icon.png" /> panditas

Build Data Pipelines using Pandas and S3. Initially this will support the following executors:
- Dagobah
- Apache Airflow
- AWS Batch
- Luigi

### Models

<img src="https://raw.githubusercontent.com/ivansabik/panditas/master/doc/models.png" />

#### Data Set

- Date Columns
- Data Types by Column
- Source (CSV, Excel, SQL)
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
- Pivot Table (for grouping by)
- Remove Duplicates
- Rename Column
- Replace Text
- Sort by Columns
- Value Mapper

#### Merge Rule

#### Data Flow

- Start Date
- End Date
- Initial Run
- Running Interval
- Parameters (Start Date, End Date, Year, Agency Number, Claim Number, Policy Number, etc)

### Credits

"gummy bear" icon by emilegraphics from the Noun Project.
