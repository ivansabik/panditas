# <img height="45" src="https://raw.githubusercontent.com/ivansabik/panditas/master/doc/icon.png" /> panditas

Build Data Pipelines using Pandas and S3. Initially this will support the following executors:
- [Dagobah](https://github.com/thieman/dagobah)
- [Airflow](https://airflow.apache.org/)
- [Luigi](https://github.com/spotify/luigi)
- [AWS Batch](https://aws.amazon.com/batch/)

### Models

<img src="https://raw.githubusercontent.com/ivansabik/panditas/master/doc/models.png" />

- Data Flow
- Data Flow Steps
  - Data Set
  - Data Transformation, available transformations include:
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
  - Single Merge Rule
  - Multiple Merge Rule

#### Example

This is a sample dataflow from the insurance industry, implemented [here](https://github.com/ivansabik/panditas/blob/master/examples/insurance_agency_experience.py):
<img src="https://raw.githubusercontent.com/ivansabik/panditas/master/doc/insurance_agency_experience.png" />

### Credits

"gummy bear" icon by emilegraphics from the Noun Project.
