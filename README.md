# <img height="32" src="https://raw.githubusercontent.com/ivansabik/panditas/master/doc/icon.png" /> Panditas

<p align="left">
    <a href="https://travis-ci.com/ivansabik/panditas"><img alt="CI Buld Status" src="https://travis-ci.com/ivansabik/panditas.svg?branch=master"/></a>
    <a href="https://github.com/ambv/black"><img alt="Code style: Black" src="https://img.shields.io/badge/code%20style-black-000000.svg"/></a>
</p>

Build Data Pipelines using Pandas and S3. Initially this will support the following executors:
- [Dagobah](https://github.com/thieman/dagobah)
- [Airflow](https://airflow.apache.org/)
- [Luigi](https://github.com/spotify/luigi)
- [AWS Batch](https://aws.amazon.com/batch/)

### Models

<p align="center">
  <img src="https://raw.githubusercontent.com/ivansabik/panditas/master/doc/models.png" width="700" />
</p>

- Data Flow
- Data Flow Steps
  - Data Set
  - Data Transformation
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

<p align="center">
  <img src="https://raw.githubusercontent.com/ivansabik/panditas/master/doc/insurance_agency_experience.png" width="850" />
</p>

### Credits

"gummy bear" icon by emilegraphics from the Noun Project.
