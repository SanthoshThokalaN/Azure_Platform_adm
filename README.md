# Insurance Solutions Data Cloud

## Repository Structure
Structure overview

|Folder|Purpose|
|-|-|
|[data_model](https://github.com/uhc-mris/Isdc/tree/feature/setup_sf_cicd/data_model/dw_db)|Snowflake Assets such as ddls, migration scripts, procedure, function, and task definitions|
|[cicd](https://github.com/uhc-mris/Isdc/tree/feature/setup_sf_cicd/cicd)|Code related to CICD process automation|
|[.github](https://github.com/uhc-mris/Isdc/tree/feature/setup_sf_cicd/.github/workflows)|Github specific files for deployments, templating, and other github features|

### data_model

    └─── ...
    └─── data_model
    │   └─── [database or environment]      # Environment reference for ETL DB or Reporting DB reference
    │   │   └─── [schema_name]              # Schema name
    │   │       └─── file_format            # File format objects
    │   │       └─── function               # Function definitions
    │   │       └─── migrations             # Migration scripts (Table DDL Creation/Update, DML,etc)
    │   │       └─── procedure              # Procedure definitions
    │   │       └─── table                  # Complete Table DDLs (reference only)
    │   │       └─── view                   # View Definitions
    │   │       └─── task                   # Task Definitions
    └─── ...

## Change Scripts

### Versioned Script Naming

The script name must follow this pattern :

With the following rules for each part of the filename:

* **Prefix**: The letter 'V' for versioned change
* **Version**: A unique version number with dots or underscores separating as many number parts as you like
* **Separator**: __ (two underscores)
* **Description**: An arbitrary description with words separated by underscores or spaces (can not include two underscores)
* **Suffix**: .sql or .sql.jinja

For example, a script name that follows this convention is: `V1.1.1__first_change.sql`. As with Flyway, the unique version string is very flexible. You just need to be consistent and always use the same convention, like 3 sets of numbers separated by periods. Here are a few valid version strings:

* 1.1
* 1_1
* 1.2.3
* 1_2_3

Every script within a database folder must have a unique version number. schemachange will check for duplicate version numbers and throw an error if it finds any. This helps to ensure that developers who are working in parallel don't accidently (re-)use the same version number.

### Repeatable Script Naming

Repeatable change scripts must follow this pattern:

`R__Add_new_table.sql`

e.g:

* R__sp_add_sales.sql
* R__fn_get_timezone.sql
* R__fn_sort_ascii.sql

All repeatable change scripts are applied each time the utility is run, if there is a change in the file.
Repeatable scripts could be used for maintaining code that always needs to be applied in its entirety. e.g. stores procedures, functions and view definitions etc.

Just like Flyway, within a single migration run, repeatable scripts are always applied after all pending versioned scripts have been executed. Repeatable scripts are applied in alphabetical order of their description.

### Always Script Naming

Always change scripts are executed with every run of schemachange.
The script name must following pattern:

`A__Some_description.sql`

e.g.

* A__add_user.sql
* A__assign_roles.sql

This type of change script is useful for an environment set up after cloning. Always scripts are applied always last.
