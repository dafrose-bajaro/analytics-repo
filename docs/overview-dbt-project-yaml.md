# Overview of dbt YAML file

How does the `dbt-project.yaml` work? Let's deconstruct its components. 

1) **Project metadata**<br>
This refers to the name, dbt version, and optional project version number. <br><br>

2) **Profile**<br>
Profile in `profiles.yaml` that contains database connection details. <br><br>

3) **Paths**<br>
Defines where to look to specific types of files and where to store SQL and artifacts after runs. Clean targets refer to a list of directories to be cleaned when running `dbt clean` and includes compiled files, artifacts, installed dbt packages, and log files. <br><br>

4) **Version**<br>
Specifies which dbt version. <br><br>

5) **Models**<br>
Default model is a table. Staging models are materialized as views. <br><br>

6) **Tests**<br>
This refers to how dbt handles test failures. It stores failed results as views in the database. <br><br>