# Overview of Dagster YAML file

How does the `dagster.yaml` work? Let's deconstruct its components. 

1) **Usage statistics**<br>
`telemetry`<br>
This is set to false, meaning that we don't send telemetry data and that no usage statistics are sent back to Dagster. <br><br>

1) **Storage**<br>
`storage`<br>
`local_artifact_storage`<br>
This shows that PostgreSQL is the storage backend for Dagster and that artifacts are stored locally. <br><br>

2) **Run execution**<br>
`run_launcher`<br>
`run_coordinator`<br>
This launches and coordinates pipeline runs. The run launcher is set in default, meaning that runs are launch immediately. The run coordinator limits execution to one run at a time.  <br><br>

3) **Logging**<br>
`compute_logs`<br>
This stores logs locally and defines the directory where logs are to be stored.<br><br>

4) **Schedulers and sensors**<br>
`sensors`<br>
`schedules`<br>
This enables multi-threaded execution and allocates four worker threads for scheduled jobs.