# Overview of Docker YAML file

How does the `docker-compose.yaml` work? Let's deconstruct its components.

## **Volumes**<br><br>
Stores data related to Dagster and keeps the Python environment consistent so that we prevent unnecessary installations. <br><br>

## **Services**<br><br>

**Dagster**<br>
Builds the Dagster service using the Dockerfile in the current directory only when the database is ready. Dagster's development server is exposed on port 3030. This means the we can access Dagster's web interface on `http://loccalhost:3030` on local. <br><br>

**Dagster Database**<br>
Use PostgreSQL image for Dagster's backend storage. PostgreSQL should be ready before Dagster starts. The check is done every 20 seconds and it retries up to 3 times.
