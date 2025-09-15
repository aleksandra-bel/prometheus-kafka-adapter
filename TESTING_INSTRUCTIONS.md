# Local Testing Instructions

I have created two new files to help you with local testing:
1.  `docker-compose.yml`: This file will start Kafka, Zookeeper, and InfluxDB services on your local machine.
2.  `run-local.sh`: This script sets up the necessary environment variables and runs the `prometheus-kafka-adapter` application.

Both files are located in the root directory of the project.

---

## How to Test

**Step 1: Start the Services**
In your terminal, run the following command to start all the necessary services in the background:
```sh
docker-compose up -d
```

**Step 2: Configure InfluxDB**
1.  Open your web browser and go to `http://localhost:8086`.
2.  You will be guided through the InfluxDB setup process. You'll need to create:
    *   **Username/Password**: Your initial user credentials.
    *   **Organization Name**: You can use `my-org` (this is the default in `run-local.sh`).
    *   **Bucket Name**: You can use `my-bucket` (this is the default in `run-local.sh`).
3.  On setup an **Access Token** will be generated for you. Save it.

**Step 3: Configure and Run the Adapter**
1.  Open the `run-local.sh` file.
2.  Replace the placeholder `<your-influxdb-api-token>` with the token you just copied from InfluxDB.
3.  You can also adjust the `INFLUXDB_MEASUREMENTS` list to include the names of the measurements you want to poll.
4.  Run the script:
    ```sh
    ./run-local.sh
    ```

**Step 4: Create a topic in Kafka**
1. Create a dedicated topic in Kafka for storing InfluxDB metrics named `influxdb-metrics`. 
    Run this command from a folder where `docker-compose.yaml` is situated:
    ```sh
    docker-compose exec kafka kafka-topics --create --topic influxdb-metrics --bootstrap-server kafka:19092 --partitions 1 --replication-factor 1
    ```

**Step 5: Generate Test Data in InfluxDB**
While the adapter is running, you can add some data to InfluxDB to see if it gets polled.
1.  Import the initial data set:
```curl
curl -sS -X POST "http://localhost:8086/api/v2/query?org=my-org" \
-H "Authorization: Token <your-influxdb-api-token>" \
-H "Accept: application/csv" \
-H "Content-Type: application/vnd.flux" \
--data-binary '
import "influxdata/influxdb/sample"

sample.data(set: "airSensor")
|> to(bucket: "my-bucket", org: "my-org")
'
```

2.  As there is a polling interval in the code you will need to refresh the data when application is started. Use this curl as an example: 
```curl
curl -sS -X POST "http://localhost:8086/api/v2/query?org=my-org" \
-H "Authorization: Token <your-influxdb-api-token>" \
-H "Accept: application/csv" \
-H "Content-Type: application/vnd.flux" \
--data-binary '
import "array"

data = [
{_time: now(), _measurement: "airSensors", _field: "temperature", _value: 85.1, host: "server01", sensor_id: "S-01"},
{_time: now(), _measurement: "airSensors", _field: "humidity", _value: 45.4, host: "server01", sensor_id: "S-01"},
]

array.from(rows: data)
|> to(bucket: "my-bucket", org: "my-org")
'
```

**Step 6: Verify the Data in Kafka**
You can use a command-line tool like `kcat` (previously `kafkacat`) to consume messages from your Kafka topic and verify that the data is arriving.
```sh
kcat -b localhost:9092 -t influxdb-metrics -C -q
```
You should see the data from InfluxDB appearing in your terminal.
