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
3.  After setup, navigate to **Load Data** > **API Tokens**.
4.  Generate a new **All Access API Token**. Copy this token.

**Step 3: Configure and Run the Adapter**
1.  Open the `run-local.sh` file.
2.  Replace the placeholder `<your-influxdb-api-token>` with the token you just copied from InfluxDB.
3.  You can also adjust the `INFLUXDB_MEASUREMENTS` list to include the names of the measurements you want to poll.
4.  Run the script:
    ```sh
    ./run-local.sh
    ```

**Step 4: Generate Test Data in InfluxDB**
While the adapter is running, you can add some data to InfluxDB to see if it gets polled.
1.  In the InfluxDB UI, go to **Explore** and open the **Script Editor**.
2.  Paste and run the following Flux script to write some sample data.
    ```flux
    import "influxdata/influxdb/sample"

    sample.data(set: "airSensor")
      |> to(bucket: "my-bucket", org: "my-org")
    ```

**Step 5: Verify the Data in Kafka**
You can use a command-line tool like `kcat` (previously `kafkacat`) to consume messages from your Kafka topic and verify that the data is arriving.
```sh
kcat -b localhost:9092 -t influxdb-metrics -C -q
```
You should see the data from InfluxDB appearing in your terminal.
