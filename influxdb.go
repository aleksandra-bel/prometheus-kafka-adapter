// Copyright 2024 Telefónica
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
)

// InfluxDBClient represents a client for interacting with InfluxDB.
type InfluxDBClient struct {
	client influxdb2.Client
	org    string
	bucket string
}

// NewInfluxDBClient creates a new InfluxDB client.
func NewInfluxDBClient(url, token, org, bucket string) *InfluxDBClient {
	client := influxdb2.NewClient(url, token)
	return &InfluxDBClient{
		client: client,
		org:    org,
		bucket: bucket,
	}
}

// Query queries InfluxDB and returns the results.
func (c *InfluxDBClient) Query(ctx context.Context, query string) (*prompb.WriteRequest, error) {
	q := c.client.QueryAPI(c.org)
	result, err := q.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("could not query influxdb: %w", err)
	}

	metrics := make(map[string]*prompb.TimeSeries)
	for result.Next() {
		record := result.Record()
		measurement := record.Measurement()
		field := record.Field()
		name := fmt.Sprintf("%s_%s", measurement, field)
		timestamp := record.Time().UnixNano() / int64(time.Millisecond)

		var value float64
		switch v := record.Value().(type) {
		case float64:
			value = v
		case int64:
			value = float64(v)
		default:
			logrus.WithFields(logrus.Fields{
				"measurement": name,
				"type":        fmt.Sprintf("%T", v),
			}).Warn("unsupported value type in influxdb record, skipping")
			continue
		}

		labels := []prompb.Label{
			{Name: "__name__", Value: name},
		}
		for key, val := range record.Values() {
			// InfluxDB tags are always strings. Fields can be other types.
			// We only want to add tags as labels.
			// We can identify tags by checking if the value is a string.
			if s, ok := val.(string); ok {
				// Also, ignore standard flux columns that are not tags.
				if key != "result" && key != "table" && key != "_start" && key != "_stop" && key != "_time" && key != "_measurement" && key != "_field" && key != "_value" {
					labels = append(labels, prompb.Label{Name: key, Value: s})
				}
			}
		}

		// Create a unique key for the time series based on its labels.
		key := fmt.Sprintf("%v", labels)

		if ts, ok := metrics[key]; ok {
			ts.Samples = append(ts.Samples, prompb.Sample{Value: value, Timestamp: timestamp})
		} else {
			ts := &prompb.TimeSeries{
				Labels: labels,
				Samples: []prompb.Sample{
					{Value: value, Timestamp: timestamp},
				},
			}
			metrics[key] = ts
		}
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("error processing query result: %w", result.Err())
	}

	writeRequest := &prompb.WriteRequest{
		Timeseries: make([]prompb.TimeSeries, 0, len(metrics)),
	}

	for _, ts := range metrics {
		writeRequest.Timeseries = append(writeRequest.Timeseries, *ts)
	}

	return writeRequest, nil
}

func generateFluxQuery(measurements, bucket string, interval time.Duration) string {
	measurementList := strings.Split(measurements, ",")
	measurementFilters := make([]string, len(measurementList))
	for i, m := range measurementList {
		measurementFilters[i] = fmt.Sprintf(`r._measurement == "%s"`, m)
	}
	filterStr := strings.Join(measurementFilters, " or ")

	return fmt.Sprintf(`from(bucket: "%s") |> range(start: -%s) |> filter(fn: (r) => %s)`, bucket, interval, filterStr)
}

// StartPolling starts polling InfluxDB for data at a given interval.
func (c *InfluxDBClient) StartPolling(ctx context.Context, producer *kafka.Producer, measurements, topic string, interval time.Duration) {
	if measurements == "" {
		logrus.Info("no influxdb measurements configured, polling disabled")
		return
	}

	query := generateFluxQuery(measurements, c.bucket, interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log := logrus.WithField("query", query)
			log.Info("polling influxdb")
			writeRequest, err := c.Query(ctx, query)
			if err != nil {
				log.WithError(err).Error("could not query influxdb")
				continue
			}

			metricsPerTopic, err := Serialize(serializer, writeRequest)
			if err != nil {
				log.WithError(err).Error("could not serialize metrics")
				continue
			}

			for _, metrics := range metricsPerTopic {
				part := kafka.TopicPartition{
					Partition: kafka.PartitionAny,
					Topic:     &topic,
				}
				for _, metric := range metrics {
					objectsWritten.Add(float64(1))
					err := producer.Produce(&kafka.Message{
						TopicPartition: part,
						Value:          metric,
					}, nil)

					if err != nil {
						objectsFailed.Add(float64(1))
						log.WithError(err).Debug(fmt.Sprintf("Failing metric %v", metric))
						log.WithError(err).Error(fmt.Sprintf("couldn't produce message in kafka topic %v", topic))
					}
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
