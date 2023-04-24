#!/bin/bash

echo "Enter the new Kafka broker list:"
read new_kafka_broker_list

for file in *.sql; do
  if [ -f "$file" ]; then
    if grep -q "kafka_broker_list = '[^']\+'" "$file"; then
      sed -i "s|kafka_broker_list = '[^']\+'|kafka_broker_list = '$new_kafka_broker_list'|g" "$file"
      echo "Updated file: $file"
    fi
  fi
done

echo "Done updating Kafka broker list in SQL files."
