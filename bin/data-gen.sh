#!/usr/bin/env bash
ksql-datagen format=json schema=../data/data-gen.avro topic=transcript key=studentID iterations=100
