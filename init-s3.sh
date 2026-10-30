#!/bin/bash
echo "Creating S3 warehouse bucket..."
aws --endpoint-url=http://localhost:4566 s3 mb s3://warehouse
echo "Bucket created successfully!"