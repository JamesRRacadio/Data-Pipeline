REM change directory to the project folder 'raw_data'
cd C://Users/Racadio/Documents/raw_data

REM Run the producer.py script to generate the fictional FI transactions
python producer.py

REM Run the consumer script to move the data to the LocalStack bucket
python consumer.py

