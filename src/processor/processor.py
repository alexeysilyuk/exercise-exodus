import os
import json
import boto3
from dotenv import load_dotenv
from db.db import MongoDB

class Processor:
    def __init__(self):
        # Load environment variables from .env
        load_dotenv()

        # Get environment variables
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_region_name = os.getenv('AWS_REGION_NAME')
        self.aws_account_id = os.getenv('AWS_ACCOUNT_ID')
        
        # Connect to MongoDB
        self.mongo_db_instance = MongoDB()
        self.collection = self.mongo_db_instance.get_collection()

        # Initialize SQS client
        self.sqs = boto3.client(
            'sqs',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region_name
        )

        # SQS queue URL
        self.queue_url = f'https://sqs.{self.aws_region_name}.amazonaws.com/{self.aws_account_id}/exercise-exodus'

    def __save_data_to_db(self, localtime, name, country, temp_c, condition):
        # Insert data into MongoDB
        self.collection.insert_one({
            'localtime': localtime,
            'city': name,
            'country': country,
            'temp_c': temp_c,
            'condition': condition
        })

    def __process_message(self, message_body):
        try:
            # Parse JSON response
            data = json.loads(message_body)
            location = data.get('location')
            current = data.get('current')

            localtime = location.get('localtime')
            name = location.get('name')
            country = location.get('country')
            temp_c = current.get('temp_c')
            condition = current.get('condition').get('text')

            # Save to the database
            self.__save_data_to_db(localtime, name, country, temp_c, condition)
            print(f"Saved weather data for {name}, {country}: {temp_c}°C, {condition}")

        except Exception as e:
            print(f"Error processing message: {e}")

    def main(self):
        while True:
            # Receive a message from SQS
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                AttributeNames=['All'],
                MaxNumberOfMessages=10,
                VisibilityTimeout=60,
                WaitTimeSeconds=0
            )

            messages = response.get('Messages', [])
            if not messages:
                # print('No messages to process.')
                continue

            for message in messages:
                message_body = message['Body']
                self.__process_message(message_body)

                # Delete the message from the queue
                self.sqs.delete_message(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )

if __name__ == '__main__':
    processor = Processor()
    processor.main()