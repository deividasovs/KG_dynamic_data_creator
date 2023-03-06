import json

# import requests

# Minimum encoder value = max_encoder_length = 9*60 //2 (~ 1 month / so do 31 days)

# max_prediction_length = 9*7  # How many datapoints will be predicted (~1 week)
# max_encoder_length = 9*60  # Determines the look back period (~2 months)pip


def lambda_handler(event, context):
    # try:
    #     ip = requests.get("http://checkip.amazonaws.com/")
    # except requests.RequestException as e:
    #     # Send some context about this error to Lambda Logs
    #     print(e)

    #     raise e

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world",
            # "location": ip.text.replace("\n", "")
        }),
    }
