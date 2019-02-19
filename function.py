import requests
import os
import boto3
from boto3.session import Session


def lambda_handler(event, context):
    dynamodb = prepare_dynamodb()
    users = fetch_user(dynamodb)
    notify_to_line(users)


def prepare_dynamodb():
    if os.environ.get('EXEC_ENV') == 'TEST':
        session = Session(profile_name='local-dynamodb-user')
        dynamodb = session.resource('dynamodb')
    else:
        dynamodb = boto3.resource('dynamodb')
    return dynamodb


def fetch_user(dynamodb):
    table = dynamodb.Table('User')
    response = table.scan()
    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    print(data)
    return data


def notify_to_line(users):
    for user in users:
        if 'NotifyToken' not in user or not user['NotifyToken']:
            continue

        url = "https://notify-api.line.me/api/notify"
        headers = {"Authorization" : "Bearer "+ user['NotifyToken']}

        message = '\n'
        for program in user['Programs']:
            if 'Notify' not in program or not bool(program['Notify']):
                continue
            message += program['Date'] + '\n' + program['Title'] + '\n' + program['Contents'] + '\n' \
                       + program['SearchWord'] + 'Not notify: https://www.dtmi/not-notify/' + program['ProgramId'] + '\n\n'

        print(message)

        res = requests.post(url, data={"message" :  message}, headers=headers)

