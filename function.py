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
    return data


def notify_to_line(users):
    for user in users:
        if 'NotifyToken' not in user or not user['NotifyToken']:
            continue

        url = "https://notify-api.line.me/api/notify"
        headers = {"Authorization" : "Bearer "+ user['NotifyToken']}

        for search_word, programs in user['Programs'].items():
            message = ''
            for program in programs:
                if 'Notify' not in program or not bool(program['Notify']):
                    continue
                message += '\n' + program['Date'] + '\n' + program['Station'] + '\n' + program['Title'] + '\n' \
                           + 'https://www.dtmi/program/' + program['ProgramId'] + '\n'

            if message:
                message = '「' + search_word + message + '」の番組'
                requests.post(url, data={"message" :  message}, headers=headers)

