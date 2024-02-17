import hashlib
import hmac
import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv
from slack_sdk import WebClient

load_dotenv()

# Set up a WebClient with the Slack OAuth token
client = WebClient(token=(os.getenv("BOT_TOKEN")))
TOWER_ACCESS_KEY = str(os.getenv("TOWER_ACCESS_KEY"))
TOWER_SECRET_KEY = str(os.getenv("TOWER_SECRET_KEY"))
LIVE_SERVER_HOST = str(os.getenv("LIVE_SERVER_HOST"))
BOT_CHANNEL = str(os.getenv("BOT_CHANNEL"))
BOT_USER_NAME = str(os.getenv("BOT_USER_NAME"))

# Cache for limits data
limits_cache = None
CACHE_EXPIRATION = 36000  # Cache expiration time in seconds (10 hour)


def get_limits():
    # Read limits from Excel file
    limits_df = pd.read_excel('balance_limits.xlsx')

    # Preprocess limits DataFrame into a dictionary for faster lookups
    limits_dict = {(row['Platform'], row['Currency']): (row['Lower Limit'], row['Upper Limit'])
                   for _, row in limits_df.iterrows()}

    return limits_dict


# Perform GET request to real server
def perform_get_request(url):
    nonce = str(int(time.time() * 1000))
    salted_key = nonce + TOWER_ACCESS_KEY
    signature = hmac.new(TOWER_SECRET_KEY.encode(), salted_key.encode(), hashlib.sha256).hexdigest()

    try:
        headers = {
            'X-Auth-Apikey': TOWER_ACCESS_KEY,
            'X-Auth-Nonce': nonce,
            'X-Auth-Signature': signature
        }
        res = requests.get(LIVE_SERVER_HOST + url, headers=headers)

        # Return the response from the real server
        return {'data': res.json(), 'status_code': res.status_code}
    except requests.exceptions.RequestException as error:
        print(f"{LIVE_SERVER_HOST}{url}: {error.response}")
        # Return empty response with error status for failed requests
        return {'data': None, 'status_code': 500}


def get_balance():
    return perform_get_request(url="/api/v2/peatio/admin/exchange_balances")


def check_limit(response_data):
    # Iterate over response data and check limits
    msg_data = []

    if response_data['status_code'] != 200:
        return msg_data

    for entry in response_data['data']:
        platform = str(entry['id']).lower()
        currency = str(entry['currency']).lower()
        balance = float(entry['balance'])

        # Retrieve limits from dictionary
        lower_limit, upper_limit = get_limits().get((platform, currency), (-1, -1))

        data = {
            'platform': platform,
            'currency': currency,
            'balance': balance,
            'upper_limit': upper_limit,
            'lower_limit': lower_limit,
            'limit_type': 'none'
        }

        # Check if balance falls below lower limit (if lower limit is specified)
        if lower_limit != -1 and balance < lower_limit:
            print(f"Warning: Balance is below for '{currency}' on {platform}"
                  f" [Limit: {lower_limit} {currency.upper()}, Balance: {balance}]")

            data['limit_type'] = 'lower'
            msg_data.append(data)

        # Check if balance exceeds upper limit (if upper limit is specified)
        if upper_limit != -1 and balance > upper_limit:
            print(f"Warning: Balance exceeds for '{currency}' on {platform}"
                  f" [Limit: {upper_limit} {currency}, Balance: {balance}]")

            data['limit_type'] = 'upper'
            msg_data.append(data)

    return msg_data


def build_messages(msg_data):
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "❗Balance Outside Acceptable Range",
            }
        },
    ]

    fallback_msgs = []

    for data in msg_data:
        platform = str(data['platform']).capitalize()
        currency = str(data['currency']).upper()
        balance = str(data['balance'])
        limit_type = str(data['limit_type'])
        upper_limit = str(data['upper_limit'])
        lower_limit = str(data['lower_limit'])

        if limit_type == 'lower':
            plain_msg = (f"Warning 😱: Balance is below for '{currency}' on {platform}"
                         f" [Limit: {lower_limit} {currency.upper()}, Balance: {balance}]")
            fallback_msgs.append(plain_msg)
        else:
            plain_msg = (f"Warning 😱: Balance exceeds for '{currency}' on {platform}"
                         f" [Limit: {upper_limit} {currency}, Balance: {balance}]")
            fallback_msgs.append(plain_msg)

        message = [
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Platform:*\n {platform}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Currency:*\n {currency}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Limit Type:*\n {limit_type.capitalize()}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Balance:*\n {balance} {currency}"
                    }
                ]
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": f"{plain_msg}",
                }
            },
            {
                "type": "divider"
            }
        ]

        blocks.extend(message)

    return [blocks, fallback_msgs]


def send_slack_message(msg_blocks):
    if not msg_blocks[1]:
        return

    print(f"Sending messages...")

    # Send a message
    client.chat_postMessage(
        channel=BOT_CHANNEL,
        blocks=msg_blocks[0],
        username=BOT_USER_NAME,
        text='/n '.join(msg_blocks[1]),
    )

    print(f"Messages sent")


if __name__ == '__main__':
    try:
        while True:
            response = get_balance()
            msgs = check_limit(response)
            if msgs:
                messages = build_messages(msgs)
                send_slack_message(messages)
            else:
                print(f'Script Running: All currencies are in the acceptable range!')

            # Sleep for 3 minutes
            time.sleep(180)
    except KeyboardInterrupt:
        print('Script Closing: You are now on your own')
