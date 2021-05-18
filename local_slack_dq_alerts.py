import json
import logging
import requests
import pandas as pd
import awswrangler as wr
from typing import List, Any


class DataQualityAlert:
    def __init__(self, slack_webhook_url: str, database: str = "ecommerce"):
        self.slack_webhook_url = slack_webhook_url
        self.database = database
        self.logger = logging.getLogger(__name__)

    def read_sql(self, query: str) -> pd.DataFrame:
        return wr.athena.read_sql_query(query, database=self.database)

    def send_slack_message(self, text: str) -> None:
        response = requests.post(url=self.slack_webhook_url, data=json.dumps({'text': text}))
        self.logger.info("Sent message to Slack. Status %d - %s. Message: %s", response.status_code, response.reason, text)

    def alert_about_outliers(self, alert_type: str, current_data: List[Any], expected: List[Any]) -> None:
        if sorted(current_data) != sorted(expected):
            detected_outliers = list(set(current_data) - set(expected))
            msg_text = f"{alert_type} check failed. Expected: `{', '.join(expected)}`. " \
                       f"Outliers: `{', '.join(detected_outliers)}`."
            self.send_slack_message(msg_text)
        else:
            self.logger.info("No outliers found. Skipping alert.")


if __name__ == '__main__':
    logging.basicConfig(format="[%(levelname)s] [%(name)s] [%(asctime)s]: %(message)s", level="INFO")
    # aws secretsmanager create-secret --name slack-webhook --secret-string '{"hook_url": "YOUR_HOOK_URL"}'
    default_webhook_url = wr.secretsmanager.get_secret_json("slack-webhook").get("hook_url")
    dqa = DataQualityAlert(slack_webhook_url=default_webhook_url)

    payments = dqa.read_sql('SELECT distinct payment_type FROM order_payments')
    curr_state_in_data = list(payments.payment_type)
    expected = ['boleto', 'credit_card', 'debit_card', 'voucher']
    dqa.alert_about_outliers("Ensure valid payment type", curr_state_in_data, expected)

    orders = dqa.read_sql('SELECT distinct order_status FROM orders')
    curr_state_in_data = list(orders.order_status)
    expected = ['approved', 'canceled', 'created', 'delivered', 'invoiced', 'processing', 'shipped']
    dqa.alert_about_outliers("Ensure valid order status", curr_state_in_data, expected)
