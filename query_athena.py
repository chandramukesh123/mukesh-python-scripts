"""
Query athena on CUR
Author: Sudharshan
"""
import os
import time
from datetime import date

import boto3


def run_query(client, query, database, catalog, s3_output):
    """
    Run the query on athena
    :param client: aws athena client
    :param query:
    :param database: db name
    :param s3_output: output location on s3
    :return:
    """
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database,
            'Catalog': 'AwsDataCatalog'
        },
        ResultConfiguration={
            'OutputLocation': s3_output,
        }
    )
    return response['QueryExecutionId']


def status_query(client, exec_id):
    """
    Get the status of the query running on athena
    :param client:
    :param exec_id:
    :return:
    """
    response = client.get_query_execution(QueryExecutionId=exec_id)
    if response['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        return True
    elif response['QueryExecution']['Status']['State'] == 'FAILED':
        print("Execution Failed")
        exit(1)
    else:
        return False


def monthly_report_query(client, database, catalog, s3_output):
    today = date.today()
    this_month, this_year = today.month, today.year
    # last_month = (today - relativedelta.relativedelta(months=+1))
    if this_month == 1:
        last_month = 12
        last_year = this_year - 1
    else:
        last_month = this_month - 1
        last_year = this_year

    start_date = date(year=this_year, month=this_month, day=1).isoformat()
    end_date = date(year=last_year, month=last_month, day=1).isoformat()
    query = """
                SELECT resource_tags_user_name AS ResourceName, SUM(line_item_unblended_cost) AS Cost
                FROM perfios_aws_cur_report
                WHERE line_item_usage_start_date >= CAST('""" + end_date + """' AS DATE)
                        AND line_item_usage_end_date <= CAST('""" + start_date + """' AS DATE)
                        AND resource_tags_user_name IS NOT NULL
                        AND resource_tags_user_name <> ''
                GROUP BY  resource_tags_user_name
                ORDER BY  SUM(line_item_unblended_cost) DESC 
            """
    execution_id = run_query(client, query, database, catalog, s3_output)

    while not status_query(client, execution_id):
        time.sleep(5)

    return execution_id


def download_results(s3, execution_id, bucket, key):
    today = date.today().isoformat()
    a = s3.download_file(bucket, key + "/" + execution_id + ".csv", os.path.join(os.getcwd(), "data", today+".csv"))


def main():
    os.environ["AWS_CONFIG_FILE"] = os.path.join(os.getcwd(), ".aws/config")
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = os.path.join(os.getcwd(), ".aws/credentials")

    today = date.today()
    session = boto3.session.Session(profile_name='default', region_name='ap-south-1')
    client = session.client('athena')
    bucket_name = "perfios-billing"
    key = "monthly-report/results/" + today.isoformat()
    s3_output = "s3://" + bucket_name + "/" + key
    catalog = 'AwsDataCatalog'
    database = 'athenacurcfn_perfios_a_w_s_c_u_r_report'
    execution_id = monthly_report_query(client, database, catalog, s3_output)
    client = session.client('s3')
    download_results(client, execution_id, bucket_name, key)


if __name__ == '__main__':
    main()
