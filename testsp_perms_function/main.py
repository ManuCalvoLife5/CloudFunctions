import functions_framework
import requests
from google.cloud import bigquery
from datetime import datetime
from pytz import timezone
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account.json"
@functions_framework.http
def test_perms(request):

    query = """
    select 
            policy_number,
            market,
        concat(
        if(market like "%WL%", "WL_",if(market like "%SQL%", "SQL_","")),
        ifnull(policy_number,""),"_",
        ifnull(t2.name,""), " ",
        ifnull(t2.last_name,""), " ",
        ifnull(t2.last_name2,"")
        ) as pdf_name

    from EXTERNAL_QUERY('projects/central-octane-307111/locations/europe-west1/connections/bbdd-1-replica', 'SELECT * FROM customer') as t1
    left join EXTERNAL_QUERY('projects/central-octane-307111/locations/europe-west1/connections/bbdd-1-replica', 'SELECT * FROM contact_lead') as t2
        on t1.lead_id = t2.id

    where lead_id in (213865, 215748, 217834)
    """

    results = run_bigquery_query(query)


    #Datetime info to insert in the table
    return results

def run_bigquery_query(query):

  """
  Runs a BigQuery query using a service account for authentication.

  Args:
      service_account_file: Path to the service account JSON file.
      project_id: Your GCP project ID.
      query: The SQL query to be executed.

  Returns:
      A list of dictionaries containing the query results.
  """


  # Create a BigQuery client
  client = bigquery.Client()

  # Run the query
  query_job = client.query(query)

  # Get query results
  results = query_job.result()

  # Convert row results to a list of dictionaries
  rows = [dict(row) for row in results]

  return rows