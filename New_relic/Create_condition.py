import requests
import json
from license import user_key

def nerdgraph_inventory(key):
  # GraphQL query to NerdGraph
  condition_create_query = """mutation {
  alertsNrqlConditionStaticCreate(
      accountId: ****
      policyId: 4021269
      condition: {
        enabled: true
        name: "Condition-testing"
        description: null
        nrql: {
          query: "SELECT count(*) FROM NrUsage FACET dateOf(timestamp )"
        }
        expiration: null
        runbookUrl: null
        signal: {
          aggregationDelay: 120
          aggregationMethod: EVENT_FLOW
          aggregationTimer: null
          fillValue: null
          aggregationWindow: 60
          fillOption: NONE
          slideBy: null
          evaluationDelay: null
        }
        terms: [
            {
              operator: ABOVE
              threshold: 1
              priority: WARNING
              thresholdDuration: 36000
              thresholdOccurrences: ALL
            }
        ]
        violationTimeLimitSeconds: 259200
      }
  ) {
    id
  }
   }"""
  
  # NerdGraph endpoint 
  endpoint = "https://api.newrelic.com/graphql"
  headers = {'API-Key': f'{key}'}
  response = requests.post(endpoint, headers=headers, json={"query": condition_create_query})

  if response.status_code == 200:
    # convert a JSON into an equivalent python dictionary
    # dict_response = json.loads(response.content)
    # print(dict_response['data']['actor']['entitySearch']['results']['entities'])

    # optional - serialize object as a JSON formatted stream    
    json_response = json.dumps(response.json(), indent=2)
    print(json_response)
  else:
    # raise an exepction with a HTTP response code, if there is an error
    raise Exception(f'Nerdgraph query failed with a {response.status_code}.')

nerdgraph_inventory(user_key)
