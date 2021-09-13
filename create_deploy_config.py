import argparse
import json
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey

#import config

def query_items(container, doc_id):
    print('\n1.4 Querying for an  Item by Id\n')

    # enable_cross_partition_query should be set to True as the container is partitioned
    items = list(container.query_items(
        query="SELECT * FROM r WHERE r.id=@id",
        parameters=[
            { "name":"@id", "value": doc_id }
        ],
        enable_cross_partition_query=True
    ))
    
    return items


def run(HOST, MASTER_KEY, DATABASE_ID, CONTAINER_ID, OUTPUT_FILE, QUERY_ID = None):

    client = cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY} )
    try:
        # setup database for this sample
        try:
            db = client.create_database(id=DATABASE_ID)

        except exceptions.CosmosResourceExistsError:
            db = client.get_database_client(DATABASE_ID)
        
        # setup container for this sample
        try:
            container = db.create_container_if_not_exists(id=CONTAINER_ID, partition_key=PartitionKey(path='/id', kind='Hash'))
            print('Container with id \'{0}\' created'.format(CONTAINER_ID))

        except exceptions.CosmosResourceExistsError:
            print('Container with id \'{0}\' was found'.format(CONTAINER_ID))

        # query item
        if QUERY_ID == None:
            items = query_items(container, "00000")
            latest_id = items[0].get("latest_id")
            items = query_items(container, latest_id)
        else:
            items = query_items(container, QUERY_ID)
        
        #print(items[0])
        print(items[0].get("backend_config"))
        with open(OUTPUT_FILE, 'w') as outfile:
            json.dump(items[0].get("backend_config"), outfile)

    except exceptions.CosmosHttpResponseError as e:
        print('\nrun_sample has caught an error. {0}'.format(e.message))

    finally:
            print("\nrun_sample done")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Program parameters needed on this application')

    parser.add_argument('--cosmosdb_host', required=True, type=str, help='The name of this device')
    parser.add_argument('--master_key', required=True, type=str, help='The name of this device')
    parser.add_argument('--database_id', required=True, type=str, help='The name of this device')
    parser.add_argument('--container_id', required=True, type=str, help='The cycle time to execute this application')
    parser.add_argument('--query_id', required=False, type=str, help='The cycle time to execute this application')
    parser.add_argument('--output_file', required=True, type=str, help='The cycle time to execute this application')

    args = parser.parse_args()

    #run(args.cosmosdb_host, args.master_key, args.database_id, args.container_id, args.query_id)
    run(args.cosmosdb_host, args.master_key, args.database_id, args.container_id, args.output_file)