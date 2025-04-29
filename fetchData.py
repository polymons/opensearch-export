from opensearchpy import OpenSearch, helpers, OpenSearchException
from opensearch_dsl import Search
import os
import re
import json
import csv
from datetime import datetime, timedelta
import sys
import traceback
import logging


def load_config(config_path='parameters.json'):
    """
    Load OpenSearch configuration from JSON file
    
    Args:
        config_path (str): Path to the config JSON file
        
    Returns:
        dict: Configuration parameters
    """
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
# Print the OpenSearch username from the configuration
            if 'opensearch' in config and 'connection' in config['opensearch'] and 'username' in config['opensearch']['connection']:
                print(f"Username: {config['opensearch']['connection']['username']}")
        return config
    except Exception as e:
        print(f"Error loading config file: {e}")
        return None


def create_client_from_config(config=None) -> OpenSearch:
    """
    Create an OpenSearch client from config parameters
    
    Args:
        config (dict): Configuration dictionary (loads from file if None)
        
    Returns:
        OpenSearch: Client instance
    """
    if config is None:
        config = load_config()
        
    if not config:
        raise ValueError("Unable to load configuration")
    
    # Get connection parameters
    conn_params = config['opensearch']['connection'].copy()
    
    return create_client(**conn_params)


def create_client(host='localhost', port=9200, username='admin', password='admin', use_ssl=True, verify_certs=False, ssl_show_warn=False) -> OpenSearch:
    """
    Create and return an OpenSearch client
    
    Args:
        host (str): OpenSearch host
        port (int): OpenSearch port
        username (str): Username for authentication
        password (str): Password for authentication
        use_ssl (bool): Whether to use SSL
        verify_certs (bool): Whether to verify SSL certificates
        ssl_show_warn (bool): Whether to show SSL warnings
        
    Returns:
        OpenSearch: Client instance
    """
    client = OpenSearch(
        hosts=[{'host': host, 'port': port}],
        http_auth=(username, password),
        use_ssl=use_ssl,
        verify_certs=verify_certs,
        ssl_show_warn=ssl_show_warn
    )
    return client


def build_query(config):
    """
    Build the OpenSearch query from configuratin
    
    Args:
        config (dict): Configuration dictionary from parameters.json
        
    Returns:
        dict: Query dictionary matching json structure
    """
    # Start with an empty query structure
    query = {
        "query": {
            "bool": {
                "must": []
            }
        }
    }
    
    # First, get the time range from the config
    start_time = config['opensearch']['timespan']['start']
    end_time = config['opensearch']['timespan']['end']
    
    # Add the timestamp range condition directly to must
    timestamp_range = {
        "range": {
            "@timestamp": {
                "gte": start_time,
                "lte": end_time
            }
        }
    }
    query["query"]["bool"]["must"].append(timestamp_range)
    
    # Add any additional bool conditions from config, rather than merging
    if 'query' in config['opensearch'] and 'bool_conditions' in config['opensearch']['query']:
        bool_conditions = config['opensearch']['query']['bool_conditions']
        
        # Check if we have 'must' conditions to add
        if 'must' in bool_conditions:
            # Skip the first element if it's the timestamp range (we already added it)
            start_idx = 1 if bool_conditions['must'] and 'range' in bool_conditions['must'][0] and '@timestamp' in bool_conditions['must'][0]['range'] else 0
            
            # Directly add all other must conditions
            for i in range(start_idx, len(bool_conditions['must'])):
                query["query"]["bool"]["must"].append(bool_conditions['must'][i])
    
    # Add _source filtering if specified
    if 'query' in config['opensearch'] and '_source' in config['opensearch']['query']:
        query['_source'] = config['opensearch']['query']['_source']
    
    return query


def fetch_data(client: OpenSearch, config):
    """
    Fetch data from OpenSearch and process it using pagination for large datasets
    
    Args:
        client (OpenSearch): OpenSearch client instance
        config (dict): Configuration dictionary
    """
    try:
        # Get index and timespan from config
        index = config['opensearch']['index']
        start_time = config['opensearch']['timespan']['start']
        end_time = config['opensearch']['timespan']['end']

        #Correct the time for the query, subtract 2 hours from the start and end time
        start_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S') - timedelta(hours=2)
        end_time = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S') - timedelta(hours=2)

        # Get scroll time
        scroll = config['opensearch'].get('scroll', '5m')
        
        # Get output configuration
        output_config = config['opensearch']['output']
        output_format = output_config.get('format', 'json')
        
        # Use platform-agnostic path handling
        relative_path = output_config.get('file_path', 'output/logstash_data.json')
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(script_dir, os.path.dirname(relative_path))
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        output_path = os.path.join(script_dir, relative_path)
        
        # Adjust output file extension if needed
        if output_format == 'csv' and output_path.endswith('.json'):
            output_path = output_path.replace('.json', '.csv')
        elif output_format == 'json' and output_path.endswith('.csv'):
            output_path = output_path.replace('.csv', '.json')
        #If the output path does not end with the correct extension, add it
        elif output_format == 'json' and not output_path.endswith('.json'):
            output_path += '.json'
        elif output_format == 'csv' and not output_path.endswith('.csv'):
            output_path += '.csv'
        
        # Get batch size from config or use default
        batch_size = output_config.get('batch_size', 1000)
        
        # Process and save the data using stream processing
        query = build_query(config)

        ## Debug print to see the final query
        #print("Final query structure:", json.dumps(query, indent=2))
        # Write query to querysent.json file
        """ try:
            with open('querysent.json', 'w') as f:
                json.dump(query, f, indent=2)
            print("Query written to querysent.json")
        except Exception as e:
            print(f"Error writing query to file: {e}") """
        
        # Use streaming approach for large datasets
        print(f"Starting data extraction from {index} (from {start_time} to {end_time})...")
        print(f"Output will be saved to: {output_path}")
        process_large_dataset(client, index, query, output_format, output_path, batch_size, scroll)
        
    except OpenSearchException as e:
        print(f"OpenSearch error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


def process_large_dataset(client, index, query, format_type, file_path, batch_size=1000, scroll='5m'):
    """
    Process large datasets using the scan/scroll API and write directly to output
    
    Args:
        client (OpenSearch): OpenSearch client
        index (str): Index name or pattern
        query (dict): Query dictionary
        format_type (str): Output format (json or csv)
        file_path (str): Path to save output
        batch_size (int): Number of records per scroll batch
    """
    try:
        # Count total documents matching the query (optional, for progress reporting)
        count_query = query.copy()
        count_query["size"] = 0
        count_result = client.search(body=count_query, index=index)
        estimated_total = count_result.get('hits', {}).get('total', {})
        if isinstance(estimated_total, dict):
            estimated_total = estimated_total.get('value', 0)
        
        print(f"Estimated matching documents: {estimated_total}")

        # Set up output file
        if format_type.lower() == 'json':
            process_json_stream(client, index, query, file_path, batch_size, estimated_total, scroll)
        elif format_type.lower() == 'csv':
            process_csv_stream(client, index, query, file_path, batch_size, estimated_total, scroll)
        else:
            print(f"Unsupported output format: {format_type}")
    except Exception as e:
        print(f"Error during data processing: {e}")
        traceback.print_exc()  # Print the traceback
        raise


def process_json_stream(client, index, query, file_path, batch_size, total_docs, scroll):
    """
    Process data and write directly to JSON file in a streaming manner
    """
    processed_docs = 0
    
    with open(file_path, 'w') as f:
        f.write('[\n')  # Start JSON array
        
        first_item = True
        for hit in helpers.scan(client, query=query, index=index, size=batch_size, scroll=scroll):
            if not first_item:
                f.write(',\n')
            else:
                first_item = False
                
            # Extract source data and add metadata
            doc = hit['_source']
            #doc['_id'] = hit['_id']
            #doc['_index'] = hit['_index']
            
            json.dump(doc, f, default=str)
            
            processed_docs += 1
            if processed_docs % 10000 == 0:
                percentage = (processed_docs / total_docs * 100) if total_docs > 0 else 0
                print(f"Processed {processed_docs} documents ({percentage:.2f}% complete)")
                
        f.write('\n]')  # End JSON array
    
    print(f"Completed processing {processed_docs} documents to {file_path}")


def process_csv_stream(client, index, query, file_path, batch_size, total_docs, scroll):
    """
    Process data and write directly to CSV file in a streaming manner
    """
    try:

        # Extract _source fields from query to define column order
        source_fields = query.get('_source', [])
        if not isinstance(source_fields, list):
            print("Warning: '_source' in query is not a list. Using default field discovery.")
            source_fields = None  # Revert to default behavior
        
        # First, scan ALL data to determine fields
        print("Collecting all field names from the dataset...")
        
        # Set to collect all possible fields
        all_fieldnames = set()
        #all_fieldnames.add('_id')
        #all_fieldnames.add('_index')
        
        # First pass: collect all possible fields from entire dataset
        doc_count = 0
        for hit in helpers.scan(client, query=query, index=index, size=batch_size, scroll=scroll):
            doc_count += 1
            _flatten_fields(hit['_source'], '', all_fieldnames)
            
            # Report progress periodically
            if doc_count % 10000 == 0:
                print(f"Scanned {doc_count} documents to determine fields...")
        
        # Use source_fields order if available, otherwise sort
        if source_fields:
            # Ensure that all fields in source_fields are actually present in the data
            # If a field is missing, it won't be included in the output
            fieldnames = [field for field in source_fields if field in all_fieldnames]
            fieldnames.extend(sorted(list(all_fieldnames - set(fieldnames))))  # Add any missing fields at the end, sorted
            print(f"Using column order from _source: {source_fields}")
        else:
            fieldnames = sorted(list(all_fieldnames))
            
        print(f"Found {len(fieldnames)} unique fields in {doc_count} documents")
        
        # Second pass: write data to CSV with complete fieldnames
        print(f"Writing data to CSV file: {file_path}")
        processed_docs = 0
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';', restval=' ')
            writer.writeheader()
            
            # Rescan the data to write it
            for hit in helpers.scan(client, query=query, index=index, size=batch_size, scroll=scroll):
                flat_result = {}
                _flatten_dict(hit['_source'], '', flat_result)
                
                # Add metadata
                #flat_result['_id'] = hit['_id']
                #flat_result['_index'] = hit['_index']
                
                writer.writerow(flat_result)
                
                processed_docs += 1
                if processed_docs % 10000 == 0:
                    percentage = (processed_docs / total_docs * 100) if total_docs > 0 else 0
                    print(f"Processed {processed_docs} documents ({percentage:.2f}% complete)")
        
        print(f"Completed processing {processed_docs} documents to {file_path}")
        
    except Exception as e:
        print(f"Error processing CSV data: {e}")
        traceback.print_exc()
        raise


def _flatten_fields(d, prefix, fieldnames):
    """Helper function to get all fieldnames from nested dictionaries and arrays"""
    if isinstance(d, dict):
        for k, v in d.items():
            key = f"{prefix}{k}" if prefix else k
            if isinstance(v, dict):
                _flatten_fields(v, f"{key}.", fieldnames)
            elif isinstance(v, list):
                # For lists, we need to examine each item
                # Always add the base field name
                fieldnames.add(key)
                
                # Process each item in the list with its index
                for i, item in enumerate(v):
                    array_key = f"{key}[{i}]"
                    # Add the indexed field
                    fieldnames.add(array_key)
                    
                    # If item is complex, recursively process it
                    if isinstance(item, dict):
                        _flatten_fields(item, f"{array_key}.", fieldnames)
                    elif isinstance(item, list):
                        _flatten_fields(item, f"{array_key}.", fieldnames)
            else:
                fieldnames.add(key)
    elif isinstance(d, list):
        # Handle top-level arrays
        for i, item in enumerate(d):
            array_key = f"{prefix}[{i}]"
            # Add the indexed field
            fieldnames.add(array_key)
            
            # Recursively process complex items
            if isinstance(item, (dict, list)):
                _flatten_fields(item, f"{array_key}.", fieldnames)


def _flatten_dict(d, prefix, result):
    """Helper function to flatten nested dictionaries and arrays for CSV output"""
    if isinstance(d, dict):
        for k, v in d.items():
            key = f"{prefix}{k}" if prefix else k
            if isinstance(v, dict):
                _flatten_dict(v, f"{key}.", result)
            elif isinstance(v, list):
                # Handle arrays by flattening each item with its index
                # Store the base array if needed
                result[key] = str(v) if len(str(v)) < 100 else f"Array with {len(v)} items"
                
                for i, item in enumerate(v):
                    array_key = f"{key}[{i}]"
                    if isinstance(item, dict):
                        # Add the item directly as a value
                        result[array_key] = str(item) if len(str(item)) < 100 else f"Object in array"
                        # And then recursively process its contents
                        _flatten_dict(item, f"{array_key}.", result)
                    elif isinstance(item, list):
                        # Add the nested list directly as a value
                        result[array_key] = str(item) if len(str(item)) < 100 else f"Nested array with {len(item)} items"
                        # And then recursively process its contents
                        _flatten_dict(item, f"{array_key}.", result)
                    else:
                        result[array_key] = item
            else:
                result[key] = v
    elif isinstance(d, list):
        # Handle top-level arrays
        for i, item in enumerate(d):
            array_key = f"{prefix}[{i}]"
            if isinstance(item, dict):
                # Add the item directly as a value
                result[array_key] = str(item) if len(str(item)) < 100 else f"Object in array"
                # And then recursively process its contents
                _flatten_dict(item, f"{array_key}.", result)
            elif isinstance(item, list):
                # Add the nested list directly as a value
                result[array_key] = str(item) if len(str(item)) < 100 else f"Nested array with {len(item)} items" 
                # And then recursively process its contents
                _flatten_dict(item, f"{array_key}.", result)
            else:
                result[array_key] = item


def main():
    """
    Main function
    """
    # Parse command line arguments for optional config path
    config_path = 'parameters.json'

    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    
    # Load config
    config = load_config(config_path)
    if not config:
        print("Failed to load configuration. Exiting.")
        sys.exit(1)
        
    try:
        # Create client using config
        client = create_client_from_config(config)

        # Print OpenSearch version
        version = client.info().get('version', {}).get('number', 'unknown')
        print(f"Connected to OpenSearch version: {version}")

        # Print available indexes
        """ indexes = client.cat.indices()
        for index in indexes:
            print(f" - {index['index']}") """
        
        # Fetch data from OpenSearch using config
        fetch_data(client, config)
    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()