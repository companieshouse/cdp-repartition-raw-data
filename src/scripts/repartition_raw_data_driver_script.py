try:
    from src.modules.helpers.logging_helpers import *
    from src.modules.helpers.datetime_helpers import *
    import sys
    import boto3
    from datetime import datetime
    import uuid
    import logging
    import json
    from awsglue.utils import getResolvedOptions

    print("All imports ok ...")
except Exception as e:
    print(f"Error Imports : {e}")
    raise Exception(f"Error Imports : {e}")

# Set up logging
setup_loggings()
# Initialize logger
logger = logging.getLogger()

# Initialize the S3 client
s3_client = boto3.client('s3')


def append_slash_if_missing(input_string):
    """
    Append a slash to the end of the input string if it does not already end with a slash.
    This is to ensure that the input string is a valid folder path.
    :param input_string: The input string to append a slash to.
    :return: The input string with a slash appended if it does not already end with a slash.
    """
    if not input_string.endswith('/'):
        input_string += '/'
    return input_string


def validate_entry(ingested_table):
    """
    Validate an ingested table entry.
    :param ingested_table: The ingested table entry to validate.
    :return: True if the entry is valid, False otherwise.
    """
    if 'SchemaName' not in ingested_table or 'TableName' not in ingested_table:
        logger.info(f"Skipping invalid entry: {ingested_table}")
        return None, None

    schema_name = ingested_table.get('SchemaName')
    table_name = ingested_table.get('TableName')
    return schema_name, table_name


def get_destination_folder_format(destination_folder, ingestion_date):
    """
    Get the destination folder format for a given table and ingestion date.
    The destination folder format is a string that contains the destination bucket, destination folder,
    and ingestion date.
    :param destination_folder: The destination folder.
    :param ingestion_date: The ingestion date.
    :return: The destination folder format.
    """
    # Append a slash to the destination folder if it does not end with one
    destination_folder_appended = append_slash_if_missing(destination_folder)
    # Get the folder partition format for the ingestion date
    destination_folder_partition = get_folder_partition_format(ingestion_date)
    # Return the destination folder format
    return destination_folder_appended + "{schema_name}/{table_name}/" + destination_folder_partition


def get_source_folder_format(source_folder):
    """
    Get the source folder format for a given table.
    The source folder format is a string that contains the source bucket and source folder.
    :param source_folder: The source folder.
    :return: The source folder format.
    """
    source_folder_appended = append_slash_if_missing(source_folder)  # raw/
    return source_folder_appended + "{schema_name}/{table_name}"


def get_folder_partition_format(ingestion_date):
    """
    Get the folder partition format for a given ingestion date.
    The folder partition format is a string that contains the year, month, and day of the ingestion date.
    :param ingestion_date: The ingestion date.
    :return: The folder partition format.
    """
    # Convert the input date string to a datetime object
    ingestion_date_obj = datetime.strptime(ingestion_date, '%Y-%m-%d')
    # Format the datetime object into the desired format
    return f"year={ingestion_date_obj.year}/month={ingestion_date_obj.month}/day={ingestion_date_obj.day}"


def get_source_objects(s3_client, bucket, source_folder):
    """
    List all objects in a folder in an S3 bucket.
    :param s3_client: The S3 client to use to list the objects.
    :param bucket: The S3 bucket containing the folder.
    :param source_folder: The folder to list the objects in.
    :return: A list of objects in the folder.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=source_folder)
        source_objects = response.get('Contents', [])
        if not source_objects:
            logger.info(f"No objects found in {source_folder}")
            return []
        return source_objects
    except Exception as e:
        logger.error(f"Error listing objects in folder {source_folder}: {str(e)}")
        return []


def copy_s3_object(s3_client, source_bucket, source_key, destination_bucket, destination_key):
    """
    Copy an object from one S3 bucket to another.
    :param s3_client: The S3 client to use to copy the object.
    :param source_bucket: The source S3 bucket containing the object.
    :param source_key: The key of the object in the source S3 bucket.
    :param destination_bucket: The destination S3 bucket to copy the object to.
    :param destination_key: The key of the object in the destination S3 bucket.
    :return: True if the object was copied successfully, False otherwise.
    """
    try:
        s3_client.copy_object(CopySource={'Bucket': source_bucket, 'Key': source_key},
                              Bucket=destination_bucket, Key=destination_key)
        logger.info(f"Copied object from {source_bucket}/{source_key} to {destination_bucket}/{destination_key}")
        return True
    except Exception as e:
        print(f"Error copying object from {source_key} to {destination_key}: {str(e)}")
        return False


def repartition_raw_data(source_bucket, source_folder_format, destination_bucket, destination_folder_format,
                         ingested_tables):
    # Assuming list_objects_in_folder and copy_s3_object are defined elsewhere and s3_client is available in this scope.
    failed_moves = []
    try:
        for ingested_table in ingested_tables:
            schema_name, table_name = validate_entry(ingested_table)
            if schema_name is None or table_name is None:
                continue

            source_folder = source_folder_format.format(schema_name=schema_name, table_name=table_name)
            destination_folder = destination_folder_format.format(schema_name=schema_name, table_name=table_name)

            try:
                source_objects = get_source_objects(s3_client, source_bucket, source_folder)
            except Exception as e:
                logger.error(f"Error listing objects in folder {source_folder}: {e}")
                continue

            if not source_objects:
                logger.info(f"No objects found in {source_folder}")
                continue

            for obj in source_objects:
                source_key = obj['Key']
                destination_key = destination_folder + source_key[len(source_folder):]

                try:
                    if not copy_s3_object(s3_client, source_bucket, source_key, destination_bucket, destination_key):
                        failed_moves.append((source_bucket, source_key))  # Append to failure list
                        logger.info(f"Failed to move {source_key} to {destination_key}")
                except Exception as e:
                    logger.error(f"Error moving {source_key} to {destination_key}: {e}")
                    failed_moves.append((source_bucket, source_key))

    except Exception as e:
        logger.error(f"An unexpected error occurred during data repartitioning: {e}")
        return False  # Return False if an unexpected error occurred

    if failed_moves:
        logger.info("Failed to move the following objects:")
        for bucket, key in failed_moves:
            logger.info(f"Source: {bucket}/{key}")
        return False  # Return False if there were failed moves

    return True  # Return True if all operations succeeded without any exceptions


try:
    # expected argument names
    args_names = ['SOURCE_BUCKET', 'SOURCE_FOLDER', 'INGESTION_DATE', 'INGESTED_TABLES', 'CURRENT_DATE',
                  'DESTINATION_BUCKET', 'DESTINATION_FOLDER']
    # retrieve the arguments
    args = getResolvedOptions(sys.argv, args_names)

    source_bucket = args['SOURCE_BUCKET']
    source_folder = args['SOURCE_FOLDER']
    ingestion_date = args['INGESTION_DATE']
    ingested_tables = args['INGESTED_TABLES']
    current_date = args['CURRENT_DATE']
    destination_bucket = args['DESTINATION_BUCKET']
    destination_folder = args['DESTINATION_FOLDER']

    # Generate the Source and Destination folder based on the provided date parameter
    source_folder_format = get_source_folder_format(source_folder)
    destination_folder_format = get_destination_folder_format(destination_folder, ingestion_date)

    if repartition_raw_data(source_bucket, source_folder_format, destination_bucket, destination_folder_format,
                            json.loads(ingested_tables)):
        logger.info("Successfully repartition raw data.")
    else:
        logger.info("Failed to repartition raw data.")
        raise Exception("Failed to repartition raw data.")
except Exception as e:
    logger.error(f"Error processing Glue job: {str(e)}")
    raise Exception(f"Error processing Glue job: {str(e)}")
