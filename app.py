import requests
import os 
import json
import logging 
import boto3 
from botocore.exceptions import ClientError

# Local setting, this needs to be change to environment variable for Lambda deployment  
STAC_BUCKET_NAME = "stac-harvest-json-dev"
GEOCORE_TEMPLATE_BUCKET_NAME = "stac-harvest-geocore-geojson-dev" 
STAC_GEOCORE_BUCKET_NAME = "stac-translate-to-geocore-geojson-dev" 
BUCKET_LOCATION = "ca-central-1"
SOURCE = 'ccmeo'


def lambda_handler(event, context):
    filename_list = s3_filenames(STAC_BUCKET_NAME)
    source = SOURCE  

    # Load GeoCore null example 
    filename_geocore_list = s3_filenames(GEOCORE_TEMPLATE_BUCKET_NAME)
    filename_geocore = filename_geocore_list[filename_geocore_list.index("geocore-geojson-format-null_v6.json")]
    
    geocore_body= open_s3_file(GEOCORE_TEMPLATE_BUCKET_NAME, filename_geocore)
    geocore_body_dict = json.loads(geocore_body)
    geocore_features_dict = geocore_body_dict['features'][0] 
    #print(json.dumps(geocore_body_dict, indent = 4, sort_keys=False)) #Pretty Printing JSON string back
    
    message = ""
    count = 0 

    for filename in filename_list:
        item_body= open_s3_file(STAC_BUCKET_NAME, filename)
        # Need to addree None here? 
        item_body_dict = json.loads(item_body) # convert json string to a python dic
        item_fields_list = list(item_body_dict.keys())
        #print(item_fields_list)
        
        try: 
            # Step 1: STAC (stac_version, type, geometry) to GeoCore features 
            updated_geocore_features_dict = stac_to_geocore_features(geocore_features_dict, item_body_dict)
        except: 
             message += "Some error occured translating step 1."
             print(f'Some error occured translating step 1 for filename {filename}')
        
        try:
            # Step 2: STAC (assets and links) to GeoCore 'features' 'properties' 'options'
            geocore_features_properties_dict = updated_geocore_features_dict['properties']
            if "assets" in item_fields_list or "links" in item_fields_list:
                geocore_features_properties_dict = stac_to_geocore_properties_options(geocore_features_properties_dict, item_body_dict)
        except: 
            message += "Some error occured translating step 2."
            print(f'Some error occured translating step 2 for filename {filename}')
         
        try:     
            # Step 3: STAC (collection, properties) - GeoCore 'features' 'properties' 
            geocore_features_properties_dict = stac_to_geocore_properties(geocore_features_properties_dict,item_body_dict,source,filename)
        except: 
            message += "Some error occured translating step 3."
            print(f'Some error occured translating step 3 for filename {filename}')
        
        try: 
            # Step 4 eturn the updated geocore full body 
            geocore_updated_body = return_updated_geocore_body(updated_geocore_features_dict,geocore_features_properties_dict)
            #print(json.dumps(geocore_updated_body, indent = 4, sort_keys=False)) #Pretty Printing JSON string 
        except: 
            message += "Some error occured translating step 3."
            print(f'Some error occured translating step 4 for filename {filename}')
        
        # Step 5: Upload the updated geocore json to S3 
        filenames1 = filename.split(".")[0] + ".geojson"
        upload_json_s3(filenames1, bucket=STAC_GEOCORE_BUCKET_NAME, json_data=geocore_updated_body, object_name=None)
        count += 1 
        print("STAC items have been translated into geocore file '" + filenames1 + "' in " + STAC_GEOCORE_BUCKET_NAME)
        
    if message == "":
        message += str(count) + " STAC items have been translated into geocore file '" + filenames1 + "' in " + STAC_GEOCORE_BUCKET_NAME
    print (message)

    
# Translate step 1 STAC (stac_version, type, geometry) to GeoCore features 
def stac_to_geocore_features(geocore_features_dict, item_body_dict):
    """Add STAC Item field 'stac_version','type', and 'geometry' to GeoCore 'features' array  
    :param geocore_features_dict: geocore geojson features body as a dict object
    :param item_body_dict: the item body as a dict object
    :return: updated geocore features body as a dict object  
    """
    updated_geocore_features_dict = geocore_features_dict
    item_fields_list =  ["stac_version", "type", "geometry"] 
    for field in item_fields_list: 
        # How to address error when field does not exist in item_body_dict? 
        field_body = item_body_dict[item_fields_list[item_fields_list.index(field)]]
        #check if file is empty. if so, skip this iteration
        if field_body == None:
            continue 
        # daupte(): update value if the key exists, if key not exist, add a new key 
        if field == "geometry":
            geometry_dict = geocore_features_dict[field]
            geometry_dict.update({"type":field_body["type"]})
            # bbox[west, south, east, north]
            bbox = item_body_dict["bbox"]
            west = round(bbox[0], 2)
            south = round(bbox[1],2)
            east = round(bbox[2],2)
            north = round(bbox[3],2)
            coordinates=[[[west, south], [east, south], [east, north], [west, north], [west, south]]]
            geometry_dict.update({"coordinates":coordinates})
            updated_geocore_features_dict.update({"geometry":geometry_dict})
        else: 
            updated_geocore_features_dict.update({field:field_body})
    return updated_geocore_features_dict 
    
    
# Translate step 2 : STAC (assets and links) to GeoCore 'features' 'properties' 'options'  
def stac_to_geocore_properties_options(geocore_features_properties_dict, item_body_dict):
    """Add STAC Item field 'assets','links' to GeoCore 'features' 'properties' 'options' array  
    :param geocore_features_properties_dict: geocore geojson features properties body as a dict object
    :param item_body_dict: the item body as a dict object
    :return: updated geocore features body as a dict object  
    """
    options_list = []
    item_fields = ["links", "assets"]
    
    for field in item_fields: 
        field_body = item_body_dict[item_fields[item_fields.index(field)]]
        #check if file is empty. if so, skip this iteration
        if field_body == None:
            continue 
        
        for value in field_body: 
            #print(value)
            if isinstance(value, dict): # links: list of dicts, value is a dict  
                value_dict = value
            elif isinstance(value, str): #assets: dict of dicts, value/key is a string 
                value_dict = field_body[value]
            
            # Replace value with the match value in 'Option'. 
            # get() can return None if key does not exist, so replacement could be None  
            url = value_dict.get('href')  
            name = value_dict.get("title")      
            option_dic = {
                "url": url,
                "protocol": None,
                "name":{
                    "en":name,
                    "fr":name
                }
            }
            options_list.append(option_dic)
            
    geocore_features_properties_dict.update({"options":options_list})
    return geocore_features_properties_dict

# Translate step 3: STAC (collection, properties) - GeoCore 'features' 'properties' 
def stac_to_geocore_properties(geocore_features_properties_dict,item_body_dict,source,filename):
    """Add ecerything in STAC Item that should put into GeoCore 'features' 'properties' array  
    :param geocore_features_properties_dict: geocore geojson features properties body as a dict object
    :param item_body_dict: the item body as a dict object
    :param source: source of STAC 
    :param filename: STAC item filename
    :return: updated geocore features body as a dict object  
    """
    
    item_properties_body = item_body_dict['properties']
    item_fields_list = item_body_dict.keys()
    
    if "collection" in item_fields_list: 
        parentIdentifier = item_properties_body["collection"]
    else: 
        parentIdentifier = None 

    description = item_properties_body.get("description")
    # If None, try search the description from the STAC item first layer fields
    if description is None: 
        description = item_body_dict.get("description") 
    #print(f'description:  {description}')

    date_created = item_properties_body.get("created")
    date_updated = item_properties_body.get("updated")
    datetime = item_properties_body.get("datetime") 

    collection_name = filename.split(".")[0].split("_")[0]
    item_id = filename.split(".")[0].split("_")[1]
    #print(f'The collection name is {collection_name}, the item name is {item_name}')

    title_dict = {
        "en": item_id,
        "fr": item_id
            }
    description_dict = {
        "en": description,
        "fr": description
            }
    date = {
        "published": {
            "text": None,
            "date": None
        },
        "created": {
            "text": 'creation; création',
            "date": date_created
        },
        "revision": {
            "text": 'revision; révision',
            "date": date_updated
        },
        "notavailable": {
            "text": None,
            "date": None
        },
        "inforce": {
            "text": None,
            "date": None
       },
        "adopted": {
            "text": None,
            "date": None
        },
        "deprecated": {
            "text": None,
            "date": None
        },
        "superceded": {
            "text": None,
            "date": None
        }
     }
    # Update the fields value in the geocore properties 
    geocore_features_properties_dict.update({"id": source + "_" + collection_name + "_" + item_id})
    geocore_features_properties_dict.update({"title":title_dict})
    geocore_features_properties_dict.update({"description":description_dict})
    geocore_features_properties_dict.update({"parentIdentifier":parentIdentifier})
    geocore_features_properties_dict.update({"date":date})
    geocore_features_properties_dict.update({"dateStamp":datetime})
    geocore_features_properties_dict.update({"sourceSystemName":source})
    geocore_features_properties_dict.update({"contact":[source]})
    #print(json.dumps(geocore_features_properties_dict, indent = 4, sort_keys=False)) #Pretty Printing JSON string
    
    return geocore_features_properties_dict
    
# Translate step 4: Updated geocore full body 
def return_updated_geocore_body(updated_geocore_features_dict,geocore_features_properties_dict):
    """Add STAC Item field 'assets','links' to GeoCore 'features' 'properties' 'options' array  
    :param updated_geocore_features_dict: updated geocore geojson features body after step 1
    :param geocore_features_properties_dict: updated geocore features properties body after step 2&3 
    :return: updated geocore body as a dict object  
    """
    # Put back the updated properties to geocore features 
    updated_geocore_features_dict.update({"properties": geocore_features_properties_dict})
    #print(json.dumps(updated_geocore_features_dict, indent = 4, sort_keys=False)) #Pretty Printing JSON string 
    
    geocore_body_updated = {
        "type": "FeatureCollection",
        "features": [updated_geocore_features_dict]

    }
    return geocore_body_updated
    
  
# Translate step 5: Upload the translted geocore body to S3 bucket 
def upload_json_s3(file_name, bucket, json_data, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :param json_data: json data to be uploded 
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)
    # boto3.client vs boto3.resources:https://www.learnaws.org/2021/02/24/boto3-resource-client/ 
    s3 = boto3.resource('s3')
    s3object = s3.Object(bucket, file_name)
    try: 
        response = s3object.put(Body=(bytes(json.dumps(json_data, indent=4, ensure_ascii=False).encode('utf-8'))))
    except ClientError as e:
        logging.error(e)
        return False 
    return True 

# Save all the filenames in a S3 bucket as a list 
def s3_filenames(bucket):
    """ List a S3 bucket to obtain file names 
    Note: if there are too many records (>999) to pricessm we may need to paginate 
    :parm bucket: name of the bucket 
    :return a list of filenames within the bucket 
    """
    s3 =boto3.resource("s3")
    my_bucket = s3.Bucket(bucket)
    filename_list = []
    count = 0 
    for my_bucket_object in my_bucket.objects.all():
        #print(my_bucket_object.key)
        filename_list.append(my_bucket_object.key)
        count += 1 
    print(f"{count} files are included in the bucket {bucket}")
    return filename_list

# Open files from s3 bucket
def open_s3_file(bucket, filename):
    """Open a S3 file from bucket and filename and return the body as a string
    :param bucket: Bucket name
    :param filename: Specific file name to open
    :return: body of the file as a string
    """
    try: 
        """
        s3 = boto3.client("s3")
        bytes_buffer = io.BytesIO()
        s3.download_fileobj(Key=filename, Bucket=bucket, Fileobj=bytes_buffer)
        file_body = bytes_buffer.getvalue().decode() #python3, default decoding is utf-8
        #print (file_body)
        #print(type(file_body))
        """
        # Second option to load file from S3 buckets 
        s3 = boto3.resource('s3')
        content_object = s3.Object(bucket, filename)
        file_body= content_object.get()['Body'].read().decode('utf-8')
        #json_content = json.loads(file_content)
        
        return str(file_body)
    except ClientError as e:
        logging.error(e)
        return False 
