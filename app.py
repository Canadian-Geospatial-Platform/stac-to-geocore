import requests
import os 
import json
import logging 
import boto3 
from datetime import datetime
from botocore.exceptions import ClientError

  
""" Prod setting 
GEOCORE_TEMPLATE_BUCKET_NAME = os.environ['GEOCORE_TEMPLATE_BUCKET_NAME']
GEOCORE_TEMPLATE_NAME = os.environ['GEOCORE_TEMPLATE_NAME']
GEOCORE_TO_PARQUET_BUCKET_NAME = os.environ['GEOCORE_TO_PARQUET_BUCKET_NAME'] 

""" 

#dev setting 
GEOCORE_TEMPLATE_BUCKET_NAME = 'webpresence-geocore-template-dev'
GEOCORE_TEMPLATE_NAME = 'geocore-format-null-template.json'
GEOCORE_TO_PARQUET_BUCKET_NAME = "webpresence-geocore-json-to-geojson-dev" #s3 for geocore to parquet translation 


 
def lambda_handler(event, context):
    api_root = 'https://datacube.services.geo.ca/api'
    geocore_template_bucket_name = GEOCORE_TEMPLATE_BUCKET_NAME
    geocore_template_name = GEOCORE_TEMPLATE_NAME
    geocore_to_parquet_bucket_name = GEOCORE_TO_PARQUET_BUCKET_NAME
    
    # Hardcoded variables for the STAC to GeoCore translation 
    status = 'unknown'
    maintenance = 'unknown' 
    useLimits_en = 'Open Government Licence - Canada http://open.canada.ca/en/open-government-licence-canada'
    useLimits_fr = 'Licence du gouvernement ouvert - Canada http://ouvert.canada.ca/fr/licence-du-gouvernement-ouvert-canada'
    spatialRepresentation = 'grid; grille'
    type = 'dataset; jeuDonnées'
    topicCategory = 'imageryBaseMapsEarthCover'
    api_name_en = "CCMEO Datacube API"
    api_name_fr = "CCCOT Cube de données API"
    root_name='ccmeo'
    contact = [{
        'organisation':{
            'en':'Government of Canada;Natural Resources Canada;Strategic Policy and Innovation Sector',
            'fr':'Gouvernement du Canada;Ressources naturelles Canada;Secteur de la politique stratégique et de l’innovation'
            }, 
            'email':{
                'en':'geoinfo@nrcan-rncan.gc.ca',
                'fr':'geoinfo@nrcan-rncan.gc.ca'
            }, 
            'individual': None, 
            'position': {
                'en': None,
                'fr': None
                },
            'telephone':{
            'en': None,
            'fr': None
            },
            'address':{
            'en': None,
            'fr': None
            },
            'city':None,
            'pt':{
                'en': None,
                'fr': None
                },
            'postalcode': None, 
            'country':{
                'en': None, 
                'fr': None
                },
            'onlineResources':{
                'onlineResources': None,
                'onlineResources_Name': None,
                'onlineResources_Protocol': None,
                'onlineResources_Description': None 
                },
            'hoursofService': None, 
            'role': None, 
            
        }]
    
    error_msg = ''
    # Check the api connectivity,  
    try: 
        response = requests.get(f'{api_root}/collections/')
    except: 
        error_msg = 'Connectivity issue: error trying to access ' + api_root + '/collections'
        
    if response.status_code == 200:
        #Step 1: Delete records from the lastRun.txt and create an empty lastRun.txt 
        delete_stac_s3(bucket_geojson=geocore_to_parquet_bucket_name, bucket_template=geocore_template_bucket_name)
        print('Creating a new lastRun.txt')
        f = open("lastRun.txt","w+") 
        
        #Step 2: Read the geocore format template 
        template= open_file_s3(geocore_template_bucket_name, geocore_template_name)
        geocore_dict = json.loads(template)
        geocore_features_dict = geocore_dict['features'][0]
        #print(type(geocore_features_dict))
        
        #Step3 load the STAC Collection and item json body 
        # json.loads loads the string data as a dict, the same as r.json see https://stackoverflow.com/questions/58048879/what-is-the-difference-between-json-method-and-json-loads
        str_data = json.loads(response.text)
        collection_data = str_data['collections']
        for collection in collection_data:
            # Extrac collection information for item mapping to GeoCore 
            collection_id = collection['id']
            title_en, title_fr,  description_en, description_fr, keywords_en, keywords_fr = extract_collection(collection)
            """
            #TODO translate collection and write to lastRun.txt 
            collection_name = root_name + '_' + collection_id + '_' + '.geojson'
            upload_json_s3(file_name, bucket=GEOCORE_TO_PARQUET_BUCKET_NAME, json_data=geocore_body_updated, object_name=None)
            f.write(f"{collection_name}\n")       
            """
            #TODO add try catch if reuqest is null 
            item_response = requests.get(f'{api_root}/collections/{collection_id}/items')
            if item_response.status_code == 200: 
                item_str = json.loads(item_response.text)                  
                # FeatureCollection per Feature (STAC item)            
                for feature in item_str['features']:
                    item_id = feature['id']
                    item_geometry = feature['geometry']
                    item_bbox = feature['bbox']
                    item_links = feature['links']
                    item_assets = feature['assets']
                    #item_collection = feature['collection'] #collection_id
                    item_properties = feature['properties']
                    print(f'Start to mapping item_id: {item_id}, collection_id : {collection_id}, title: {title_en}') 
                    # Step 4 STAC item to GeoCore mapping 
                    #TODO add some try catch for the mapping process 
                    # Mapping #1: translate feature_geometry
                    geomery_update = features_geometry(geocore_features_dict, item_bbox, item_geometry)
                    geocore_features_dict.update({"geometry": geomery_update})
                    # Mapping #2: translate feature_properties       
                    properties_dict = features_properties(geocore_features_dict, item_id, item_properties, item_bbox, item_links, item_assets, api_name_en, api_name_fr,title_en, title_fr,description_en,description_fr,
                            keywords_en, keywords_fr,collection_id,type,topicCategory,spatialRepresentation,status,maintenance, 
                            useLimits_en,useLimits_fr,contact)
                    # Mapping #3: update the geocore 
                    geocore_features_dict.update({"properties": properties_dict})
                    geocore_features_dict.update({"geometry": geomery_update})
                    geocore_body_updated = {
                        "type": "FeatureCollection",
                        "features": [geocore_features_dict]
                        }
                    # print(geocore_body_updated)
                      
                    #Step 5: Upload the mapped .geojson to the S3 bucket 
                    item_name = root_name + '_' + collection_id + '_' + item_id + '.geojson'
                    msg = upload_json_s3(item_name, bucket=geocore_to_parquet_bucket_name, json_data=geocore_body_updated, object_name=None)
                    if msg == True: 
                        print(f'Finished mapping : {item_id}, uploaded the file to bucket: {geocore_to_parquet_bucket_name}') 
    
                    # Step 5 Update lastun.txt 
                    f.write(f"{item_name}\n")
                    
                    
        # Step 6: Upload the last Run.txt to the S3 bucket after the datecube is all process 
        f.close()   
        msg = upload_file_s3(filename='lastRun.txt', bucket=geocore_template_bucket_name, object_name=None)
        if msg == True: 
           print(f'Finished mapping the STAC datacube and uploaded the lastRun.txt to bucket: {geocore_template_name}')    
    else:
        error_msg = 'Connectivity is fine but not return a HTTP 200 OK for '+  api_root + '/collections' + ' STAC translation is not initiated'
        #return error_msg
    print(error_msg)

    

# S3 related functions 
# Remove one file from S3 bucket 
def delete_file_s3(filename, bucket): 
    """Delete a file from an S3 bucket
    :param file_name: File to delete
    :param bucket: Bucket for delete file 
    :return: True if file was deleted, else False
    """
    s3 = boto3.resource('s3')
    try: 
        s3object = s3.Object(bucket, filename)
        response = s3object.delete()
        print("Response: ", response)
        print(f"Deleted filenames: {filename} from bucket {bucket}")
    except ClientError as e: 
        logging.error(e)  
        return False    
    return True 


def delete_files_s3(deleted_filelist, bucket):
    """ Delete the geojson files in uuid_deleted_list from a s3 bucket
    Return a message to the user: delete xx uuid from xx bucket 
    :parm uuid_deleted_list: a list of uuid needs to be deleted 
    :parm bucket:bucket to delete from 
    """
    error_msg = None 
    count = 0 
    for filename in deleted_filelist:    
        try: 
            if delete_file_s3(filename, bucket):
                count += 1
        except ClientError as e: 
            logging.error(e)
            error_msg += e
    print('Deleted ', count, " records from S3 ", bucket)
    return error_msg

# Requires open_s3_file(bucket, filename),  s3_list_filenames(bucket), delete_files_s3(filename_list, bucket)
def delete_stac_s3(bucket_geojson, bucket_template):
    error_msg = None 
    filenames_list = list_filenames_s3(bucket_template)    
    if 'lastRun.txt' in filenames_list: 
        lastRun = open_file_s3(bucket_template, 'lastRun.txt')
        #ccmeo_napl-ottawa_napl-ottawa-2001.geojson, or lastRun.replace('\r\n', ' ').split(' ')
        lastRun_list = lastRun.splitlines()
        e = delete_files_s3(deleted_filelist=lastRun_list, bucket=bucket_geojson)
        if e!= None: 
            error_msg += e
    else: 
        print("No existing lastRun.txt")
    return error_msg


# Open files from s3 bucket
def open_file_s3(bucket, filename):
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
    
    
def list_filenames_s3(bucket):
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

# Upload a json file to S3 
def upload_json_s3(filename, bucket, json_data, object_name=None):
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :param json_data: json data to be uploded 
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(filename)
    # boto3.client vs boto3.resources:https://www.learnaws.org/2021/02/24/boto3-resource-client/ 
    s3 = boto3.resource('s3')
    s3object = s3.Object(bucket, filename)
    try: 
        response = s3object.put(Body=(bytes(json.dumps(json_data, indent=4, ensure_ascii=False).encode('utf-8'))))
    except ClientError as e:
        logging.error(e)
        return False 
    return True 

# Upload a (text) file to S3 
def upload_file_s3(filename, bucket, object_name=None):
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(filename)
    # boto3.client vs boto3.resources:https://www.learnaws.org/2021/02/24/boto3-resource-client/ 
    s3_client = boto3.client('s3')
    try: 
        response = s3_client.upload_file(filename, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False 
    return True 




# STAC to GeoCore translation functions 
#stac_to_feature_geometry
def features_geometry(geocore_features_dict, item_bbox, item_geometry): 
    geometry_dict = geocore_features_dict['geometry']
    geometry_dict.update({"type":item_geometry["type"]})
    # bbox[west, south, east, north]
    west = round(item_bbox[0], 2)
    south = round(item_bbox[1],2)
    east = round(item_bbox[2],2)
    north = round(item_bbox[3],2)
    coordinates=[[[west, south], [east, south], [east, north], [west, north], [west, south]]]
    geometry_dict.update({"coordinates":coordinates})
    return geometry_dict

# A function to map STAC links to GeoCore option 
def links_properties_options(item_links, item_id, api_name_en, api_name_fr, title_en, title_fr): 
    links_list = []
    for var in item_links: 
        href = var.get('href')  
        rel = var.get('rel')  
        type = var.get('type')
        if type is None:
            type = 'unknown'
        if type: 
            type_str=type.replace(';', ',')

        if rel == 'collection' or rel == 'dedrived_from':
            continue 
        if rel == 'self':
            name_en = 'Parent - ' + item_id
            name_fr =  'Soi - ' + item_id
        elif rel == 'root':
            name_en = 'Root - ' + api_name_en
            name_fr = 'Racine - ' + api_name_fr
        elif rel == 'parent':
            name_en = 'Parent - ' + title_en 
            name_fr = 'Parente - ' + title_fr
        else: 
            name_en = 'Unknown'
            name_fr = 'Inconnue'
        option_dic = {
                    "url": href,
                    "protocol": 'Unknown',
                    "name":{
                        "en":name_en,
                        "fr":name_fr
                    },
                    "description":{
                        "en":'unknown' + ';' + type_str + ';' +'eng',
                        "fr":'unknown' + ';' + type_str + ';' +'fra'
                    }
                }
        links_list.append(option_dic)
    return (links_list)


# A function to map STAC assets to GeoCore option 
def assets_properties_options(item_assets): 
    assets_list = []
    for var in item_assets: 
        var_dict = item_assets[var]
        href = var_dict.get('href')
        type= var_dict.get('type')
        if type: 
            type_str=type.replace(';', ',')
        name = var_dict.get('title')
        if name:
            try: 
                name_en,name_fr = name.split('/')
            except: 
                name_en = name
                name_fr = None      
        option_dic = {
                    "url": href,
                    "protocol": 'Unknown',
                    "name":{
                        "en":name_en,
                        "fr":name_fr
                    },
                    "description":{
                        "en":'unknown' + ';' + type_str + ';' +'eng',
                        "fr":'unknown' + ';' + type_str + ';' +'fra'
                    }
                }
        assets_list.append(option_dic)
    return (assets_list)

# stac_to_features_properties 
from datetime import datetime
def features_properties(geocore_features_dict, item_id, item_properties, item_bbox, item_links, item_assets, api_name_en, api_name_fr,title_en, title_fr,description_en,description_fr,
                        keywords_en, keywords_fr,collection_id,type,topicCategory,spatialRepresentation,status,maintenance, 
                        useLimits_en,useLimits_fr,contact): 
    properties_dict = geocore_features_dict['properties']
    #properties_dict.keys()
    #id
    properties_dict.update({"id": item_id})
    #title 
    item_date= datetime.strptime(item_properties['datetime'], '%Y-%m-%dT%H:%M:%SZ')
    yr = item_date.strftime("%Y")
    properties_dict['title'].update({"en": yr + '-' + title_en})
    properties_dict['title'].update({"fr": yr + '-' + title_fr})

    #descrption 
    properties_dict['description'].update({"en": description_en})
    properties_dict['description'].update({"fr": description_fr})
    #keywords
    properties_dict['keywords'].update({"en": 'STAC item, ' + keywords_en})
    properties_dict['keywords'].update({"fr": 'STAC item, ' + keywords_fr})
    # topicCategory 
    properties_dict.update({"topicCategory": topicCategory})
    #parentIdentifier 
    properties_dict.update({"parentIdentifier": collection_id})
    # date
    if 'created' in item_properties.keys(): 
        item_created = item_properties['created']
        properties_dict['date']['published'].update({"text": 'publication; publication'})
        properties_dict['date']['published'].update({"date": item_created})
        properties_dict['date']['created'].update({"text": 'creation; création'})
        properties_dict['date']['created'].update({"date": item_created})    
    if 'updated' in item_properties.keys(): 
        item_updated = item_properties['updated']
        properties_dict['date']['revision'].update({"text": 'revision; révision'})
        properties_dict['date']['revision'].update({"date": item_updated})
    #type
    properties_dict.update({"type": type})
    
    #geometry 
    west = round(item_bbox[0], 2)
    south = round(item_bbox[1],2)
    east = round(item_bbox[2],2)
    north = round(item_bbox[3],2)
    geometry_str = "POLYGON((" + str(west) + " " + str(south) +', ' + str(east) +" "+ str(south) + ", " + str(east) +" "+ str(north) + ", " + str(west) +" "+ str(north) + ", " + str(west) +" "+ str(south) + "))"
    properties_dict.update({"geometry":geometry_str})
    #temporalExtent and spatialrepresentation 
    properties_dict.update({"spatialRepresentation":spatialRepresentation})
    properties_dict['temporalExtent'].update({"begin": item_date.strftime("%Y-%m-%d")})
    #skipped: refsys, refSys_version  
    properties_dict.update({"status":status})
    properties_dict.update({"maintenance":maintenance})
    # Skipped metadataStandard, metadataStandardVersion, metadataStandardVersion, graphicOverview, distributionFormat_name, distributionFormat_format
    #useLimits 
    properties_dict['useLimits'].update({"en": useLimits_en})
    properties_dict['useLimits'].update({"fr": useLimits_fr})
    #skipped: accessConstraints, otherConstraints, dateStamp, dataSetURI, locale,language
    #skipped: characterSet, environmentDescription,supplementalInformation
    # Contact
    properties_dict.update({'contact': contact})
    # Skipped: credits, cited, distributor,sourceSystemName
    # options 
    links_list = links_properties_options(item_links, item_id, api_name_en, api_name_fr,title_en, title_fr)
    assets_list = assets_properties_options(item_assets)     
    options_list = links_list+assets_list
    options_list = [i for n, i in enumerate(options_list) if i not in options_list[n + 1:]] # delete duplicates 
    properties_dict.update({'options': options_list})
    return (properties_dict)

#TODO solve error UnboundLocalError: local variable 'collection_title_en' referenced before assignment
def extract_collection(collection_dict): 
    #collection_id = c['id']
    try:
        collection_title = collection_dict['title']
    except KeyError: 
        collection_title = None
    try: 
        collection_description = collection_dict['description']
    except: 
        collection_description = None 
    try: 
        collection_keywords = collection_dict['keywords']
    except: 
        collection_keywords = None 
    if collection_title: 
        try: 
            title_en,title_fr = collection_title.split('/')
        except ValueError: 
            title_en = collection_title
            title_fr = None
    if collection_description: 
        try: 
            description_en,description_fr = collection_description.split('/')
        except ValueError: 
            description_en = collection_description
            description_fr = None
    if collection_keywords: 
        try:
            keywords_en = collection_keywords[0:int(len(collection_keywords)/2)]
            keywords_en = ', '.join(str(l) for l in keywords_en)
            keywords_fr = collection_keywords[int(len(collection_keywords)/2): int(len(collection_keywords))]
            keywords_fr = ', '.join(str(l) for l in keywords_fr)
        except KeyError:
            keywords_en = collection_keywords
            keywords_fr = None
    return title_en, title_fr,  description_en, description_fr, keywords_en, keywords_fr