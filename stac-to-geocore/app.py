import requests
import os 
import re 
import json
import logging 
import boto3 
from datetime import datetime
from botocore.exceptions import ClientError

from pagination import *
from s3_operations import *
from stac_to_geocore import *

"""
# environment variables for lambda
geocore_template_bucket_name = os.environ['GEOCORE_TEMPLATE_BUCKET_NAME']
geocore_template_name = os.environ['GEOCORE_TEMPLATE_NAME']
geocore_to_parquet_bucket_name = os.environ['GEOCORE_TO_PARQUET_BUCKET_NAME']
api_root = os.environ['STAC_API_ROOT']
root_name = os.environ['ROOT_NAME']
source = os.environ['SOURCE']
"""

#dev setting  -- comment out for release
geocore_template_bucket_name = 'webpresence-geocore-template-dev'
geocore_template_name = 'geocore-format-null-template.json'
geocore_to_parquet_bucket_name = "webpresence-geocore-json-to-geojson-dev" #s3 for geocore to parquet translation 
api_root = 'https://datacube.services.geo.ca/api'
root_name = "CCMEO Datacube API / CCCOT Cube de données API" #must provide en and fr 
source='ccmeo'


 
def lambda_handler(event, context):
    """STAC harvesting and mapping workflow 
        1. Before harvesting the stac records, we delete the previous harvested stac records logged in lastRun.txt.
        2. Create an empty lastRun.txt to log the current harvest
        3. Harvest and translate STAC catalog (root api endpoint)
        4. Loop through each STAC collection, harvest the collection json body and then mapp collection to GeoCore
        5. Loop through items within the collection, harvest the item json bady and map item to GeoCore. 
        6. Update lastRun.txt 
    """
    error_msg = ''
    
    #Change directory to /tmp folder, required if new files are created for lambda 
    os.chdir('/tmp')    
    if not os.path.exists(os.path.join('mydir')):
        os.makedirs('mydir')

    # Before harvesting the STAC api, we check the root api connectivity first   
    try: 
        response = requests.get(f'{api_root}/collections/')
        print(response)
    except: 
        error_msg = 'Connectivity issue: error trying to access: ' + api_root + '/collections'
    
    # Start the harvest and translation process if connection is okay 
    if response.status_code == 200:
        #Delete previous harvest included in lastRun.txt 
        e = delete_stac_s3(bucket_geojson=geocore_to_parquet_bucket_name, bucket_template=geocore_template_bucket_name)
        if e != None: 
            error_msg += e
        # Create a new log file and write into log file for each sucessfull harvest 
        print('Creating a new lastRun.txt')
        with open('/tmp/lastRun.txt', 'w') as f:
            #Catalog    
            response_root = requests.get(f'{api_root}/')
            #root_data = json.loads(response_root.text)
            root_data = response_root.json()
            root_id = root_data['id']
            if root_id.isspace()==False:
                root_id=root_id.replace(' ', '-')
            root_des = root_data['description']
            root_links = root_data['links']
            # GeoCore properties bounding box is a required for frontend, here we use the first collection
            #TBD using first collection bounding box could cause potential issues when collections have different extent, a solution is required. 
            str_data = response.json()
            collection_data = str_data['collections']
            coll_bbox = collection_data[1]['extent']['spatial']['bbox'][0] 
            
            # Get null geocore features body as a dictionary 
            geocore_features_dict = get_geocore_template(geocore_template_bucket_name,geocore_template_name) 
            # Mapping to geocore features geometry and properties 
            root_geometry_dict =to_features_geometry(geocore_features_dict, bbox=coll_bbox, geometry_type='Polygon')
            root_properties_dict = root_to_features_properties(geocore_features_dict,root_name, root_links, root_id, source, root_des, coll_bbox, status,maintenance, useLimits_en,useLimits_fr,spatialRepresentation,contact, type_data,topicCategory)
            # Update the geocore body and finish mapping 
            root_geocore_updated = update_geocore_dict(geocore_features_dict=geocore_features_dict, properties_dict =root_properties_dict ,geometry_dict=root_geometry_dict)
            # upload the stac geocore to a S3 
            root_upload = source + '-root-' + root_id + '.geojson'
            msg = upload_file_s3(root_upload, bucket=geocore_to_parquet_bucket_name, json_data=root_geocore_updated, object_name=None)
            if msg == True: 
                print(f'Finished mapping root : {root_id}, uploaded the file to bucket: {geocore_to_parquet_bucket_name}')    
                f.write(f"{root_upload}\n") 
        
            # Collection mapping 
            for coll_dict in collection_data:
                coll_id, coll_bbox, time_begin, time_end, coll_links, coll_assets, title_en, title_fr,  description_en, description_fr, keywords_en, keywords_fr = get_collection_fields(coll_dict=coll_dict)   
            
                coll_features_dict = get_geocore_template(geocore_template_bucket_name, geocore_template_name)
                coll_geometry_dict =to_features_geometry(geocore_features_dict=coll_features_dict, bbox=coll_bbox, geometry_type='Polygon')
                coll_properties_dict = to_features_properties(geocore_features_dict=coll_features_dict, coll_dict=coll_dict, item_dict=None,stac_type='collection', root_name=root_name, root_id = root_id,source=source, 
                                                          status=status,maintenance=maintenance, useLimits_en=useLimits_en,
                                                          useLimits_fr=useLimits_fr,spatialRepresentation=spatialRepresentation,contact=contact, type_data=type_data,topicCategory=topicCategory)
                coll_geocore_updated = update_geocore_dict(geocore_features_dict=coll_features_dict, properties_dict =coll_properties_dict, geometry_dict=coll_geometry_dict)
            
                coll_name = source + '-' + coll_id + '.geojson'
                msg = upload_file_s3(coll_name, bucket=geocore_to_parquet_bucket_name, json_data=coll_geocore_updated, object_name=None)
                if msg == True: 
                    print(f'Finished mapping Collection : {coll_id}, and uploaded the file to bucket: {geocore_to_parquet_bucket_name}')
                    f.write(f"{coll_name}\n")    
                    
            #Item with paginate 
            pages = search_pages_get(url=api_root +'/search')
            for page in pages: 
                #Each page has 30 items 
                r = requests.get(page)
                j = r.json()
                items_list = j['features']
                for item in items_list: 
                    #item is a dict
                    item_id, item_bbox, item_links, item_assets, item_properties,coll_id = get_item_fields(item)
                    print(f'Start to mapping item_id: {item_id}, collection_id : {coll_id}, title: {title_en}')
                    
                    """
                    Map items to collections 
                    1) get collection id from 'collection'
                    2) Pre-build a dictionaru of {collection id: collection name}
                    3) Fix to_features_properties lin 176-line199
                    """
                    
                    #TODO add error handling for the item mapping to geocore 
                    item_features_dict = get_geocore_template(geocore_template_bucket_name,geocore_template_name)
                    item_geometry_dict =to_features_geometry(geocore_features_dict=item_features_dict, bbox=item_bbox, geometry_type='Polygon')
                    item_properties_dict = to_features_properties(geocore_features_dict=item_features_dict, coll_dict=coll_dict, item_dict=item,stac_type='item', root_name=root_name, root_id=root_id, source=source,
                                                                  status=status,maintenance=maintenance, useLimits_en=useLimits_en,
                                                                  useLimits_fr=useLimits_fr,spatialRepresentation=spatialRepresentation,contact=contact, type_data=type_data,topicCategory=topicCategory) 
                    item_geocore_updated = update_geocore_dict(geocore_features_dict=item_features_dict, properties_dict =item_properties_dict ,geometry_dict=item_geometry_dict)
                    item_name = source + '-' + coll_id + '-' + item_id + '.geojson'
                    msg = upload_file_s3(item_name, bucket=geocore_to_parquet_bucket_name, json_data=item_geocore_updated, object_name=None)
                    if msg == True: 
                        print(f'Finished mapping item : {item_id}, uploaded the file to bucket: {geocore_to_parquet_bucket_name}')  
                        f.write(f"{item_name}\n")
        f.close()
        msg = upload_file_s3(filename='lastRun.txt', bucket=geocore_template_bucket_name, json_data = None, object_name=None)
        if msg == True: 
            print(f'Finished mapping the STAC datacube and uploaded the lastRun.txt to bucket: {geocore_template_name}')   
    else:
        error_msg = 'Connectivity is fine but not return a HTTP 200 OK for '+  api_root + '/collections' + ' STAC translation is not initiated'
        #return error_msg
    print(error_msg)


# requires open_file_s3()
def get_geocore_template(geocore_template_bucket_name,geocore_template_name):
    """Getting GeoCore null template from S3 bucket  
    :param geocore_template_bucket_name: bucket name tht stores the geocore template file 
    :param geocore_template_name: geocore template file name
    :return: geocore feature in dictionary format    
    """  
    template= open_file_s3(geocore_template_bucket_name, geocore_template_name)
    geocore_dict = json.loads(template)
    geocore_features_dict = geocore_dict['features'][0]
    return geocore_features_dict  