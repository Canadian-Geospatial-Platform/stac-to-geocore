import requests
import os 
import re 
import json
import logging 
import boto3 
from datetime import datetime
from botocore.exceptions import ClientError

""" environment variables for lambda
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
    
# Hardcoded variables for the STAC to GeoCore translation 
status = 'unknown'
maintenance = 'unknown' 
useLimits_en = 'Open Government Licence - Canada http://open.canada.ca/en/open-government-licence-canada'
useLimits_fr = 'Licence du gouvernement ouvert - Canada http://ouvert.canada.ca/fr/licence-du-gouvernement-ouvert-canada'
spatialRepresentation = 'grid; grille'
type_data = 'dataset; jeuDonnées'
topicCategory = 'imageryBaseMapsEarthCover'
disclaimer_en = '\\n\\n**This third party metadata element follows the Spatio Temporal Asset Catalog (STAC) specification.**'
disclaimer_fr = '\\n\\n**Cet élément de métadonnées tiers suit la spécification Spatio Temporal Asset Catalog (STAC).** **Cet élément de métadonnées provenant d’une tierce partie a été traduit à l\'aide d\'un outil de traduction automatisée (Amazon Translate).**'
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
 
def lambda_handler(event, context):
    error_msg = ''
    # Before harvesting the STAC api, we check the api connectivity first   
    try: 
        response = requests.get(f'{api_root}/collections/')
        print(response)
    except: 
        error_msg = 'Connectivity issue: error trying to access: ' + api_root + '/collections'
    if response.status_code == 200:
        """STAC harvesting and mapping workflow 
        Before harvesting the stac records, we delete the previous harvested stac records logged in lastRun.txt.
        Then creating an empty lastRun.txt to log the current harvest following 
        - Harvest and translate STAC catalog (root api endpoint)
        - Loop through each STAC collection, harvest the collection json body and then mapp collection to GeoCore
        - Loop through items within the collection, harvest the item json bady and map item to GeoCore. 
        """ 
        e = delete_stac_s3(bucket_geojson=geocore_to_parquet_bucket_name, bucket_template=geocore_template_bucket_name)
        if e != None: 
            error_msg += e
        print('Creating a new lastRun.txt')
        #f = open("lastRun.txt","r+")         
        str_data = json.loads(response.text)
        collection_data = str_data['collections']
              
        #Catalog    
        response_root = requests.get(f'{api_root}/')
        root_data = json.loads(response_root.text)   
        root_id = root_data['id']
        if root_id.isspace()==False:
            root_id=root_id.replace(' ', '-')
        root_des = root_data['description']
        root_links = root_data['links']
        coll_bbox = collection_data[1]['extent']['spatial']['bbox'][0] # required for geocore properties bounding box, here we use the first collection 
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
            print(f'Finished mapping root : {root_id}, and uploaded the file to bucket: {geocore_to_parquet_bucket_name}')    
           # f.write(f"{root_upload}\n") 
        
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
                #f.write(f"{coll_name}\n")        
            # STAC item to GeoCore mapping 
            #TODO add error handling if reuqest is null 
            item_response = requests.get(f'{api_root}/collections/{coll_id}/items')
            if item_response.status_code == 200: 
                item_str = json.loads(item_response.text)                  
                # FeatureCollection per Feature (STAC item)            
                for item_dict in item_str['features']:
                    item_id, item_bbox, item_links, item_assets, item_properties = get_item_fields(item_dict)
                    print(f'Start to mapping item_id: {item_id}, collection_id : {coll_id}, title: {title_en}')
                     
                    #TODO add error handling for the item mapping to geocore 
                    item_features_dict = get_geocore_template(geocore_template_bucket_name,geocore_template_name)
                    item_geometry_dict =to_features_geometry(geocore_features_dict=item_features_dict, bbox=item_bbox, geometry_type='Polygon')
                    item_properties_dict = to_features_properties(geocore_features_dict=item_features_dict, coll_dict=coll_dict, item_dict=item_dict,stac_type='item', root_name=root_name, root_id=root_id, source=source,
                                                                  status=status,maintenance=maintenance, useLimits_en=useLimits_en,
                                                                  useLimits_fr=useLimits_fr,spatialRepresentation=spatialRepresentation,contact=contact, type_data=type_data,topicCategory=topicCategory) 
                    item_geocore_updated = update_geocore_dict(geocore_features_dict=item_features_dict, properties_dict =item_properties_dict ,geometry_dict=item_geometry_dict)
                    item_name = source + '-' + coll_id + '-' + item_id + '.geojson'
                    msg = upload_file_s3(item_name, bucket=geocore_to_parquet_bucket_name, json_data=item_geocore_updated, object_name=None)
                    if msg == True: 
                        print(f'Finished mapping item : {item_id}, uploaded the file to bucket: {geocore_to_parquet_bucket_name}')  
                        #f.write(f"{item_name}\n")       
        # Step 6: Upload the last Run.txt to the S3 bucket after the datecube is all process 
        #f.close()   
        #msg = upload_file_s3(filename='lastRun.txt', bucket=geocore_template_bucket_name, json_data = None, object_name=None)
        if msg == True: 
            print(f'Finished mapping the STAC datacube and uploaded the lastRun.txt to bucket: {geocore_template_name}')    
    else:
        error_msg = 'Connectivity is fine but not return a HTTP 200 OK for '+  api_root + '/collections' + ' STAC translation is not initiated'
        #return error_msg
    print(error_msg)


def delete_filelist_s3(deleted_filelist, bucket):
    """ Delete the STAC JSON files in deleted_filelist from an s3 bucket
    Return a message to the user: "Deleted xx records from S3 yy bucket"
    :parm deleted_filelist: a list of s3 files to be deleted 
    :parm bucket: s3 bucket to delete from 
    """
    s3 = boto3.resource('s3')
    error_msg = None 
    count = 0
    for filename in deleted_filelist:    
        try: 
            s3object = s3.Object(bucket, filename)
            response = s3object.delete()
            count += 1
            #print("Response: ", response)
            #print(f"Deleted filenames: {filename} from bucket {bucket}")
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
        e = delete_filelist_s3(deleted_filelist=lastRun_list, bucket=bucket_geojson)
        if e != None: 
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
    s3 = boto3.resource("s3")
    my_bucket = s3.Bucket(bucket)
    filename_list = []
    count = 0 
    for my_bucket_object in my_bucket.objects.all():
        #print(my_bucket_object.key)
        filename_list.append(my_bucket_object.key)
        count += 1 
    print(f"{count} files are included in the bucket {bucket}")
    return filename_list


# Upload a a text or json file to S3 
def upload_file_s3(filename, bucket, json_data, object_name=None):
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param json_data: json_data to be updated, can be none 
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(filename)
    # boto3.client vs boto3.resources:https://www.learnaws.org/2021/02/24/boto3-resource-client/ 
    s3_client = boto3.client('s3')  
    if json_data: 
        try:
            response = s3_client.put_object(Body=(bytes(json.dumps(json_data, indent=4, ensure_ascii=False).encode('utf-8'))), 
                                            Bucket=bucket,
                                            Key = filename)
        except ClientError as e:
            logging.error(e)
            return False    
    else:     
        try: 
            response = s3_client.upload_file(filename, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False 
    return True 

# STAC to GeoCore translation functions 
#stac_to_feature_geometry
def to_features_geometry(geocore_features_dict, bbox,  geometry_type='Polygon'):
    """Mapping to GeoCore features geometry field
    :param bbox: list of bounding box [west, south, east, north]
    :param geometry_type: string of item or collection type, always be Polygon  
    """
    geometry_dict = geocore_features_dict['geometry']
    geometry_dict.update({"type":geometry_type})
    west = round(bbox[0], 2)
    south = round(bbox[1],2)
    east = round(bbox[2],2)
    north = round(bbox[3],2)
    coordinates=[[[west, south], [east, south], [east, north], [west, north], [west, south]]]
    geometry_dict.update({"coordinates":coordinates})
    return geometry_dict

# A function to map STAC links to GeoCore option 
def links_to_properties_options(links_list, id, root_name, title_en, title_fr, stac_type): 
    """Mapping STAC Links object to GeoCore features properties options  
    :param links_list: STAC collection or item links object
    :param id: collection id or item id 
    :param api_name_en/api_name_fr: STAC datacube English/French nama, hardcoded variables 
    :param coll_title: 
    :param stac_type: item or collection 
    """   
    return_list = []
    root_name_en,root_name_fr = root_name.split('/')
    for var in links_list: 
        href = var.get('href')  
        rel = var.get('rel')  
        type_str = var.get('type')
        name=var.get('title')
        if type_str: 
            type_str=type_str.replace(';', ',') # for proper display on metadata page 
        # rel type: https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#relation-types
        if rel == 'collection' or rel == 'dedrived_from':
            continue 
        if rel == 'self'and stac_type != 'root':
            name_en = 'Self - ' + id
            name_fr =  'Soi - ' + id
        elif rel == 'self' and stac_type == 'root':
            name_en = 'Root - ' + root_name_en
            name_fr = 'Racine - ' + root_name_fr
        elif rel == 'root':
            name_en = 'Root - ' + root_name_en
            name_fr = 'Racine - ' + root_name_fr
        elif stac_type == 'item' and rel == 'parent' and title_en!=None and title_fr!=None:
            name_en = 'Parent - ' + title_en 
            name_fr = 'Parente - ' + title_fr
        elif stac_type == 'collection' and rel == 'parent':
            name_en = 'Parent links '  
            name_fr = 'Parente liens' 
        elif rel == 'items': # this rel type is only for collection 
            name_en = 'Items listings' 
            name_fr = 'Èléments la liste' 
        elif name:
            name_en = name
            name_fr = name 
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
        return_list.append(option_dic)
    return (return_list)

# A function to map STAC assets to GeoCore option 
def assets_to_properties_options(assets_list): 
    """Mapping STAC Links object to GeoCore features properties options  
    :param assets_list: STAC collection or item assets object
    :return return list: geocore features properties option list  
    """ 
    return_list = []
    for var in assets_list: 
        var_dict = assets_list[var]
        href = var_dict.get('href')
        type_str= var_dict.get('type')
        if type_str: 
            type_str=type_str.replace(';', ',')
        name = var_dict.get('title')
        if name:
            try: 
                name_en,name_fr = name.split('/')
            except: 
                name_en = name
                name_fr = name      
        option_dic = {
                    "url": href,
                    "protocol": 'Unknown',
                    "name":{
                        "en":'Asset - ' + name_en,
                        "fr":'Asset - ' + name_fr
                    },
                    "description":{
                        "en":'unknown' + ';' + type_str + ';' +'eng',
                        "fr":'unknown' + ';' + type_str + ';' +'fra'
                    }
                }
        return_list.append(option_dic)
    return (return_list)

# stac_to_features_properties 
#TODO implement *args and **kwargs for properties function 
# stac_to_features_properties 
def to_features_properties(geocore_features_dict, coll_dict, item_dict,stac_type, root_name,root_id,source,status,maintenance, useLimits_en,useLimits_fr,spatialRepresentation,contact, type_data,topicCategory): 
    properties_dict = geocore_features_dict['properties']
    coll_id, bbox, time_begin, time_end, coll_links, coll_assets, title_en, title_fr, description_en, description_fr, keywords_en, keywords_fr = get_collection_fields(coll_dict)
    if stac_type == 'item':
        item_id, bbox, item_links, item_assets, item_properties = get_item_fields(item_dict) 
         #id
        properties_dict.update({"id": source + '-' + coll_id + '-' + item_id})
        #title 
        item_date= datetime.strptime(item_properties['datetime'], '%Y-%m-%dT%H:%M:%SZ')
        yr = item_date.strftime("%Y")  
        custom_coll = ["monthly-vegetation-parameters-20m-v1", "hrdem-lidar"]
        if title_en != None and title_fr!= None and coll_id not in  custom_coll: 
            properties_dict['title'].update({"en": yr + ' - ' + title_en})
            properties_dict['title'].update({"fr": yr + ' - ' + title_fr})
        elif title_en != None and title_fr!= None and coll_id == "monthly-vegetation-parameters-20m-v1": 
            properties_dict['title'].update({"en": item_id.split('-')[-1] + ' - ' + title_en})
            properties_dict['title'].update({"fr": item_id.split('-')[-1] + ' - ' + title_fr})
            print('test properti title is ', properties_dict['title'])
        elif title_en != None and title_fr!= None and coll_id == "hrdem-lidar": 
            title_header = re.search(r"(?<=-)[A-Za-z_]+", item_id).group().replace('_', ' ')
            properties_dict['title'].update({"en": yr + ' ' + title_header + ' - '+ title_en})
            properties_dict['title'].update({"fr": yr + ' ' + title_header + ' - '+ title_fr})
            print('test properti title is ', properties_dict['title'])
        #parentIdentifier
        properties_dict.update({"parentIdentifier": source + '-'+ coll_id})
        #date
        if 'created' in item_properties.keys(): 
            item_created = item_properties['created']
            properties_dict['date']['published'].update({"text": 'publication; publication'})
            properties_dict['date']['published'].update({"date": item_created})
            properties_dict['date']['created'].update({"text": 'creation; création'})
            properties_dict['date']['created'].update({"date": item_created})
            properties_dict['temporalExtent'].update({"begin": item_date.strftime("%Y-%m-%d")})
        #temporalExtent: only begin date for itmem 
        properties_dict['temporalExtent'].update({"begin": item_date.strftime("%Y-%m-%d")})   
        #options  
        links_list = links_to_properties_options(links_list=item_links, id=item_id, root_name=root_name, title_en=title_en, title_fr=title_fr, stac_type='item')
        if item_assets:
            assets_list = assets_to_properties_options(assets_list=item_assets)
            options_list = links_list+assets_list
        else: 
            options_list = links_list
        options_list = [i for n, i in enumerate(options_list) if i not in options_list[n + 1:]] # delete duplicates

    else:       
        #id
        properties_dict.update({"id": source + '-' + coll_id})
        #title 
        if title_en != None and title_fr!= None: 
            properties_dict['title'].update({"en": 'Collection - ' + title_en})
            properties_dict['title'].update({"fr": 'Collection - ' + title_fr})
        #parentIdentifier: root id 
        properties_dict.update({"parentIdentifier":  source + '-root-'+ root_id})
        # date: None for STAC collection 
        # Type: hardcoded 
        #temporalExtent
        if time_begin: 
            time_begin= datetime.strptime(time_begin, '%Y-%m-%dT%H:%M:%SZ')
            properties_dict['temporalExtent'].update({"begin": time_begin.strftime("%Y-%m-%d")})
        if time_end:  
            time_end= datetime.strptime(time_end, '%Y-%m-%dT%H:%M:%SZ')
            properties_dict['temporalExtent'].update({"end": time_end.strftime("%Y-%m-%d")})
        #options  
        links_list = links_to_properties_options(links_list=coll_links, id=coll_id, root_name=root_name, title_en=title_en, title_fr=title_fr, stac_type='item')
        if coll_assets:
            assets_list = assets_to_properties_options(assets_list=coll_assets)
            options_list = links_list+assets_list
        else: 
            options_list = links_list
        options_list = [i for n, i in enumerate(options_list) if i not in options_list[n + 1:]] # delete duplicates
        
        
    # The shared attributes between Items and Collections  
    #descrption 
    if description_en!= None and description_fr != None: 
        properties_dict['description'].update({"en": description_en + ' ' + disclaimer_en})
        properties_dict['description'].update({"fr": description_fr + ' ' + disclaimer_fr})
    else: 
        properties_dict['description'].update({"en": disclaimer_en})
        properties_dict['description'].update({"fr": disclaimer_fr})
     #keywords
    if keywords_en!= None and keywords_fr != None: 
        properties_dict['keywords'].update({"en": 'SpatioTemporal Asset Catalog, ' + 'stac, ' + keywords_en})
        properties_dict['keywords'].update({"fr": 'SpatioTemporal Asset Catalog, ' + 'stac, ' + keywords_fr})
    # topicCategory 
    properties_dict.update({"topicCategory": topicCategory})
    properties_dict.update({"type": type_data})
    #geometry 
    west = round(bbox[0], 2)
    south = round(bbox[1],2)
    east = round(bbox[2],2)
    north = round(bbox[3],2)
    geometry_str = "POLYGON((" + str(west) + " " + str(south) +', ' + str(east) +" "+ str(south) + ", " + str(east) +" "+ str(north) + ", " + str(west) +" "+ str(north) + ", " + str(west) +" "+ str(south) + "))"
    properties_dict.update({"geometry":geometry_str})
    #Spatialrepresentation 
    properties_dict.update({"spatialRepresentation":spatialRepresentation})      
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
    properties_dict.update({'options': options_list})
    return (properties_dict)

def get_collection_fields(coll_dict): 
    """Get the collection fields needed for the geocore mapping 
    :param coll_dict: dictionary of a singel STAC collection 
    """
    
    try:
        coll_id = coll_dict['id']
    except KeyError: 
        coll_id = None
    try:
        coll_title = coll_dict['title']
    except KeyError: 
        coll_title = None
    try: 
        coll_description = coll_dict['description']
    except KeyError: 
        coll_description = None 
    try: 
        coll_keywords = coll_dict['keywords']
    except KeyError: 
        coll_keywords = None 
    try: 
        coll_extent = coll_dict['extent']
    except KeyError: 
        coll_extent = None 
    try: 
        coll_links = coll_dict['links']
    except KeyError: 
        coll_links = None 
    try:
        coll_assets = coll_dict['assets']
    except KeyError: 
        coll_assets = None
        
    # Get bbox and time 
    if coll_extent: 
        try: 
            coll_bbox = coll_extent['spatial']['bbox'][0]
            time_begin = coll_extent['temporal']['interval'][0][0]
            time_end = coll_extent['temporal']['interval'][0][1]
        except: 
            coll_bbox = None 
            time_begin = None 
            time_end = None   
    else:  
        coll_bbox = None 
        time_begin = None 
        time_end = None      
    # get En and fr for des, keywords, and title 
    if coll_title: 
        try: 
            title_en,title_fr = coll_title.split('/')
        except ValueError: 
            title_en = coll_title
            title_fr = None
    else: 
        title_en = coll_id # Title can not be none for geocoer search 
        title_fr = coll_id
    if coll_description: 
        try: 
            description_en,description_fr = coll_description.split('/')
        except ValueError: 
            description_en = coll_description
            description_fr = None
    else: 
        description_en = None
        description_fr = None
    if coll_keywords: 
        try:
            keywords_en = coll_keywords[0:int(len(coll_keywords)/2)]
            keywords_en = ', '.join(str(l) for l in keywords_en)
            keywords_fr = coll_keywords[int(len(coll_keywords)/2): int(len(coll_keywords))]
            keywords_fr = ', '.join(str(l) for l in keywords_fr)
        except KeyError:
            keywords_en = coll_keywords
            keywords_fr = None
    else:
        keywords_en = None
        keywords_fr = None
    return coll_id, coll_bbox, time_begin, time_end, coll_links, coll_assets, title_en, title_fr,  description_en, description_fr, keywords_en, keywords_fr;  


def get_item_fields(item_dict): 
    """Get the collection fields needed for the geocore mapping 
    :param item_dict: dictionary of a singel STAC item  
    """
    try:
        item_id  = item_dict['id']
    except KeyError: 
        item_id  = None
    try:
        item_bbox = item_dict['bbox']
    except KeyError:
        item_bbox = None 
    try: 
        item_links = item_dict['links']
    except KeyError: 
        item_links = None 
    try: 
        item_assets = item_dict['assets']
    except KeyError: 
        item_assets = None 
    try: 
        item_properties = item_dict['properties']
    except KeyError:
        item_properties = None 
    return item_id, item_bbox, item_links, item_assets, item_properties; 

def root_to_features_properties(geocore_features_dict,root_name, root_links, root_id,source,root_des, coll_bbox, status,maintenance, useLimits_en,useLimits_fr,spatialRepresentation,contact, type_data,topicCategory): 
    properties_dict = geocore_features_dict['properties']
    root_name_en,root_name_fr = root_name.split('/')
    #id
    properties_dict.update({"id": source + '-root-' + root_id})
    #title = root name 
    properties_dict['title'].update({"en": 'Root  - ' + root_name_en})
    properties_dict['title'].update({"fr": 'Racine - ' + root_name_fr})
    #parentIdentifier: None for STAC catalog   
    #properties_dict.update({"parentIdentifier": None})
    #date: None for STAC collection 
    #Type: hardcoded 
    #options  
    links_list = links_to_properties_options(links_list=root_links, id=root_id, root_name=root_name, title_en=None, title_fr=None, stac_type='root')
    options_list = links_list
    options_list = [i for n, i in enumerate(options_list) if i not in options_list[n + 1:]] # delete duplicates
        
        
    # The shared attributes between Items and Collections  
    #descrption 
    if root_des!= None: 
        properties_dict['description'].update({"en": root_des + '.' + disclaimer_en})
        properties_dict['description'].update({"fr": root_des + '.' + disclaimer_fr })
    else: 
        properties_dict['description'].update({"en": disclaimer_en})
        properties_dict['description'].update({"fr": disclaimer_fr })
     #keywords
    properties_dict['keywords'].update({"en": 'SpatioTemporal Asset Catalog, ' + 'stac, '+ source})
    properties_dict['keywords'].update({"fr": 'SpatioTemporal Asset Catalog, ' + 'stac, ' + source})
    # topicCategory 
    properties_dict.update({"topicCategory": topicCategory})
    properties_dict.update({"type": type_data})
    #geometry 
    west = round(coll_bbox[0], 2)
    south = round(coll_bbox[1],2)
    east = round(coll_bbox[2],2)
    north = round(coll_bbox[3],2)
    geometry_str = "POLYGON((" + str(west) + " " + str(south) +', ' + str(east) +" "+ str(south) + ", " + str(east) +" "+ str(north) + ", " + str(west) +" "+ str(north) + ", " + str(west) +" "+ str(south) + "))"
    properties_dict.update({"geometry":geometry_str})
    #Spatialrepresentation 
    properties_dict.update({"spatialRepresentation":spatialRepresentation})      
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
    properties_dict.update({'options': options_list})
    return (properties_dict)

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

def update_geocore_dict(geocore_features_dict, properties_dict,geometry_dict):
    """Update the GeoCore geocore_features_dict null template with the updated propoerties dict and geometry dict  
    :param geocore_features_dict: null template of geocore_features_dict in dict format  
    :param properties_dict: mapped/updatedd STAC geocore properties dict 
    :param geometry_dict: mapped/updatedd STAC geocore geometry dict
    :return: STAC geocore dict     
    """ 
    geocore_features_dict.update({"properties": properties_dict})
    geocore_features_dict.update({"geometry": geometry_dict})
    geocore_updated = {
        "type": "FeatureCollection",
        "features": [geocore_features_dict]
            }
    return geocore_updated
