import json 
from datetime import datetime
import re 

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
    try:
        coll_id  = item_dict['collection']
    except KeyError: 
        coll_id  = None
    return item_id, item_bbox, item_links, item_assets, item_properties, coll_id; 

# stac_to_features_properties 
#TODO implement *args and **kwargs for properties function 
def to_features_properties(geocore_features_dict, coll_dict, item_dict,stac_type, root_name,root_id,source,status,maintenance, useLimits_en,useLimits_fr,spatialRepresentation,contact, type_data,topicCategory): 
    properties_dict = geocore_features_dict['properties']
    coll_id, bbox, time_begin, time_end, coll_links, coll_assets, title_en, title_fr, description_en, description_fr, keywords_en, keywords_fr = get_collection_fields(coll_dict)
    if stac_type == 'item':
        item_id, bbox, item_links, item_assets, item_properties,coll_id = get_item_fields(item_dict) 
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
            #title_header = re.search(r"(?<=-)[A-Za-z_]+", item_id).group().replace('_', ' ')
            match = re.search(r"(?<=-)[A-Za-z_]+", item_id)
            if match:
                title_header = match.group().replace('_', ' ')
            else:# Handle the case where no match is found, e.g., set a default value or raise a custom error
                title_header = item_id
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
        #temporalExtent: begin is the datatime, hard coded 'Present'as end   
        properties_dict['temporalExtent'].update({"begin": item_date.strftime("%Y-%m-%d")})   
        properties_dict['temporalExtent'].update({"end": 'Present'})   
        
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
        else:
            properties_dict['temporalExtent'].update({"begin": '0001-01-01'}) 
        
        if time_end:  
            time_end= datetime.strptime(time_end, '%Y-%m-%dT%H:%M:%SZ')
            properties_dict['temporalExtent'].update({"end": time_end.strftime("%Y-%m-%d")})
        else: #hard code end 'Present', required for page to load 
            properties_dict['temporalExtent'].update({"end": 'Present'})
            
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
    # Hard code end 'Present'
    properties_dict['temporalExtent'].update({"end":'Present'})
    properties_dict['temporalExtent'].update({"begin": '0001-01-01'}) 
    return (properties_dict)

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

