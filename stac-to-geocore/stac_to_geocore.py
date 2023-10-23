import json 
from datetime import datetime
import re 
import requests

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

# Mapping from STAC assets type to GeoCore Data Resources format  
# STAC media types: https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#common-media-types-in-stac
# Note: there is an 'Other' as a catch-all, other in English, Autre in French
assets_type = {
    'image/tiff; application=geotiff': 'TIFF', 
    'image/tiff; application=geotiff; profile=cloud-optimized': 'TIFF', 
    'image/jp2': 'JPEG 2000 (JP2)', 
    'image/png': 'PNG', 
    'image/jpeg': 'JPEG',
    'text/xml': 'XML',  
    'application/xml':'XML',
    'application/json':'JSON', 
    'text/plain': 'TXT',
    'application/geo+json': 'GeoJSON',
    'application/geopackage+sqlite3': 'GeoPackage (GPKG)',
    'application/x-hdf5': 'HDF',
    'application/x-hdf': 'HDF',
    'application/zip ': 'ZIP'
        } 
# Mapping from STAC assets role to GeoCore Data Resources type  
# STAC roles: https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#asset-role-types
# Note: there is an 'Other' as a catch-all, other in English, Autre in French
# GCGEO content type: API, Application, Dataset, Supporting Document, Web Service

assets_role = {
    'thumbnail': 'Thumbnail',
    'overview' : 'Overview', 
    'data': 'Data', 
    'metadata': 'Metadata'
}

# Mapping from STAC options rel to GeoCore type and format 
# link_rel includes rel in catalog, collection, and item 
links_rel = {
    'item': {
        'type': 'STAC Item / OGC API - Features',
        'format': 'GeoJSON'
        },
    'collection': {
        'type': 'STAC Collection',
        'format': 'JSON'
        },
    'root': {
        'type': 'STAC API',
        'format': 'JSON'
        },
    'drived_from': {
        'type': 'Supporting Document',
        'format': 'JSON'
        },
    'license': {
        'type': 'Supporting Document',
        'format': 'JSON'
        },
    'data': {
        'type': 'STAC Collection',
        'format': 'JSON'
        },
    'service-desc': {
        'type': 'Supporting Document',
        'format': 'JSON'
        },    
    'service-doc': {
        'type': 'Supporting Document',
        'format': 'HTML'
        },        
     'conformance': {
        'type': 'Supporting Document',
        'format': 'JSON'
        },        
     'search': {
        'type': 'Web Service',
        'format': 'GeoJSON'
        }              
}

# STAC to GeoCore translation functions 
def update_dict(target_dict, updates):
    """Utility function to update a dictionary with new key-value pairs.

    Parameters:
    - target_dict: The original dictionary to update.
    - updates: A dictionary containing the updates.

    Returns:
    - The updated dictionary.
    """
    target_dict.update(updates)
    return target_dict

def update_geocore_dict(geocore_features_dict, properties_dict, geometry_dict):
    """Update the GeoCore geocore_features_dict null template with the updated properties and geometry dictionaries.
    
    Parameters:
    - geocore_features_dict: The initial GeoCore dictionary.
    - properties_dict: The updated properties dictionary.
    - geometry_dict: The updated geometry dictionary.
    
    Returns:
    - A new GeoCore dictionary with updated features.
    """
    if not isinstance(properties_dict, dict) or not isinstance(geometry_dict, dict):
        raise ValueError("properties_dict and geometry_dict must be dictionaries.")
    
    updated_dict = geocore_features_dict.copy()
    updated_dict = update_dict(updated_dict, {"properties": properties_dict, "geometry": geometry_dict})
    return {
        "type": "FeatureCollection",
        "features": [updated_dict]
    }

#stac_to_feature_geometry
def to_features_geometry(geocore_features_dict, bbox, geometry_type='Polygon'):
    """Mapping to GeoCore features geometry field.
    
    :param bbox: list of bounding box [west, south, east, north]
    :param geometry_type: string of item or collection type, default is 'Polygon'
    """
    geometry_dict = geocore_features_dict['geometry']
    west, south, east, north = [round(coord, 2) for coord in bbox]
    coordinates=[[[west, south], [east, south], [east, north], [west, north], [west, south]]]
    # Update the geometry dictionary
    updates = {
        "type": geometry_type,
        "coordinates": coordinates
    }
    update_dict(geometry_dict, updates)
    
    return geometry_dict

# A function to map STAC Root links to GeoCore option 
def root_links_to_properties_options(links_list, id, root_name, title_en, title_fr, stac_type): 
    """Mapping STAC Links object to GeoCore features properties options  
    GeoCore options is a json array 
        "options": [
		{
			"url":null,
			"protocol":null,
			"name": {
				"en":null,
				"fr":null
				}
            "description":{
                "en":"type;format;languages",
                "fr":"type;format;languages"
            }
	    }]
	 """
    return_list = []
    root_name_en,root_name_fr = root_name.split('/')
    
    for var in links_list: 
        href, rel, name = var.get('href'), var.get('rel'), var.get('title')
        name_en, name_fr = {
            'self': ('Self - ' + id if stac_type != 'root' else 'Root - ' + root_name_en, 'Soi - ' + id if stac_type != 'root' else 'Racine - ' + root_name_fr),
            'root': ('Root - ' + root_name_en, 'Racine - ' + root_name_fr),
            'parent': ('Parent - ' + title_en if stac_type == 'item' and title_en else 'Parent links', 'Parente - ' + title_fr if stac_type == 'item' and title_fr else 'Parente liens'),
            'child':('Collection - ' + name, 'Collection - ' + name),
            'data': ('Collections Listing', 'Collection Listing')
           # 'service-desc': (name, name), 
           # 'service-doc': (name, name), 
           # 'conformance': (name, name), 
           # 'search': (name, name)
        }.get(rel, (name if name else 'Unknown', name if name else 'Inconnue'))
        # If rel is not a key in the dictionary, it defaults to a tuple where both elements are either the value of name (if name is not None) 
        # or the strings 'Unknown' and 'Inconnue' for English and French, respectively.   
                
        # Convert to options type and format 
        type, format = {
            'self': (links_rel.get('root', {}).get('type'), links_rel.get('root', {}).get('format')),
            'root': (links_rel.get('root', {}).get('type'), links_rel.get('root', {}).get('format')),
            'parent': (links_rel.get('root', {}).get('type'), links_rel.get('root', {}).get('format')),
            'child':(links_rel.get('collection', {}).get('type'), links_rel.get('collection', {}).get('format')),
            'data': (links_rel.get('data', {}).get('type'), links_rel.get('data', {}).get('format')),
            'service-desc': (links_rel.get('service-desc', {}).get('type'), links_rel.get('service-desc', {}).get('format')),
            'service-doc': (links_rel.get('service-doc', {}).get('type'), links_rel.get('service-doc', {}).get('format')),
            'conformance': (links_rel.get('conformance', {}).get('type'), links_rel.get('conformance', {}).get('format')),
            'search': (links_rel.get('search', {}).get('type'), links_rel.get('search', {}).get('format'))
        }.get(rel, ("Other", "Autre"))
                
        if name_en and name_fr:
            option_dic = {
                "url": href,
                "protocol": 'Unknown',
                "name": {"en": name_en, "fr": name_fr},
                "description": {"en": f'{type};{format};eng', "fr": f'{type};{format};fra'}
            }        
            return_list.append(option_dic)
    return (return_list)

# A function to map STAC Collection links to GeoCore option 
def coll_links_to_properties_options(links_list, id, root_name, stac_type): 
    return_list = []
    root_name_en,root_name_fr = root_name.split('/')
    
    for var in links_list: 
        href, rel, name = var.get('href'), var.get('rel'), var.get('title')
        name_en, name_fr = {
            'self': ('Self - ' + id if stac_type != 'root' else 'Root - ' + root_name_en, 'Soi - ' + id if stac_type != 'root' else 'Racine - ' + root_name_fr),
            'root': ('Root - ' + root_name_en, 'Racine - ' + root_name_fr),
            'parent': ('Root - ' + root_name_en, 'Racine - ' + root_name_fr),
            'child':('Item - ' + (name if name is not None else 'Unknown'), 'Item - ' + (name if name is not None else 'Unknown')),
            'item':('Item - ' + (name if name is not None else 'Unknown'), 'Item - ' + (name if name is not None else 'Unknown')),
            'items': ('Items Listing', 'Items Listing')
            # 'license': (name, name), 
            # 'derived_ from': (name, name),
        }.get(rel, (name if name else 'Unknown', name if name else 'Inconnue'))
        # If rel is not a key in the dictionary, it defaults to a tuple where both elements are either the value of name (if name is not None) 
        # or the strings 'Unknown' and 'Inconnue' for English and French, respectively.   
                
        # Convert to options type and format 
        type, format = {
            'self': (links_rel.get('collection', {}).get('type'), links_rel.get('collection', {}).get('format')),
            'root': (links_rel.get('root', {}).get('type'), links_rel.get('root', {}).get('format')),
            'parent': (links_rel.get('root', {}).get('type'), links_rel.get('root', {}).get('format')),
            'child':(links_rel.get('item', {}).get('type'), links_rel.get('item', {}).get('format')),
            'items': (links_rel.get('item', {}).get('type'), links_rel.get('item', {}).get('format')),
            'license': (links_rel.get('license', {}).get('type'), links_rel.get('license', {}).get('format')),
            'derived_ from': (links_rel.get('derived_ from', {}).get('type'), links_rel.get('derived_ from', {}).get('format'))
        }.get(rel, ("Other", "Autre"))
                
        if name_en and name_fr:
            option_dic = {
                "url": href,
                "protocol": 'Unknown',
                "name": {"en": name_en, "fr": name_fr},
                "description": {"en": f'{type};{format};eng', "fr": f'{type};{format};fra'}
            }        
            return_list.append(option_dic)
    return (return_list)
    
# A function to map STAC Item links to GeoCore option 
def item_links_to_properties_options(links_list, id, root_name, coll_id, stac_type): 
    return_list = []
    root_name_en,root_name_fr = root_name.split('/')
    
    for var in links_list: 
        href, rel, name = var.get('href'), var.get('rel'), var.get('title')
        
        # Skip the iteration if rel is 'collection', because it points to a relative url "../collection.json"
        if rel == 'collection':
            continue
        
        name_en, name_fr = {
            'self': ('Self - ' + id if stac_type != 'root' else 'Root - ' + root_name_en, 'Soi - ' + id if stac_type != 'root' else 'Racine - ' + root_name_fr),
            'root': ('Root - ' + root_name_en, 'Racine - ' + root_name_fr),
            'parent': ('Collection - ' + coll_id, 'Collection - ' + coll_id),
            'collection':('Collection - ' + coll_id, 'Collection - ' + coll_id),
            # 'derived_ from': (name, name),
        }.get(rel, (name if name else 'Unknown', name if name else 'Inconnue'))
        # If rel is not a key in the dictionary, it defaults to a tuple where both elements are either the value of name (if name is not None) 
        # or the strings 'Unknown' and 'Inconnue' for English and French, respectively.   
                
        # Convert to options type and format 
        type, format = {
            'self': (links_rel.get('item', {}).get('type'), links_rel.get('item', {}).get('format')),
            'root': (links_rel.get('root', {}).get('type'), links_rel.get('root', {}).get('format')),
            'parent': (links_rel.get('collection', {}).get('type'), links_rel.get('collection', {}).get('format')),
            'collection': (links_rel.get('collection', {}).get('type'), links_rel.get('collection', {}).get('format')),
            'derived_ from': (links_rel.get('derived_ from', {}).get('type'), links_rel.get('derived_ from', {}).get('format'))
        }.get(rel, ("Other", "Autre"))
                
        if name_en and name_fr:
            option_dic = {
                "url": href,
                "protocol": 'Unknown',
                "name": {"en": name_en, "fr": name_fr},
                "description": {"en": f'{type};{format};eng', "fr": f'{type};{format};fra'}
            }        
            return_list.append(option_dic)
    return (return_list)
    
# A function to map STAC assets to GeoCore option 
def assets_to_properties_options(assets_list, assets_type, assets_role): 
    """Mapping STAC Links object to GeoCore features properties options  
    GeoCore options is a json array 
        "options": [
		{
			"url":null,
			"protocol":null,
			"name": {
				"en":null,
				"fr":null
				}
            "description":{
                "en":"type;format;languages",
                "fr":"type;format;languages"
            }
	    }]
    :param assets_list: STAC collection or item assets object
    :return return list: geocore features properties option list  
    """ 
    return_list = []
    for var_dict in assets_list.values():
        href, type_str, name, role = var_dict.get('href'), var_dict.get('type', ''), var_dict.get('title', 'Unknown/Inconnu'), ', '.join(var_dict.get('roles'))
        name_en, name_fr = name.split('/') if '/' in name else (name, name)
        # Convert stac type_str to GeoCore format,return "Other" if type_str is not in assets_type
        format = assets_type.get(type_str,"Other") 
        format_en, format_fr = (format, format) if format != "Other" else ("Other", "Autre")
        # Convert stac role to GeoCore type,return "Other" if role is not in assets_role
        type =  assets_role.get(role,"Other") 
        type_en, type_fr = (type, type) if type != "Other" else ("Other", "Autre")
        #print(f'Type_str is: {type_str},  format: {format}')
        #print(f'role is {role}, type: {type}')     
        option_dic = {
            "url": href,
            "protocol": 'Unknown',
            "name": {"en": f'Asset - {name_en}', "fr": f'Asset - {name_fr}'},
            "description": {"en": f'{type_en};{format_en};eng', "fr": f'{type_fr};{format_fr};fra'}
        }
        return_list.append(option_dic)
        #print(json.dumps(return_list, indent=2))
    return return_list


#root_to_features_properties 
def root_to_features_properties(params, geocore_features_dict): 
    # Get the parameters 
    root_name = params['root_name']
    root_links = params['root_links']
    root_id = params['root_id']
    source = params['source']
    root_des = params['root_des'] 
    root_bbox = params['root_bbox'] 
    status = params['status']
    maintenance = params['maintenance'] 
    useLimits_en = params['useLimits_en']
    useLimits_fr = params['useLimits_fr']
    spatialRepresentation = params['spatialRepresentation']
    contact = params['contact']
    type_data = params['type_data']
    topicCategory = params['topicCategory']
    
    
    properties_dict = geocore_features_dict['properties']
    root_name_en,root_name_fr = root_name.split('/')
    #id
    update_dict(properties_dict, {"id": f"{source}-root-{root_id}"})
    #title 
    update_dict(properties_dict['title'], {"en": f" Root  - {root_name_en}"})
    update_dict(properties_dict['title'], {"fr": f" Racine - {root_name_fr}"})    

    #options
    links_list = root_links_to_properties_options(links_list=root_links, id=root_id, root_name=root_name, title_en=None, title_fr=None, stac_type='root')
    options_list = links_list
    print(f'This is option list before delete duplication: {json.dumps(options_list, indent=2)}')
    options_list = [i for n, i in enumerate(options_list) if i not in options_list[n + 1:]] # delete duplicates
    print(f'This is option list after delete duplication: {json.dumps(options_list, indent=2)}')
    #Descrption 
    en_desc = root_des + '.' + disclaimer_en if root_des else disclaimer_en
    fr_desc = root_des + '.' + disclaimer_fr if root_des else disclaimer_fr
    update_dict(properties_dict['description'], {'en': en_desc, 'fr': fr_desc})
    
    #Keywords 
    keywords_common = 'SpatioTemporal Asset Catalog, stac'
    update_dict(properties_dict['keywords'], {'en': f"{keywords_common}, {source}", 'fr': f"{keywords_common}, {source}"})
 
    #Geometry 
    west, south, east, north = [round(coord, 2) for coord in root_bbox]
    geometry_str = f"POLYGON(({west} {south}, {east} {south}, {east} {north}, {west} {north}, {west} {south}))"
    update_dict(properties_dict, {"geometry": geometry_str})
    
    # Other properties
    update_dict(properties_dict, {
        'topicCategory': topicCategory,
        'type': type_data,
        'spatialRepresentation': spatialRepresentation,
        'status': status,
        'maintenance': maintenance,
        'contact': contact,
        'options': options_list,
        'useLimits': {'en': useLimits_en, 'fr': useLimits_fr},
        'temporalExtent': {'end': 'Present', 'begin': '0001-01-01'}
    })
    #parentIdentifier: None for STAC catalog   
    #date: None for STAC collection 
    #skipped: refsys, refSys_version  
    #skipped metadataStandard, metadataStandardVersion, metadataStandardVersion, graphicOverview, distributionFormat_name, distributionFormat_format
    #skipped: accessConstraints, otherConstraints, dateStamp, dataSetURI, locale,language
    #skipped: characterSet, environmentDescription,supplementalInformation
    #skipped: credits, cited, distributor,sourceSystemName
    return (properties_dict)

#collection_to_features_properties 
def coll_to_features_properties(params, coll_dict,geocore_features_dict): 
    # Get the parameters 
    root_name = params['root_name']
    root_id = params['root_id']
    source = params['source']
    status = params['status']
    maintenance = params['maintenance'] 
    useLimits_en = params['useLimits_en']
    useLimits_fr = params['useLimits_fr']
    spatialRepresentation = params['spatialRepresentation']
    contact = params['contact']
    type_data = params['type_data']
    topicCategory = params['topicCategory']
    
    properties_dict = geocore_features_dict['properties']
    
    coll_id, coll_bbox, time_begin, time_end, coll_links, coll_assets, title_en, title_fr, description_en, description_fr, keywords_en, keywords_fr = get_collection_fields(coll_dict)     
    #id
    update_dict(properties_dict, {"id": source + '-' + coll_id})
    #title 
    if title_en != None and title_fr!= None: 
        update_dict(properties_dict, {'title':{'en':'Collection - ' + title_en, 'fr':'Collection - ' + title_fr}})
        
    #parentIdentifier: root id 
    update_dict(properties_dict, {"parentIdentifier":  source + '-root-'+ root_id})
    #temporalExtent
    time_begin_str = datetime.strptime(time_begin, '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d") if time_begin else '0001-01-01'
    time_end_str = datetime.strptime(time_end, '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d") if time_end else 'Present'
    temporal_extent_updates = {"begin": time_begin_str, "end": time_end_str}
    update_dict(properties_dict['temporalExtent'], temporal_extent_updates)

    #options  
    links_list = coll_links_to_properties_options(links_list=coll_links, id=coll_id, root_name=root_name, stac_type='collection')
    assets_list = assets_to_properties_options(assets_list=coll_assets, assets_type=assets_type, assets_role=assets_role) if coll_assets else []
    options_list = links_list+assets_list
    options_list = [i for n, i in enumerate(options_list) if i not in options_list[n + 1:]] # delete duplicates


    # The shared attributes between Items and Collections  
    description_en_str = f"{description_en or ''} {disclaimer_en}"
    description_fr_str = f"{description_fr or ''} {disclaimer_fr}"
    keywords_en_str = f"SpatioTemporal Asset Catalog, stac, {keywords_en or ''}"
    keywords_fr_str = f"SpatioTemporal Asset Catalog, stac, {keywords_fr or ''}"
    
    #Geometry 
    west, south, east, north = [round(coord, 2) for coord in coll_bbox]
    geometry_str = f"POLYGON(({west} {south}, {east} {south}, {east} {north}, {west} {north}, {west} {south}))"
    
    # Other properties 
    update_dict(properties_dict, {
        "topicCategory": topicCategory, 
        "type": type_data, 
        "spatialRepresentation":spatialRepresentation,
        "status":status,
        "maintenance":maintenance,
        'useLimits': {'en': useLimits_en, 'fr': useLimits_fr},
        'contact': contact,
        'options': options_list, 
        'description': {'en': description_en_str, 'fr': description_fr_str},
        'keywords': {'en': keywords_en_str, 'fr': keywords_fr_str},
        "geometry": geometry_str, 
          
    })
     
    #skipped: date: None for STAC collection 
    #skipped: refsys, refSys_version  
    #skipped metadataStandard, metadataStandardVersion, metadataStandardVersion, graphicOverview, distributionFormat_name, distributionFormat_format
    #skipped: accessConstraints, otherConstraints, dateStamp, dataSetURI, locale,language
    #skipped: characterSet, environmentDescription,supplementalInformation
    #skipped: credits, cited, distributor,sourceSystemName
    # options 
    return (properties_dict)

def get_collection_fields(coll_dict): 
    """Get the collection fields needed for the geocore mapping 
    :param coll_dict: dictionary of a singel STAC collection 
    """
    # Directly extract values using .get() method
    #.get() method allows you to provide a default value (in this case, None) if the key is not found.
    coll_id = coll_dict.get('id')
    coll_title = coll_dict.get('title')
    coll_description = coll_dict.get('description')
    coll_keywords = coll_dict.get('keywords')
    coll_extent = coll_dict.get('extent')
    coll_links = coll_dict.get('links')
    coll_assets = coll_dict.get('assets')
        
    # Get bbox and time 
    coll_bbox, time_begin, time_end = None, None, None
    if coll_extent:
        coll_bbox = coll_extent.get('spatial', {}).get('bbox', [None])[0]
        temporal_interval = coll_extent.get('temporal', {}).get('interval', [[None, None]])[0]
        time_begin, time_end = temporal_interval  
   
    # Get English and French for description, keywords, and title
    title_en, title_fr = (coll_title.split('/') + [coll_id, coll_id])[:2] if coll_title else (coll_id, coll_id)
    description_en, description_fr = (coll_description.split('/') + [None, None])[:2] if coll_description else (None, None)
    
    if coll_keywords:
        half_length = len(coll_keywords) // 2
        keywords_en = ', '.join(str(l) for l in coll_keywords[:half_length])
        keywords_fr = ', '.join(str(l) for l in coll_keywords[half_length:])
    else:
        keywords_en, keywords_fr = None, None
    
    return coll_id, coll_bbox, time_begin, time_end, coll_links, coll_assets, title_en, title_fr, description_en, description_fr, keywords_en, keywords_fr

def create_coll_dict(api_root):
    response_collection = requests.get(f'{api_root}/collections/')
    collection_data_list = response_collection.json().get('collections', [])
    
    coll_id_dict = {
        coll_dict['id']: {
            "title": {'en': fields[6], 'fr': fields[7]},
            'description': {'en': fields[8], 'fr': fields[9]},
            'keywords': {'en': fields[10], 'fr': fields[11]},
        }
        for coll_dict in collection_data_list
        for fields in [get_collection_fields(coll_dict)]
    }
    return coll_id_dict 


#Item_to_features_properties
def item_to_features_properties(params, geocore_features_dict, item_dict, coll_id_dict):
    root_name = params['root_name']
    root_id = params['root_id']
    source = params['source']
    status = params['status']
    maintenance = params['maintenance'] 
    useLimits_en = params['useLimits_en']
    useLimits_fr = params['useLimits_fr']
    spatialRepresentation = params['spatialRepresentation']
    contact = params['contact']
    type_data = params['type_data']
    topicCategory = params['topicCategory']
    
    properties_dict = geocore_features_dict['properties']
    # Get item level lelments 
    item_id, item_bbox, item_links, item_assets, item_properties,coll_id = get_item_fields(item_dict) 
    
    # Get collection level keywords, title, and description 
    coll_data = coll_id_dict.get(coll_id, {})
    title_en = coll_data.get('title', {}).get('en')
    title_fr = coll_data.get('title', {}).get('fr')
    description_en = coll_data.get('description', {}).get('en')
    description_fr = coll_data.get('description', {}).get('fr')
    keywords_en = coll_data.get('keywords', {}).get('en')
    keywords_fr = coll_data.get('keywords', {}).get('fr')
    
    #id
    properties_dict.update({"id": source + '-' + coll_id + '-' + item_id})
    #title 
    item_date= datetime.strptime(item_properties['datetime'], '%Y-%m-%dT%H:%M:%SZ')
    yr = item_date.strftime("%Y")  
    custom_coll = ["monthly-vegetation-parameters-20m-v1", "hrdem-lidar", "hrdem-arcticdem"]
    if title_en != None and title_fr!= None and coll_id not in  custom_coll: 
         update_dict(properties_dict, {'title':{'en':yr + ' - ' + title_en, 'fr':yr + ' - ' + title_fr}})
    elif title_en != None and title_fr!= None and coll_id == "monthly-vegetation-parameters-20m-v1": 
        # month+coll-title
        update_dict(properties_dict, {'title':{'en':item_id.split('-')[-1] + ' - ' + title_en, 'fr':item_id.split('-')[-1] + ' - ' + title_fr}})
    elif title_en != None and title_fr!= None and coll_id == "hrdem-arcticdem" or coll_id == "hrdem-lidar": 
        update_dict(properties_dict, {'title':{'en':yr + ' - ' + item_id + '-' + title_en, 'fr':yr + ' - ' + item_id + '-' + title_fr}})
        
    """
    elif title_en != None and title_fr!= None and coll_id == "hrdem-lidar": 
        # Get the location Seine Rat River from id MB-Seine_Rat_River-1m
        match = re.search(r"(?<=-)[A-Za-z_]+", item_id)
        if match:
            title_header = match.group().replace('_', ' ')
        else:# Handle the case where no match is found, e.g., set a default value or raise a custom error
            title_header = item_id
        update_dict(properties_dict, {'title':{'en':yr + ' ' + title_header + ' - '+ title_en, 'fr':yr + ' ' + title_header + ' - '+ title_fr}}) 
    """
    
    #parentIdentifier
    update_dict(properties_dict, {"parentIdentifier":  source + '-'+ coll_id})
    
    #TemporalExtent 
    if 'created' in item_properties.keys(): 
        item_created = item_properties['created']
        update_dict(properties_dict['date']['published'], {
        "text": 'publication; publication',
        "date": item_created
        })
        
        update_dict(properties_dict['date']['created'], {
        "text": 'creation; création',
        "date": item_created
        })
    #temporalExtent: begin is the datatime, hard coded 'Present'as end   
    update_dict(properties_dict['temporalExtent'], {
    "begin": item_date.strftime("%Y-%m-%d"),
    "end": 'Present'})
    
    #options 
    links_list = item_links_to_properties_options(links_list=item_links, id=item_id, root_name=root_name,coll_id=coll_id, stac_type='item')
    assets_list = assets_to_properties_options(assets_list=item_assets, assets_type=assets_type, assets_role=assets_role) if item_assets else []
    options_list = links_list+assets_list
    options_list = [i for n, i in enumerate(options_list) if i not in options_list[n + 1:]] # delete duplicates
        
    # The shared attributes between Items and Collections  
    description_en_str = f"{description_en or ''} {disclaimer_en}"
    description_fr_str = f"{description_fr or ''} {disclaimer_fr}"
    keywords_en_str = f"SpatioTemporal Asset Catalog, stac, {keywords_en or ''}"
    keywords_fr_str = f"SpatioTemporal Asset Catalog, stac, {keywords_fr or ''}"
    
    #Geometry 
    west, south, east, north = [round(coord, 2) for coord in item_bbox]
    geometry_str = f"POLYGON(({west} {south}, {east} {south}, {east} {north}, {west} {north}, {west} {south}))"
    
    # Other properties 
    update_dict(properties_dict, {
        "topicCategory": topicCategory, 
        "type": type_data, 
        "spatialRepresentation":spatialRepresentation,
        "status":status,
        "maintenance":maintenance,
        'useLimits': {'en': useLimits_en, 'fr': useLimits_fr},
        'contact': contact,
        'options': options_list, 
        'description': {'en': description_en_str, 'fr': description_fr_str},
        'keywords': {'en': keywords_en_str, 'fr': keywords_fr_str},
        "geometry": geometry_str, 
          
    })
     
    #skipped: date: None for STAC collection 
    #skipped: refsys, refSys_version  
    #skipped metadataStandard, metadataStandardVersion, metadataStandardVersion, graphicOverview, distributionFormat_name, distributionFormat_format
    #skipped: accessConstraints, otherConstraints, dateStamp, dataSetURI, locale,language
    #skipped: characterSet, environmentDescription,supplementalInformation
    #skipped: credits, cited, distributor,sourceSystemName
    # options 
    return (properties_dict)


def get_item_fields(item_dict): 
    """Get the collection fields needed for the geocore mapping 
    :param item_dict: dictionary of a singel STAC item  
    """
    item_id = item_dict.get('id')
    item_bbox = item_dict.get('bbox')
    item_links = item_dict.get('links')
    item_assets = item_dict.get('assets')
    item_properties = item_dict.get('properties')
    coll_id = item_dict.get('collection')
    return item_id, item_bbox, item_links, item_assets, item_properties, coll_id; 