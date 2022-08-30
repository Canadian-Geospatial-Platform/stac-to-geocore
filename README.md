# STAC_to_GeoCore
An AWS Lambda that iterates through a S3 Bucket containing STAC items (see [STAC_Harvester](https://github.com/Canadian-Geospatial-Platform/STAC_harvester)) and translate each STAC item to a single GeoCore file. For more details about the GeoCore format, refer to [GeoCore format wiki](https://redmine.gcgeo.gc.ca/redmine/projects/geo-ca/wiki/Current_GeoCore_format). 

STAC items are simply GeoJSON Features with a well-defined set of additional attributes ("foreign members"). For more information, refer to the [STAC Specification](https://github.com/radiantearth/stac-spec). 

## STAC to GeoCore translation rules 

1. For STAC item fields that are 1) non-GeoJSON inheritated and 2) required  
 * **stac_version**: put it to geocore 'features' 
 * **id**: Do not put into GeoCore (it might be a conflict with the "id" in geocore geojson property)  
 * **bbox**: Do not put into GeoCore 
 * **links**: Put into 'properties' 'options'
 * **assets**: Put into 'properties' 'options'. 
 * **collection**: Put into 'properties' 'parentIdentifier' 
2. For STAC item fields that are GeoJSON inheritated 
 * **type**
     * "type": always be 'feature" for item, put it into geocore geojson 'features' 'type' 
 * **geometry**
     * "type": "polygon". "line", or "point", replace  "features" - "geometry" - "type" 
     * "coordinate": STAC item "coordinate" include abundant information that we do not need, we will remove the stac "coordinate" and replace the geocore geojson "coordinate" with the STAC item "bbox" field using format [[ west, south ], [ east, south ], [ east, north ], [ west, north ], [ west, south]] 
 * **properties**
 <br>STAC properties include 1) common fields that are listed in [commond metadata](https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md#instrument) 2) custimized propoerties from STAC extension. 
     * Common STAC properties 
         * Fields can be translate to geocore propertites 
             * "description": GeoCore 'properties' 'description'. we duplicate the english value for the french value. Note sometime, STAC description is put on the first layer of fields 
             * "created" - GeoCore 'properties' 'date' 'created'
             * "updated" - GeoCore 'properties' 'date' 'revision'
             * "datetime": GeoCore 'properties' 'dateStamp' **must include for a STAC Item**  
         * Fields that can not be translated to geocore properties 
             * "title": do not put into GeoCore - we use item id for geocore 'properties' 'title' 
             * "start_datetime": do not put into GeoCore 
             * "end_datetime": do not put into GeoCore
             * "license": do not put into GeoCore
             * "providers":  do not put into GeoCore
             * "platform": do not put into GeoCore
             * "instruments": do not put into GeoCore
             * "constellation": do not put into GeoCore
             * "mission": do not put into GeoCore
             * "gsd": do not put into GeoCore
         * STAC properties from extension - **do not put into GeoCore** 
             * "proj":"epsg"  
             * "proj":"shape" 
             * "proj":"geometry"  These three "proj" are form the [projection extention](https://github.com/stac-extensions/projection/)
     * GeoCore GeoJSON Propertites that are essential for the search
         * "id": source + collection name + item id
         * "title": item id, duplicate the english value for the french value
         * "keywords_en": nullable
         * "topicCategory": nullable
         * "description_en": nullable
         * "sourceSystemName": use source ("ccmeo") 
         * "contact": use source

# Deployment as an image using AWS SAM 
In the Cloud9 terminal (or whatever IDE you are using for building serverless local test)
```
cd stac_to_geocore
sam build 
sam local invoke
```
