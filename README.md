[![Build stac-to-geocore AWS Lambda](https://github.com/Canadian-Geospatial-Platform/stac-to-geocore/actions/workflows/zip-deploy.yaml/badge.svg)](https://github.com/Canadian-Geospatial-Platform/stac-to-geocore/actions/workflows/zip-deploy.yaml)

# stac-to-geocore transformation
SpatioTemporal Asset Catalog (STAC) is a Cloud-native geospatial standard. STAC key components are items, catalogs, collections, and the STAC API. STAC items are simply GeoJSON Features with a well-defined set of additional attributes ("foreign members"). For more information, refer to the [STAC Specification](https://github.com/radiantearth/stac-spec), [STAC page](https://stacspec.org/en), and [STAC index](https://stacindex.org/catalogs)

The STAC harvest and translate process is Harvest -> STAC to GeoCore Tranform -> GeoCore to Parquet -> Geo (search).

STAC_to_GeoCore is an AWS Lambda that iterates through a S3 Bucket containing STAC items (see [STAC_Harvester](https://github.com/Canadian-Geospatial-Platform/STAC_harvester)) and translate each STAC item to a single GeoCore file. For more details about the GeoCore format, refer to [GeoCore format wiki](https://redmine.gcgeo.gc.ca/redmine/projects/geo-ca/wiki/Current_GeoCore_format). 


## STAC to GeoCore translation rules 
STAC items are  GeoJSON Features with a well-defined set of additional attributes (i.e., [GeoJSON features](https://datatracker.ietf.org/doc/html/rfc7946) + additional features). 
1. Mandatory STAC item fields    
 * **stac_version** ->  GeoCore 'features' 
 * **id**: Do not put into GeoCore (it might be a conflict with the "id" in geocore geojson property)  
 * **bbox**: Do not put into GeoCore 
 * **links** -> GeoCore'properties' 'options'
 * **assets** -> GeoCore 'properties' 'options'. 
 * **collection** ->GeoCore 'properties' 'parentIdentifier' 
2. STAC item fields that are GeoJSON inherited features  
 * **type**
     * "type" ->  GeoCore 'features' 'type' ( always be 'feature" for item)
 * **geometry**
     * "type": ->  GeoCore "features" - "geometry" - "type" ("polygon". "line", or "point",)
     * "coordinate" -> STAC item "coordinate" include abundant information that we do not need, we will remove the stac "coordinate" and replace the geocore geojson "coordinate" with the STAC item "bbox" field using format [[ west, south ], [ east, south ], [ east, north ], [ west, north ], [ west, south]] 
 * **properties**
 <br>STAC properties include 1) common fields that are listed in [commond metadata](https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md#instrument) 2) custimized propoerties from STAC extension. 
     * Common STAC properties 
         * Fields can be translate to geocore propertites 
             * "description" -> GeoCore 'properties' 'description'. Duplicate the english value for the french value. Note sometime, STAC description is put on the first layer of fields 
             * "created" -> GeoCore 'properties' 'date' 'created'
             * "updated" -> GeoCore 'properties' 'date' 'revision'
             * "datetime"-> GeoCore 'properties' 'dateStamp' **must include for a STAC Item**  
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
             * "proj":"geometry"  These three "proj" are form the [projection extention](https://github.com/stac-extensions/
3. GeoCore mandatory fields for geo.ca search
    * "properties.id" -> "source" + "collection name" + "item id"
    * "properties.title_en/fr" -> "item id", duplicate the english value for the french value
    * "properties.keywords_en/fr": "SpatialTemporal Asset Catalogs (STAC)",nullable
    * "properties.topicCategory": nullable
    * "properties.description_en/fr": nullable
    * "sourceSystemName" -> "source" ("ccmeo") 
    * "properties.contact.organization.en/fr" -> "source"
    * "properties.contact.role" ->
    * "properties.geometry" -> translated from STAC item "bbox"

# Deployment as an image using AWS SAM 
In the Cloud9 terminal (or whatever IDE you are using for building serverless local test)
```
cd stac_to_geocore
sam build 
sam local invoke
```
