import requests

def _search_pages_get(url:str,payload:dict=None)->list:
    """
    A valid list of urls based on stac api link['next'] for the search endpoint
    
    Franklin STAC API generates a next link even when there is no next page
    (https://datacube.services.geo.ca/api/collections/landcover/items)

    This pagenator verifies the validity of the next link and returns a list
    of valid pages.

    Parameters
    ----------
    url : str
        The stac api endpoint.

    Returns
    -------
    pages: list
        A list of valid page urls to paginate through.
    payload: dict
        The POST payload.
        The default is None.
    
    Example
    -------
    url = 'datacube.services.geo.ca/collections/msi/items'
    pages = stac_api_paginate(url)
    for page in pages:
        r = requests.get(page)
        ...

    """
    # Get a list of collections from /collections endpoint
    pages = []
    next_page = url
    returned = 0
    matched = 0
   
    while next_page:
        r = requests.get(next_page)
        if r.status_code == 200:
            j = r.json()                     
            # Test the returns total against total matched
            returned += j['context']['returned']
            matched = j['context']['matched']
            if returned > 0:
                pages.append(next_page)
            if returned < matched:
                links = j['links']
                next_page = _get_next_page(links)
            else:
                next_page = None
        else:
            next_page = None
                            
    r.close()
    return pages

def _get_next_page(links:list):
    """Returns the next page link or None from STAC API links list"""
    next_page = None
    for link in links:
        if link['rel'] == 'next':
            next_page = link['href']
    return next_page
