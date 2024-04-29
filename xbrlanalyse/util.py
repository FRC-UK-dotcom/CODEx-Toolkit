import re
from urllib.parse import urljoin, urlparse
import requests
import os
import tempfile

temp_dir = None

def is_url(url):
    return re.match('https?://', url) is not None

#
# Join the components of the specified path or URL
#
def url_path_join(*args):
    if is_url(args[0]):
        url = args[0]
        for part in args[1:]:
            if not url.endswith('/'):
                url += '/'
            url = urljoin(url, part) 
        return url
        
    else:
        return os.path.join(*args)

#
# Load a JSON file from the specified path or URL
#
def load_json(url_or_path):
    if is_url(url_or_path):
        r = requests.get(url_or_path)
        # r.raise_for_status()
        if r.status_code < 400:   
            return r.json()
        else:
            return None
    else:
        with open(url_or_path, "r") as fin:
            return json.load(fin)

# Only works to save locally currently but should add option to POST file to URL.
# Op append ('a' or 'w')
# data - list of lists
# hdrs - list
def saveCSV(url_or_path, op, data, hdrs=None):
    import csv

    with open('url_or_path' + '.csv', op, newline='') as f:
        wr = csv.writer(f, quoting=csv.QUOTE_ALL)
        if hdrs is not None:
            wr.writerow(hdrs)
        wr.writerows(data)

# New version so can cope with FCA paths - base is now also an argument JT 23/4/24
def local_file(url_or_path, base, cache_dir = None, savePath = None):
    # Changed - not sure if this should be here at all! JT 23/4/24
    if not is_url(base):
        return base + url_or_path 

    if cache_dir is None:
        raise ValueError("Cache dir must be specified if access filings via URLs")
    
    if savePath is None:
        file_path = os.path.join(cache_dir, *url_or_path.split('/'))
    else:
        file_path = cache_dir + savePath


    # path = urlparse(url_or_path).path
    # file_path = os.path.join(cache_dir, *url_or_path.split('/'))
    os.makedirs(os.path.dirname(file_path), exist_ok = True)

    if not os.path.exists(file_path):
        print("Downloading %s to %s" % (url_or_path, file_path))
        try:
            with requests.get(base + url_or_path, stream=True) as r:
                r.raise_for_status()
                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
        except requests.exceptions.HTTPError as err:
            print("Downloading %s to %s failed" % (url_or_path, file_path))
            return None

    return file_path


#
# Takes a path or URL, and returns a path to a local file.  In the case of a
# path, it will simply return the path.  For a URL, the file will be downloaded
# to the specified cache directory, if it does not already exist there.
#
def local_file2(url_or_path, cache_dir = None):
    if not is_url(url_or_path):
        return url_or_path

    if cache_dir is None:
        raise ValueError("Cache dir must be specified if access filings via URLs")

    path = urlparse(url_or_path).path
    file_path = os.path.join(cache_dir, *path.split('/'))
    os.makedirs(os.path.dirname(file_path), exist_ok = True)

    if not os.path.exists(file_path):
        print("Downloading %s to %s" % (url_or_path, file_path))

        with requests.get(url_or_path, stream=True) as r:
            r.raise_for_status()
            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

    return file_path

