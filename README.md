# webmon
![tests](https://github.com/aautushka/webmon/actions/workflows/tests.yml/badge.svg)

This is a simple web monitoring application. It will call the HTTP GET on the list of URLs and save results into the database. The app can take some beating, since it's mostly async and multiprocess. 

#### Installing and running tests
Install like a regular Python package, here is an editable install, for example
```
pip install -e .
```

If you want to run tests you'll need to install some additional dependencies
```
pip install -e '.[test]' && pytest
```
---
#### CLI and examples
There is a script coming with this package also aptly named *webmon*. There are two things you need for this sript to run:
1. List of URLs to watch [mandatory]
2. Database configuration (user name and password and others) [optional]

Example 1: Monitor an URL every second and search the response for a regex
```
webmon --config '[{"url": "https://acme.com", "schedule": 1, "regex": "lab"}]' 
```

Example 2: Load URLs from JSON file and put the measurements into the database
```
webmon --config=./config.json --user=newuser --password=password \
  --database=webmon --host=localhost --ssl='prefer' 
```

Config is a JSON array
```
[
    {"url": "https://acme.com", "schedule": 3},
    {"url": "https://www.wikipedia.org/", "schedule": 6, "regex": "Wikipedia"},
    {"url": "https://github.com/", "schedule": 9, "regex": "GitHub"},
    {"url": "https://edition.cnn.com/", "schedule": 12, "regex": "CNN"}
]
```

---
#### Limitations
There is a number of flaws in this app:
1. Works only with PosgreSQL
2. Relies on slow regex engine supplied with the standard Python
3. One regex per URL
4. Does not follow redirects (intentionally)
5. Supports only GET method
6. When running low on available resources we will start discarding collected data
7. There are many hard limits hardcoded into the app with no way of changing them dynamically
  * maximum number of active HTTP connetions
  * maximum response size for the regex to work with
  * maximum number of database connection
  * the schedule is configurable in seconds and must be in range [1, 300]
  * impossible to add a single URL without reloading the whole config and starting from scratch
    
