# Note: 
Since Elasticsearch 8.1, a new feature called [`geohex_grid` aggregation](https://www.elastic.co/guide/en/elasticsearch/reference/8.1/search-aggregations-bucket-geohexgrid-aggregation.html) provides a similar functionality. However, the `elasticsearch-ingest-h3` plugin allows one to **pre-compute and index** those H3 indexes, whereas the new aggregation computes them in real-time based on a geo-point, on every request.

# Elasticsearch H3 Ingest Processor

This ingest processor provides support for creating [Uber H3](https://uber.github.io/h3) 
indexes for a given location field and a given resolution range. 

## Usage


```
PUT _ingest/pipeline/h3-pipeline
{
  "description": "A pipeline to create H3 geo indexes",
  "processors": [
    {
      "h3" : {
        "field" : "my_location",
        "target_field": "h3",
        "from": 0,
        "to: 15
      }
    }
  ]
}

PUT /my-index/_doc/1?pipeline=h3-pipeline
{
  "my_location" : {
    "lat" : "34.03747"
    "lon" : "-118.41587",
  }
}

GET /my-index/_doc/1
{
  "my_location" : {
    "lat" : "34.03747"
    "lon" : "-118.41587",
  },
  "h3": {
    "0" : "8029fffffffffff",
    "1" : "8129bffffffffff",
    "2" : "8229a7fffffffff",
    "3" : "8329a1fffffffff",
    "4" : "8429a19ffffffff",
    "5" : "8529a19bfffffff",
    "6" : "8629a1987ffffff",
    "7" : "8729a1982ffffff",
    "8" : "8829a199c9fffff",
    "9" : "8929a199c93ffff",
    "10" : "8a29a199c937fff",
    "11" : "8b29a199c932fff",
    "12" : "8c29a199c9327ff",
    "13" : "8d29a199c9326bf",
    "14" : "8e29a199c93268f",
    "15" : "8f29a199c932688"
  }
}
```

Since the H3 indexes don't change often (unless the location changes) and there are a finite number of them, indexing them can make sense instead of computing them on the fly on every query.
A simple `terms` aggregation can then be used to aggregate your geolocated assets at any H3 resolution. 
```
POST my-index/_doc/_search
{
    "size": 0,
    "aggs": {
        "h3": {
            "terms": {
                "field": "h3.7"
            }
        }
    }
}
```

## Configuration

| Parameter    | Use | Required              |
|--------------| --- |-----------------------|
| field        | The location field to index | Yes                   |
| target_field | The field to store the H3 indexes into | No (defaults to `h3`) |
| from         | The lowest resolution to generate H3 indexes for | No (defaults to `0`)  |
| to          | The highest resolution to generate H3 indexes for | No (defaults to `15`) |

## Setup

In order to install this plugin, you need to create a zip distribution first by running

```bash
gradle clean check
```

This will produce a zip file in `build/distributions`.

After building the zip file, you can install it like this

```bash
bin/elasticsearch-plugin install file:///path/to/ingest-h3/build/distributions/ingest-h3-1.0.0-SNAPSHOT.zip
```

## Bugs & TODO

* Support more H3 utility methods

