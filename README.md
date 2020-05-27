# ES03

Prerequisite : Java 8 and Later version 

## Basic Example

### Start Elasticsearch
1) Download elasticsearch from [here](https://www.elastic.co/downloads/elasticsearch)   
2) Extract downloaded elasticsearch     
3) cd elasticsearch-6.8.5       
4) $ bin/elasticsearch     

### Create a index called 'annotated-products' 

curl -X PUT "localhost:9200/annotated-products" -H 'Content-Type: application/json' -d'
{
    "settings" : {
        "index" : {
            "number_of_shards" : 3,
            "number_of_replicas" : 2
        }
    }
}
'

### Populate with content from Bo Anderson's Complete Guide to Elasticsearch class (Udemy)

curl -H 'Content-Type: application/json' -XPOST "localhost:9200/annotated-products/default/_bulk?pretty" --data-binary "@test-data.json"

### Run project 
    $ gradle build
    $ gradle -PmainClass=com.company.ProductMain execute

### Delete index

curl -X DELETE "localhost:9200/annotated-products" -H 'Content-Type: application/json'
