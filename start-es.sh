#!/usr/bin/env bash

set -x

VERSION=6.8.6

docker run -f --rm --name elasticsearch -p 127.0.0.1:9200:9200 -p 127.0.0.1:9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:${VERSION}
docker run -f --rm --name kibana -v `pwd`/kibana.yml:/usr/share/kibana/config/kibana.yml:ro --link elasticsearch:elasticsearch -p 127.0.0.1:5601:5601 docker.elastic.co/kibana/kibana:${VERSION}
