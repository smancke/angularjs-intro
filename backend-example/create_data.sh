#!/bin/bash

curl -XPOST -d '{"title": "Programming in goolang", "author": "Dave Cheney"}' http://127.0.0.1:8080/api/lib/book
curl -XPOST -d '{"title": "Java ist auch eine Insel", "author": "Christian Ullenboom"}' http://127.0.0.1:8080/api/lib/book
curl -XPOST -d '{"title": "The Hitchhikers Guide to the Galaxy", "author": "Douglas Adams"}' http://127.0.0.1:8080/api/lib/book
