#!/bin/bash

curl "http://localhost:8080/put?key=testkey&value=testval"
echo
curl "http://localhost:8080/get?key=testkey"
echo