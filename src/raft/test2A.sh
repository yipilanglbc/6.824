#!/bin/bash
i=0
while(($i<100))
do
    go test -run 2A
    let "i++"
done