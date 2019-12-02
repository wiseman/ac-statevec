#!/bin/bash

WIDTH=1920
HEIGHT=1080
for INPUT in "$@"
do
    geoproject "d3.geoConicEqualArea().parallels([34, 40.5]).scale(0.5).rotate([120, 0]).fitSize([$WIDTH, $HEIGHT], d)" < "$INPUT" > /tmp/$$.json
    geo2svg -w $WIDTH -h $HEIGHT < /tmp/$$.json > /tmp/$$.svg
    convert /tmp/$$.svg ${INPUT}.png
    echo Rendered ${INPUT}.png
done
