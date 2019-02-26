#!/bin/bash

WIDTH=1920
HEIGHT=1080

INPUT="$1"

geoproject "d3.geoConicEqualArea().parallels([34, 40.5]).rotate([120, 0]).fitSize([$WIDTH, $HEIGHT], d)" < "$INPUT" > /tmp/$$.json
geo2svg -w 960 -h 960 < /tmp/$$.json > /tmp/$$.svg
convert /tmp/$$.svg ${INPUT}.png
