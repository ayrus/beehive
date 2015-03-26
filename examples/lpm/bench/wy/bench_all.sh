#!/bin/bash

i=0
while [  $i -lt 1182 ]; do
     echo "Running ip$i.txt"
     instr="go run ../lpmbench.go -file=input/ip$i.txt -out=output/bench$i.out > /dev/null"
     eval $instr
     let i=i+1
done
