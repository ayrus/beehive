#!/bin/bash

i=0
while [  $i -lt 38 ]; do
     echo "Analyzing bench$i.out"
     instr="python ../bench.py output/bench$i.out > output_avg/metric$i.txt"
     eval $instr
     let i=i+1
done
