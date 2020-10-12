#!/bin/bash
block_size=500
for i in {0..2000}
do
	block_offset=$((block_size * $i))
	echo $block_offset
	nohup spark-submit --master spark://10.0.0.14:7077 /home/ubuntu/offbeat/src/spark_processor.py --number "$block_size" --offset "$block_offset" --source "msd" --folder ''
done
