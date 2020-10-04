#!/bin/bash
for i in {A..Z}
do 
	for j in {A..Z}
	do
		for k in {A..Z}
		do
			FILE_PATH="data/$i/$j/$k"
			echo $FILE_PATH
			nohup spark-submit --master spark://10.0.0.14:7077 /home/ubuntu/offbeat/src/spark_processor.py --source "msd" --folder $FILE_PATH
		done
	done
done
