if [ $# -ne 1 ]; then
    echo "Invalid number of parameters!"
    echo "Usage: ./driver.sh [output_location]"
    exit 1
fi


spark-submit \
--master yarn \
--deploy-mode client \
--num-executors 3 \
--py-files video_method.py VideoTrendAnalysis.py \
--input /share \
--output $1
