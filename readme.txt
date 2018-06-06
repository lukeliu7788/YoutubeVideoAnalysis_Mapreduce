This project use Mapreduce and Spark do the analysis with the Trending Youtube Video Statistics data from Kaggle.
DataSet: https://www.kaggle.com/datasnaek/youtube-new

In the mapreduce folder, cou_driver.sh is use to ran the mapreduce programming.which need four input. The usage pattern is like below:
Usage: ./cou_driver.sh [input_location] [output_location] [country1] [country2]


In spark folder,the programming need the environment export PYSPARK_PYTHON = python2.7
driver.sh is use to run the spark programming which only need one input. The usage pattern is like below:
Usage: ./driver.sh [output_location]

 
