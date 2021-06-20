# Ref

https://thenewstack.io/the-good-bad-and-ugly-apache-spark-for-data-science-work/?fbclid=IwAR2JdXmkinD2jUzUVNHd9uTvQ9ZjlkqB4ePw4Qmr3qfGiX8vAchZ2AuiUwQ

# Good

* Appealing APIs(極具吸引力的API) - Python, R, Scala, Java, with SQL like DataFrame API
* Lazy Execution - helpful when you define a complex series of transformation
* East Conversion - `toPandas()` - Do aggregation by spark, plot by pandas!
* Open Source Community - [sparknlp](https://github.com/JohnSnowLabs/spark-nlp), [h2o](https://github.com/h2oai/sparkling-water) and so on 

# bad

* Cluster management - really difficult to maintain, OOM error is very common.

* Debugging - OOM errors, logging in UDF is hard.

* Slowness of Pyspark UDFs - parsing python object into JVM

* Hard-to-Guarantee Maximal Parallelism - it's control by spark.

# Ugly

* API Awkwardness - accessing array elements is very hard, a lot of Spark-ML functions return arrays.

* Lack of Maturity and Feature Completeness - MLLib and ML
	* Random Forest did not have feature importance in its new ML library unitl Spark 2.0
	* Gradient Boostied Tree did not expose a probability score until Spark 2.2
	* model exposing a floating point score, an ArrayType is returned, [0.25, 0.75], Shockingly, there is no built-in function to extract that 0.75. It requries a UDF. As a result, we fund ourselves falling back to training models locally using the more mature scikit-learn library.