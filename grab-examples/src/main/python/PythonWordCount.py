import sys

from pyspark import SparkContext

if __name__ == "__main__":
    file=sys.argv[1]
    threshold=int(sys.argv[2])
    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile(file, 1)
    counts = lines.flatMap(lambda x: x.split(' ')) \
                .map(lambda x: (x, 1))  \
                .reduceByKey(lambda a, b: a + b)  \
                .filter(lambda (a, b) : b >= threshold)  \
                .flatMap(lambda (a, b): list(a))  \
                .map(lambda x: (x, 1))  \
                .reduceByKey(lambda a, b: a + b)

    print ",".join(str(t) for t in counts.collect())
    sc.stop()