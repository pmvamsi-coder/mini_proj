from utils.util import *


def main():
    dic = {}
    spark = Spark("Vamsi", dic, "PMV").spark
    for i in spark.sparkContext.getConf().getAll():
        print(i)


if __name__ == "__main__":
    main()
