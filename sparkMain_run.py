import time

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import jieba.posseg as pseg

# 创建 SparkSession
spark = SparkSession.builder.appName("PersonNameExtraction").getOrCreate()
conf = SparkConf().setAppName("sg1")
sc = SparkContext.getOrCreate(conf=conf)

def extract_person_names(text):
    words = pseg.cut(text)
    return [word for word, flag in words if flag=='nr']
def batch_lines(rdd, batch_size):
    return rdd.zipWithIndex() \
              .map(lambda x: (x[1] // batch_size, x[0])) \
              .groupByKey() \
              .map(lambda x: ''.join(x[1]))
def main(filename):
    """
    主函数，执行人名提取和分析。
    """
    input_path = f"obs://0d00/{filename}.txt"  # 替换为实际 OBS 文件路径
    output_path = f"./{filename}res.txt"
    batch_size = 100
    start_time = time.time()
    # 读取文本文件为 RDD，每行作为一个元素
    lines = spark.read.text(input_path).rdd.map(lambda r: r[0])
    batched_lines = batch_lines(lines, batch_size)
    # 提取人名
    names = batched_lines.flatMap(lambda text: extract_person_names(text))  # 使用 flatMap 展平人名列表
    # 统计人名频次
    name_counts = names.map(lambda name: (name, 1)).reduceByKey(lambda x, y: x + y)

    output = name_counts.collect()
    output.sort(key=lambda x:x[1], reverse=True)
    end_time = time.time()
    # 转换结果为字符串格式，便于保存
    f = open(output_path, 'w+')
    print("cost time:",end_time-start_time,"s\n",file=f)
    for (word, count) in output:
        print("%s: %i" % (word, count), "\n",file=f)
        print("%s: %i" % (word, count), "\n")
    # 删除 OBS 上已存在的目标路径

if __name__ == '__main__':
    main('斗破苍穹')

