import os
import time
from collections import Counter
import pandas as pd
from tqdm import tqdm
import jieba.posseg as pseg
import warnings

# 忽略警告信息
warnings.filterwarnings('ignore')

def extract_person_names(text):
    """
    使用 jieba 进行中文人名提取。

    :param text: 输入的中文文本
    :return: 人名列表
    """
    words = pseg.cut(text)  # 使用 jieba 分词和词性标注
    return [word for word, flag in words if flag == 'nr']  # 提取词性为 'nr' 的人名

def load_and_preprocess_text(file_path, batch_size=1000):
    """
    读取并预处理文本，将每 batch_size 行合并为一个大字符串。

    :param file_path: 文本文件路径
    :param batch_size: 每批合并的行数
    :return: 合并后的文本列表
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()
        input_list = text.split('\n')
        print(f"Loaded text with {len(input_list)} sentences.")
        # 合并每 batch_size 行
        return [
            ''.join(input_list[i:i + batch_size])
            for i in range(0, len(input_list), batch_size)
        ]

def analyze_person_names(merged_list):
    """
    分析人名的频次，保存到 CSV 文件并返回数据。

    :param merged_list: 合并后的文本列表
    :return: Pandas DataFrame，包含人名和频次
    """
    names_list = []
    # 开始分析

    pbar = tqdm(total=len(merged_list), desc='Processing Text')
    for text in merged_list:
        person_names = extract_person_names(text)
        names_list.extend(person_names)
        pbar.update(1)
    pbar.close()

    # 统计人名出现频次
    name_counts = Counter(names_list)

    # 转换为 Pandas DataFrame
    df = pd.DataFrame(name_counts.items(), columns=['Name', 'Frequency'])
    df = df.sort_values(by='Frequency', ascending=False)

    return df

def main(filename):
    """
    主函数，执行人名分析流程。
    """
    file_path = f"{filename}.txt"  # 替换为小说的实际路径
    batch_size = 1000  # 每批处理 1000 行


    try:
        start_time = time.time()
        # 加载并预处理文本
        merged_list = load_and_preprocess_text(file_path, batch_size=batch_size)

        # 提取和分析人名
        df = analyze_person_names(merged_list)
        end_time = time.time()
        # 保存结果到 CSV
        output_path = f"{filename}res.txt"
        f = open(output_path, "w+")
        f.write(f"cost time: {end_time - start_time} s \n")
        for i in range(len(df.index)):
            f.write(f"{df.index[i]}  {df.iloc[i, 0]}, {df.iloc[i, 1]}\n")
        print(f"cost time: {end_time - start_time} s \n")
        print(f"Results saved to {output_path}")

        # 打印结果预览
        print(df.head(10))

    except Exception as e:
        print(f"Error during analysis: {e}")


if __name__ == '__main__':
    main('斗破苍穹')
