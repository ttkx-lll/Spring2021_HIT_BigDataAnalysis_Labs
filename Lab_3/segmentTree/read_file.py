# -*-encoding:utf-8-*-

import csv


def read_file(attr_str):
    with open("D:\\1学习\\大数据分析\\实验\\实验三\\data.csv", "r") as f:
        reader = csv.DictReader(f)
        column = [float(row[attr_str]) for row in reader]
    return column


def generate_data(attr_str, chunk_size):
    count = 0
    chunk_sum = 0
    chunk_square_sum = 0
    column = read_file(attr_str)
    result_sum = []
    result_square_sum = []
    for i in column:
        if count < chunk_size:
            chunk_sum += i
            chunk_square_sum += i * i
            count += 1
        else:
            result_sum.append(chunk_sum)
            result_square_sum.append(chunk_square_sum)
            chunk_sum = i
            chunk_square_sum = i * i
            count = 1
    result_sum.append(chunk_sum)
    result_square_sum.append(chunk_square_sum)
    return column, result_sum, result_square_sum


if __name__ == '__main__':
    # c = read_file('b')
    # print(c)
    # print(len(c))
    res = generate_data('b', 5)
    print(res)
    print(len(res))
