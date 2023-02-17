import numpy as np
import random
from time import *

order_data = np.loadtxt('D:\\1学习\\大数据分析\\实验\\实验四\\order.txt')
half_order_data = np.loadtxt('D:\\1学习\\大数据分析\\实验\\实验四\\halforder.txt')
reverse_data = np.loadtxt('D:\\1学习\\大数据分析\\实验\\实验四\\reverse.txt')

num = 1000


def neighbor_tester(test_data, num):
    for i in range(0, num):
        ran = random.randint(0, len(test_data)-2)
        if test_data[ran] > test_data[ran+1]:
            return 0
    return 1


def random_pair_tester(test_data, num):
    for i in range(0, num):
        ran1 = random.randint(0, len(test_data)-2)
        ran2 = random.randint(ran1+1, len(test_data)-1)
        if test_data[ran1] > test_data[ran2]:
            return 0
    return 1


def hamming_tester(test_data, num):
    ran_list = random.sample(range(0, len(test_data)), num)
    for i in ran_list:
        if binary_search(test_data, test_data[i], 0, len(test_data)-1) == -1:
            return 0
    return 1


def binary_search(data_list, n, left, right):
    if left <= right:
        middle = (left + right) // 2
        if n < data_list[middle]:
            return binary_search(data_list, n, left, middle - 1)
        elif n > data_list[middle]:
            return binary_search(data_list, n, middle + 1, right)
        else:
            return middle
    else:
        return -1


print('neighbor_tester, order_data:')
start_time = time()
print(neighbor_tester(order_data, num))
end_time = time()
total_time = end_time - start_time
print('运行时间为', total_time)

print('random_pair_tester, order_data:')
start_time = time()
print(random_pair_tester(order_data, num))
end_time = time()
total_time = end_time - start_time
print('运行时间为', total_time)

print('hamming_tester, order_data:')
start_time = time()
print(hamming_tester(order_data, num))
end_time = time()
total_time = end_time - start_time
print('运行时间为', total_time)

print('neighbor_tester, half_order_data:')
start_time = time()
print(neighbor_tester(half_order_data, num))
end_time = time()
total_time = end_time - start_time
print('运行时间为', total_time)

print('random_pair_tester, half_order_data:')
start_time = time()
print(random_pair_tester(half_order_data, num))
end_time = time()
total_time = end_time - start_time
print('运行时间为', total_time)

print('hamming_tester, half_order_data:')
start_time = time()
print(hamming_tester(half_order_data, num))
end_time = time()
total_time = end_time - start_time
print('运行时间为', total_time)

print('neighbor_tester, reverse_data:')
start_time = time()
print(neighbor_tester(reverse_data, num))
end_time = time()
total_time = end_time - start_time
print('运行时间为', total_time)

print('random_pair_tester, reverse_data:')
start_time = time()
print(random_pair_tester(reverse_data, num))
end_time = time()
total_time = end_time - start_time
print('运行时间为', total_time)

print('hamming_tester, reverse_data:')
start_time = time()
print(hamming_tester(reverse_data, num))
end_time = time()
total_time = end_time - start_time
print('运行时间为', total_time)
