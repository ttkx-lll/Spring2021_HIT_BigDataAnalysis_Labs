import numpy as np
from time import *

import read_file
import segment_tree
import query_operation as q

# chunk_size = 50
chunk_size = 100
# chunk_size = 200
# chunk_size = 1000
# chunk_size = 2000
# chunk_size = 500000

pr_list, pr_sum_arr, pr_square_sum_arr = read_file.generate_data('PRECTOT', chunk_size)
ts_list, ts_sum_arr, ts_square_sum_arr = read_file.generate_data('TS', chunk_size)

pr_sum_tree = np.zeros(len(pr_sum_arr)*4)
pr_square_sum_tree = np.zeros(len(pr_square_sum_arr)*4)
ts_sum_tree = np.zeros(len(ts_sum_arr)*4)
ts_square_sum_tree = np.zeros(len(ts_square_sum_arr)*4)

segment_tree.build_tree(pr_sum_arr, pr_sum_tree, 0, 0, len(pr_sum_arr) - 1)
print("pr_sum_tree is completed!")
segment_tree.build_tree(pr_square_sum_arr, pr_square_sum_tree, 0, 0, len(pr_square_sum_arr) - 1)
print("pr_square_sum_tree is completed!")
segment_tree.build_tree(ts_sum_arr, ts_sum_tree, 0, 0, len(ts_sum_arr) - 1)
print("ts_sum_tree is completed!")
segment_tree.build_tree(ts_square_sum_arr, ts_square_sum_tree, 0, 0, len(ts_square_sum_arr) - 1)
print("ts_square_sum_tree is completed!")

print()
print()
print('============================= result =============================')
# print('segment_tree, chunk_size =', chunk_size)
#
# begin_time = time()
#
# print(q.check_avg(pr_list, pr_sum_arr, pr_sum_tree, chunk_size, 3000, 5000))
# print(q.check_var(pr_list, pr_sum_arr, pr_square_sum_arr, pr_sum_tree, pr_square_sum_tree, chunk_size, 52000, 55000))
# print(q.check_avg(ts_list, ts_sum_arr, ts_sum_tree, chunk_size, 120550, 125555))
# print(q.check_var(ts_list, ts_sum_arr, ts_square_sum_arr, ts_sum_tree, ts_square_sum_tree, chunk_size, 789000, 800000))
# print(q.check_avg(pr_list, pr_sum_arr, pr_sum_tree, chunk_size, 10000, 500000))
# print(q.check_var(pr_list, pr_sum_arr, pr_square_sum_arr, pr_sum_tree, pr_square_sum_tree, chunk_size, 122000, 780000))
# print(q.check_avg(ts_list, ts_sum_arr, ts_sum_tree, chunk_size, 207830, 605600))
# print(q.check_var(ts_list, ts_sum_arr, ts_square_sum_arr, ts_sum_tree, ts_square_sum_tree, chunk_size, 170000, 950000))
# print(q.check_avg(pr_list, pr_sum_arr, pr_sum_tree, chunk_size, 352000, 425000))
# print(q.check_var(ts_list, ts_sum_arr, ts_square_sum_arr, ts_sum_tree, ts_square_sum_tree, chunk_size, 480000, 657900))
# print(q.check_avg(pr_list, pr_sum_arr, pr_sum_tree, chunk_size, 10322, 56800))
# print(q.check_var(pr_list, pr_sum_arr, pr_square_sum_arr, pr_sum_tree, pr_square_sum_tree, chunk_size, 35, 50))
# print(q.check_avg(ts_list, ts_sum_arr, ts_sum_tree, chunk_size, 120, 130))
# print(q.check_var(ts_list, ts_sum_arr, ts_square_sum_arr, ts_sum_tree, ts_square_sum_tree, chunk_size, 1, 1048576))
# print(q.check_avg(pr_list, pr_sum_arr, pr_sum_tree, chunk_size, 897500, 899000))
# print(q.check_var(pr_list, pr_sum_arr, pr_square_sum_arr, pr_sum_tree, pr_square_sum_tree, chunk_size, 89000, 1000000))
# print(q.check_avg(ts_list, ts_sum_arr, ts_sum_tree, chunk_size, 37000, 370000))
# print(q.check_var(ts_list, ts_sum_arr, ts_square_sum_arr, ts_sum_tree, ts_square_sum_tree, chunk_size, 9900, 10500))
# print(q.check_avg(pr_list, pr_sum_arr, pr_sum_tree, chunk_size, 89600, 900000))
# print(q.check_var(ts_list, ts_sum_arr, ts_square_sum_arr, ts_sum_tree, ts_square_sum_tree, chunk_size, 15, 3750))
#
# # print(q.check_avg(pr_list, pr_sum_arr, pr_sum_tree, chunk_size, 1, 2))
# # print(q.check_var(pr_list, pr_sum_arr, pr_square_sum_arr, pr_sum_tree, pr_square_sum_tree, chunk_size, 1, 2))
#
# end_time = time()
#
# run_time = end_time - begin_time
# print('运行时间为', run_time * 1000, 'ms')
#
#
# print()
# print('scan')
#
# begin_time = time()
#
# print(q.check_avg_scan(pr_list, 3000, 5000))
# print(q.check_var_scan(pr_list, 52000, 55000))
# print(q.check_avg_scan(ts_list, 120550, 125555))
# print(q.check_var_scan(ts_list, 789000, 800000))
# print(q.check_avg_scan(pr_list, 10000, 500000))
# print(q.check_var_scan(pr_list, 122000, 780000))
# print(q.check_avg_scan(ts_list, 207830, 605600))
# print(q.check_var_scan(ts_list, 170000, 950000))
# print(q.check_avg_scan(pr_list, 352000, 425000))
# print(q.check_var_scan(ts_list, 480000, 657900))
# print(q.check_avg_scan(pr_list, 10322, 56800))
# print(q.check_var_scan(pr_list, 35, 50))
# print(q.check_avg_scan(ts_list, 120, 130))
# print(q.check_var_scan(ts_list, 1, 1048576))
# print(q.check_avg_scan(pr_list, 897500, 899000))
# print(q.check_var_scan(pr_list, 89000, 1000000))
# print(q.check_avg_scan(ts_list, 37000, 370000))
# print(q.check_var_scan(ts_list, 9900, 10500))
# print(q.check_avg_scan(pr_list, 89600, 900000))
# print(q.check_var_scan(ts_list, 15, 3750))
#
#
# # print(q.check_avg(pr_list, pr_sum_arr, pr_sum_tree, chunk_size, 1, 2))
# # print(q.check_var(pr_list, pr_sum_arr, pr_square_sum_arr, pr_sum_tree, pr_square_sum_tree, chunk_size, 1, 2))
#
#
# end_time = time()
#
# run_time = end_time - begin_time
# print('运行时间为', run_time * 1000, 'ms')

begin_time = time()
for i in range(1000):
    q.check_avg(ts_list, ts_sum_arr, ts_sum_tree, chunk_size, 120550, 125555)
    q.check_avg(pr_list, pr_sum_arr, pr_sum_tree, chunk_size, 10000, 500000)
    q.check_var(ts_list, ts_sum_arr, ts_square_sum_arr, ts_sum_tree, ts_square_sum_tree, chunk_size, 170000, 950000)
    q.check_var(ts_list, ts_sum_arr, ts_square_sum_arr, ts_sum_tree, ts_square_sum_tree, chunk_size, 1, 1048576)
    q.check_var(pr_list, pr_sum_arr, pr_square_sum_arr, pr_sum_tree, pr_square_sum_tree, chunk_size, 89000, 1000000)
end_time = time()
run_time = end_time - begin_time
print('1000次运行时间为', run_time * 1000, 'ms')
print('chunk_size =', chunk_size)
