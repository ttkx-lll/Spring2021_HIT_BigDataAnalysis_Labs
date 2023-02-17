"""
    segment tree
"""
import numpy as np


def build_tree(arr, tree, node, start, end):
    """
        构建线段树
    :param arr: 原数组
    :param tree: 线段树存放数组，大小为4*n
    :param node: 当前根节点编号
    :param start: 0
    :param end: size-1
    :return:
    """
    if start == end:
        tree[node] = arr[start]
    else:
        mid = (start + end) // 2
        left_node = 2 * node + 1
        right_node = 2 * node + 2
        build_tree(arr, tree, left_node, start, mid)
        build_tree(arr, tree, right_node, mid+1, end)
        tree[node] = tree[left_node] + tree[right_node]


def query_tree(arr, tree, node, start, end, L, R):
    """
        线段树查询
    :param arr: 原数组
    :param tree: 线段树存放数组
    :param node: 当前根节点
    :param start: arr数组左端
    :param end: arr数组右端
    :param L: 待查询区间左端
    :param R: 待查询区间右端
    :return:
    """
    if R < start or L > end:
        return 0
    elif start >= L and end <= R:
        return tree[node]
    elif start == end:
        return tree[node]
    else:
        mid = (start + end) // 2
        left_node = 2 * node + 1
        right_node = 2 * node + 2
        sum_left = query_tree(arr, tree, left_node, start, mid, L, R)
        sum_right = query_tree(arr, tree, right_node, mid+1, end, L, R)
        return sum_left + sum_right


def main():
    arr = [1, 3, 5, 7, 9, 11]
    tree = np.zeros(50)

    build_tree(arr, tree, 0, 0, len(arr)-1)
    for i in range(15):
        print(tree[i])
    print(query_tree(arr, tree, 0, 0, len(arr)-1, 2, 5))


if __name__ == "__main__":
    main()

