import segment_tree


def check_avg(raw_list, sum_arr, sum_tree, chunk_size, start, end):

    chunk_start = (start-1) // chunk_size + 1
    chunk_end = (end+1) // chunk_size - 1

    list_sum = 0

    if chunk_start <= chunk_end:
        l_end = chunk_start * chunk_size - 1
        r_start = (chunk_end + 1) * chunk_size
        for temp in range(start, l_end):
            list_sum = list_sum + raw_list[temp]
        for temp in range(r_start, end):
            list_sum = list_sum + raw_list[temp]
        list_sum = list_sum + segment_tree.query_tree(sum_arr, sum_tree, 0, 0, len(sum_arr) - 1, chunk_start, chunk_end)
    else:
        for temp in range(start, end):
            list_sum = list_sum + raw_list[temp]

    return list_sum / (end - start + 1)


def check_var(raw_list, sum_arr, sum_square_arr, sum_tree, sum_square_tree, chunk_size, start, end):

    chunk_start = (start - 1) // chunk_size + 1
    chunk_end = (end + 1) // chunk_size - 1

    list_sum = 0
    list_sum_square = 0

    if chunk_start <= chunk_end:
        l_end = chunk_start * chunk_size - 1
        r_start = (chunk_end + 1) * chunk_size
        for temp in range(start, l_end):
            list_sum = list_sum + raw_list[temp]
            list_sum_square = list_sum_square + raw_list[temp] * raw_list[temp]
        for temp in range(r_start, end):
            list_sum = list_sum + raw_list[temp]
            list_sum_square = list_sum_square + raw_list[temp] * raw_list[temp]
        list_sum = list_sum + segment_tree.query_tree(sum_arr, sum_tree, 0, 0, len(sum_arr) - 1, chunk_start, chunk_end)
        list_sum_square = list_sum_square + segment_tree.query_tree(sum_square_arr, sum_square_tree, 0, 0, len(sum_square_arr)-1, chunk_start, chunk_end)
    else:
        for temp in range(start, end):
            list_sum = list_sum + raw_list[temp]
            list_sum_square = list_sum_square + raw_list[temp] * raw_list[temp]

    total_len = end - start + 1

    return list_sum_square / total_len - (list_sum / total_len) * (list_sum / total_len)


def check_avg_scan(raw_list, start, end):

    list_sum = 0
    for temp in range(start, end):
        list_sum = list_sum + raw_list[temp]
    return list_sum / (end - start + 1)


def check_var_scan(raw_list, start, end):
    list_sum = 0
    list_sum_square = 0
    for temp in range(start, end):
        list_sum = list_sum + raw_list[temp]
        list_sum_square = list_sum_square + raw_list[temp] * raw_list[temp]
    total_len = end - start + 1

    return list_sum_square / total_len - (list_sum / total_len) * (list_sum / total_len)
