base_type = {}
f = open('columns.txt', 'r')
for lines in f.readlines():
    basetype = lines.split('|')[2]
    if basetype not in base_type.keys():
        base_type[basetype] = 1
    else:
        base_type[basetype] += 1

l = sorted(base_type.items(), key = lambda x: x[1])

f.close()
f_result = open('count_columns.txt', 'w')
for k, v in l:
    f_result.write(k.strip() + '    \t' + str(v) + '\n')
f_result.close()
