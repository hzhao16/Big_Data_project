import pandas as pd
f_read = open("../output/type_result.txt", 'r')
#f_write = open("type_result.csv", 'w')

column_index = 0
result = [[] for i in range(52)]
for lines in f_read.readlines():
	if lines.startswith('Column'):
		lines = lines.replace(' ', '').strip()
		result[int(column_index)] = [0] * 5
		result[int(column_index)][0] = lines[7:]
	if lines.startswith('Valid'):
		lines = lines.replace('\t', '').strip()
		result[int(column_index)][1] = lines[5:]
	if lines.startswith('NULL'):
		lines = lines.replace('\t', '').strip()
		result[int(column_index)][2] = lines[4:]	
	if lines.startswith('Invalid'):
		lines = lines.replace(' ', '').strip()
		result[int(column_index)][3] = lines[7:]
	if lines.startswith('Non-empty'):
		lines = lines.replace(' ', '').strip()
		result[int(column_index)][4] = lines[20:]
	if lines == '\n':
		column_index += 0.5
re = pd.DataFrame(result)
re.to_csv('../output/type_result.csv')
f_read.close()
