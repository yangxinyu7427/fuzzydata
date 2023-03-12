import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

path = r'../tmp/fuzzydatatest_7/'

arr = []
for file_name in os.listdir(path):
    if file_name == "pandas_example":
        continue
    arr.append(file_name)
print(arr)

clientPref = {}
opList = {}
memPref = {}
clientMemPref = {}

# 遍历所有client
for i in arr:

    filePath = path + i + '/'

    for file_name in os.listdir(filePath):
        if os.path.splitext(file_name)[0].endswith("perf"):
            print(filePath + file_name)
            df = pd.read_csv(filePath + file_name)
            dict = {}
            countDict = {}
            memDict = {}
            # 遍历df的每一行
            for row in df.index:
                if pd.isnull(df.at[row, "op_list"]):
                    if "load" in countDict:
                        countDict["load"] = countDict["load"] + 1
                        dict["load_" + str(countDict["load"])] = df.loc[row]["elapsed_time"]
                        memDict["load_" + str(countDict["load"])] = df.loc[row]["mem"]
                    else:
                        countDict["load"] = 1
                        dict["load_1"] = df.loc[row]["elapsed_time"]
                        memDict["load_1"] = df.loc[row]["mem"]
                    continue
                if df.loc[row]["op_list"] in countDict:
                    countDict[df.loc[row]["op_list"]] = countDict[df.loc[row]["op_list"]] + 1
                    dict[df.loc[row]["op_list"] + "_" + str(countDict[df.loc[row]["op_list"]])] = df.loc[row][
                        "elapsed_time"]
                    memDict[df.loc[row]["op_list"] + "_" + str(countDict[df.loc[row]["op_list"]])] = df.loc[row][
                        "mem"]
                else:
                    countDict[df.loc[row]["op_list"]] = 1
                    dict[df.loc[row]["op_list"] + "_1"] = df.loc[row]["elapsed_time"]
                    memDict[df.loc[row]["op_list"] + "_1"] = df.loc[row]["mem"]

            clientPref[i] = dict
            clientMemPref[i] = memDict

            # # 特化逻辑
            # if i=="modin_dask":
            # 统计算子列表
            opList = dict

print(clientPref)
print(opList)

# 算子列表
x_labels = list(opList.keys())
size = len(x_labels)
# x坐标
x = np.arange(size)
# 总宽度
total_width = 0.8
kind = len(clientPref)
width = total_width / kind
x = x - (total_width - width) / 2

# print(x)
# print(list(clientPref["modin_dask"].values()))

# 画图
count = -1
for client in arr:
    plt.bar(x + count * width, list(clientPref[client].values()), width=width, label=client)
    # 顶部加数字
    for i, j in zip(x + count * width, list(clientPref[client].values())):
        plt.text(i, j + 0.01, "%.4f" % j, ha="center", va="bottom", fontsize=7)
    count = count + 1

# 替换x轴
plt.xticks(x, x_labels)
# 显示图例
plt.legend()
# 显示柱状图
plt.show()



# 画图mem
count = -1
for client in arr:
    plt.bar(x + count * width, list(clientMemPref[client].values()), width=width, label=client)
    # 顶部加数字
    for i, j in zip(x + count * width, list(clientMemPref[client].values())):
        plt.text(i, j + 0.01, "%.4f" % j, ha="center", va="bottom", fontsize=7)
    count = count + 1

# 替换x轴
plt.xticks(x, x_labels)
# 显示图例
plt.legend()
# 显示柱状图
plt.show()
