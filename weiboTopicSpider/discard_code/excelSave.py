import xlrd
import xlwt
from xlutils.copy import copy

def write_excel_xls(book_name_xls, sheet_name, value):
    index = len(value)  # 获取需要写入数据的行数
    workbook = xlwt.Workbook()  # 新建一个工作簿
    sheet = workbook.add_sheet(sheet_name)  # 在工作簿中新建一个表格
    for i in range(0, index):
        for j in range(0, len(value[i])):
            sheet.write(i, j, value[i][j])  # 像表格中写入数据（对应的行和列）
    workbook.save(book_name_xls)  # 保存工作簿
    print("xls格式表格写入数据成功！")

def read_excel_xls(book_name_xls):
    data = []
    workbook = xlrd.open_workbook(book_name_xls)  # 打开工作簿
    worksheet = workbook.sheet_by_index(0)  # 获取工作簿中所有表格中的的第一个表格
    if worksheet.nrows == 1:
        print("目前是第一行")
    else:
        for i in range(1, worksheet.nrows): #从第二行取值
            dataTemp = []
            for j in range(0, worksheet.ncols):
                #print(worksheet.cell_value(i, j), "\t", end="")  # 逐行逐列读取数据
                dataTemp.append(worksheet.cell_value(i, j))
            data.append(dataTemp)
    return data
     
def write_excel_xls_append_noRepeat(book_name_xls, value):
    workbook = xlrd.open_workbook(book_name_xls)  # 打开工作簿
    worksheet = workbook.sheet_by_index(0)  # 获取工作簿中所有表格中的的第一个表格
    rows_old = worksheet.nrows  # 获取表格中已存在的数据的行数
    new_workbook = copy(workbook)  # 将xlrd对象拷贝转化为xlwt对象
    new_worksheet = new_workbook.get_sheet(0)  # 获取转化后工作簿中的第一个表格
    rid = rows_old

    current_data = read_excel_xls(book_name_xls)
    weibo_id_list = []
    user_id_set = set()
    for m in range(0, len(current_data)):
        weibo_id_list.append(current_data[m][1])

    for i in range(0, len(value)):
        user_id_set.add(value[i][3])
        #重复微博ID判定
        if value[i][1] not in weibo_id_list:
            for j in range(0, len(value[i])):
                new_worksheet.write(rid, j, value[i][j])  # 追加写入数据，注意是从i+rows_old行开始写入
            rid = rid + 1
            new_workbook.save(book_name_xls)  # 保存工作簿
            print("xls格式表格(追加)写入数据成功！", '\n')
            weibo_id_list.append(value[i][1])
        else:
            print("微博ID重复，拒绝写入", '\n')
    return user_id_set
