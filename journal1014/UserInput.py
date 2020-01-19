"""
-------------------------------------------------
   File Name：     UserInput.py
   Description :   用户输入
   Author :        Cai Yufan
   date：          2019/9/30
-------------------------------------------------
   Change Activity:
       get input from the file and input the year to parse
       magazine_value stands for 期刊名称
-------------------------------------------------
"""
__author__ = 'Cai Yufan'

# 各个条件在传输时候的固定标识符
condition_value_list = {
    'a': 'SU$%=|',
    'b': 'KY',
    'c': 'TI',
    'd': 'AB',
    'e': 'FT',
    'f': 'RF',
    'g': 'CLC$=|??'
}
#  各个条件的中文意思
condition_list = {
    'a': '主题',
    'b': '关键词',
    'c': '篇名',
    'd': '摘要',
    'e': '全文',
    'f': '被引文献',
    'g': '中图分类号'
}
# 各个条件类型的固定标识符
condition_type_list = {'a': 'and', 'b': 'or', 'c': 'not'}


def get_uesr_inpt(word, year):
    '''
    处理用户所需搜索的全部条件
    '''
    condition_fields = search_condition(word)
    source_fields = search_source(word, year)
    #print(condition_fields)
    #print(source_fields)

    fields={**condition_fields,**source_fields}
    print('正在检索', word)
    return fields


def search_condition(word):
    '''
    用户输入检索条件
    '''
    if word:
        select_condition = 'a'
        # 搜索字段待填充list
        condition_field_list = {}
        # 遍历用户选择，构造搜索条件部分字段
        for index, term in enumerate(select_condition):
            condition_value = ''
            # 第一个不能选择条件类型，所以没有这个字段
            if index != 0:
                condition_type_value = ''
                condition_field_list['txt_' + str(index + 1) +
                                     '_logical'] = condition_type_list.get(
                                         condition_type_value)
            condition_field_list['txt_' + str(index + 1) +
                                 '_sel'] = condition_value_list.get(term)
            condition_field_list['txt_' + str(index + 1) +
                                 '_value1'] = condition_value
            condition_field_list['txt_' + str(index + 1) +
                                 '_relation'] = '#CNKI_AND'
            condition_field_list['txt_' + str(index + 1) + '_special1'] = '='

        return condition_field_list
    print('－－－－－－－－－－－－－－－－－－－－－－－－－－')
    print("|　　　　　　　　　　　　　　　　　　　　　　　　　|")
    print('|　请选择检索条件：（可多选）　　　　　　　　　　　|')
    print("|（ａ）主题　　　（ｂ）关键词　　　（ｃ）篇名　　　|")
    print("|（ｄ）摘要　　　（ｅ）全文　　　　（ｆ）被引文献　|")
    print("|（ｇ）中图分类号　　　　　　　　　　　　　　　|")
    print("|　　　　　　　　　　　　　　　　　　　　　　　　　|")
    print('－－－－－－－－－－－－－－－－－－－－－－－－－－')
    select_condition = input("请选择（以空格分割，如a c）：").strip()
    select_condition = select_condition.split(' ')
    print('－－－－－－－－－－－－－－－－－－－－－－－－－－')
    print('您选择的是：')
    input_check = ' '
    # 用户二次检查
    for term in select_condition:
        input_check += condition_list.get(term) + ' | '
    print(input_check)
    print('－－－－－－－－－－－－－－－－－－－－－－－－－－')
    # 搜索字段待填充list
    condition_field_list = {}
    # 遍历用户选择，构造搜索条件部分字段
    for index, term in enumerate(select_condition):
        condition_value = input('请输入【' + condition_list.get(term) +
                                '】：').strip()
        # 第一个不能选择条件类型，所以没有这个字段
        if index != 0:
            condition_type_value = input('请输入【' + condition_list.get(term) +
                                         '】条件类型:（ａ）并且　（ｂ）或者　（ｃ）不含 ').strip()
            condition_field_list['txt_' + str(index + 1) +
                                 '_logical'] = condition_type_list.get(
                                     condition_type_value)
        condition_field_list['txt_' + str(index + 1) +
                             '_sel'] = condition_value_list.get(term)
        condition_field_list['txt_' + str(index + 1) +
                             '_value1'] = condition_value
        condition_field_list['txt_' + str(index + 1) +
                             '_relation'] = '#CNKI_AND'
        condition_field_list['txt_' + str(index + 1) + '_special1'] = '='
    return condition_field_list


def search_source(word, year):
    '''
    搜索期刊来源
    '''
    if word:
        return {'year_from':year, 'year_to':year, 'year_type':'echar', 'magazine_value1': word, 'magazine_special1': '%'}
    is_search_source = input('是否需要规定文献来源（y/n）？')
    if is_search_source=='n':
        return {}
    else:
        source=input('输入文献来源期刊名称：')
        return {'degreedanwei_value1': source, 'degreedanwei_special1': '%'}

