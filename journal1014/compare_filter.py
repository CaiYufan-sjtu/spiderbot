with open('finish_journal.txt', 'r', encoding='utf-8') as file1, \
        open('all_journal.txt', 'r', encoding='utf-8') as file2, \
        open('test_journal.txt', 'w', encoding='utf-8') as file3:
    lines1 = file1.readlines()
    lines2 = file2.readlines()
    for i in lines2:
        if i not in lines1:
            print(i)
            file3.write(i)
