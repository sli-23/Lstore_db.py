base_page = grades_table.page_directory['base']
def buffer_base(index):
    column = []
    for i in range(len(base_page)):
        val = grades_table.bufferpool.get_record('Grades', i, 0, 0, index, 'Base_Page')
        val = int.from_bytes(val, byteorder='big')
        column.append(val)
    return column

tail_page = grades_table.page_directory['tail']

def buffer_tail(index):
    #table_name, column_id, page_range_id, record_id, base_or_tail
    column = []
    for i in range(len(tail_page)):
        val = grades_table.bufferpool.get_tail_record('Grades', i, 0, index, 'Tail_Page')
        val = int.from_bytes(val, byteorder='big')
        column.append(val)
    return column