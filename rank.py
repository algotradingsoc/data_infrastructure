import typing
Row = typing.Dict[str, str]
Table = typing.List[Row]
def rank(data : Table) -> typing.List[str]:
    """  
    takes a Table (data) and returns a list of ids ranked by volumn  
    """
    def sort(table : Table) -> Table:
        """  sorts Table by volumn  """
        return sorted(table, key=lambda x: x['volumn'], reverse=True)
    return [x['id'] for x in sort(data)]


# example, ranking a single day, replace with database when ready
def line2row(line):
    cells = line.split(',')
    symbol, volumn = cells[0], int(cells[7])
    return { 'id' : symbol, 'volumn' : volumn }
with open("sample.csv") as file:
    print(rank([line2row(line) for line in file.readlines()[1:]])[:10])
