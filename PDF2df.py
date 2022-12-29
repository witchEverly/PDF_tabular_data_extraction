import PyPDF2
import tabula as tb
import pandas as pd


def get_reader(file):
    """Returns a reader object from a file by using PdfReader() from the PyPDF2 library"""
    reader = PyPDF2.PdfReader(file)
    return reader


### remove named variable


def extract_names(file):
    """Extracts text from each page of a PDF and selects the string containing the appropriate name of a table on a
    given page"""
    content = list()
    reader = get_reader(file)
    for page_number in range(len(reader.pages)):  # change range style
        page = reader.getPage(page_number)
        page_content = page.extractText()
        spilt_content = page_content.split()
        for i in range(len(spilt_content)):
            if spilt_content[i] == 'Type:':
                content.append(''.join(spilt_content[i + 1:i + 5]))

    return content


# rename this `extract tables`
def get_tables(file):
    """takes a file and creates a reader object, that returns a table on pdf page, returns as a pandas
     dataframe. Note that this function does not take an 'area' arg in `read_pdf()`"""
    tables = list()
    reader = get_reader(file)
    for page_number in range(len(reader.pages)):
        tables.append(tb.read_pdf(file,
                                  pages=str(page_number + 1),  # tabula does use a zero index
                                  lattice=True,
                                  # area=area,
                                  pandas_options={'header': 0})[0])
    # removed stream = True arg and results seem unaffected

    return tables


def strip_commas(df):
    """
    Removes the commas in a pandas dataframe by first changing all series to str type,
    then stripping the commons in each value in the dataframe
    """
    df = df.dropna()  # some dfs contain full rows on NA values for unknown reasons
    df = df.astype(dtype='string')
    stripped = lambda x: x.replace(',', '')
    return df.applymap(stripped)


def concat_df(df1, df2):
    """
    Concatenates a list of dataframes by first renaming all columns
    to match the columns of the first dataframe given.
    """
    col_list = list(df1.columns)
    df2.columns = col_list
    df = pd.concat([df1, df2], ignore_index=True)
    return df


def convert_df(df):
    """
    Takes a dataframe and converts the data type of each series to optimal format
    then returns a list of clean pandas dataframes
    """
    df = df.convert_dtypes()
    df = df.astype(dtype='float', errors='ignore')
    df = df.convert_dtypes()
    return df


def concat_tables(tables):
    """
    Concatenates a list of two dataframes by first renaming all columns
    to match the columns of the first dataframe given.
    """

    concat_dfs = []
    converted_dfs = []

    if len(tables) == 10:
        for i in range(0, 10, 2):
            concat_dfs.append(concat_df(tables[i], tables[i + 1]))
        for df in concat_dfs:
            converted_dfs.append(convert_df(df))

    if len(tables) == 5:
        for i in range(5):
            converted_dfs.append(convert_df(tables[i]))

    return [strip_commas(df) for df in converted_dfs]
