import pdfplumber
import pandas as pd
import re

#DATA EXTRACTION ========================================================================================================================
filename = "maal_vaegt_portionsstoerrelser_marts_13_cropped.pdf"

def pprint(matrix):
    for row in matrix:
        for element in row:
            print(f'"{element}"', end="\t")  # Use "\t" for tab spacing
        print()  # Move to the next line after each row

def select_rects(rects):
    selected_rects = []
    threshold = 3

    for i, rect in enumerate(rects):
        if rect['height'] < threshold or rect['width'] < threshold:
            selected_rects.append(rect)
    
    return selected_rects

def draw_rects(page, rects):
    im = page.to_image(resolution = 400)
    im = im.draw_rects(rects)
    im.show()    

def is_double_row(table : pd.DataFrame) -> bool:
    double_header_indicator = table.loc[1,1]
    
    return bool(re.fullmatch(r'^([^\d\*-]+)$', double_header_indicator))

def set_header(table : pd.DataFrame):
    first_row = table.loc[0,].to_list()
    if is_double_row(table):
        #Handle empty strings in row 0 (indicator of merged cells)
        second_row = table.loc[1,].to_list()
        for i, cell in enumerate(first_row):
            for j in range(i+1,len(first_row)-1): #Excluding last cell for consideration since this would always be None, due to "Kilde" in second row (hence -1)

                if first_row[j] == "":
                    first_row[j] = first_row[i]
                else:
                    break
        first_row[len(first_row)-1] = "Kilde"

        #Merge second row into first row        
        for i in range(0,len(first_row)-1): #Excluding last cell as this is already set to "Kilde" as it should be (hence -1)
            first_row[i] = first_row[i] + "\n" + second_row[i]
        
        table = table.loc[2:,]        

    else: #Single row only
        table = table.loc[1:,] 
    
    table.columns = first_row

    return table

def handle_source(table):
    pass

def transform_table(table : pd.DataFrame) -> pd.DataFrame:

    #TODO: handle_source(table)

    headers = table.columns.to_list()

    food_category = headers[0] #Extracting the category of the food item

    headers[0] = "Madvare" #Setting the first column header to "Madvare" to create consistency for transformed table
    table.columns = headers

    tf_tbl : pd.DataFrame = table.melt(
            id_vars=["Madvare"],
            value_vars=headers[1:],
            var_name="Enhed",
            value_name="Konverteringsfaktor"
            )
    
    #Adding category column
    food_category = food_category.replace("\nØ = diameter", "") #Removal of unnecessary detail from header
    tf_tbl['Kategori'] = [food_category] * len(tf_tbl.index) 
    tf_tbl = tf_tbl[['Madvare', 'Kategori', 'Enhed', 'Konverteringsfaktor']] #Reorganising headers
    return tf_tbl
    
def extract_pdf_tables(filename) -> list[pd.DataFrame]:
    tables = []
    
    with pdfplumber.open(filename) as pdf:
        for i, page in enumerate(pdf.pages):
            selected_rects = select_rects(page.rects)
            # draw_rects(page, page.rects)
            

            page_tables = page.extract_tables({
                "vertical_strategy": "explicit",
                "horizontal_strategy": "explicit",
                "explicit_vertical_lines": selected_rects,
                "explicit_horizontal_lines": selected_rects,
            })

            if page_tables:
                for i, table in enumerate(page_tables):
                    if table:
                        df_table = pd.DataFrame(table)
                        df_table = df_table.fillna("") #Setting all NoneTypes to empty string
                        df_table = set_header(df_table)
                        tables.append(df_table)
    
    return tables
    

def create_conversion_factor_table(filename : str) -> pd.DataFrame:

    pdf_tables : list[pd.DataFrame] = extract_pdf_tables(filename)

    result = pd.DataFrame()

    for i, table in enumerate(pdf_tables):
       
        transformed : pd.DataFrame = transform_table(table)

        if i == 0:
            result = pd.DataFrame(columns=transformed.columns.tolist()) #Setting columns name dynamically to take future changes into account
               
        #Merge transformed
        result = pd.concat([result, transformed], ignore_index=True)

    return result

#DATA CLEANING ========================================================================================================================
def remove_newline_chars(value : str) -> str:
    pattern = r'(-\n)'
    value = re.sub(pattern,"",value)

    pattern = r'(\n)'
    value = re.sub(pattern," ",value)
    
    return value

def condense_unit(cell):
    selection_list = ['lille', 'mellem', 'stor', 'spsk', 'tsk', 'dl']   #Maybe a bit to weak search criterias (i.e., should be more specific, like "g/dl"). 
                                                                        #However, I have checked data thoroughly and it seems like this is sufficient.
    cell = cell.lower()
    for string in selection_list:
        if string in cell:
            return string
    return cell

def condense_value(value):
    #Change decimal separator
    value = str(value).replace(',', '.')
    
    #Remove bruttoweights
    pattern = r'(\nbrutto[\s\S]*)'
    value = re.sub(pattern,"",value)
    
    #Remove parantheses
    pattern = r'(\([^\)]+\))'
    value = re.sub(pattern,"",value)

    #When having ranges, compute average
    pattern = r'(\d+\.?\d*)-(\d+\.?\d*)'
    match = re.match(pattern, value)
    if match:
        n1 = float(match.group(1))
        n2 = float(match.group(2))
        avg = (n1+n2)/2

        return str(avg)
    
    #When having fractions compute decimal number
    pattern = r'(\d+)\/(\d+)'
    match = re.match(pattern, value)
    if match:
        n1 = float(match.group(1))
        n2 = float(match.group(2))
        decimal_number = n1/n2

        return str(decimal_number)
    
    #Return only numbers
    pattern = r'\d+\.?\d*'
    match = re.search(pattern, value)
    if match:
        return match.group(0)

    return value

def clean_data(df : pd.DataFrame) -> pd.DataFrame:
    #List containing string values that if present in units will exclude the row of the dataframe
    excl_list = ["Kilde","svind","ortion", "Indhold, ml"]
    df = df[~df['Enhed'].str.contains('|'.join(excl_list))]

    #Condense units (i.e. remove unnecessary text)
    df['Enhed'] = df['Enhed'].apply(condense_unit)

    #Remove row where unit conversion factors == "" or "-"
    df = df[~df['Konverteringsfaktor'].isin(["","-","*"])]
    
    #Condense values (i.e. brutto values - for now at least)
    df['Konverteringsfaktor'] = df['Konverteringsfaktor'].apply(condense_value)

    #Remove/replace newline characters
    df = df.map(remove_newline_chars)

    #Reset indices
    df = df.reset_index(drop=True)

    return df

#DATA SELECTION/EXCLUSION ========================================================================================================================
def select_data(df : pd.DataFrame) -> pd.DataFrame:
    #Unit-based selection
    selected_units = ['dl', 'spsk', 'tsk', 'lille', 'mellem', 'stor']
    result = df[df['Enhed'].isin(selected_units)]
    
    return result

#DATA EXPORT ========================================================================================================================
def export_data(data : pd.DataFrame) -> None:
    with open("output.txt", "w") as f:
        f.write(data.to_markdown()) 
          

#MAIN ========================================================================================================================
if __name__=="__main__":
    filename = "maal_vaegt_portionsstoerrelser_marts_13_cropped.pdf"
    data : pd.DataFrame = create_conversion_factor_table(filename)
    data = clean_data(data)
    data = select_data(data)
    export_data(data)
    print("") 
    print("Main-function finished") 
    print("") 
