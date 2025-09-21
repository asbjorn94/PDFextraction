import pdfplumber
import pandas as pd
import tabulate
import re
from thefuzz import fuzz
from db_fetch import dsk_table

#Pseudo code for database population procedure from PDF

#Version 1, page 1

#Preperation ===============================================================================================
# Make Pandas dataframe containing the following columns:
# Conversion_table_name, DSK_name, DSK_id, unit

#Procedure ===============================================================================================
# 1. Properly extract data (clean up from unnecessary information etc.)
# 2. Extract units (e.g. regex on "g / dl", get only "dl")
# 3. Transform data
# 4 Match fooditem from in conversion table PDF with carbon_footprint to assign product id to conversion_table
#   a. Using fuzzy string matching show the best matching product from the carbon_footprint database with corresponding product_id
# 5. Extract conversion factor 
# 
def rename_columns(df):
    new_names = []

    for i in range(0,len(df.columns.to_list())):
        pattern = r"g\s*/\s*(.*)"

        # Extracting the substring
        match = re.search(pattern, df.columns[i])
        if match: 
            new_names.append(match.group(1))
        else:
            new_names.append(df.columns[i])
    
    df.columns = new_names

    return df

def transform_table(df: pd.DataFrame):

    headers = df.columns.to_list()


    return df.melt(
            id_vars=["Madvare"],
            value_vars=headers[1:],
            var_name="Enhed",
            value_name="Konverteringsfaktor"
            )

#Used for initial extraction
def extract_pdf_tables(filename : str) -> dict:
    def extract_coordinates(rect):
        return (rect['x0'], rect['x1'],rect['y0'],rect['y1'])

    def convert_float_to_int(t):
        return tuple(int(x) for x in t)    

    def is_nested(rect, other_rect):    
        rect_coords = extract_coordinates(rect)
        other_rect_coords = extract_coordinates(other_rect)
        rect_coords = convert_float_to_int(rect_coords) #Convert floating point to integers to avoid issues caused by rounding errors
        other_rect_coords = convert_float_to_int(other_rect_coords) #Convert floating point to integers to avoid issues caused by rounding errors
        (x0,x1,y0,y1) = rect_coords
        (x0_o,x1_o,y0_o,y1_o) = other_rect_coords

        return x0_o <= x0 and x1_o >= x1 and y0_o <= y0 and y1_o >= y1

    def find_cells_on_page(page):
        selected_rects = []

        initial_j = 0

        for i in range(0,len(page.rects)):
            rect = page.rects[i]
            if rect['height'] < 1 or rect['width'] < 1: continue #1 Ignore small rectangles

            #Look for rectangles that might encapsulate current rectangle on page
            for j in range(initial_j,len(page.rects)):
                #print(f"(i,j): ({i},{j})")
                other_rect = page.rects[j]
                if j == i: #Assumption: rectangles with index > i will never encapsulate rectangle i.
                    #print(f"added rect {i} to selected rects")
                    selected_rects.append(rect)
                    break
                elif is_nested(rect, other_rect):
                    initial_j = j #Displace the starting point to search for rectangles that nest rectangle i.
                    break
        
        return selected_rects

    def curate_table(table):
        table = table[1:] #Removing first item since it is irrelevant
        #table = [[replace_newlines(cell) for cell in row] for row in table] #Replace newline characters with whitespace #TODO
        headers = table[0] #Extracting headers    
        table = table[1:] #Removing headers from data
        return pd.DataFrame(table, columns=headers)
    
    tables = []

    with pdfplumber.open(filename) as pdf:
        # Iterate through each page
        for page in pdf.pages:

            #Find cells on page
            cells_found = find_cells_on_page(page) 
            
            # Extract tables from the page
            page_tables = page.extract_tables({
                "vertical_strategy": "explicit",
                "horizontal_strategy": "explicit",
                "explicit_vertical_lines": cells_found,
                "explicit_horizontal_lines": cells_found,
            })
    
            if page_tables:
                for table in page_tables:
                    if table:
                        curated_table = curate_table(table)
                        tables.append(curated_table)
                        # print(curated_table.to_markdown())
                        # print()
    
    return tables

#Used for raw printing
def print_tables(tables : dict):
    for i, table in enumerate(tables):
        page = table['page']
        if page == 13: break # For testing
        print(f"Page {table['page']}")
        print(f"\nTable {i+1} out of {len(tables)}")
        df = pd.DataFrame(table['data'])
        print(df.to_markdown())

def curate_tables(tables):

    final_df = pd.DataFrame(columns=['Madvare','Enhed','Konverteringsfaktor'])

    #CURATION CODE ====================
    for i, table in enumerate(tables):
        if i == 5: #For first 5 tables for now - only that works

            #Clean conversion factors from NaN
            final_df = final_df[final_df['Konverteringsfaktor'].notna()]
            return final_df
        
        df = pd.DataFrame(table['data'])

        # Remove empty colums and rows
        df.replace("", float("NaN"), inplace=True)
        df.dropna(how='all', axis=1, inplace=True) #Columns
        df.dropna(how = 'all', axis = 0,  inplace = True) #Rows

        # Remove everything above headers
        matching_rows = df[df.apply(lambda row: row.astype(str).str.contains('Madvare', case=False, na=False).any(), axis=1)]

        # Get the indices of the matching rows
        idx = matching_rows.index.tolist()[0]

        #Extract the cells of this row as these are going to function as header
        new_columns = df.loc[int(idx)].to_list()
        df.columns = new_columns

        #Slice away from "Madvare"-row to the top
        df = df.loc[idx+1:]

        df = rename_columns(df)
        df = transform_table(df)
        
        # #print(df)

        # #Concatenate to final df
        final_df = pd.concat([final_df, df], axis=0, ignore_index=True)

def enable_word_comparability(s : str):
    s = s.lower()
    s = s.replace(",","")
    word_list = s.split(" ")
    return word_list

#TODO: Maybe move to other py-file
def map_to_dsk_items(df : pd.DataFrame):
    #To append DSK data
    merged_df = df

    #Insert new columns
    merged_df[["DSK_id","DSK_product"]] = pd.NA

    #Iterate through dataframe to match with food item from DSK
    for i, conv_item in merged_df.iterrows():
        
        #Ratio, id, dsk_name
        highest_ratio = (0, pd.NA, pd.NA)
        conv_item_words : str = enable_word_comparability(conv_item['Madvare'])
        
        for j, dsk_item in dsk_table.iterrows():
            
            dsk_item_words : str = enable_word_comparability(dsk_item['product'])

            #Minimum requirement: One word overlap
            if any(i in conv_item_words for i in dsk_item_words):
                #If two lists have overlapping word, evaluate ratio
                ratio = fuzz.token_set_ratio(conv_item['Madvare'], dsk_item['product'])

                #If higher than the current highest, update the highest ratio
                if ratio > highest_ratio[0]:
                    highest_ratio = (ratio, dsk_item['id'], dsk_item['product'])
            else: #Otherwise ignore
                continue


        #Set the highest ratio in the merged_df
        merged_df.at[i,'DSK_id'] = highest_ratio[1]        
        merged_df.at[i,'DSK_product'] = highest_ratio[2]

    return merged_df

if __name__=="__main__":
    extracted : dict = extract_pdf_tables("mvfodevarer.pdf")
    
    for table in extracted:
        print(table.to_markdown())
        print()
    
    # curated : pd.DataFrame = curate_tables(extracted)
    # # print(curated.to_markdown())
    # # print_tables(extracted)
    # mapped_df = map_to_dsk_items(curated)
    
    # print(mapped_df.to_markdown())        
    
       
        
    


         



    #print(final_df.to_markdown())









#
#
#Version 2
#Preperation ===============================================================================================
# Make Pandas dataframe containing the following columns:
# Conversion_table_name, DSK_name, DSK_id, unit, conversion_factor_bf_std, conversion_factor_af_std
#


#Version 3
# Handle netto/brutto concerns