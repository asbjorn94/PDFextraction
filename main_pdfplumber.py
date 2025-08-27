import pdfplumber
import pandas as pd
import tabulate

#Pseudo code for database population procedure from PDF

#Version 1, page 1

#Preperation ===============================================================================================
# Make Pandas dataframe containing the following columns:
# Conversion_table_name, DSK_name, DSK_id, unit

#Procedure ===============================================================================================
# 1 Match fooditem from in conversion table PDF with carbon_footprint to assign product id to conversion_table
#   a. Using fuzzy string matching show the best matching product from the carbon_footprint database with corresponding product_id
# 2. Extract conversion factor 
# 


tables = []

with pdfplumber.open("mvfodevarer.pdf") as pdf:
    # Iterate through each page
    for page in pdf.pages:
        # Extract tables from the page
        page_tables = page.extract_tables({
           'vertical_strategy': 'lines',
           'horizontal_strategy': 'lines',
           'intersection_x_tolerance': 8,
           'intersection_y_tolerance': 8
        })
 

        if page_tables:
            for table in page_tables:
                if table:
                    tables.append({
                        'page': pdf.pages.index(page) + 1,
                        'data': table
                    })


#EXTRACTION CODE ====================
df = pd.DataFrame(tables[0]['data'])

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

print(df)

















#
#
#Version 2
#Preperation ===============================================================================================
# Make Pandas dataframe containing the following columns:
# Conversion_table_name, DSK_name, DSK_id, unit, conversion_factor_bf_std, conversion_factor_af_std
#
#Procedure ===============================================================================================
# 1 Match fooditem from in conversion table PDF with carbon_footprint to assign product id to conversion_table
#   a. Using fuzzy string matching show the best matching product from the carbon_footprint database with corresponding product_id
# 2. Extract conversion factor together 
#   a. convert to SI-standardized unit (e.g. "g/dl" is converted assigned to the unit "L" with 
#      a factor 10 multiplcation of the extracted conversion factor)
#
#
#