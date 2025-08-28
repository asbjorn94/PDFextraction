import pdfplumber
import pandas as pd
import tabulate
import re

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

def append_tables():
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

    final_df = pd.DataFrame(columns=['Madvare','Enhed','Konverteringsfaktor'])

    #EXTRACTION CODE ====================
    for i, table in enumerate(tables):
        if i == 5: #For first 5 tables for now

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
        
        #print(df)

        #Concatenate to final df
        final_df = pd.concat([final_df, df], axis=0, ignore_index=True)


if __name__=="__main__":
    final_df = append_tables()
    print(final_df.to_markdown())









#
#
#Version 2
#Preperation ===============================================================================================
# Make Pandas dataframe containing the following columns:
# Conversion_table_name, DSK_name, DSK_id, unit, conversion_factor_bf_std, conversion_factor_af_std
#


#Version 3
# Handle netto/brutto concerns