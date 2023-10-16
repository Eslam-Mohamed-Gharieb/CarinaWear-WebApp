import streamlit as st
import pandas as pd
from io import BytesIO
import base64
import os
import xlsxwriter
import pyodbc
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import bcrypt


def upload_data_to_sql_server(df_uploaded):
    # Define your SQL Server connection parameters
    server_name = "192.168.2.186"
    database_name = 'carina'
    username = 'sa'
    password = 'P@$$w0rd'

    try:
        # Establish a connection to the SQL Server database
        conn = pyodbc.connect(
            f'DRIVER={{SQL Server}};SERVER={server_name};DATABASE={database_name};UID={username};PWD={password}'
        )

        # Create a cursor object to execute SQL commands
        cursor = conn.cursor()

        # SQL query to select all data from the table
        department_sql_query = "SELECT * FROM Department"
        category_sql_query = "SELECT * FROM Category"
        tax_sql_query = "SELECT * FROM Tax"
        supplier_sql_query = "SELECT * FROM Supplier"

        # Use Pandas to read the data into DataFrames
        df_department = pd.read_sql(department_sql_query, conn)
        df_category = pd.read_sql(category_sql_query, conn)
        df_tax = pd.read_sql(tax_sql_query, conn)
        df_supplier = pd.read_sql(supplier_sql_query, conn)

        # Map values in df_uploaded to match with the existing tables
        df_uploaded['DepartmentID'] = df_uploaded['Department'].map(df_department.set_index('Name')['ID'].to_dict())
        df_uploaded['CategoryID'] = df_uploaded['Category'].map(df_category.set_index('Name')['ID'].to_dict())
        df_uploaded['CategoryID'].fillna(-1, inplace=True)
        df_uploaded['SupplierID'] = df_uploaded['Supplier'].map(df_supplier.set_index('SupplierName')['ID'].to_dict())
        df_uploaded['TaxID'] = df_uploaded['Tax'].map(df_tax.set_index('Description')['ID'].to_dict())

        # SQL query to select all data from the table
        ItemClass_sql_query = f"SELECT * FROM ItemClass"

        # Use Pandas to read the data into a DataFrame
        df_ItemClass = pd.read_sql(ItemClass_sql_query, conn)

        # Check if df_ItemClass is not NaN
        if not df_ItemClass.empty:
            # Determine the next available ID
            next_id = df_ItemClass['ID'].max() + 1
        else:
            next_id = 1

        # Create a list of DataFrames to concatenate
        dfs_to_concat = []

        # Append rows from df_uploaded to df_ItemClass
        for index, row in df_uploaded.iterrows():
            new_row = {
                'ID': next_id,
                'Description': row['Description'],
                'Dimensions': 2,  # Replace with actual values
                'Title1': row['Item Lookup Code'].split("-")[0] + "_coloer",  # Replace with actual values
                'Title2': row['Item Lookup Code'].split("-")[0] + "_Size",  # Replace with actual values
                'Title3': "",  # Replace with actual values
                'ClassType': 0,  # Replace with actual values
                'DBTimeStamp': None,  # Replace with actual values
                'UseComponentPrice': 0,  # Replace with actual values
                'HQID': 0,  # Replace with actual values
                'ItemLookupCode': row['Item Lookup Code'].split("-")[0],  # Replace with actual values
                'DepartmentID': int(row['DepartmentID']),  # Replace with actual values
                'CategoryID': int(row['CategoryID']),  # Replace with actual values
                'Price': float(row['Price']),  # Replace with actual values
                'Cost': 0,  # Replace with actual values
                'SupplierID': int(row['SupplierID']),  # Replace with actual values
                'BarcodeFormat': int(row['BarcodeFormat']),  # Replace with actual values
                'SubDescription1': row['Family'],  # Replace with actual values
                'SubDescription2': row['Tybe'],  # Replace with actual values
                'SubDescription3': row['Season'],  # Replace with actual values
                'TaxID': row['TaxID'],  # Replace with actual values
                'Notes': "",  # Replace with actual values

                # Add other columns as needed
            }

            # Append the new row as a DataFrame to the list
            new_df = pd.DataFrame([new_row])
            dfs_to_concat.append(new_df)

            next_id += 1

        # Concatenate all DataFrames in the list
        df_ItemClass = pd.concat([df_ItemClass] + dfs_to_concat, ignore_index=True)

        # Remove duplicate rows based on the 'Item Lookup Code' column
        df_ItemClass = df_ItemClass.drop_duplicates(subset='ItemLookupCode', keep='first')

        # Reset the index after removing duplicates
        df_ItemClass.reset_index(drop=True, inplace=True)

        df_ItemClass.dropna(subset=['ID'], inplace=True)

        truncate_query_ItemClass = "TRUNCATE TABLE ItemClass"
        cursor.execute(truncate_query_ItemClass)

        # Enable IDENTITY_INSERT for the ItemClass table
        enable_identity_query = "SET IDENTITY_INSERT ItemClass ON"
        cursor.execute(enable_identity_query)

        # Loop through each row in the DataFrame and insert it into the SQL Server table
        for index, row in df_ItemClass.iterrows():
            # Construct the SQL INSERT query dynamically based on the column names
            columns = ', '.join(row.index)
            values = ', '.join(['DEFAULT' if col == 'DBTimeStamp' else '?' for col in row.index])

            # Filter out the row values, excluding 'DBTimeStamp'
            row_values = [val for val, col in zip(row, row.index) if col != 'DBTimeStamp']

            insert_query = f"INSERT INTO ItemClass ({columns}) VALUES ({values})"
            cursor.execute(insert_query, *row_values)

        # Commit the changes
        conn.commit()

        # Disable IDENTITY_INSERT for the ItemClass table
        disable_identity_query = "SET IDENTITY_INSERT ItemClass OFF"
        cursor.execute(disable_identity_query)

        print(f'Data appended to table ItemClass successfully.')

        # SQL query to select all data from the table
        Item_sql_query = f"SELECT * FROM Item"

        # Use Pandas to read the data into a DataFrame
        df_Item = pd.read_sql(Item_sql_query, conn)

        # Check if df_Item is not NaN
        if not df_Item.empty:
            # Determine the next available ID
            next_id = df_Item['ID'].max() + 1
        else:
            next_id = 1

        # Create a list of DataFrames to concatenate
        Item_dfs_to_concat = []

        # Append rows from df_uploaded to df_ItemClass
        for index, row in df_uploaded.iterrows():
            new_row = {
                'ID': next_id,
                "BinLocation": "",
                "BuydownPrice": 0,
                "BuydownQuantity": 0,
                "CommissionAmount": 0,
                "CommissionMaximum": 0,
                "CommissionMode": 1,
                "CommissionPercentProfit": 0,
                "CommissionPercentSale": 0,
                "FoodStampable": 0,
                "HQID": 0,
                "ItemNotDiscountable": 0,
                "LastReceived": None,
                "LastUpdated": datetime.datetime.now(),
                "QuantityCommitted": 0,
                "SerialNumberCount": 0,
                "TareWeightPercent": 0,
                "MessageID": 0,
                "PriceA": 0,
                "PriceB": 0,
                "PriceC": 0,
                "SalePrice": 0,
                "SaleStartDate": None,
                "SaleEndDate": None,
                "QuantityDiscountID": 0,
                "ItemType": 0,
                "Quantity": 0,
                "ReorderPoint": 0,
                "RestockLevel": 0,
                "TareWeight": 0,
                "TagAlongItem": 0,
                "TagAlongQuantity": 0,
                "ParentItem": 0,
                "ParentQuantity": 0,
                "PriceLowerBound": 0,
                "PriceUpperBound": 0,
                "PictureName": "",
                "LastSold": None,
                "UnitOfMeasure": "",
                "SubCategoryID": 0,
                "QuantityEntryNotAllowed": 0,
                "PriceMustBeEntered": 0,
                "BlockSalesReason": "",
                "BlockSalesAfterDate": None,
                "Weight": 0,
                "Taxable": 1,
                "BlockSalesBeforeDate": None,
                "LastCost": 0,
                "ReplacementCost": 0,
                "WebItem": 0,
                "BlockSalesType": 0,
                "BlockSalesScheduleID": 0,
                "SaleType": 0,
                "SaleScheduleID": 0,
                "Consignment": 0,
                "Inactive": 0,
                "LastCounted": None,
                "DoNotOrder": 0,
                "MSRP": 0,
                "DateCreated": datetime.datetime.now(),
                "UsuallyShip": '10002102',
                "ExtendedDescription": "",
                "Content": "",
                "NumberFormat": None,
                "ItemCannotBeRet": None,
                "ItemCannotBeSold": None,
                "IsAutogenerated": None,
                "IsGlobalvoucher": 0,
                "DeleteZeroBalanceEntry": None,
                "TenderID": 0,
                'Description': row['Description'],
                'DBTimeStamp': None,  # Replace with actual values
                'ItemLookupCode': row['Item Lookup Code'],  # Replace with actual values
                'DepartmentID': int(row['DepartmentID']),  # Replace with actual values
                'CategoryID': int(row['CategoryID']),  # Replace with actual values
                'Price': float(row['Price']),  # Replace with actual values
                'Cost': 0,  # Replace with actual values
                'SupplierID': int(row['SupplierID']),  # Replace with actual values
                'BarcodeFormat': int(row['BarcodeFormat']),  # Replace with actual values
                'SubDescription1': row['Family'],  # Replace with actual values
                'SubDescription2': row['Tybe'],  # Replace with actual values
                'SubDescription3': row['Season'],  # Replace with actual values
                'TaxID': row['TaxID'],  # Replace with actual values
                'Notes': None  # Replace with actual values

                # Add other columns as needed
            }

            # Append the new row as a DataFrame to the list
            new_df = pd.DataFrame([new_row])
            Item_dfs_to_concat.append(new_df)

            next_id += 1

        # Concatenate all DataFrames in the list
        df_Item = pd.concat([df_Item] + Item_dfs_to_concat, ignore_index=True)

        df_Item.dropna(subset=['ID'], inplace=True)

        truncate_query_Item = "TRUNCATE TABLE Item"
        cursor.execute(truncate_query_Item)

        # Enable IDENTITY_INSERT for the Item table
        enable_identity_query = "SET IDENTITY_INSERT Item ON"
        cursor.execute(enable_identity_query)

        # Loop through each row in the DataFrame and insert it into the SQL Server table
        for index, row in df_Item.iterrows():
            # Construct the SQL INSERT query dynamically based on the column names
            columns = ', '.join(row.index)
            values = ', '.join(['DEFAULT' if col == 'DBTimeStamp' else '?' for col in row.index])

            # Filter out the row values, excluding 'DBTimeStamp'
            row_values = [val for val, col in zip(row, row.index) if col != 'DBTimeStamp']

            insert_query = f"INSERT INTO Item ({columns}) VALUES ({values})"
            cursor.execute(insert_query, *row_values)

        # Commit the changes
        conn.commit()

        # Disable IDENTITY_INSERT for the Item table
        disable_identity_query = "SET IDENTITY_INSERT Item OFF"
        cursor.execute(disable_identity_query)

        # SQL query to select all data from the table
        ItemClassComponent = f"SELECT * FROM ItemClassComponent"

        # Use Pandas to read the data into a DataFrame
        df_ItemClassComponent = pd.read_sql(ItemClassComponent, conn)

        # Check if df_ItemClass is not NaN
        if not df_ItemClassComponent.empty:
            # Determine the next available ID
            next_id = df_ItemClassComponent['ID'].max() + 1
        else:
            next_id = 1

        # Create a list of DataFrames to concatenate
        ItemClassComponent_dfs_to_concat = []

        for index, row in df_uploaded.iterrows():
            ItemClassID_row = df_ItemClass[df_ItemClass['ItemLookupCode'] == row['Item Lookup Code'].split("-")[0]]
            ItemClassID =  ItemClassID_row.iloc[0]['ID']
            new_row = {
                "ID":next_id,
                "ItemClassID":ItemClassID,
                "ItemID":next_id,
                "Quantity":1,
                "Detail1":row['Color'],
                "Detail2":row['Size'],
                "Detail3":"",
                "LastUpdated":datetime.datetime.now(),
                "Price": 0,
            }

            # Append the new row as a DataFrame to the list
            new_df = pd.DataFrame([new_row])
            ItemClassComponent_dfs_to_concat.append(new_df)

            next_id += 1


        # Concatenate all DataFrames in the list
        df_ItemClassComponent = pd.concat([df_ItemClassComponent] + ItemClassComponent_dfs_to_concat, ignore_index=True)

        df_ItemClassComponent.dropna(subset=['ID'], inplace=True)

        truncate_query_ItemClassComponent = "TRUNCATE TABLE ItemClassComponent"
        cursor.execute(truncate_query_ItemClassComponent)

        # Enable IDENTITY_INSERT for the Item table
        enable_identity_query = "SET IDENTITY_INSERT ItemClassComponent ON"
        cursor.execute(enable_identity_query)

        # Loop through each row in the DataFrame and insert it into the SQL Server table
        for index, row in df_ItemClassComponent.iterrows():
            # Construct the SQL INSERT query dynamically based on the column names
            columns = ', '.join(row.index)
            values = ', '.join(['DEFAULT' if col == 'DBTimeStamp' else '?' for col in row.index])

            # Filter out the row values, excluding 'DBTimeStamp'
            row_values = [val for val, col in zip(row, row.index) if col != 'DBTimeStamp']

            insert_query = f"INSERT INTO ItemClassComponent ({columns}) VALUES ({values})"
            cursor.execute(insert_query, *row_values)

        # Commit the changes
        conn.commit()

        # Disable IDENTITY_INSERT for the Item table
        disable_identity_query = "SET IDENTITY_INSERT ItemClassComponent OFF"
        cursor.execute(disable_identity_query)

        # Create a list to store the grouped data
        grouped_data = []

        for _, row in df_uploaded.iterrows():
            code_prefix = row['Item Lookup Code'].split('-')[0]  # Get the prefix of the Item Lookup Code
            item_code = code_prefix

            # Add rows for sizes
            grouped_data.append({
                'ItemLookupCode': item_code,
                'Attribute': row['Size'],
                'Type': 2,  # Indicate that it's a size
            })

            # Add rows for colors
            grouped_data.append({
                'ItemLookupCode': item_code,
                'Attribute': row['Color'],
                'Type': 1,  # Indicate that it's a color
            })

        # Create a DataFrame from the list of dictionaries
        grouped_df = pd.DataFrame(grouped_data)

        # Remove duplicated rows
        grouped_df = grouped_df.drop_duplicates()

        # Add a "display_order" column
        grouped_df['display_order'] = grouped_df.groupby(['ItemLookupCode', 'Type']).cumcount() + 1


        # Sample data for dim_df
        dim_data = {
            "dim_name": [
                "Black", "White", "Off White", "Ivory", "Med.Grey Chine",
                "D.Grey", "Med.Grey", "Silver", "Navy", "Tobaco", "Nude",
                "Light Beige", "Med.Beige", "Dark Beige", "Blue", "Dark Brown",
                "Coffee", "Havan", "Taupe", "Camel", "Greish", "Cigar", "kaki",
                "Light Yellow", "Bright Yellow", "Canaria Yellow", "Dark Yellow",
                "Mustard", "GoldenYellow", "Lime", "Pistache", "Light Mint",
                "Classic Green", "Dark Teal", "green grey", "Olive", "Emerald Green",
                "Deep Green", "Dark Aqua", "Aqua", "Turquoise", "Olive Green",
                "Teal Green", "Blue Petroleum", "Light Blue", "True Blue", "Royal Blue",
                "Blue Marine", "Pastel Light Blue", "Light Indigo", "Dark Indigo",
                "Purple", "Deep Purple", "Light Orchide", "Bois De Rose", "Lavender",
                "Light Lilac", "Deep Lilac", "Violet", "Rose", "Hot Pink", "Peach Blossom",
                "Fucshia", "Magenta", "Wine", "Peach Skin", "Pink Violet", "Watermelon",
                "Blush", "Coral", "Coral Pink", "peach", "Pale Peach", "Pink", "Red",
                "Chile Peper", "Red Orange", "Red Brique", "Orange", "Neon Orange",
                "Orange Simon", "Light Orange", "Marigold", "Light Simon", "Shrimp Orange",
                "Light Burgundy", "Interacid", "Colored", "S", "M", "L", "XL", "2XL",
                "3XL", "4XL", "SM", "LXL", "23X", "OS", "ML", "X2X", "1-2", "2-4",
                "4-6", "6-8", "8-10", "10-12", "12-14", "14-16"
            ],
            "dim_id": [
                "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
                "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
                "21", "22", "23", "24", "25", "27", "28", "29", "30", "31",
                "32", "33", "34", "35", "36", "37", "38", "39", "40", "41",
                "42", "43", "44", "45", "46", "47", "48", "49", "50", "51",
                "52", "53", "54", "55", "56", "57", "58", "59", "60", "61",
                "62", "63", "64", "65", "66", "67", "68", "70", "71", "72",
                "73", "74", "75", "76", "77", "78", "80", "81", "83", "84",
                "85", "86", "87", "88", "89", "90", "91", "99", "S", "M", "L",
                "XL", "2XL", "3XL", "4XL", "SM", "LXL", "23X", "OS", "ML", "X2X",
                "1-2", "2-4", "4-6", "6-8", "8-10", "10-12", "12-14", "14-16"
            ]
        }

        dim_df = pd.DataFrame(dim_data)

        # Merge the dim_df and grouped_df to add the "attribute_id" column
        result_df = grouped_df.merge(dim_df, left_on="Attribute", right_on="dim_name", how="left")

        # Rename the "dim_id" column to "attribute_id" and drop the "dim_name" column
        result_df = result_df.rename(columns={"dim_id": "attribute_id"}).drop(columns=["dim_name"])


        # SQL query to select all data from the table
        MatrixAttributeDisplayOrder = f"SELECT * FROM MatrixAttributeDisplayOrder"

        # Use Pandas to read the data into a DataFrame
        df_MatrixAttributeDisplayOrder = pd.read_sql(MatrixAttributeDisplayOrder, conn)

        # Check if df_ItemClass is not NaN
        if not df_MatrixAttributeDisplayOrder.empty:
            # Determine the next available ID
            next_id = df_MatrixAttributeDisplayOrder['ID'].max() + 1
        else:
            next_id = 1

        # Create a list of DataFrames to concatenate
        MatrixAttributeDisplayOrder_dfs_to_concat = []

        for index, row in result_df.iterrows():
            ItemClassID_row = df_ItemClass[df_ItemClass['ItemLookupCode'] == row['ItemLookupCode']]
            ItemClassID =  ItemClassID_row.iloc[0]['ID']

            new_row = {
                "ID":next_id,
                "ItemClassID":ItemClassID,
                "Dimension":row['Type'],
                "Attribute":row['Attribute'],
                "Code":row['attribute_id'],
                "DisplayOrder":row["display_order"],
                "Inactive":0,
                "HQID":0,
            }

            # Append the new row as a DataFrame to the list
            new_df = pd.DataFrame([new_row])
            MatrixAttributeDisplayOrder_dfs_to_concat.append(new_df)

            next_id += 1


        # Concatenate all DataFrames in the list
        df_MatrixAttributeDisplayOrder = pd.concat([df_MatrixAttributeDisplayOrder] + MatrixAttributeDisplayOrder_dfs_to_concat, ignore_index=True)

        df_MatrixAttributeDisplayOrder.dropna(subset=['ID'], inplace=True)

        truncate_query_MatrixAttributeDisplayOrder = "TRUNCATE TABLE MatrixAttributeDisplayOrder"
        cursor.execute(truncate_query_MatrixAttributeDisplayOrder)

        # Enable IDENTITY_INSERT for the Item table
        enable_identity_query = "SET IDENTITY_INSERT MatrixAttributeDisplayOrder ON"
        cursor.execute(enable_identity_query)

        # Loop through each row in the DataFrame and insert it into the SQL Server table
        for index, row in df_MatrixAttributeDisplayOrder.iterrows():
            # Construct the SQL INSERT query dynamically based on the column names
            columns = ', '.join(row.index)
            values = ', '.join(['DEFAULT' if col == 'DBTimeStamp' else '?' for col in row.index])

            # Filter out the row values, excluding 'DBTimeStamp'
            row_values = [val for val, col in zip(row, row.index) if col != 'DBTimeStamp']

            insert_query = f"INSERT INTO MatrixAttributeDisplayOrder ({columns}) VALUES ({values})"
            cursor.execute(insert_query, *row_values)

        # Commit the changes
        conn.commit()

        # Disable IDENTITY_INSERT for the Item table
        disable_identity_query = "SET IDENTITY_INSERT MatrixAttributeDisplayOrder OFF"
        cursor.execute(disable_identity_query)

        # Close the database connection
        conn.close()
        print(f'Data appended to table Item successfully.')
    except Exception as e:
        print(f'Error uploading data to SQL Server: {str(e)}')








# Function to process the Excel file
def process_excel_file(uploaded_file):
    try:
        # Load the Excel workbook
        df = pd.read_excel(uploaded_file)

        # Rename the '%' column to a valid name (e.g., 'Percentage')
        df.rename(columns={'%': 'Percentage'}, inplace=True)

        # Your data processing code here (unchanged)
        dfs_to_concat = []  # Create a list to store DataFrames for concatenation
        stores_to_loop_over = df['Store ID'].unique()
        for i in stores_to_loop_over:
            df_sample = df.loc[df['Store ID'] == i].copy()  # Create a copy to avoid SettingWithCopyWarning
            sum_of_25 = sum(df_sample['SALES14%'].loc[(df_sample['SALES14%'] < 2500) | (df_sample['Employee ID'].isnull()) | (df_sample['job'] == "Store Manager") | (df_sample['job'] == "Stock Controller") | (df_sample['job'] == "Cashier")].values)
            df_sample.loc[(df_sample['SALES14%'] < 2500) | (df_sample['Employee ID'].isnull()) | (df_sample['job'] == "Store Manager") | (df_sample['job'] == "Stock Controller") | (df_sample['job'] == "Cashier"), 'Distiribution'] = 0
            length = len(df_sample) - len(df_sample['SALES14%'].loc[(df_sample['SALES14%'] < 2500) | (df_sample['Employee ID'].isnull()) | (df_sample['job'] == "Store Manager") | (df_sample['job'] == "Stock Controller") | (df_sample['job'] == "Cashier")])
            df_sample.loc[df_sample['Distiribution'] != 0, 'Distiribution'] = sum_of_25 / length
            df_sample["NET SALES"] = df_sample['Distiribution'] + df_sample["SALES14%"]
            df_sample.loc[(df_sample['SALES14%'] < 2500) | (df_sample['Employee ID'].isnull()) | (df_sample['job'] == "Store Manager") | (df_sample['job'] == "Stock Controller") | (df_sample['job'] == "Cashier"), 'NET SALES'] = 0
            df_sample["comm"] = df_sample['Percentage'] * df_sample["NET SALES"]
            sum_of_all = sum(df_sample['comm'])
            df_sample.loc[df_sample['job'] == "Store Manager", 'comm'] = sum_of_all
            df_sample.loc[df_sample['job'] == "Stock Controller", 'comm'] = sum_of_all * 0.15
            df_sample.loc[df_sample['job'] == "Cashier", 'comm'] = sum_of_all * 0.2
            dfs_to_concat.append(df_sample)

        # Concatenate DataFrames
        df2 = pd.concat(dfs_to_concat, ignore_index=True)

        # Return the processed DataFrame
        return df2, None  # Return the DataFrame and no error message

    except Exception as e:
        return None, str(e)  # Return no DataFrame and the error message

def fetch_data_from_wellness_data(employee_id):
    wellness_all_df = pd.read_excel("https://docs.google.com/spreadsheets/d/1MydjNCc6GY2pFwLVvvp6vhERBh-FBU5oCOZ5t5kFa7M/export?format=xlsx&gid=783663279")

    # Filter the DataFrame to get data for the entered employee_id
    employee_filtered_wellness_df = wellness_all_df[wellness_all_df['Please Enter your Code — برجاء إدخال كودك'] == int(employee_id)]
    # Extract the desired columns and rename them
    employee_filtered_wellness_df = employee_filtered_wellness_df.iloc[:, 2:6]  # Extract columns 3 to 6
    wellness_all_df = wellness_all_df.iloc[:, 2:6] 
    employee_filtered_wellness_df.columns = ["Date", "Sleep Quality %", "Steps Count", "# of Burned Calories"]
    wellness_all_df.columns = ["Date", "Sleep Quality %", "Steps Count", "# of Burned Calories"]
    #employee_filtered_wellness_df["Date"] = pd.to_datetime(employee_filtered_wellness_df["Date"]).dt.date
    #wellness_all_df["Date"]= pd.to_datetime(wellness_all_df["Date"]).dt.date

    return employee_filtered_wellness_df, wellness_all_df

# Function to fetch data from Google Sheets based on employee ID
def fetch_data_from_google_sheets(employee_id):
    # Use the Google Sheets API to fetch data
    sheet_id = "15tknLvLFrBn8Pa-d8qI-yc7msINjphT5mXa0XIGLJ7M"
    url = "https://docs.google.com/spreadsheets/d/15tknLvLFrBn8Pa-d8qI-yc7msINjphT5mXa0XIGLJ7M/edit?usp=sharing"
    df = pd.read_excel(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx")

    # Filter the DataFrame to get data for the entered employee_id
    employee_data = df[df['Employee Code'] == int(employee_id)]

    if employee_data.empty:
        st.warning("No data found for the given Employee ID.")
    else:
        # Create the output DataFrame
        output_df = pd.DataFrame(columns=['Employee code', 'Job title', 'Employee Name', 'Relationship'])

        # Extract employee details
        employee_code = employee_data['Employee Code'].iloc[0]
        job_title = employee_data['Job Title'].iloc[0]
        employee_name = employee_data['Employee Name'].iloc[0]

        # Add the employee as "Self" to the output DataFrame
        output_df.loc[0] = [employee_code, job_title, employee_name, 'Self']

        # Add subordinates and their subordinates recursively
        subordinates = df[(df['Direct Report Code'] == employee_code) | (df['Line Manager Code'] == employee_code)]
        for _, subordinate in subordinates.iterrows():
            subordinate_name = subordinate['Employee Name']
            output_df.loc[len(output_df)] = [subordinate['Employee Code'], subordinate['Job Title'], subordinate_name, 'Subordinate']

            # Find subordinates of the current subordinate
            sub_subordinates = df[(df['Direct Report Code'] == subordinate['Employee Code']) | (df['Line Manager Code'] == subordinate['Employee Code'])]
            for _, sub_subordinate in sub_subordinates.iterrows():
                sub_subordinate_name = sub_subordinate['Employee Name']
                output_df.loc[len(output_df)] = [sub_subordinate['Employee Code'], sub_subordinate['Job Title'], sub_subordinate_name, 'Subordinate']

        # Add direct managers
        direct_manager_codes = [employee_data['Direct Report Code'].iloc[0]]
        while direct_manager_codes[-1] != employee_data['Line Manager Code'].iloc[0]:
            direct_manager = df[df['Employee Code'].isin(direct_manager_codes[-1:])]
            direct_manager_name = direct_manager['Employee Name'].iloc[0]
            output_df.loc[len(output_df)] = [direct_manager['Employee Code'].iloc[0], direct_manager['Job Title'].iloc[0], direct_manager_name, 'Direct Manager']
            direct_manager_codes.append(direct_manager['Direct Report Code'].iloc[0])

        # Add line manager
        line_manager = df[df['Employee Code'] == employee_data['Line Manager Code'].iloc[0]]
        line_manager_name = line_manager['Employee Name'].iloc[0]
        output_df.loc[len(output_df)] = [line_manager['Employee Code'].iloc[0], line_manager['Job Title'].iloc[0], line_manager_name, 'Line Manager']

        # Add peers
        peers = df[(df['Department'] == employee_data['Department'].iloc[0]) & (df['Employee Code'] != employee_code)]
        for _, peer in peers.iterrows():
            peer_name = peer['Employee Name']
            if peer_name not in output_df['Employee Name'].values:
                output_df.loc[len(output_df)] = [peer['Employee Code'], peer['Job Title'], peer_name, 'Peer']

        # Display the output DataFrame
        return output_df

# Function to plot wellness data with smoothed curves for an employee and all employees
def plot_smoothed_wellness_data(employee_filtered_wellness_df, wellness_all_df):
    # Convert the 'Date' column to datetime
    wellness_all_df['Date'] = pd.to_datetime(wellness_all_df['Date'])
    employee_filtered_wellness_df['Date'] = pd.to_datetime(employee_filtered_wellness_df['Date'])

    # Group by 'Date' and calculate the mean Steps Count and Sleep Quality % for both DataFrames
    wellness_all_steps_mean = wellness_all_df.groupby('Date')['Steps Count'].mean().reset_index()
    employee_filtered_steps_mean = employee_filtered_wellness_df.groupby('Date')['Steps Count'].mean().reset_index()
    wellness_all_sleep_mean = wellness_all_df.groupby('Date')['Sleep Quality %'].mean().reset_index()
    employee_filtered_sleep_mean = employee_filtered_wellness_df.groupby('Date')['Sleep Quality %'].mean().reset_index()

    # Apply rolling mean to smoothen the lines
    window_size = 7  # Adjust the window size as needed
    wellness_all_steps_mean['Steps Count'] = wellness_all_steps_mean['Steps Count'].rolling(window=window_size).mean()
    employee_filtered_steps_mean['Steps Count'] = employee_filtered_steps_mean['Steps Count'].rolling(window=window_size).mean()
    wellness_all_sleep_mean['Sleep Quality %'] = wellness_all_sleep_mean['Sleep Quality %'].rolling(window=window_size).mean()
    employee_filtered_sleep_mean['Sleep Quality %'] = employee_filtered_sleep_mean['Sleep Quality %'].rolling(window=window_size).mean()

    # Create a Streamlit web app
    st.title('Your Customized Wellness Data Plot')

    # Create a modern theme using seaborn
    sns.set_style("whitegrid")
    sns.set_palette("colorblind")

    # Create subplots for "Steps Count" and "Sleep Quality %"
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    # Plot "Steps Count" on the first subplot
    sns.lineplot(data=wellness_all_steps_mean, x='Date', y='Steps Count', label='All Employees Data', marker='o', ax=ax1)
    sns.lineplot(data=employee_filtered_steps_mean, x='Date', y='Steps Count', label='Your Data', marker='o', ax=ax1)
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Steps Count')
    ax1.set_title('Steps Count Over Time')
    ax1.legend()
    ax1.grid(True)

    # Plot "Sleep Quality %" on the second subplot
    sns.lineplot(data=wellness_all_sleep_mean, x='Date', y='Sleep Quality %', label='All Employees Data', marker='o', ax=ax2)
    sns.lineplot(data=employee_filtered_sleep_mean, x='Date', y='Sleep Quality %', label='Your Data', marker='o', ax=ax2)
    ax2.set_xlabel('Date')
    ax2.set_ylabel('Sleep Quality %')
    ax2.set_title('Sleep Quality % Over Time')
    ax2.legend()
    ax2.grid(True)

    # Adjust layout
    plt.tight_layout()

    # Display the Streamlit app
    st.pyplot(fig)









import streamlit as st
import pandas as pd
from io import BytesIO
import base64
import os
import xlsxwriter
import pyodbc
import datetime
import matplotlib.pyplot as plt
import seaborn as sns


def upload_data_to_sql_server(df_uploaded):
    # Define your SQL Server connection parameters
    server_name = "192.168.2.186"
    database_name = 'carina'
    username = 'sa'
    password = 'P@$$w0rd'

    try:
        # Establish a connection to the SQL Server database
        conn = pyodbc.connect(
            f'DRIVER={{SQL Server}};SERVER={server_name};DATABASE={database_name};UID={username};PWD={password}'
        )

        # Create a cursor object to execute SQL commands
        cursor = conn.cursor()

        # SQL query to select all data from the table
        department_sql_query = "SELECT * FROM Department"
        category_sql_query = "SELECT * FROM Category"
        tax_sql_query = "SELECT * FROM Tax"
        supplier_sql_query = "SELECT * FROM Supplier"

        # Use Pandas to read the data into DataFrames
        df_department = pd.read_sql(department_sql_query, conn)
        df_category = pd.read_sql(category_sql_query, conn)
        df_tax = pd.read_sql(tax_sql_query, conn)
        df_supplier = pd.read_sql(supplier_sql_query, conn)

        # Map values in df_uploaded to match with the existing tables
        df_uploaded['DepartmentID'] = df_uploaded['Department'].map(df_department.set_index('Name')['ID'].to_dict())
        df_uploaded['CategoryID'] = df_uploaded['Category'].map(df_category.set_index('Name')['ID'].to_dict())
        df_uploaded['CategoryID'].fillna(-1, inplace=True)
        df_uploaded['SupplierID'] = df_uploaded['Supplier'].map(df_supplier.set_index('SupplierName')['ID'].to_dict())
        df_uploaded['TaxID'] = df_uploaded['Tax'].map(df_tax.set_index('Description')['ID'].to_dict())

        # SQL query to select all data from the table
        ItemClass_sql_query = f"SELECT * FROM ItemClass"

        # Use Pandas to read the data into a DataFrame
        df_ItemClass = pd.read_sql(ItemClass_sql_query, conn)

        # Check if df_ItemClass is not NaN
        if not df_ItemClass.empty:
            # Determine the next available ID
            next_id = df_ItemClass['ID'].max() + 1
        else:
            next_id = 1

        # Create a list of DataFrames to concatenate
        dfs_to_concat = []

        # Append rows from df_uploaded to df_ItemClass
        for index, row in df_uploaded.iterrows():
            new_row = {
                'ID': next_id,
                'Description': row['Description'],
                'Dimensions': 2,  # Replace with actual values
                'Title1': row['Item Lookup Code'].split("-")[0] + "_coloer",  # Replace with actual values
                'Title2': row['Item Lookup Code'].split("-")[0] + "_Size",  # Replace with actual values
                'Title3': "",  # Replace with actual values
                'ClassType': 0,  # Replace with actual values
                'DBTimeStamp': None,  # Replace with actual values
                'UseComponentPrice': 0,  # Replace with actual values
                'HQID': 0,  # Replace with actual values
                'ItemLookupCode': row['Item Lookup Code'].split("-")[0],  # Replace with actual values
                'DepartmentID': int(row['DepartmentID']),  # Replace with actual values
                'CategoryID': int(row['CategoryID']),  # Replace with actual values
                'Price': float(row['Price']),  # Replace with actual values
                'Cost': 0,  # Replace with actual values
                'SupplierID': int(row['SupplierID']),  # Replace with actual values
                'BarcodeFormat': int(row['BarcodeFormat']),  # Replace with actual values
                'SubDescription1': row['Family'],  # Replace with actual values
                'SubDescription2': row['Tybe'],  # Replace with actual values
                'SubDescription3': row['Season'],  # Replace with actual values
                'TaxID': row['TaxID'],  # Replace with actual values
                'Notes': "",  # Replace with actual values

                # Add other columns as needed
            }

            # Append the new row as a DataFrame to the list
            new_df = pd.DataFrame([new_row])
            dfs_to_concat.append(new_df)

            next_id += 1

        # Concatenate all DataFrames in the list
        df_ItemClass = pd.concat([df_ItemClass] + dfs_to_concat, ignore_index=True)

        # Remove duplicate rows based on the 'Item Lookup Code' column
        df_ItemClass = df_ItemClass.drop_duplicates(subset='ItemLookupCode', keep='first')

        # Reset the index after removing duplicates
        df_ItemClass.reset_index(drop=True, inplace=True)

        df_ItemClass.dropna(subset=['ID'], inplace=True)

        truncate_query_ItemClass = "TRUNCATE TABLE ItemClass"
        cursor.execute(truncate_query_ItemClass)

        # Enable IDENTITY_INSERT for the ItemClass table
        enable_identity_query = "SET IDENTITY_INSERT ItemClass ON"
        cursor.execute(enable_identity_query)

        # Loop through each row in the DataFrame and insert it into the SQL Server table
        for index, row in df_ItemClass.iterrows():
            # Construct the SQL INSERT query dynamically based on the column names
            columns = ', '.join(row.index)
            values = ', '.join(['DEFAULT' if col == 'DBTimeStamp' else '?' for col in row.index])

            # Filter out the row values, excluding 'DBTimeStamp'
            row_values = [val for val, col in zip(row, row.index) if col != 'DBTimeStamp']

            insert_query = f"INSERT INTO ItemClass ({columns}) VALUES ({values})"
            cursor.execute(insert_query, *row_values)

        # Commit the changes
        conn.commit()

        # Disable IDENTITY_INSERT for the ItemClass table
        disable_identity_query = "SET IDENTITY_INSERT ItemClass OFF"
        cursor.execute(disable_identity_query)

        print(f'Data appended to table ItemClass successfully.')

        # SQL query to select all data from the table
        Item_sql_query = f"SELECT * FROM Item"

        # Use Pandas to read the data into a DataFrame
        df_Item = pd.read_sql(Item_sql_query, conn)

        # Check if df_Item is not NaN
        if not df_Item.empty:
            # Determine the next available ID
            next_id = df_Item['ID'].max() + 1
        else:
            next_id = 1

        # Create a list of DataFrames to concatenate
        Item_dfs_to_concat = []

        # Append rows from df_uploaded to df_ItemClass
        for index, row in df_uploaded.iterrows():
            new_row = {
                'ID': next_id,
                "BinLocation": "",
                "BuydownPrice": 0,
                "BuydownQuantity": 0,
                "CommissionAmount": 0,
                "CommissionMaximum": 0,
                "CommissionMode": 1,
                "CommissionPercentProfit": 0,
                "CommissionPercentSale": 0,
                "FoodStampable": 0,
                "HQID": 0,
                "ItemNotDiscountable": 0,
                "LastReceived": None,
                "LastUpdated": datetime.datetime.now(),
                "QuantityCommitted": 0,
                "SerialNumberCount": 0,
                "TareWeightPercent": 0,
                "MessageID": 0,
                "PriceA": 0,
                "PriceB": 0,
                "PriceC": 0,
                "SalePrice": 0,
                "SaleStartDate": None,
                "SaleEndDate": None,
                "QuantityDiscountID": 0,
                "ItemType": 0,
                "Quantity": 0,
                "ReorderPoint": 0,
                "RestockLevel": 0,
                "TareWeight": 0,
                "TagAlongItem": 0,
                "TagAlongQuantity": 0,
                "ParentItem": 0,
                "ParentQuantity": 0,
                "PriceLowerBound": 0,
                "PriceUpperBound": 0,
                "PictureName": "",
                "LastSold": None,
                "UnitOfMeasure": "",
                "SubCategoryID": 0,
                "QuantityEntryNotAllowed": 0,
                "PriceMustBeEntered": 0,
                "BlockSalesReason": "",
                "BlockSalesAfterDate": None,
                "Weight": 0,
                "Taxable": 1,
                "BlockSalesBeforeDate": None,
                "LastCost": 0,
                "ReplacementCost": 0,
                "WebItem": 0,
                "BlockSalesType": 0,
                "BlockSalesScheduleID": 0,
                "SaleType": 0,
                "SaleScheduleID": 0,
                "Consignment": 0,
                "Inactive": 0,
                "LastCounted": None,
                "DoNotOrder": 0,
                "MSRP": 0,
                "DateCreated": datetime.datetime.now(),
                "UsuallyShip": '10002102',
                "ExtendedDescription": "",
                "Content": "",
                "NumberFormat": None,
                "ItemCannotBeRet": None,
                "ItemCannotBeSold": None,
                "IsAutogenerated": None,
                "IsGlobalvoucher": 0,
                "DeleteZeroBalanceEntry": None,
                "TenderID": 0,
                'Description': row['Description'],
                'DBTimeStamp': None,  # Replace with actual values
                'ItemLookupCode': row['Item Lookup Code'],  # Replace with actual values
                'DepartmentID': int(row['DepartmentID']),  # Replace with actual values
                'CategoryID': int(row['CategoryID']),  # Replace with actual values
                'Price': float(row['Price']),  # Replace with actual values
                'Cost': 0,  # Replace with actual values
                'SupplierID': int(row['SupplierID']),  # Replace with actual values
                'BarcodeFormat': int(row['BarcodeFormat']),  # Replace with actual values
                'SubDescription1': row['Family'],  # Replace with actual values
                'SubDescription2': row['Tybe'],  # Replace with actual values
                'SubDescription3': row['Season'],  # Replace with actual values
                'TaxID': row['TaxID'],  # Replace with actual values
                'Notes': None  # Replace with actual values

                # Add other columns as needed
            }

            # Append the new row as a DataFrame to the list
            new_df = pd.DataFrame([new_row])
            Item_dfs_to_concat.append(new_df)

            next_id += 1

        # Concatenate all DataFrames in the list
        df_Item = pd.concat([df_Item] + Item_dfs_to_concat, ignore_index=True)

        df_Item.dropna(subset=['ID'], inplace=True)

        truncate_query_Item = "TRUNCATE TABLE Item"
        cursor.execute(truncate_query_Item)

        # Enable IDENTITY_INSERT for the Item table
        enable_identity_query = "SET IDENTITY_INSERT Item ON"
        cursor.execute(enable_identity_query)

        # Loop through each row in the DataFrame and insert it into the SQL Server table
        for index, row in df_Item.iterrows():
            # Construct the SQL INSERT query dynamically based on the column names
            columns = ', '.join(row.index)
            values = ', '.join(['DEFAULT' if col == 'DBTimeStamp' else '?' for col in row.index])

            # Filter out the row values, excluding 'DBTimeStamp'
            row_values = [val for val, col in zip(row, row.index) if col != 'DBTimeStamp']

            insert_query = f"INSERT INTO Item ({columns}) VALUES ({values})"
            cursor.execute(insert_query, *row_values)

        # Commit the changes
        conn.commit()

        # Disable IDENTITY_INSERT for the Item table
        disable_identity_query = "SET IDENTITY_INSERT Item OFF"
        cursor.execute(disable_identity_query)

        # SQL query to select all data from the table
        ItemClassComponent = f"SELECT * FROM ItemClassComponent"

        # Use Pandas to read the data into a DataFrame
        df_ItemClassComponent = pd.read_sql(ItemClassComponent, conn)

        # Check if df_ItemClass is not NaN
        if not df_ItemClassComponent.empty:
            # Determine the next available ID
            next_id = df_ItemClassComponent['ID'].max() + 1
        else:
            next_id = 1

        # Create a list of DataFrames to concatenate
        ItemClassComponent_dfs_to_concat = []

        for index, row in df_uploaded.iterrows():
            ItemClassID_row = df_ItemClass[df_ItemClass['ItemLookupCode'] == row['Item Lookup Code'].split("-")[0]]
            ItemClassID =  ItemClassID_row.iloc[0]['ID']
            new_row = {
                "ID":next_id,
                "ItemClassID":ItemClassID,
                "ItemID":next_id,
                "Quantity":1,
                "Detail1":row['Color'],
                "Detail2":row['Size'],
                "Detail3":"",
                "LastUpdated":datetime.datetime.now(),
                "Price": 0,
            }

            # Append the new row as a DataFrame to the list
            new_df = pd.DataFrame([new_row])
            ItemClassComponent_dfs_to_concat.append(new_df)

            next_id += 1


        # Concatenate all DataFrames in the list
        df_ItemClassComponent = pd.concat([df_ItemClassComponent] + ItemClassComponent_dfs_to_concat, ignore_index=True)

        df_ItemClassComponent.dropna(subset=['ID'], inplace=True)

        truncate_query_ItemClassComponent = "TRUNCATE TABLE ItemClassComponent"
        cursor.execute(truncate_query_ItemClassComponent)

        # Enable IDENTITY_INSERT for the Item table
        enable_identity_query = "SET IDENTITY_INSERT ItemClassComponent ON"
        cursor.execute(enable_identity_query)

        # Loop through each row in the DataFrame and insert it into the SQL Server table
        for index, row in df_ItemClassComponent.iterrows():
            # Construct the SQL INSERT query dynamically based on the column names
            columns = ', '.join(row.index)
            values = ', '.join(['DEFAULT' if col == 'DBTimeStamp' else '?' for col in row.index])

            # Filter out the row values, excluding 'DBTimeStamp'
            row_values = [val for val, col in zip(row, row.index) if col != 'DBTimeStamp']

            insert_query = f"INSERT INTO ItemClassComponent ({columns}) VALUES ({values})"
            cursor.execute(insert_query, *row_values)

        # Commit the changes
        conn.commit()

        # Disable IDENTITY_INSERT for the Item table
        disable_identity_query = "SET IDENTITY_INSERT ItemClassComponent OFF"
        cursor.execute(disable_identity_query)

        # Create a list to store the grouped data
        grouped_data = []

        for _, row in df_uploaded.iterrows():
            code_prefix = row['Item Lookup Code'].split('-')[0]  # Get the prefix of the Item Lookup Code
            item_code = code_prefix

            # Add rows for sizes
            grouped_data.append({
                'ItemLookupCode': item_code,
                'Attribute': row['Size'],
                'Type': 2,  # Indicate that it's a size
            })

            # Add rows for colors
            grouped_data.append({
                'ItemLookupCode': item_code,
                'Attribute': row['Color'],
                'Type': 1,  # Indicate that it's a color
            })

        # Create a DataFrame from the list of dictionaries
        grouped_df = pd.DataFrame(grouped_data)

        # Remove duplicated rows
        grouped_df = grouped_df.drop_duplicates()

        # Add a "display_order" column
        grouped_df['display_order'] = grouped_df.groupby(['ItemLookupCode', 'Type']).cumcount() + 1


        # Sample data for dim_df
        dim_data = {
            "dim_name": [
                "Black", "White", "Off White", "Ivory", "Med.Grey Chine",
                "D.Grey", "Med.Grey", "Silver", "Navy", "Tobaco", "Nude",
                "Light Beige", "Med.Beige", "Dark Beige", "Blue", "Dark Brown",
                "Coffee", "Havan", "Taupe", "Camel", "Greish", "Cigar", "kaki",
                "Light Yellow", "Bright Yellow", "Canaria Yellow", "Dark Yellow",
                "Mustard", "GoldenYellow", "Lime", "Pistache", "Light Mint",
                "Classic Green", "Dark Teal", "green grey", "Olive", "Emerald Green",
                "Deep Green", "Dark Aqua", "Aqua", "Turquoise", "Olive Green",
                "Teal Green", "Blue Petroleum", "Light Blue", "True Blue", "Royal Blue",
                "Blue Marine", "Pastel Light Blue", "Light Indigo", "Dark Indigo",
                "Purple", "Deep Purple", "Light Orchide", "Bois De Rose", "Lavender",
                "Light Lilac", "Deep Lilac", "Violet", "Rose", "Hot Pink", "Peach Blossom",
                "Fucshia", "Magenta", "Wine", "Peach Skin", "Pink Violet", "Watermelon",
                "Blush", "Coral", "Coral Pink", "peach", "Pale Peach", "Pink", "Red",
                "Chile Peper", "Red Orange", "Red Brique", "Orange", "Neon Orange",
                "Orange Simon", "Light Orange", "Marigold", "Light Simon", "Shrimp Orange",
                "Light Burgundy", "Interacid", "Colored", "S", "M", "L", "XL", "2XL",
                "3XL", "4XL", "SM", "LXL", "23X", "OS", "ML", "X2X", "1-2", "2-4",
                "4-6", "6-8", "8-10", "10-12", "12-14", "14-16"
            ],
            "dim_id": [
                "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
                "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
                "21", "22", "23", "24", "25", "27", "28", "29", "30", "31",
                "32", "33", "34", "35", "36", "37", "38", "39", "40", "41",
                "42", "43", "44", "45", "46", "47", "48", "49", "50", "51",
                "52", "53", "54", "55", "56", "57", "58", "59", "60", "61",
                "62", "63", "64", "65", "66", "67", "68", "70", "71", "72",
                "73", "74", "75", "76", "77", "78", "80", "81", "83", "84",
                "85", "86", "87", "88", "89", "90", "91", "99", "S", "M", "L",
                "XL", "2XL", "3XL", "4XL", "SM", "LXL", "23X", "OS", "ML", "X2X",
                "1-2", "2-4", "4-6", "6-8", "8-10", "10-12", "12-14", "14-16"
            ]
        }

        dim_df = pd.DataFrame(dim_data)

        # Merge the dim_df and grouped_df to add the "attribute_id" column
        result_df = grouped_df.merge(dim_df, left_on="Attribute", right_on="dim_name", how="left")

        # Rename the "dim_id" column to "attribute_id" and drop the "dim_name" column
        result_df = result_df.rename(columns={"dim_id": "attribute_id"}).drop(columns=["dim_name"])


        # SQL query to select all data from the table
        MatrixAttributeDisplayOrder = f"SELECT * FROM MatrixAttributeDisplayOrder"

        # Use Pandas to read the data into a DataFrame
        df_MatrixAttributeDisplayOrder = pd.read_sql(MatrixAttributeDisplayOrder, conn)

        # Check if df_ItemClass is not NaN
        if not df_MatrixAttributeDisplayOrder.empty:
            # Determine the next available ID
            next_id = df_MatrixAttributeDisplayOrder['ID'].max() + 1
        else:
            next_id = 1

        # Create a list of DataFrames to concatenate
        MatrixAttributeDisplayOrder_dfs_to_concat = []

        for index, row in result_df.iterrows():
            ItemClassID_row = df_ItemClass[df_ItemClass['ItemLookupCode'] == row['ItemLookupCode']]
            ItemClassID =  ItemClassID_row.iloc[0]['ID']

            new_row = {
                "ID":next_id,
                "ItemClassID":ItemClassID,
                "Dimension":row['Type'],
                "Attribute":row['Attribute'],
                "Code":row['attribute_id'],
                "DisplayOrder":row["display_order"],
                "Inactive":0,
                "HQID":0,
            }

            # Append the new row as a DataFrame to the list
            new_df = pd.DataFrame([new_row])
            MatrixAttributeDisplayOrder_dfs_to_concat.append(new_df)

            next_id += 1


        # Concatenate all DataFrames in the list
        df_MatrixAttributeDisplayOrder = pd.concat([df_MatrixAttributeDisplayOrder] + MatrixAttributeDisplayOrder_dfs_to_concat, ignore_index=True)

        df_MatrixAttributeDisplayOrder.dropna(subset=['ID'], inplace=True)

        truncate_query_MatrixAttributeDisplayOrder = "TRUNCATE TABLE MatrixAttributeDisplayOrder"
        cursor.execute(truncate_query_MatrixAttributeDisplayOrder)

        # Enable IDENTITY_INSERT for the Item table
        enable_identity_query = "SET IDENTITY_INSERT MatrixAttributeDisplayOrder ON"
        cursor.execute(enable_identity_query)

        # Loop through each row in the DataFrame and insert it into the SQL Server table
        for index, row in df_MatrixAttributeDisplayOrder.iterrows():
            # Construct the SQL INSERT query dynamically based on the column names
            columns = ', '.join(row.index)
            values = ', '.join(['DEFAULT' if col == 'DBTimeStamp' else '?' for col in row.index])

            # Filter out the row values, excluding 'DBTimeStamp'
            row_values = [val for val, col in zip(row, row.index) if col != 'DBTimeStamp']

            insert_query = f"INSERT INTO MatrixAttributeDisplayOrder ({columns}) VALUES ({values})"
            cursor.execute(insert_query, *row_values)

        # Commit the changes
        conn.commit()

        # Disable IDENTITY_INSERT for the Item table
        disable_identity_query = "SET IDENTITY_INSERT MatrixAttributeDisplayOrder OFF"
        cursor.execute(disable_identity_query)

        # Close the database connection
        conn.close()
        print(f'Data appended to table Item successfully.')
    except Exception as e:
        print(f'Error uploading data to SQL Server: {str(e)}')








# Function to process the Excel file
def process_excel_file(uploaded_file):
    try:
        # Load the Excel workbook
        df = pd.read_excel(uploaded_file)

        # Rename the '%' column to a valid name (e.g., 'Percentage')
        df.rename(columns={'%': 'Percentage'}, inplace=True)

        # Your data processing code here (unchanged)
        dfs_to_concat = []  # Create a list to store DataFrames for concatenation
        stores_to_loop_over = df['Store ID'].unique()
        for i in stores_to_loop_over:
            df_sample = df.loc[df['Store ID'] == i].copy()  # Create a copy to avoid SettingWithCopyWarning
            sum_of_25 = sum(df_sample['SALES14%'].loc[(df_sample['SALES14%'] < 2500) | (df_sample['Employee ID'].isnull()) | (df_sample['job'] == "Store Manager") | (df_sample['job'] == "Stock Controller") | (df_sample['job'] == "Cashier")].values)
            df_sample.loc[(df_sample['SALES14%'] < 2500) | (df_sample['Employee ID'].isnull()) | (df_sample['job'] == "Store Manager") | (df_sample['job'] == "Stock Controller") | (df_sample['job'] == "Cashier"), 'Distiribution'] = 0
            length = len(df_sample) - len(df_sample['SALES14%'].loc[(df_sample['SALES14%'] < 2500) | (df_sample['Employee ID'].isnull()) | (df_sample['job'] == "Store Manager") | (df_sample['job'] == "Stock Controller") | (df_sample['job'] == "Cashier")])
            df_sample.loc[df_sample['Distiribution'] != 0, 'Distiribution'] = sum_of_25 / length
            df_sample["NET SALES"] = df_sample['Distiribution'] + df_sample["SALES14%"]
            df_sample.loc[(df_sample['SALES14%'] < 2500) | (df_sample['Employee ID'].isnull()) | (df_sample['job'] == "Store Manager") | (df_sample['job'] == "Stock Controller") | (df_sample['job'] == "Cashier"), 'NET SALES'] = 0
            df_sample["comm"] = df_sample['Percentage'] * df_sample["NET SALES"]
            sum_of_all = sum(df_sample['comm'])
            df_sample.loc[df_sample['job'] == "Store Manager", 'comm'] = sum_of_all
            df_sample.loc[df_sample['job'] == "Stock Controller", 'comm'] = sum_of_all * 0.15
            df_sample.loc[df_sample['job'] == "Cashier", 'comm'] = sum_of_all * 0.2
            dfs_to_concat.append(df_sample)

        # Concatenate DataFrames
        df2 = pd.concat(dfs_to_concat, ignore_index=True)

        # Return the processed DataFrame
        return df2, None  # Return the DataFrame and no error message

    except Exception as e:
        return None, str(e)  # Return no DataFrame and the error message

def fetch_data_from_wellness_data(employee_id):
    wellness_all_df = pd.read_excel("https://docs.google.com/spreadsheets/d/1MydjNCc6GY2pFwLVvvp6vhERBh-FBU5oCOZ5t5kFa7M/export?format=xlsx&gid=783663279")

    # Filter the DataFrame to get data for the entered employee_id
    employee_filtered_wellness_df = wellness_all_df[wellness_all_df['Please Enter your Code — برجاء إدخال كودك'] == int(employee_id)]
    # Extract the desired columns and rename them
    employee_filtered_wellness_df = employee_filtered_wellness_df.iloc[:, 2:6]  # Extract columns 3 to 6
    wellness_all_df = wellness_all_df.iloc[:, 2:6] 
    employee_filtered_wellness_df.columns = ["Date", "Sleep Quality %", "Steps Count", "# of Burned Calories"]
    wellness_all_df.columns = ["Date", "Sleep Quality %", "Steps Count", "# of Burned Calories"]
    #employee_filtered_wellness_df["Date"] = pd.to_datetime(employee_filtered_wellness_df["Date"]).dt.date
    #wellness_all_df["Date"]= pd.to_datetime(wellness_all_df["Date"]).dt.date

    return employee_filtered_wellness_df, wellness_all_df

# Function to fetch data from Google Sheets based on employee ID
def fetch_data_from_google_sheets(employee_id):
    # Use the Google Sheets API to fetch data
    sheet_id = "15tknLvLFrBn8Pa-d8qI-yc7msINjphT5mXa0XIGLJ7M"
    url = "https://docs.google.com/spreadsheets/d/15tknLvLFrBn8Pa-d8qI-yc7msINjphT5mXa0XIGLJ7M/edit?usp=sharing"
    df = pd.read_excel(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx")

    # Filter the DataFrame to get data for the entered employee_id
    employee_data = df[df['Employee Code'] == int(employee_id)]

    if employee_data.empty:
        st.warning("No data found for the given Employee ID.")
    else:
        # Create the output DataFrame
        output_df = pd.DataFrame(columns=['Employee code', 'Job title', 'Employee Name', 'Relationship'])

        # Extract employee details
        employee_code = employee_data['Employee Code'].iloc[0]
        job_title = employee_data['Job Title'].iloc[0]
        employee_name = employee_data['Employee Name'].iloc[0]

        # Add the employee as "Self" to the output DataFrame
        output_df.loc[0] = [employee_code, job_title, employee_name, 'Self']

        # Add subordinates and their subordinates recursively
        subordinates = df[(df['Direct Report Code'] == employee_code) | (df['Line Manager Code'] == employee_code)]
        for _, subordinate in subordinates.iterrows():
            subordinate_name = subordinate['Employee Name']
            output_df.loc[len(output_df)] = [subordinate['Employee Code'], subordinate['Job Title'], subordinate_name, 'Subordinate']

            # Find subordinates of the current subordinate
            sub_subordinates = df[(df['Direct Report Code'] == subordinate['Employee Code']) | (df['Line Manager Code'] == subordinate['Employee Code'])]
            for _, sub_subordinate in sub_subordinates.iterrows():
                sub_subordinate_name = sub_subordinate['Employee Name']
                output_df.loc[len(output_df)] = [sub_subordinate['Employee Code'], sub_subordinate['Job Title'], sub_subordinate_name, 'Subordinate']

        # Add direct managers
        direct_manager_codes = [employee_data['Direct Report Code'].iloc[0]]
        while direct_manager_codes[-1] != employee_data['Line Manager Code'].iloc[0]:
            direct_manager = df[df['Employee Code'].isin(direct_manager_codes[-1:])]
            direct_manager_name = direct_manager['Employee Name'].iloc[0]
            output_df.loc[len(output_df)] = [direct_manager['Employee Code'].iloc[0], direct_manager['Job Title'].iloc[0], direct_manager_name, 'Direct Manager']
            direct_manager_codes.append(direct_manager['Direct Report Code'].iloc[0])

        # Add line manager
        line_manager = df[df['Employee Code'] == employee_data['Line Manager Code'].iloc[0]]
        line_manager_name = line_manager['Employee Name'].iloc[0]
        output_df.loc[len(output_df)] = [line_manager['Employee Code'].iloc[0], line_manager['Job Title'].iloc[0], line_manager_name, 'Line Manager']

        # Add peers
        peers = df[(df['Department'] == employee_data['Department'].iloc[0]) & (df['Employee Code'] != employee_code)]
        for _, peer in peers.iterrows():
            peer_name = peer['Employee Name']
            if peer_name not in output_df['Employee Name'].values:
                output_df.loc[len(output_df)] = [peer['Employee Code'], peer['Job Title'], peer_name, 'Peer']

        # Display the output DataFrame
        return output_df

# Function to plot wellness data with smoothed curves for an employee and all employees
def plot_smoothed_wellness_data(employee_filtered_wellness_df, wellness_all_df):
    # Convert the 'Date' column to datetime
    wellness_all_df['Date'] = pd.to_datetime(wellness_all_df['Date'])
    employee_filtered_wellness_df['Date'] = pd.to_datetime(employee_filtered_wellness_df['Date'])

    # Group by 'Date' and calculate the mean Steps Count and Sleep Quality % for both DataFrames
    wellness_all_steps_mean = wellness_all_df.groupby('Date')['Steps Count'].mean().reset_index()
    employee_filtered_steps_mean = employee_filtered_wellness_df.groupby('Date')['Steps Count'].mean().reset_index()
    wellness_all_sleep_mean = wellness_all_df.groupby('Date')['Sleep Quality %'].mean().reset_index()
    employee_filtered_sleep_mean = employee_filtered_wellness_df.groupby('Date')['Sleep Quality %'].mean().reset_index()

    # Apply rolling mean to smoothen the lines
    window_size = 7  # Adjust the window size as needed
    wellness_all_steps_mean['Steps Count'] = wellness_all_steps_mean['Steps Count'].rolling(window=window_size).mean()
    employee_filtered_steps_mean['Steps Count'] = employee_filtered_steps_mean['Steps Count'].rolling(window=window_size).mean()
    wellness_all_sleep_mean['Sleep Quality %'] = wellness_all_sleep_mean['Sleep Quality %'].rolling(window=window_size).mean()
    employee_filtered_sleep_mean['Sleep Quality %'] = employee_filtered_sleep_mean['Sleep Quality %'].rolling(window=window_size).mean()

    # Create a Streamlit web app
    st.title('Your Customized Wellness Data Plot')

    # Create a modern theme using seaborn
    sns.set_style("whitegrid")
    sns.set_palette("colorblind")

    # Create subplots for "Steps Count" and "Sleep Quality %"
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    # Plot "Steps Count" on the first subplot
    sns.lineplot(data=wellness_all_steps_mean, x='Date', y='Steps Count', label='All Employees Data', marker='o', ax=ax1)
    sns.lineplot(data=employee_filtered_steps_mean, x='Date', y='Steps Count', label='Your Data', marker='o', ax=ax1)
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Steps Count')
    ax1.set_title('Steps Count Over Time')
    ax1.legend()
    ax1.grid(True)

    # Plot "Sleep Quality %" on the second subplot
    sns.lineplot(data=wellness_all_sleep_mean, x='Date', y='Sleep Quality %', label='All Employees Data', marker='o', ax=ax2)
    sns.lineplot(data=employee_filtered_sleep_mean, x='Date', y='Sleep Quality %', label='Your Data', marker='o', ax=ax2)
    ax2.set_xlabel('Date')
    ax2.set_ylabel('Sleep Quality %')
    ax2.set_title('Sleep Quality % Over Time')
    ax2.legend()
    ax2.grid(True)

    # Adjust layout
    plt.tight_layout()

    # Display the Streamlit app
    st.pyplot(fig)







# Define a user database (you can use a more secure storage method)
users = {
    'fares': bcrypt.hashpw('fares'.encode('utf-8'), bcrypt.gensalt()),
    'eslam': bcrypt.hashpw('eslam'.encode('utf-8'), bcrypt.gensalt())
}

# Function to check user credentials
def authenticate_user(username, password):
    stored_password = users.get(username)
    if stored_password and bcrypt.checkpw(password.encode('utf-8'), stored_password):
        return True
    return False

# Streamlit app
def main():
    # Enable wide layout for the sidebar
    st.set_page_config(layout="wide")

    if not st.session_state.get("logged_in", False):
        # User is not logged in, display the login section
        st.title("Login")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")

        if st.button("Login"):
            if authenticate_user(username, password):
                st.success(f"Login successful! Welcome, {username}!")
                st.session_state.logged_in = True  # Store the login status in the session
                # Navigate to the home page (Add this section to redirect)
                st.experimental_rerun()
            else:
                st.error("Login failed. Invalid username or password.")
    else:
        # User is logged in, display the app content
        st.sidebar.title("Menu")

        # Define Font Awesome icons for the menus
        menu_icons = {
            "Home": "🏠",
            "Carina Commission": "💰",
            "Empower360": "🚀",
            "Merchandising": "🛍️",
            "Wellness Program": "🏃‍",
        }

        selected_option = st.sidebar.radio("Select an Option", list(menu_icons.keys()), format_func=lambda x: f"{menu_icons[x]} {x}")

        if selected_option == "Home":
            # Add a title with custom font and color using HTML and CSS
            st.markdown(
                """
                <style>
                .pink-title {
                    font-family: 'Felix Titling';
                    font-size: 36px;
                    color: #d9027d;
                }
                .description {
                    font-family: 'JetBrains Mono';
                    font-size: 18px;
                    color: #1a1a1a;
                }
                </style>
                """,
                unsafe_allow_html=True,
            )
            st.markdown('<h1 class="pink-title">Carina Automation Hub</h1>', unsafe_allow_html=True)
            st.markdown(
                """
                <p class="description">I'm a Data Analyst at Carina Wear, and I've worn so many hats that I'm starting to lose track!

                Not content with just crunching numbers, I've taken on roles as an automation engineer, and now, I've even dabbled in front end and back end development.
                Who knew data analysis could lead to such a wild career evolution? I'm like a one-person tech circus – juggling data, automation, and coding with a healthy dose of humor!
                </p>
                """,
                unsafe_allow_html=True,
            )

            # Display scrolling pictures in an album side by side using columns layout
            col1, col2, col3 = st.columns(3)
            with col1:
                st.image("Album1.jpg", use_column_width=True)
            with col2:
                st.image("Album2.jpg", use_column_width=True)
            with col3:
                st.image("Album3.jpg", use_column_width=True)

        # Add other menu options and content here
        elif selected_option == "Carina Commission":
            st.title("Carina Commission Calculator")
            uploaded_file = st.file_uploader("Upload an Excel file", type=["xlsx"])

            if uploaded_file:
                st.success("File uploaded successfully!")

                if st.button("Calculate Caria Commissions"):
                    processed_df, error_message = process_excel_file(uploaded_file)
                    if processed_df is not None:
                        st.success("Carina Commissions calculated successfully!")

                        # Create an Excel workbook
                        output = BytesIO()

                        # Reset the position of the output stream to the beginning
                        output.seek(0)

                        # Write the processed DataFrame to the Excel workbook
                        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                            processed_df.to_excel(writer, sheet_name='Sheet1', index=False)

                        # Close the workbook
                        output.seek(0)  # Reset the position again after writing

                        # Create a download link for the Excel workbook
                        b64 = base64.b64encode(output.read()).decode()
                        href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="Carina_CommissionDone.xlsx">Download Carina Commission Done</a>'
                        st.markdown(href, unsafe_allow_html=True)
                    else:
                        st.error(f"Error processing Excel file: {error_message}")

        elif selected_option == "Empower360":
            st.title("Empower360")
            employee_id = st.text_input("Enter your employee ID")

            if st.button("Fetch Data"):
                if not employee_id:
                    st.warning("Please enter your employee ID.")
                else:
                    st.success("Fetched data...")

                    # Fetch data from Google Sheets based on employee ID
                    data = fetch_data_from_google_sheets(employee_id)

                    # Display the resulting DataFrame
                    st.write("Your Empower360 Required Input Data:")
                    st.write(data)
        elif selected_option == "Merchandising":
            st.title("Merchandising")
            uploaded_item_file = st.file_uploader("Upload an Excel file for the items", type=["xlsx"])

            if uploaded_item_file:
                st.success("File uploaded successfully!")

                if st.button("Start Uploading Items"):
                    # Process the uploaded Excel file for merchandising (replace with your processing logic)
                    df_uploaded = pd.read_excel(uploaded_item_file)

                    # Display the processed items (replace with appropriate display)
                    st.write("Uploading these Items....")
                    st.write(df_uploaded)
                    upload_data_to_sql_server(df_uploaded)
                    st.success("Congrats! Item & ItemClass Tables have been Updated")

        elif selected_option == "Wellness Program":
            st.title("Wellness Program")
            employee_id = st.text_input("Enter your employee ID")

            if st.button("Fetch Data"):
                if not employee_id:
                    st.warning("Please enter your employee ID.")
                else:
                    st.success("Fetching Data...")

                    # Fetch data from Google Sheets based on employee ID
                    employee_filtered_wellness_df, wellness_all_df = fetch_data_from_wellness_data(employee_id)

                    # Check if data is found for the entered Employee ID
                    if employee_filtered_wellness_df.empty:
                        st.warning('No data found for the entered Employee ID.')
                    else:
                        st.write('Progress Data for Employee ID:', employee_id)
                        st.write(employee_filtered_wellness_df)
                        # Visualize progress data
                        st.subheader('Visualize Your Progress')

                        plot_smoothed_wellness_data(employee_filtered_wellness_df, wellness_all_df)


if __name__ == "__main__":
    main()