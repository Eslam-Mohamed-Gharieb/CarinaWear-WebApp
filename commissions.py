import streamlit as st
import pandas as pd
from io import BytesIO
import base64
import os
import xlsxwriter
import pyodbc
import datetime


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
            next_id = 0

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
                'Cost': float(row['Cost']),  # Replace with actual values
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

        # Check if df_ItemClass is not NaN
        if not df_ItemClass.empty:
            # Determine the next available ID
            next_id = df_ItemClass['ID'].max() + 1
        else:
            next_id = 0

        # Create a list of DataFrames to concatenate
        Item_dfs_to_concat = []

        # Append rows from df_uploaded to df_ItemClass
        for index, row in df_uploaded.iterrows():
            new_row = {
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
                'ID': next_id,
                'Description': row['Description'],
                'DBTimeStamp': None,  # Replace with actual values
                'ItemLookupCode': row['Item Lookup Code'],  # Replace with actual values
                'DepartmentID': int(row['DepartmentID']),  # Replace with actual values
                'CategoryID': int(row['CategoryID']),  # Replace with actual values
                'Price': float(row['Price']),  # Replace with actual values
                'Cost': float(row['Cost']),  # Replace with actual values
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


    
# Create the Streamlit app
def main():
    # Enable wide layout for the sidebar
    st.set_page_config(layout="wide")

    # Create a sidebar menu
    st.sidebar.title("Menu")


    # Define Font Awesome icons for the menus
    menu_icons = {
        "Home": "🏠",
        "Carina Commission": "💰",
        "Empower360": "🚀",
        "Merchandising": "🛍️",  # Add an icon for the "Merchandising" menu
    }

    # Add buttons or radio buttons to choose functionalities
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
        st.markdown('<h1 class="pink-title">Harnessing Technology: Escaping the Limits of Manual Work</h1>', unsafe_allow_html=True)
        st.markdown(
            """
            <p class="description">I'm a Senior Data Analyst at Carina Wear, and I've worn so many hats that I'm starting to lose track!

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
if __name__ == "__main__":
    main()