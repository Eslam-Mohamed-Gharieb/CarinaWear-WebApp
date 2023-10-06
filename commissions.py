import streamlit as st
import pandas as pd
from io import BytesIO
import base64
import os
import xlsxwriter

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
            df_sample.loc[(df_sample['SALES14%'] < 2500) | (df_sample['Employee ID'].isnull()) | (df_sample['job'] == "Store Manager") | (df_sample['job'] == "Stock Controller") | (df_sample['job'] == "Cashier"), 'التوزيع'] = 0
            length = len(df_sample) - len(df_sample['SALES14%'].loc[(df_sample['SALES14%'] < 2500) | (df_sample['Employee ID'].isnull()) | (df_sample['job'] == "Store Manager") | (df_sample['job'] == "Stock Controller") | (df_sample['job'] == "Cashier")])
            df_sample.loc[df_sample['التوزيع'] != 0, 'التوزيع'] = sum_of_25 / length
            df_sample["NET SALES"] = df_sample['التوزيع'] + df_sample["SALES14%"]
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
        return df, None  # Return the DataFrame and no error message

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

    # Add buttons or radio buttons to choose functionalities
    selected_option = st.sidebar.radio("Select an Option", ["Home", "Commission Calculator","Empower360", "Other Functionality"])

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

    elif selected_option == "Commission Calculator":
        st.title("Commission Calculator")
        uploaded_file = st.file_uploader("Upload an Excel file", type=["xlsx"])

        if uploaded_file:
            st.success("File uploaded successfully!")

            if st.button("Calculate Commissions"):
                processed_df, error_message = process_excel_file(uploaded_file)
                if processed_df is not None:
                    st.success("Commissions calculated successfully!")

                    # Create an Excel workbook
                    output = BytesIO()
                    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
                    worksheet = workbook.add_worksheet()

                    # Write the processed DataFrame to the Excel workbook
                    processed_df.to_excel(output, index=False, sheet_name="Commission_Report")

                    # Close the workbook
                    workbook.close()

                    # Create a download link for the Excel workbook
                    b64 = base64.b64encode(output.getvalue()).decode()
                    href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="Carina_Commission_Done.xlsx">Download Carina Commission Done</a>'
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

if __name__ == "__main__":
    main()