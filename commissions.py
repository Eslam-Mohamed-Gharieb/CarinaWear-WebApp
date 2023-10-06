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

# Create the Streamlit app
def main():
    # Enable wide layout for the sidebar
    st.set_page_config(layout="wide")

    # Create a sidebar menu
    st.sidebar.title("Menu")

    # Add buttons or radio buttons to choose functionalities
    selected_option = st.sidebar.radio("Select an Option", ["Home", "Commission Calculator", "Other Functionality"])

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

    elif selected_option == "Other Functionality":
        st.title("Other Functionality")
        # Add code for other functionality here...

if __name__ == "__main__":
    main()