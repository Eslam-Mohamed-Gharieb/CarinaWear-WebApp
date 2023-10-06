import streamlit as st
import pandas as pd
from io import BytesIO
import base64
import tkinter as tk
from tkinter import filedialog
import os
from tempfile import NamedTemporaryFile

# Function to process the Excel file
def process_excel_file(uploaded_file, output_file_name):
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

        # Get the user's home directory
        user_home = os.path.expanduser("~")

        # Construct the full output path (e.g., Desktop or Documents folder)
        output_folder = os.path.join(user_home, "Desktop")  # Change "Desktop" to "Documents" if needed
        output_file_path = os.path.join(output_folder, output_file_name + ".xlsx")

        # Save the DataFrame to the specified folder
        df2.to_excel(output_file_path, index=False)
        return output_file_path

    except Exception as e:
        return str(e)

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

            output_file_name = st.text_input("Choose the name of your file")

            if st.button("Calculate Commissions") and output_file_name:
                output_file_path = process_excel_file(uploaded_file, output_file_name)
                if not isinstance(output_file_path, str):
                    st.error(f"Error processing Excel file: {output_file_path}")
                else:
                    st.success(f"Commissions calculated and saved to: {output_file_path}")

    elif selected_option == "Other Functionality":
        st.title("Other Functionality")
        # Add code for other functionality here...

if __name__ == "__main__":
    main()
