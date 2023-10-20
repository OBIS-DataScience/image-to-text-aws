import boto3
import os
import streamlit as st
import snowflake.connector

# Snowflake connection parameters


def detect_text_and_lookup(image, query_type):
    session = boto3.Session(region_name='us-west-2',aws_access_key_id='accesskeyid',
                    aws_secret_access_key='accesskey')
    client = session.client('rekognition')

    response = client.detect_text(Image={'Bytes': image })

    textDetections = response['TextDetections']

    # Gets the version
    ctx = snowflake.connector.connect(
        user='dbuser',
        password='dbpassword',
        account='snowflakeaccountname'
    )
    cur = ctx.cursor()
    ctx.cursor().execute('warehouse name')
    ctx.cursor().execute('schema name')
    cursor = ctx.cursor()

    query_results = []

    for text in textDetections:
        confidence = text['Confidence']
        if confidence > 90:
            detected_text = text['DetectedText']

            # Perform a Snowflake lookup for each detected text based on query_type
            if query_type == 1:
                query = f"SELECT equipment_code, equipment_status_description FROM tablename WHERE equipment_code = '{detected_text}'"
            else:
                query = f"select top 10 equip_num,meter_reading_date,eq_day_from_gps_reading,gps_daily_hours from tablename where equip_num='{detected_text}' order by meter_reading_date desc"

            cursor.execute(query)

            # Get the query result
            result = cursor.fetchall()
            query_results.append((detected_text, result))

    ctx.close()  # Close the Snowflake connection

    return query_results

def main():
    st.title("Image Text Detection and Snowflake Query")

    uploaded_image = st.file_uploader("Upload an image", type=["jpg", "png", "jpeg"])
    query_type = st.radio("Select Query Type", [1, 2], format_func=lambda x: "Get current equipment status" if x == 1 else "Get last 10 days of telematics history")
   
    # Initialize a button click state
    button_clicked = False

    if uploaded_image is not None:
        image = uploaded_image.read()

        st.image(uploaded_image, caption='Uploaded Image', use_column_width=True)

        # Check if the button has been clicked and prevent repeated execution
        if st.button("Detect Text and Run Snowflake Query") and not button_clicked:
            st.write("Detecting text and running Snowflake queries...")
            query_results = detect_text_and_lookup(image, query_type)
            if not query_results:
                st.write("No valid equipment code found.")
            else:
                unique_results = {}  # Use a dictionary to store unique values
                for detected_text, result in query_results:
                    # Only add the result if it's not in the dictionary
                    if detected_text not in unique_results:
                        unique_results[detected_text] = result

                for detected_text, result in unique_results.items():
                    st.write(f"Query result for text '{detected_text}': {result}")
            button_clicked = True  # Set the button click state to True

if __name__ == "__main__":
    main()
