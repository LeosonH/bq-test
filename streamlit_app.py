import streamlit as st
from openai import OpenAI
from google.oauth2 import service_account
from google.cloud import bigquery
import os

# Show title and description.
st.title("💬 Chat with BigQuery")
st.write(
    "This is a chatbot that uses OpenAI's GPT-3.5 model to query your database using Natural Language. "
    "To use this app, you need to provide an OpenAI API key, which you can get [here](https://platform.openai.com/account/api-keys). "
    )

# Ask user for their OpenAI API key via `st.text_input`.
# Alternatively, you can store the API key in `./.streamlit/secrets.toml` and access it
# via `st.secrets`, see https://docs.streamlit.io/develop/concepts/connections/secrets-management
openai_api_key = st.text_input("OpenAI API Key", type="password")
bigquery_table_name = st.text_input("BigQuery Table Name", type="password")
if not openai_api_key:
    st.info("Please add your OpenAI API key to continue.", icon="🗝️")
if not bigquery_table_name:
    st.info("Please add your Google BigQuery Table Name to continue.")
else:        

    # Create an OpenAI client.
    client = OpenAI(api_key=openai_api_key)

    # Create a session state variable to store the chat messages. This ensures that the
    # messages persist across reruns.
    if "messages" not in st.session_state:
        st.session_state.messages = []
    # Display the existing chat messages via `st.chat_message`.
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Create a chat input field to allow the user to enter a message. This will display
    # automatically at the bottom of the page.
    if prompt := st.chat_input("Get me the first 10 rows."):

        # Store and display the current prompt.
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Generate a response using the OpenAI API.
        stream = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages= [{"role": "system", "content": f"You are a BigQuery SQL generator. Based on this table schema,\
                         respond with only the SQL query needed to answer the user's question. \
                         The table name is {bigquery_table_name}.  Interpret the user's question according to the following schema and instructions. The table includes the following columns, where the only values of each categorical column are spelled out in the brackets:\
                         Vendor_group (AWS compute, GCP other, AWS other), Vendor_service (VMware, Redshift, BigQuery, CloudSQL), Analysis_type (no region and org specific, region related, org related, machine region and org and machine type), Cloud_region (US, EU, China),\
                              Fin_reporting_products (IT, Cortex, CDL,Access, Finance), Org_name (null, main, support), Product_instance_type (E2.xlarge, R6G.large, F2.core), Usage_commitment_type (3 -year commitment, 1-year commitment, on-demand), \
                         Usage_date, Total_usage_cost, SKU_numbers, Count_of_machine, Folders, Unit_price. Folders is an INTEGER column."}] + 
                      [{"role": m["role"], "content": m["content"]} for m in st.session_state.messages],
            stream=True,
        )

        # Stream the response to the chat using `st.write_stream`, then store it in 
        # session state.
        with st.chat_message("assistant"):
            response = st.write_stream(stream)
        st.session_state.messages.append({"role": "assistant", "content": response})

        # BQ data pull
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        bigquery_client = bigquery.Client(credentials=credentials)             
        QUERY = response[6:-3]
        print(QUERY)
        #Write Query on BQ
        #QUERY = f"SELECT * FROM {bigquery_table_name} LIMIT 10"
        Query_Results = bigquery_client.query(QUERY)
        data = Query_Results.to_dataframe()
        
        st.dataframe(data, use_container_width=True)