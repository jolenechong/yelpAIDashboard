import streamlit as st
import time
import json
from confluent_kafka import Producer
from configs import conf

st.set_page_config(page_title="Stream and Dashboard Demo", page_icon="📈", layout="wide")
st.title("Stream and Dashboard Demo")

# Topic name
topic = 'kafka_wiu_stream_reviews'

# Create Kafka Producer
p = Producer(conf)

def delivery_callback(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg}]")

col1, divider, col2 = st.columns([3, 0.2, 3])

with col1:
    st.header("Simulate a Real-Time Stream with Unseen data")

    # Dropdown for selecting number of runs
    num_runs = st.selectbox("Select Number of Runs", list(range(1, 2000, 100)))

    # Button to trigger the runs
    run_button = st.button("Run")

    if run_button:
        st.write(f"Running {num_runs} times...")
        with open('demo.json') as f:
            lines = f.readlines()
            for i in range(num_runs):
                try:
                    data = json.loads(lines[i].strip())

                    # Replace the following lines with your Kafka producer logic
                    p.produce(topic, json.dumps(data).encode('utf-8'), callback=delivery_callback)
                    p.poll(0)

                    # For now, just print the data for demonstration
                    print(data)
                except Exception as e:
                    print(f"Error sending message: {e}")

                # Wait for a minute
                time.sleep(10)
        st.write(f"Stream ended!")
        p.flush()

# Divider
# st.markdown("---")

with col2:
    st.header("Try it out with your own input!")

    # Text input with smaller integer inputs
    input_text = st.text_input("Enter your review:")

    col1, col2, col3 = st.columns(3)
    input1 = col1.number_input("cool", value=0, step=1)
    input2 = col2.number_input("funny", value=0, step=1)
    input3 = col3.number_input("useful", value=0, step=1)
    business_name = st.text_input("business_name")

    # Button to submit input
    submit_button = st.button("Submit")

if submit_button:
    # produce to stream created json
    body = {'text': input_text, 'cool': input1, 'funny': input2, 'useful': input3, "business_name": business_name}
    p.produce(topic, json.dumps(body).encode('utf-8'), callback=delivery_callback)
    p.poll(0)
    p.flush()

