import streamlit as st
import pandas as pd
import threading
from confluent_kafka import Consumer, KafkaError
import time
import json
import plotly.express as px
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from configs import consumer_conf

# Initialize an empty DataFrame
data = pd.DataFrame(columns=['text', 'cool', 'funny', 'useful', 'food', 'service', 'ambience', 'price', 'customer_satisfaction', 'business_name'])
st.set_page_config(page_title="Stream and Dashboard Demo", page_icon="📈", layout="wide")
st.title("Stream and Dashboard Demo")

def consume_kafka_topic(topic_name):
    c = Consumer(consumer_conf)
    c.subscribe([topic_name])

    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        message = msg.value().decode('utf-8')
        message = json.loads(message)

        # concat to data
        global data
        data = pd.concat([data, pd.DataFrame(message, index=[0])], ignore_index=True)

        print(f"Received: {message}")

# Start Kafka message polling in a separate thread
kafka_thread = threading.Thread(target=consume_kafka_topic, args=('kafka_wiu_stream_reviews_processed',))
kafka_thread.daemon = True
kafka_thread.start()

placeholder = st.container()
with placeholder:
    m_1 = st.empty()

counter = 0
if 'business_names_selected' not in st.session_state:
    st.session_state.business_names_selected = []

while True:

    # check if data is a dataframe
    if isinstance(data, pd.DataFrame):
        if len(data) > 1:
            st.session_state.data = data.to_json()

    # use placegholder
    if "data" in st.session_state and len(st.session_state.data) > 0:
        data = pd.read_json(st.session_state.data)
        selected_data = data.copy(deep=True)

        with m_1.container():
            col1, col2 = st.columns([1, 2])
            with col1:
                business_names = st.multiselect("Filter by Business Name", selected_data["business_name"].unique(), key=f"business_name_{counter}")
                st.session_state.business_names_selected = business_names   
                if business_names:
                    selected_data = selected_data[selected_data['business_name'].isin(business_names)]

                col1_metric, col2_metric = st.columns(2)
                col1_metric.metric("Average Sentiment Scores", round(selected_data[['food', 'service', 'ambience', 'price']].mean().mean(), 2))
                col2_metric.metric("Total Records", selected_data.shape[0])
            with col2:
                very_satisfied_text = ' '.join(selected_data[selected_data['customer_satisfaction'] == 'Very Satisfied']['tokenized'])
                not_satisfied_text = ' '.join(selected_data[selected_data['customer_satisfaction'] == 'Not Satisfied']['tokenized'])

                # check if more than 1 word
                if len(very_satisfied_text) > 0 and len(not_satisfied_text) > 0:
                    # Create two WordCloud objects
                    plt.close()
                    wordcloud_very_satisfied = WordCloud(width=400, height=150, background_color='#13171a').generate(very_satisfied_text)
                    wordcloud_not_satisfied = WordCloud(width=400, height=150, background_color='#13171a').generate(not_satisfied_text)

                    # Plot the word clouds using Matplotlib
                    fig_wordcloud, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
                    fig_wordcloud.set_facecolor('#13171a')
                    ax1.imshow(wordcloud_very_satisfied, interpolation='bilinear')
                    ax1.set_title('Very Satisfied', color='white')
                    ax1.axis('off')

                    ax2.imshow(wordcloud_not_satisfied, interpolation='bilinear')
                    ax2.set_title('Not Satisfied', color='white')
                    ax2.axis('off')

                    # Display the word clouds in Streamlit
                    st.pyplot(fig_wordcloud)

            # Bar chart and line chart
            col1, col2 = st.columns(2)

            # Display the histogram chart in the first column
            with col1:
                mean_df = selected_data[["customer_satisfaction", "food", "service", "ambience", "price"]].groupby('customer_satisfaction').mean().reset_index()
                mean_df['count'] = selected_data.groupby('customer_satisfaction').count().reset_index()['food']

                fig1 = make_subplots(specs=[[{"secondary_y": True}]])
                df = mean_df

                fig1.add_trace(
                    go.Scatter(x=df['customer_satisfaction'], y=df['food'], name="food", mode="lines", line=dict(color="red")),
                    secondary_y=True
                )
                fig1.add_trace(
                    go.Scatter(x=df['customer_satisfaction'], y=df['service'], name="service", mode="lines", line=dict(color="pink")),
                    secondary_y=True
                )
                fig1.add_trace(
                    go.Scatter(x=df['customer_satisfaction'], y=df['ambience'], name="ambience", mode="lines", line=dict(color="green")),
                    secondary_y=True
                )
                fig1.add_trace(
                    go.Scatter(x=df['customer_satisfaction'], y=df['price'], name="price", mode="lines", line=dict(color="purple")),
                    secondary_y=True
                )
                fig1.update_yaxes(title_text="aspects", secondary_y=True)

                fig1.add_trace(
                    go.Bar(x=df['customer_satisfaction'], y=df['count'], name="count", marker=dict(color="blue"), opacity=0.5, yaxis="y"),
                    secondary_y=False
                )
                fig1.update_yaxes(title_text="count", secondary_y=False)
                fig1.update_xaxes(title_text="customer_satisfaction")

                # reduce margins
                fig1.update_layout(width=400, height=400, autosize=False, margin=dict(l=0, r=0, t=0, b=0))

                st.plotly_chart(fig1, use_container_width=True)

            with col2:
                selected_data['satisfaction_num'] = selected_data['customer_satisfaction'].map({'Not Satisfied': -1, 'Satisfied': 0, 'Very Satisfied': 1})
                fig = px.imshow(selected_data[['food', 'service', 'ambience', 'price', 'satisfaction_num']].corr(), color_continuous_scale='RdBu', title='Correlation Matrix')
                fig.update_layout(
                        coloraxis_colorbar=dict(title="Correlation"),
                        width=400, height=400, autosize=False)
                
                # remove margins
                fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))

                st.plotly_chart(fig, use_container_width=True)

            st.header("Detailed Data View")
            st.dataframe(selected_data[['text', 'cool', 'funny', 'useful', 'food', 'service', 'ambience', 'price', 'business_name', 'customer_satisfaction']])
            counter += 1  # Increment the counter to generate a new key in the next iteration



    time.sleep(1)