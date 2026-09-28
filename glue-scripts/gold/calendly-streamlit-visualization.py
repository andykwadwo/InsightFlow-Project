from turtle import color
import streamlit as st
import pandas as pd
from PIL import Image
import awswrangler as wr
import numpy as np
import plotly.express as px

# copy data from S3 bucket to a pandas dataframe using awswrangler
# Define your S3 bucket and file key
bucket_name = "calandly-market-bucket-oseikwadwo"
key1 = "booking-trend-overtime-gold"

# Construct the S3 URI
s3_uri_key1 = f"s3://{bucket_name}/{key1}"

df_trend = wr.s3.read_csv(s3_uri_key1)



# Create a bar chart using plotly express to visualize the booking trend over time
fig = px.bar(df_trend, x='date', y='booking_count_per_channel_per_date', color='channel', barmode='group', title='Booking Trend Over Time')
fig2 = px.pie(df_trend, names='channel', values='booking_count_per_channel_per_date', title='Booking Distribution by Channel')


st.plotly_chart(fig)
st.plotly_chart(fig2)





