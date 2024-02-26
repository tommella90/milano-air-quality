import streamlit as st
import pandas as pd
import plotly.express as px
from sklearn.preprocessing import MinMaxScaler
from google.cloud import storage
from io import BytesIO
import plotly.graph_objects as go


def download_blob_to_dataframe(bucket_name, source_blob_name):
    """Downloads a blob from the bucket and returns it as a pandas DataFrame."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    # Download the blob to an in-memory file.
    in_memory_file = BytesIO()
    blob.download_to_file(in_memory_file)
    in_memory_file.seek(0)  # Reset file pointer to the start.

    # Read the file into a pandas DataFrame
    if '.csv' in source_blob_name:
        df = pd.read_csv(in_memory_file)
    elif '.parquet' in source_blob_name:
        df = pd.read_parquet(in_memory_file)
    else:
        raise ValueError("Unsupported file type")

    return df

bucket_name = "milano-data"
source_blob_name = "air-quality-clean.parquet"
df = download_blob_to_dataframe(bucket_name, source_blob_name)


def normalize_df(df):
    scaler = MinMaxScaler()
    scaled_data = scaler.fit_transform(df)
    normalized_df = pd.DataFrame(scaled_data, index=df.index, columns=df.columns)
    return normalized_df


def create_graph(df, title, graph_width=1000, graph_height=500):
    fig = go.Figure()
    for col in df.columns:
        monthly_avg = df[col].resample('M').mean()
        fig.add_trace(go.Scatter(x=monthly_avg.index, y=monthly_avg, mode='lines+markers', name=col, line=dict(dash='dot')))
    
    # Add overall average line
    overall_avg = df.mean(axis=1).resample('M').mean()
    fig.add_trace(go.Scatter(x=overall_avg.index, y=overall_avg, mode='lines', name='Overall Average', line=dict(color='black', width=2)))

    fig.update_layout(
        title=title, 
        width=graph_width, 
        height=graph_height,
        font=dict(color='black'), 
        title_font_color='black'  
    )
    return fig


def create_yearly_avg_bar_chart(df, title="Yearly Averages of All Pollutants", graph_width=1000, graph_height=500):
    yearly_avg = df.resample('Y').mean()
    fig = px.bar(
        yearly_avg, 
        x=yearly_avg.index.year, 
        y=yearly_avg.mean(axis=1), 
        labels={'y': 'Yearly Average', 'x': 'Year'},
        color=yearly_avg.mean(axis=1),  # Color based on the mean value
        color_continuous_scale='Viridis'  # Specify the color scale
    )
    fig.update_layout(
        title={
            'text': title,
            'y': 0.9,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'
        },
        font=dict(color='black'),  # Set all text to black
        title_font_color='black',  # Ensure title color is black
        width=graph_width,  # Set width
        height=graph_height  # Set height
    )
    return fig


# Download data
bucket_name = "milano-data"
source_blob_name = "air-quality-clean.parquet"
df = download_blob_to_dataframe(bucket_name, source_blob_name)

# Normalize data
df_normalized = normalize_df(df)

# Set page to light mode
st.set_page_config(page_title="Milan Air Quality Monitor", page_icon=":chart_with_upwards_trend:", layout="wide", initial_sidebar_state="expanded")

with open('README.md', 'r') as file:
    readme_content = file.read()

# Add a toggle button to show/hide the README content

# Streamlit App
st.title("Milan Air Quality Monitor")

if st.checkbox("PARTICLES EXPLANATION"):
    st.markdown(readme_content)

# Dropdown for selection
selected_pollutant = st.selectbox("Select Pollutant", options=['All'] + list(df.columns))

# Display Graphs
if selected_pollutant == 'All':

    # Yearly average bar chart
    yearly_avg_fig = create_yearly_avg_bar_chart(df_normalized)
    st.plotly_chart(yearly_avg_fig, config={"font.color": "black", "title.font_color": "black"})


    # Monthly average line chart
    fig1 = create_graph(df_normalized, "Normalized Monthly Averages of All Pollutants")
    st.plotly_chart(fig1, config={"font.color": "black", "title.font_color": "black"})


else:
    # Line chart for selected pollutant
    df_interpolated = df.interpolate()
    monthly_avg = df_interpolated[selected_pollutant].resample('M').mean()
    fig = px.line(df_interpolated, x=df.index, y=selected_pollutant, title=f"{selected_pollutant} over time")
    fig.add_trace(go.Scatter(x=monthly_avg.index, y=monthly_avg, mode='lines', name='Monthly Average'))
    st.plotly_chart(fig)
