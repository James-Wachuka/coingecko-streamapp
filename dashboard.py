from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time

# Initialize the Dash application
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Define the layout of the dashboard
app.layout = html.Div([
    dbc.Container([
        dbc.Row([
            dbc.Col(
                dcc.Graph(id='price-chart')
            )
        ]),
        dbc.Row([
            dbc.Col(
                dcc.Graph(id='volume-chart')
            )
        ]),
        dbc.Row([
            dbc.Col(
                dcc.Graph(id='scatter-plot')
            )
        ]),
        dbc.Row([
            dbc.Col(
                dcc.Graph(id='bar-chart')
            )
        ]),
        dbc.Row([
            dbc.Col(
                dcc.Graph(id='pie-chart')
            )
        ]),
        dbc.Row([
            dbc.Col(
                dcc.Dropdown(
                    id='currency-filter',
                    options=[
                        {'label': 'All Coins', 'value': 'all'},
                        {'label': 'Bitcoin', 'value': 'bitcoin'},
                        {'label': 'Ethereum', 'value': 'ethereum'},
                        {'label': 'Litecoin', 'value': 'litecoin'}
                    ],
                    value=['all'],  # Set initial value as a list containing 'all'
                    multi=True  # Enable multiple coin selections
                )
            )
        ]),
        dbc.Row([
            dbc.Col(
                html.Div(id='last-fetched')
            )
        ]),
        dcc.Interval(
            id='interval-component',
            interval=60000,  # Update data every 1 minute (60000 milliseconds)
            n_intervals=0
        )
    ])
])

# Define the callback for updating the price chart based on the selected currencies
@app.callback(Output('price-chart', 'figure'), Output('last-fetched', 'children'), Input('currency-filter', 'value'), Input('interval-component', 'n_intervals'))
def update_price_chart(currencies, n):
    # Establish a connection to PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="coin",
        user="data_eng",
        password="data_eng"
    )

    # Create a list to store the selected queries
    queries = []

    # Create a list to store the selected dataframes
    dfs = []

    # Query the data from the coingecko_data table based on the selected currencies
    for currency in currencies:
        if currency == 'all':
            query = "SELECT * FROM coingecko_data ORDER BY current_price DESC LIMIT 20"
        else:
            query = f"SELECT * FROM coingecko_data WHERE id = '{currency}'"
        
        df = pd.read_sql_query(query, conn)
        queries.append(query)
        dfs.append(df)

    # Close the connection
    conn.close()

    # Convert the 'last_updated' column to datetime
    for df in dfs:
        df['last_updated'] = pd.to_datetime(df['last_updated'])

    # Calculate price change as percentage
    for df in dfs:
        df['price_change'] = df.groupby('id')['current_price'].pct_change()

    # Generate a line plot using Plotly
    fig = go.Figure()
    for i, df in enumerate(dfs):
        fig.add_trace(go.Scatter(x=df['last_updated'], y=df['price_change'], mode='lines', name=f'Price Change - {currencies[i]}'))
    fig.update_layout(title='Price Change Over Time', xaxis_title='Date', yaxis_title='Price Change')

    # Get the current time
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Create a string to display the last fetched time
    last_fetched_text = f"Last fetched: {current_time}"

    return fig, last_fetched_text

# Define the callback for updating the volume chart based on the selected currencies
@app.callback(Output('volume-chart', 'figure'), Input('currency-filter', 'value'), Input('interval-component', 'n_intervals'))
def update_volume_chart(currencies, n):
    # Establish a connection to PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="coin",
        user="data_eng",
        password="data_eng"
    )

    # Create a list to store the selected queries
    queries = []

    # Create a list to store the selected dataframes
    dfs = []

    # Query the data from the coingecko_data table based on the selected currencies
    for currency in currencies:
        if currency == 'all':
            query = "SELECT * FROM coingecko_data ORDER BY current_price DESC LIMIT 20"
        else:
            query = f"SELECT * FROM coingecko_data WHERE id = '{currency}'"
        
        df = pd.read_sql_query(query, conn)
        queries.append(query)
        dfs.append(df)

    # Close the connection
    conn.close()

    # Generate a bar plot using Plotly
    fig = go.Figure()
    for i, df in enumerate(dfs):
        fig.add_trace(go.Bar(x=df['current_price'], y=df['name'], name=f'Volume - {currencies[i]}'))
    fig.update_layout(title='Volume', xaxis_title='Current Price', yaxis_title='Name')

    return fig

# Define the callback for updating the scatter plot based on the selected currencies
@app.callback(Output('scatter-plot', 'figure'), Input('currency-filter', 'value'), Input('interval-component', 'n_intervals'))
def update_scatter_plot(currencies, n):
    # Establish a connection to PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="coin",
        user="data_eng",
        password="data_eng"
    )

    # Create a list to store the selected queries
    queries = []

    # Create a list to store the selected dataframes
    dfs = []

    # Query the data from the coingecko_data table based on the selected currencies
    for currency in currencies:
        if currency == 'all':
            query = "SELECT * FROM coingecko_data ORDER BY current_price DESC LIMIT 20"
        else:
            query = f"SELECT * FROM coingecko_data WHERE id = '{currency}'"
        
        df = pd.read_sql_query(query, conn)
        queries.append(query)
        dfs.append(df)

    # Close the connection
    conn.close()

    # Generate a scatter plot using Plotly
    fig = go.Figure()
    for i, df in enumerate(dfs):
        fig.add_trace(go.Scatter(x=df['current_price'], y=df['last_updated'], mode='markers', name=f'Scatter Plot - {currencies[i]}'))
    fig.update_layout(title='Price vs. Last Updated', xaxis_title='Current Price', yaxis_title='Last Updated')

    return fig

# Define the callback for updating the bar chart based on the selected currencies
@app.callback(Output('bar-chart', 'figure'), Input('currency-filter', 'value'), Input('interval-component', 'n_intervals'))
def update_bar_chart(currencies, n):
    # Establish a connection to PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="coin",
        user="data_eng",
        password="data_eng"
    )

    # Create a list to store the selected queries
    queries = []

    # Create a list to store the selected dataframes
    dfs = []

    # Query the data from the coingecko_data table based on the selected currencies
    for currency in currencies:
        if currency == 'all':
            query = "SELECT * FROM coingecko_data ORDER BY current_price DESC LIMIT 20"
        else:
            query = f"SELECT * FROM coingecko_data WHERE id = '{currency}'"
        
        df = pd.read_sql_query(query, conn)
        queries.append(query)
        dfs.append(df)

    # Close the connection
    conn.close()

    # Generate a bar chart using Plotly
    fig = go.Figure()
    for i, df in enumerate(dfs):
        fig.add_trace(go.Bar(x=df['symbol'], y=df['current_price'], name=f'Current Prices - {currencies[i]}'))
    fig.update_layout(title='Current Prices', xaxis_title='Symbol', yaxis_title='Current Price')

    return fig

# Define the callback for updating the pie chart based on the selected currencies
@app.callback(Output('pie-chart', 'figure'), Input('currency-filter', 'value'), Input('interval-component', 'n_intervals'))
def update_pie_chart(currencies, n):
    # Establish a connection to PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="coin",
        user="data_eng",
        password="data_eng"
    )

    # Create a list to store the selected queries
    queries = []

    # Create a list to store the selected dataframes
    dfs = []

    # Query the data from the coingecko_data table based on the selected currencies
    for currency in currencies:
        if currency == 'all':
            query = "SELECT * FROM coingecko_data ORDER BY current_price DESC LIMIT 20"
        else:
            query = f"SELECT * FROM coingecko_data WHERE id = '{currency}'"
        
        df = pd.read_sql_query(query, conn)
        queries.append(query)
        dfs.append(df)

    # Close the connection
    conn.close()

    # Generate a pie chart using Plotly
    fig = go.Figure()
    for i, df in enumerate(dfs):
        fig.add_trace(go.Pie(labels=df['name'], values=df['current_price'], name=f'Pie Chart - {currencies[i]}'))
    fig.update_layout(title='Cryptocurrency Distribution')

    return fig

# Run the Dash application
if __name__ == '__main__':
    app.run_server(debug=True)
