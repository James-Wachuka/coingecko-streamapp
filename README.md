Certainly! Here's an updated version of the README file that includes Spark streaming capabilities:

# Cryptocurrency Streaming App and Dashboard

This project consists of a streaming app and a dashboard for visualizing cryptocurrency data fetched from the CoinGecko API. The streaming app retrieves real-time cryptocurrency information using Spark Streaming and stores it in a PostgreSQL database. The dashboard provides interactive visualizations of the data, allowing user to explore and analyze cryptocurrency trends.

## Features

- Real-time streaming of cryptocurrency data from the CoinGecko API using Spark Streaming
- Storage of data in a PostgreSQL database
- Dashboard with multiple charts for visualizing cryptocurrency trends
- Interactive currency selection using a dropdown component
- Automatic data updates at regular intervals
- Responsive and user-friendly design using Bootstrap

## Technologies Used

- Python
- Spark (for streaming cryptocurrency data)
- Dash (a Python framework for building analytical web applications)
- PostgreSQL
- Plotly (for creating interactive plots)
- Pandas (for data manipulation)
- Psycopg2 (for connecting to PostgreSQL)
- HTML/CSS (for building the dashboard layout)
- Bootstrap (for styling the dashboard)

## Prerequisites

To run this project locally, you need to have the following installed:

- Python (version 3.6 or higher)
- Spark (version 2.4 or higher)
- PostgreSQL database
- Required Python packages (specified in the `requirements.txt` file)

## Installation

1. Clone the repository:

   ```
   git clone https://github.com/James-Wachuka/coingecko-streamapp.git
   ```

2. Navigate to the project directory:

   ```
   cd coingecko-streamapp
   ```

3. Install the required Python packages:

   ```
   pip install -r requirements.txt
   ```

4. Update the PostgreSQL connection details:

   - In the streaming app code (`streamingapp.py`), update the host, port, database name, username, and password in the `psycopg2.connect` function.
   - In the dashboard code (`dashboard.py`), update the same PostgreSQL connection details in each callback function.

5. Set up Spark Streaming:

   - Install Apache Spark following the instructions provided in the official [Spark documentation](https://spark.apache.org/downloads.html).
   - Configure Spark to connect to your local Spark cluster or set up a standalone cluster for testing purposes.

6. Create the necessary database in postgres:

   - connect to psql using  `sudo -u postgres psql` and creaete a database `CREATE DATABASE database_name`;


## Usage

1. Start the Spark streaming app:

   ```
   python streamingapp.py
   ```

   The streaming app will continuously fetch cryptocurrency data from the CoinGecko API using Spark Streaming and store it in the PostgreSQL database.

2. Start the dashboard:

   ```
   python dashboard.py
   ```

   The dashboard will be accessible in your web browser at `http://localhost:8050`.

   - Use the dropdown component to select the desired cryptocurrencies for visualization.
   - The charts on the dashboard will update automatically based on the selected currencies.
   - The last fetched time will be displayed at the bottom of the dashboard.

## dashboard
![price-change](imgs/dashboard.PNG)


## Contributing

Contributions are welcome! If you have any suggestions, improvements, or bug fixes, please open an issue or submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).

## USEFUL LINKS

- [Dash Documentation](https://dash.plotly.com/)
- [Plotly Documentation](https://plotly.com/python/)
- [Spark Documentation](https://spark.apache.org/documentation.html)

