import os
import requests
import pandas as pd
from datetime import date


class Utility:
    def __init__(self):
        # Define the relative path to the 'etc' folder (above the 'lib' folder)
        self.etc_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'etc')

        # Ensure the 'etc' folder exists
        os.makedirs(self.etc_folder, exist_ok=True)

        # Set today's date and the instruments file path
        self.today = date.today()
        self.instruments_file = f"{self.etc_folder}/instruments_{self.today.strftime('%Y%m%d')}.csv"

        self.data_dict = {}
        self.df = None

    def get_instruments_file(self):
        """
        Get the name of the instruments file.

        Returns:
            str: The full path to the instruments file.
        """
        return self.instruments_file

    def is_new_day(self):
        """
        Check if the instruments file for today exists.
        Returns:
            bool: True if it's a new day (file does not exist), False otherwise.
            str: The name of the instruments file.
        """
        return not os.path.isfile(self.instruments_file), self.instruments_file

    def download_csv(self, url):
        """
        Downloads the instruments CSV file from the specified URL.

        Args:
            url (str): The URL to download the CSV from.
        """
        response = requests.get(url)
        if response.status_code == 200:
            with open(self.instruments_file, "w") as file:
                file.write(response.text)
            # print(f"CSV file downloaded successfully to {self.instruments_file}.")
            self.df = pd.read_csv(self.instruments_file)
        else:
            print("Failed to retrieve CSV data from the website.")
            self.df = None

    def load_csv(self):
        """
        Load the instruments CSV file into a DataFrame.
        """
        if os.path.isfile(self.instruments_file):
            self.df = pd.read_csv(self.instruments_file)
            # print(f"CSV file loaded successfully from {self.instruments_file}.")
        else:
            print("CSV file does not exist.")

    def process(self, url):
        """
        The main method to check if a new day has started and process the CSV accordingly.

        Args:
            url (str): The URL to download the CSV from if needed.
        """
        new_day, _ = self.is_new_day()

        if new_day:
            print("It's a new day. Downloading the instruments file.")
            self.download_csv(url)
        else:
            print("CSV file already exists for today. Loading the existing file.")
            self.load_csv()

        if self.df is not None:
            print(f"DataFrame loaded with {len(self.df)} rows.")


    def get_todays_instruments_file(self):
        """
        Fetch the instruments file for today's date if it exists.

        Returns:
            str: Path to today's instruments file if it exists, None otherwise.
        """
        if os.path.isfile(self.instruments_file):
        # Return the relative path only
            return os.path.relpath(self.instruments_file, os.getcwd())
        return None



class ContractHub:
    def __init__(self, instruments_file):
        """
        Initialize the ContractHub with the instruments file.

        Args:
            instruments_file (str): The full path to the instruments file.
        """
        self.instruments_file = instruments_file
        self.df = None
        self.contract_hub = {}

    def load_data(self):
        """
        Load the instruments file into a pandas DataFrame and print its contents.
        """
        try:
            self.df = pd.read_csv(self.instruments_file)
            # print(f"Data loaded from {self.instruments_file}.")
        except FileNotFoundError:
            print(f"Error: The file {self.instruments_file} does not exist.")
        except pd.errors.EmptyDataError:
            print(f"Error: The file {self.instruments_file} is empty.")
        except Exception as e:
            print(f"An unexpected error occurred while loading the file: {e}")

    def generate_contract_hub(self):
        """
        Generate the contract hub dictionary, grouping instruments by index, expiry, and type (CE, PE).

        Returns:
            dict: A dictionary containing contract details for each index, expiry, and option type.
        """
        if self.df is None:
            print("DataFrame is not loaded. Please load the data first.")
            return {}, {}

        indices = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"]
        contract_hub = {}
        token_to_details_hub = {}

        for index in indices:
            index_data = self.df[self.df['tradingsymbol'].str.contains(index, na=False)]
            index_dict = {}
            token_to_details = {}

            for expiry, group in index_data.groupby('expiry'):
                expiry_key = f"{index.lower()}_{expiry}"

                # Get the instrument tokens for CE and PE
                ce_tokens = group[group['tradingsymbol'].str.contains("CE")]['instrument_token'].tolist()
                pe_tokens = group[group['tradingsymbol'].str.contains("PE")]['instrument_token'].tolist()

                # Populate contract hub dictionary
                index_dict[expiry_key] = {
                    "ce": ce_tokens,
                    "pe": pe_tokens
                }

                # Populate token_to_details dictionary
                for _, row in group.iterrows():
                    # Ensure 'strike' is formatted as a string without '.0' if it's a float
                    strike_value = int(row['strike']) if isinstance(row['strike'], (int, float)) and row['strike'] == int(row['strike']) else row['strike']
                    
                    token_to_details[row['instrument_token']] = {
                        "tradingsymbol": row['tradingsymbol'],
                        "name": strike_value
                    }

            contract_hub[index.upper()] = index_dict
            token_to_details_hub[index.upper()] = token_to_details

        self.contract_hub = contract_hub
        self.token_to_details_hub = token_to_details_hub

        return contract_hub, token_to_details_hub

    def get_contract_hub_dict(self):
        """
        Return the contract hub dictionary.

        Returns:
            dict: The contract hub dictionary.
        """
        return self.contract_hub

    def prepare_token_to_tradingsymbol_dict(self):
        """
        Prepare a dictionary with `instrument_token` as the key and `tradingsymbol` as the value.
        Filters rows where the `name` column is in the specified list and the `segment` column matches the criteria.

        Returns:
            dict: A dictionary with instrument_token as key and tradingsymbol as value.
        """
        if self.df is None:
            print("DataFrame is not loaded. Please load the data first.")
            return {}

        # Define filters
        valid_names = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'BANKEX', 'SENSEX']
        valid_segments = ['NFO-OPT', 'BFO-FUT', 'BFO-OPT', 'NFO-OPT']

        # Apply filters
        filtered_df = self.df[
            (self.df['name'].isin(valid_names)) &
            (self.df['segment'].isin(valid_segments))
        ]

        # Generate dictionary
        token_to_tradingsymbol = dict(zip(filtered_df['instrument_token'], filtered_df['tradingsymbol']))

        return token_to_tradingsymbol


import os
import logging
from datetime import datetime

class Logger:
    def __init__(self):
        self.log_file_path = self.setup_logger()

    def setup_logger(self):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        log_folder = os.path.join(base_dir, 'logs')

        today_date = datetime.today().strftime('%d_%m_%Y')
        date_folder = os.path.join(log_folder, today_date)
        os.makedirs(date_folder, exist_ok=True)

        current_time = datetime.now().strftime('%H%M%S')
        log_file_name = f"logfile_{today_date}_{current_time}.log"
        log_file_path = os.path.join(date_folder, log_file_name)

        # Configure logging
        logging.basicConfig(
            filename=log_file_path,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

        return log_file_path

    def log_message(self, *args):
        msg = ' '.join(map(str, args))
        print(msg)
        logging.info(msg)
