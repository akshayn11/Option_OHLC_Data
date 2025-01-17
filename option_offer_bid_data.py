from lib.utility import Utility, ContractHub
from datetime import datetime,date, timedelta
import webbrowser
import configparser
from kiteconnect import KiteConnect
from kiteconnect import KiteTicker
import os
import time
import requests





utility = Utility()
instruments_file_name = utility.get_instruments_file()

instruments_url = "https://api.kite.trade/instruments"
utility.process(instruments_url)
contract_hub = ContractHub(instruments_file_name)
contract_hub.load_data()
instrument_file_path= utility.get_todays_instruments_file()
trading_symbol_token=contract_hub.prepare_token_to_tradingsymbol_dict()
contract_hub_dict,token_symbol_dict=contract_hub.generate_contract_hub()

# print(f"api key is {api_key} access token is {access_token}")

# input()

freeze_lotsize_dict = requests.get("https://9mbqrxwx53.execute-api.ap-south-1.amazonaws.com/default/lotsize_freeze_limits").json()

def read_api_details():
    global api_key, api_secret

    config = configparser.ConfigParser()
    try:
        config.read(r'etc\config.ini') 
        api_key = config.get('client_details', 'api_key')
        api_secret = config.get('client_details', 'secret_key')
    except configparser.NoSectionError:
        print("Error: 'client_details' section not found in the config.ini file.")
        exit(1)
    except configparser.NoOptionError as e:
        print(f"Error: {e}")
        exit(1)

def get_access_token_filename():
    today = datetime.today().strftime('%Y%m%d')
    return f"etc/access_token_{today}.txt"  

def read_access_token_from_file():
    filename = get_access_token_filename()
    try:
        with open(filename, 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        return None

def write_access_token_to_file(access_token):
    filename = get_access_token_filename()
    with open(filename, 'w') as file:
        file.write(access_token)


read_api_details()

loggedIn = False
access_token = read_access_token_from_file()

if access_token:
    try:
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        print(f"\t[+] Welcome : {kite.profile()['user_name']}")
        loggedIn = True
    except Exception as e:
        print("Error using stored access token: ", e)

if not loggedIn:
    while not loggedIn:
        try:
            kite = KiteConnect(api_key=api_key)
            webbrowser.open(kite.login_url())
            request_token = input(f"[+] Paste your request token: ")

            data = kite.generate_session(request_token, api_secret=api_secret)
            access_token = data.get('access_token')
            kite.set_access_token(access_token)
            write_access_token_to_file(access_token)

            print(f"\t[+] Welcome : {kite.profile()['user_name']}")
            loggedIn = True
        except Exception as e:
            print("Error while login: ", e)

# Create KiteTicker object using the access token
kws = KiteTicker(api_key=api_key, access_token=access_token)


map_dict={"NIFTY 50":{'NAME':'NIFTY','SEGMENT':'NFO-OPT'},
          'NIFTY BANK':{'NAME':'BANKNIFTY','SEGMENT':'NFO-OPT'},
          'NIFTY FIN SERVICE':{'NAME':'FINNIFTY','SEGMENT':'NFO-OPT'},
          'NIFTY MID SELECT':{'NAME':'MIDCPNIFTY','SEGMENT':'NFO-OPT'},
          'SENSEX':{'NAME':'SENSEX','SEGMENT':'BFO-OPT'},
          'BANKEX':{'NAME':'BANKEX','SEGMENT':'BFO-OPT'},

          }

ltp_dict={}
instrument_token_hub=[]



def on_ticks(ws, ticks):
    for tick in ticks:
        # print(len(ticks))
        instrument_token = tick['instrument_token']
        ltp_dict[instrument_token] = tick 

def on_connect(ws, response):
    
    ws.subscribe([256265,260105,257801,288009,265,274441])
    ws.set_mode(ws.MODE_FULL, [256265,260105,257801,288009,265,274441])

def on_close(ws, code, reason):
    pass

# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close
kws.connect(threaded=True)


# -------------------------------------------------Option data Excel logic------------------------------------------------------

import xlwings as xw
import pandas as pd
import time

file_path = str(instrument_file_path)



data = pd.read_csv(file_path)

nifty_data = data[(data['name'] == 'NIFTY') & (data['exchange'] == 'NFO')]
banknifty_data = data[(data['name'] == 'BANKNIFTY') & (data['exchange'] == 'NFO')]
finnifty_data = data[(data['name'] == 'FINNIFTY') & (data['exchange'] == 'NFO')]
midcpnifty_data = data[(data['name'] == 'MIDCPNIFTY') & (data['exchange'] == 'NFO')]
sensex_data =  data[(data['name'] == 'SENSEX') & (data['exchange'] == 'BFO')]
bankex_data =  data[(data['name'] == 'BANKEX') & (data['exchange'] == 'BFO')]

nifty_strikes = sorted([strike for strike in nifty_data['strike'].unique() if strike != 0])
banknifty_strikes = sorted([strike for strike in banknifty_data['strike'].unique() if strike != 0])
finnifty_strikes = sorted([strike for strike in finnifty_data['strike'].unique() if strike != 0])
midcpnifty_strikes = sorted([strike for strike in midcpnifty_data['strike'].unique() if strike != 0])
sensex_strikes = sorted([strike for strike in sensex_data['strike'].unique() if strike != 0])
bankex_strikes = sorted([strike for strike in bankex_data['strike'].unique() if strike != 0])

nifty_expiries = sorted(nifty_data['expiry'].unique())
banknifty_expiries = sorted(banknifty_data['expiry'].unique())
finnifty_expiries = sorted(finnifty_data['expiry'].unique())
midcpnifty_expiries = sorted(midcpnifty_data['expiry'].unique())
sensex_expiries = sorted(sensex_data['expiry'].unique())
bankex_expiries = sorted(bankex_data['expiry'].unique())

instrument_tokens = []

def atm_predictor(ltp : int,strike_mul : int):

    remainder_value= ltp % strike_mul
    
    if remainder_value >= strike_mul/2:
        atm = ltp + (strike_mul - remainder_value)
    else:
        atm = ltp - remainder_value
    
    return int(atm)

def fetch_tokens_from_contract_hub(contract_hub, index, expiry):
    try:
        if isinstance(expiry, datetime):
            formatted_expiry = expiry.strftime("%Y-%m-%d")
        elif isinstance(expiry, str):
            formatted_expiry = datetime.strptime(expiry, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
        else:
            return []

        expiry_key = f"{index.lower()}_{formatted_expiry}"
        # print(f"key is {expiry_key} index is {index}")

        if index.upper() in contract_hub and expiry_key in contract_hub[index.upper()]:
            ce_tokens = contract_hub[index.upper()][expiry_key].get('ce', [])
            pe_tokens = contract_hub[index.upper()][expiry_key].get('pe', [])
            combined_tokens = ce_tokens + pe_tokens
            instrument_tokens.extend(combined_tokens)
            return combined_tokens  
        else:
            return []
    except Exception as e:
        print(f"Error fetching tokens: {e}")
        return []

def update_ltp_in_excel(sheet, selected_value, strikes, instrument_tokens, ltp_dict, ce_ltp_column='G', ce_buy_column='E',   
                        ce_sell_column='F', pe_ltp_column='I', pe_sell_column='K', pe_buy_column='J', strike_column='H', last_state=None):

    if last_state is None:
        last_state = {"last_highlighted_row": None, "last_selected_value": None}

    # Reset all highlights in the strike column range
    start_row = 5
    end_row = start_row + len(strikes) - 1  # Assuming the length of strikes determines the range
    reset_range = f"{ce_buy_column}{start_row}:{pe_sell_column}{end_row}"
    sheet.range(reset_range).color = None  # Reset color to default

    # Update last state for selected value
    if last_state["last_selected_value"] != selected_value:
        # print(f"Selected value changed to {selected_value}. Resetting highlights.")
        last_state["last_selected_value"] = selected_value
        last_state["last_highlighted_row"] = None  # Reset the last highlighted row

    # Extract the instrument token and strike multiplier
    index_instrument_token = {"NIFTY": "256265", "BANKNIFTY": "260105", "FINNIFTY": "257801", "MIDCPNIFTY": "288009", "SENSEX": "265", 'BANKEX': "274441"}
    instrument_token = index_instrument_token.get(selected_value)
    strike_mul = freeze_lotsize_dict.get(selected_value, {}).get("strike_mul", 0)

    if not instrument_token or strike_mul == 0:
        print(f"Invalid selected value: {selected_value}")
        return

    if int(instrument_token) not in ltp_dict:
        print(f"Instrument token {instrument_token} not found in LTP dict.")
        return

    # Fetch the LTP for the selected index from ltp_dict
    ltp = ltp_dict.get(int(instrument_token), {}).get('last_price', None)

    if ltp is None:
        print(f"LTP not found for {selected_value}")
        return

    # Calculate ATM strike
    atm_strike = atm_predictor(ltp, strike_mul)
    # print("ATM strike is", atm_strike)

    # Update LTP and quantities for CE and PE options
    ce_ltp_values, ce_buy_qty_values, ce_sell_qty_values = [], [], []
    pe_ltp_values, pe_sell_qty_values, pe_buy_qty_values = [], [], []

    for strike in strikes:
        ce_token = next((token for token in instrument_tokens if f"{int(strike)}CE" in trading_symbol_token.get(token, "")), None)
        pe_token = next((token for token in instrument_tokens if f"{int(strike)}PE" in trading_symbol_token.get(token, "")), None)

        ce_ltp_values.append(ltp_dict.get(ce_token, {}).get('last_price', None) if ce_token else None)
        ce_buy_qty_values.append(ltp_dict.get(ce_token, {}).get('total_buy_quantity', None) if ce_token else None)
        ce_sell_qty_values.append(ltp_dict.get(ce_token, {}).get('total_sell_quantity', None) if ce_token else None)

        pe_ltp_values.append(ltp_dict.get(pe_token, {}).get('last_price', None) if pe_token else None)
        pe_sell_qty_values.append(ltp_dict.get(pe_token, {}).get('total_sell_quantity', None) if pe_token else None)
        pe_buy_qty_values.append(ltp_dict.get(pe_token, {}).get('total_buy_quantity', None) if pe_token else None)

    # Populate the sheet with updated values
    sheet.range(f"{ce_ltp_column}{start_row}").value = [[v] for v in ce_ltp_values]
    sheet.range(f"{ce_buy_column}{start_row}").value = [[v] for v in ce_buy_qty_values]
    sheet.range(f"{ce_sell_column}{start_row}").value = [[v] for v in ce_sell_qty_values]
    sheet.range(f"{pe_ltp_column}{start_row}").value = [[v] for v in pe_ltp_values]
    sheet.range(f"{pe_sell_column}{start_row}").value = [[v] for v in pe_sell_qty_values]
    sheet.range(f"{pe_buy_column}{start_row}").value = [[v] for v in pe_buy_qty_values]

    # Highlight the ATM strike row
    for row, strike in enumerate(strikes, start=start_row):
        if strike == atm_strike:
            strike_row_range = f"{ce_buy_column}{row}:{pe_sell_column}{row}"  # Adjust range as needed
            sheet.range(strike_row_range).color = (0, 255, 0)  # Green color (RGB)
            # print(f"Highlighted row for strike {strike} in green.")
            last_state["last_highlighted_row"] = row  # Update last highlighted row
            break

    return


def monitor_excel_cell():
    global kws
    wb = xw.Book('option_data.xlsx')
    sheet = wb.sheets['Sheet1']       
    last_selected_value = None        
    last_sort_order = None            
    last_expiry_value = None 

    while True:
        try:
            selected_value = sheet.range('A2').value
            sort_order = sheet.range('C2').value
            expiry_selected = sheet.range('B2').value

            # Ensure the values are strings for proper comparison
            selected_value = str(selected_value).strip() if selected_value else None
            sort_order = str(sort_order).strip() if sort_order else None
            expiry_selected = str(expiry_selected).strip() if expiry_selected else None

            if selected_value is None or sort_order is None or expiry_selected is None:
                print("Invalid input in cells A2, B2, or C2.")
                time.sleep(1)
                continue

            # Check if the values have changed and print only when there is a change
            # if (selected_value != last_selected_value or 
            #     sort_order != last_sort_order or 
            #     expiry_selected != last_expiry_value):
                
            #     print(f"Change value is - Selected Value: {selected_value}, Sort Order: {sort_order}, Expiry: {expiry_selected}")
            


            strikes = []
            if selected_value == "NIFTY":
                strikes = nifty_strikes
            elif selected_value == "BANKNIFTY":
                strikes = banknifty_strikes
            elif selected_value == "FINNIFTY":
                strikes = finnifty_strikes
            elif selected_value == "MIDCPNIFTY":
                strikes = midcpnifty_strikes
            elif selected_value == "SENSEX":
                strikes = sensex_strikes
            elif selected_value == "BANKEX":
                strikes = bankex_strikes

            if sort_order == "ASC":
                strikes = sorted(strikes)
            elif sort_order == "DSC":
                strikes = sorted(strikes, reverse=True)

            if strikes:
                sheet.range(f'H5:H{4 + len(strikes)}').value = [[strike] for strike in strikes]

            expiries = []
            if selected_value == "NIFTY":
                expiries = nifty_expiries
            elif selected_value == "BANKNIFTY":
                expiries = banknifty_expiries
            elif selected_value == "FINNIFTY":
                expiries = finnifty_expiries
            elif selected_value == "MIDCPNIFTY":
                expiries = midcpnifty_expiries
            elif selected_value == "SENSEX":
                expiries = sensex_expiries
            elif selected_value == "BANKEX":
                expiries = bankex_expiries

            today = datetime.now().date()
            expiries = [expiry for expiry in expiries if datetime.strptime(expiry, '%Y-%m-%d').date() >= today]

            expiry_range = ','.join(expiries)  
            sheet.range('B2').api.Validation.Delete() 
            sheet.range('B2').api.Validation.Add(
                Type=3, 
                AlertStyle=1,  
                Operator=1,  
                Formula1=expiry_range  
            )

            if expiries:
                sheet.range('B2').value = expiries[0]

            if expiry_selected != last_expiry_value:
                if selected_value and expiry_selected:
                    tokens = fetch_tokens_from_contract_hub(contract_hub_dict, selected_value, expiry_selected)  
                    kws.subscribe(tokens)
                    kws.set_mode(kws.MODE_FULL, tokens)
                    # print(f"Ltp dict is {ltp_dict}\n\n")\
                update_ltp_in_excel(sheet,selected_value, strikes, instrument_tokens, ltp_dict)
                
            # last_selected_value = selected_value
            # last_sort_order = sort_order
            # last_expiry_value = expiry_selected

            time.sleep(1)  

        except Exception as e:
            print(f"Error while reading Excel cell: {e}")
            break
            # continue  


monitor_excel_cell()
