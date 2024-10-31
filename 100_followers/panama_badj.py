import os
import argparse

import pandas as pd

from datetime import datetime

from pandas.tseries.offsets import BDay
import holidays


def get_month_code_mapping():
    return {
        'F': 1,
        'G': 2,
        'H': 3,
        'J': 4,
        'K': 5,
        'M': 6,
        'N': 7,
        'Q': 8,
        'U': 9,
        'V': 10,
        'X': 11,
        'Z': 12
    }


def parse_expiration_year_and_month_from_tv_fname(trading_view_csv_filename):
    expiration_code = trading_view_csv_filename.split('_')[2].split(',')[0]
    expiration_year = int(expiration_code[-4:])
    expiration_month_code = expiration_code[-5:-4]
    expiration_month = get_month_code_mapping()[expiration_month_code]
    return expiration_year, expiration_month


def get_active_contracts(symbol_data_dir):
    current_year = datetime.now().year
    current_month = datetime.now().month

    active_contracts_df = pd.DataFrame()
    csv_files = [f for f in os.listdir(symbol_data_dir) if f.endswith('.csv')]
    for csv_filename in csv_files:
        contract_df = pd.read_csv(f"{symbol_data_dir}/{csv_filename}")
        contract_df['local_symbol'] = csv_filename.split(',')[0].split('_')[-1]

        expiration_year, expiration_month = parse_expiration_year_and_month_from_tv_fname(csv_filename)
        if expiration_year >= current_year or (expiration_year == current_year and expiration_month > current_month):
            expiration_day = 30
            contract_df['exp_date_iso'] = datetime(expiration_year, expiration_month, expiration_day)
            active_contracts_df = pd.concat([active_contracts_df, contract_df])

    active_contracts_df = active_contracts_df.sort_values(by='exp_date_iso', ascending=False)
    unique_active_contracts_df = active_contracts_df.drop_duplicates(subset='local_symbol', keep='first')
    return unique_active_contracts_df[['local_symbol', 'exp_date_iso']]


def filter_for_rolling_contracts(ohlcv_df, roll_months_codes):
    month_code_mapping = get_month_code_mapping()
    reversed_mapping = {v: k for k, v in month_code_mapping.items()}
    ohlcv_df = ohlcv_df[ohlcv_df['exp_date_iso'].apply(lambda x: reversed_mapping[x.month]).isin(roll_months_codes)]
    return ohlcv_df.reset_index(drop=True)


def parse_ohlcv_from_tv_csv_files(symbol_data_dir):
    current_year = datetime.now().year
    current_month = datetime.now().month

    ohlcv_df = pd.DataFrame()
    for csv_filename in os.listdir(symbol_data_dir):
        df = pd.read_csv(f"{symbol_data_dir}/{csv_filename}")
        df['date'] = pd.to_datetime(df['time'], unit='s')
        df['date_iso'] = pd.to_datetime(df['date'].dt.date)

        expiration_year, expiration_month = parse_expiration_year_and_month_from_tv_fname(csv_filename)
        if expiration_year < current_year or (expiration_year == current_year and expiration_month < current_month):
            expiration_day = df['date_iso'].iloc[-1].day
        else:
            expiration_day = 30
        df['exp_date_iso'] = datetime(expiration_year, expiration_month, expiration_day)

        df['ticker'] = csv_filename.split(',')[0].split('_')[-1]
        ohlcv_df = pd.concat([ohlcv_df, df])
    ohlcv_df.set_index('date', inplace=True)
    return ohlcv_df.sort_values(by=['exp_date_iso', 'time'])[['time', 'date_iso', 'ticker', 'close', 'exp_date_iso']]


def find_next_trading_day(roll_date):
    us_holidays = holidays.US()
    while roll_date.weekday() >= 5 or roll_date in us_holidays:
        roll_date -= BDay(1)
    return roll_date


def find_valid_roll_row(pivoted_dfs, roll_date):
    while True:
        try:
            roll_row = pivoted_dfs.loc[roll_date]
            break
        except KeyError:
            # if there's no data for the roll date, try the previous day
            roll_iso_date = datetime.strptime(roll_date, '%Y-%m-%d')
            roll_date = (roll_iso_date - BDay(1)).strftime('%Y-%m-%d')

    return roll_row


def panama_backadjust(ohlcv_df, roll_t_d):
    pivoted_dfs = ohlcv_df.pivot(columns='exp_date_iso', values='close')
    pivoted_dfs.sort_index(inplace=True)
    pivoted_dfs['backadjusted'] = pd.Series(dtype='float64')
    pivoted_dfs['unadjusted'] = pd.Series(dtype='float64')

    expiration_dates = ohlcv_df['exp_date_iso'].unique()
    expiration_dates = pd.Series(expiration_dates).sort_values(ascending=False).reset_index(drop=True)
    if len(expiration_dates) < 2:
        print("couldn't find enough contracts to backadjust", len(expiration_dates), ohlcv_df['ticker'].iloc[0])

    for i in range(0, len(expiration_dates) - 1):
        roll_from_exp_date = expiration_dates[i + 1]
        roll_into_exp_date = expiration_dates[i]

        roll_iso_date = find_next_trading_day(roll_from_exp_date - BDay(roll_t_d))
        roll_date = roll_iso_date.strftime('%Y-%m-%d')

        roll_row = find_valid_roll_row(pivoted_dfs, roll_date)

        # print('rolling', roll_from_exp_date, 'into', roll_into_exp_date, 'on', roll_date)

        if pivoted_dfs['backadjusted'].isna().all():
            offset = (roll_iso_date + BDay(1)).strftime('%Y-%m-%d')

            pivoted_dfs.loc[offset:, 'backadjusted'] = pivoted_dfs.loc[offset:, roll_into_exp_date]
            pivoted_dfs.loc[offset:, 'unadjusted'] = pivoted_dfs.loc[offset:, roll_into_exp_date]
            if datetime.strptime(offset, "%Y-%m-%d") >= datetime.today():
                pivoted_dfs.loc[:offset, 'backadjusted'] = pivoted_dfs.loc[:offset, roll_into_exp_date]
                pivoted_dfs.loc[:offset, 'unadjusted'] = pivoted_dfs.loc[:offset, roll_into_exp_date]

        if 'backadjusted' in pivoted_dfs.columns and pivoted_dfs['backadjusted'].notna().any():
            backadjust_diff = roll_row[roll_into_exp_date] - roll_row[roll_from_exp_date]

            # print('backadjust_diff', backadjust_diff)
            pivoted_dfs.loc[:roll_date, 'backadjusted'] = pivoted_dfs.loc[:roll_date, roll_into_exp_date] + backadjust_diff
            pivoted_dfs.loc[:roll_date, 'unadjusted'] = pivoted_dfs.loc[:roll_date, roll_into_exp_date]

    return pivoted_dfs


def main():
    print("Usage python panama_badj.py --trade_into_backmonth [True/False] --plot [True/False]")

    print("The data needs to be TradingView exported CSV chart data containing 'close' and is stored under data/[SYMBOL]")
    print("Example: Corn Futures")
    print("data/")
    print("└── ZC/")
    print("    └── CBOT_DL_ZCZ1980, D.csv")
    print("    └── CBOT_DL_ZCZ1981, D.csv")
    print("    └── CBOT_DL_ZCZ1982, D.csv")
    print("    └── [...], D.csv")
    print("    └── CBOT_DL_ZCZ2024, D.csv")
    print("    └── CBOT_DL_ZCZ2025, D.csv")

    roll_frequencies = {
        'MES': {'roll_month_codes': ['H', 'M', 'U', 'Z'], 'roll_t_d': 3},  # 3 days before expiration
        'ZC': {'roll_month_codes': ['Z'], 'roll_t_d': 30},  # 30 days before expiration
    }

    trading_day = datetime.today() - BDay(1)  # last business day close

    parser = argparse.ArgumentParser(description='Process trade_into_backmonth flag.')
    parser.add_argument('--plot', type=lambda x: (str(x).lower() == 'true'), default=False, help='Set plot to True or False')
    parser.add_argument('--trade_into_backmonth', type=lambda x: (str(x).lower() == 'true'), default=True, help='Set trade_into_backmonth to True or False')
    args = parser.parse_args()

    data_dir = './data'
    symbol_dirs = [f for f in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, f))]
    for symbol in symbol_dirs:
        print('backadjusting', symbol)
        print('trading into backmonth:', args.trade_into_backmonth)
        roll_month_codes = roll_frequencies[symbol]['roll_month_codes']
        roll_t_d = roll_frequencies[symbol]['roll_t_d']

        symbol_data_dir = f"{data_dir}/{symbol}"

        active_contracts_df = get_active_contracts(symbol_data_dir)
        active_rolling_contracts = filter_for_rolling_contracts(active_contracts_df, roll_month_codes)[::-1].reset_index(drop=True)

        trading_contract = active_rolling_contracts.iloc[0]  # frontmonth
        if args.trade_into_backmonth:
            trading_contract = active_rolling_contracts.iloc[1]  # backmonth

        print('trading contract:', trading_contract['local_symbol'], trading_contract['exp_date_iso'])

        ohlcv_df = parse_ohlcv_from_tv_csv_files(symbol_data_dir)
        rolling_ohlcv_df = filter_for_rolling_contracts(ohlcv_df, roll_month_codes)
        rolling_ohlcv_df.set_index('date_iso', inplace=True)

        rolling_ohlcv_df = rolling_ohlcv_df[rolling_ohlcv_df['exp_date_iso'] <= trading_contract['exp_date_iso']]
        rolling_ohlcv_df = rolling_ohlcv_df[rolling_ohlcv_df.index <= trading_day]

        backadjusted_price_series = panama_backadjust(rolling_ohlcv_df, roll_t_d)
        # print(backadjusted_price_series)
        backadjusted_price_series[['backadjusted', 'unadjusted']].to_csv(f"./{symbol}_proces.csv")

        if args.plot:
            print('plotting', symbol)
            from matplotlib import pyplot as plt

            plt.figure(dpi=300)
            backadjusted_price_series['unadjusted'].plot()
            plt.savefig(f"./{symbol}_unadjusted.png")
            plt.close()

            plt.figure(dpi=300)
            backadjusted_price_series['backadjusted'].plot()
            plt.savefig(f"./{symbol}_backadjusted.png")
            plt.close()

        print('done\n')


if __name__ == "__main__":
    main()
