import pandas as pd
import quantstats as qs


class TradeBookAnalyzer:
    def __init__(self, tradebook_file):
        """
            Initialize TradeBookAnalyzer object.

            Args:
                tradebook_file (str): Path to the tradebook file.
        """
        self.tradebook_file = tradebook_file
        self.columns = ['symbol', 'isin', 'trade_type', 'quantity', 'price', 'trade_date']
        self.buy_sell_pair = []
        self.buy_sell_pairs = []
        self.buy_transactions = []
        self.sell_transactions = []

    def load_tradebook(self):
        """
           Load tradebook data from the specified file.

           Returns:
               DataFrame: The loaded tradebook data.
        """
        return pd.read_csv(self.tradebook_file)

    def process_trades(self, portfolio_data):
        """
            Process the trades from the tradebook data.

            Args:
                portfolio_data (DataFrame): The tradebook data to process.
        """
        grouped = portfolio_data.groupby(['symbol', 'isin', 'trade_type'])
        for group, group_df in grouped:
            if group[2] == "buy":
                self.buy_sell_pair.append([group[0], group[1], group[2], group_df["quantity"].sum(),
                                           group_df["trade_date"].min(), group_df["price"].mean()])
            elif group[2] == "sell":
                self.buy_sell_pair.append([group[0], group[1], group[2], group_df["quantity"].sum(),
                                           group_df["trade_date"].max(), group_df["price"].mean()])

    def generate_buy_sell_pairs(self):
        """
            Generate buy-sell pairs from the processed trades.
        """
        self.buy_sell_pair = pd.DataFrame(self.buy_sell_pair, columns=self.columns)

        # Create empty lists to store buy and sell transactions
        # Iterate through the DataFrame
        # Group transactions by symbol and isin

        for index, row in self.buy_sell_pair.iterrows():
            if row['trade_type'] == 'buy':
                self.buy_transactions.append(
                    (row['symbol'], row['isin'], row['quantity'], row['trade_date'], row['price']))
            elif row['trade_type'] == 'sell':
                self.sell_transactions.append(
                    (row['symbol'], row['isin'], row['quantity'], row['trade_date'], row['price']))

        # Process buy-sell pairs
        for buy in self.buy_transactions:
            for sell in self.sell_transactions:
                if buy[0] == sell[0] and buy[1] == sell[1]:
                    if buy[2] == sell[2]:  # If quantities match, create buy-sell pair
                        self.buy_sell_pairs.append({
                            'symbol': buy[0],
                            'isin': buy[1],
                            'buy_trade_date': buy[4],
                            'sell_trade_date': sell[4],
                            'quantity': buy[2],
                            'buy price': buy[3],
                            'sell price': sell[3]
                        })
                        self.sell_transactions.remove(sell)
                        self.buy_transactions.remove(buy)

                    elif buy[2] > sell[2]:  # More buy quantity than sell quantity
                        self.buy_sell_pairs.append({
                            'symbol': buy[0],
                            'isin': buy[1],
                            'buy_trade_date': buy[4],
                            'sell_trade_date': sell[4],
                            'quantity': sell[2],
                            'buy price': buy[3],
                            'sell price': sell[3]
                        })

                        buy = (buy[0], buy[1], buy[2] - sell[2], buy[3], buy[4])  # Update buy quantity

                        self.sell_transactions.remove(sell)

        # Add remaining buy transactions as separate transactions
        today = pd.Timestamp.today().strftime("%m/%d/%Y")
        for buy in self.buy_transactions:
            self.buy_sell_pairs.append({
                'symbol': buy[0],
                'isin': buy[1],
                'buy_trade_date': buy[4],
                'sell_trade_date': today,
                'quantity': buy[2],
                'buy price': buy[3],
                'sell price': 0
            })

    def calculate_portfolio_percentage(self):
        """
            Calculate portfolio percentage based on buy-sell pairs.

            Returns:
                DataFrame: DataFrame with portfolio percentage calculated.
        """
        df = pd.DataFrame(self.buy_sell_pairs)

        df["Ticker"] = df.symbol.str[:] + '.NS'
        df["Total_Amount"] = df["quantity"] * df["buy price"]
        total_amount_sum = df['Total_Amount'].sum()
        df["portfolio_percentage"] = df["Total_Amount"] / total_amount_sum
        return df

    def generate_report(self):
        """
           Generate report based on the tradebook data.

           This function generates a report with portfolio analytics and optimization.
        """
        portfolio_data = self.load_tradebook()
        self.process_trades(portfolio_data)
        self.generate_buy_sell_pairs()
        df = self.calculate_portfolio_percentage()
        buy_trade_date_min = df["buy_trade_date"].min()
        sell_trade_date_max = df["sell_trade_date"].max()
        stock_list = list(df["Ticker"].unique())
        date_index = pd.date_range(start=buy_trade_date_min, end=sell_trade_date_max)
        stock = qs.utils.download_returns(ticker=stock_list, period=date_index)
        qs.reports.html(returns=stock, benchmark="^NSEI", title="Portfolio Analytics", output="output/tearsheet.html")


if __name__ == "__main__":
    tradebook_analyzer = TradeBookAnalyzer("tradebook.csv")
    tradebook_analyzer.generate_report()
