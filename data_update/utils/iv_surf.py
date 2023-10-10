import pymssql
import numpy as np
from scipy.stats import norm
from scipy.interpolate import CloughTocher2DInterpolator
from scipy.optimize import newton, brentq
from datetime import datetime
import matplotlib.pyplot as plt
import config

r = 0.01  # Risk-free rate


# Define the Black-Scholes formula
def black_scholes(S, K, T, r, sigma, option_type='call'):
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    
    if option_type == 'call':
        return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    else:
        return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


def implied_volatility(option_price, S, K, T, r, option_type='call'):
    def loss_function(sigma):
        return black_scholes(S, K, T, r, sigma, option_type) - option_price

    try:
        # Try Newton-Raphson first
        return newton(loss_function, 0.2, maxiter=100)
    except RuntimeError:
        # Check the signs at the endpoints
        fa = loss_function(0.001)
        fb = loss_function(5)
        if np.sign(fa) == np.sign(fb):
            # If they have the same sign, return None or handle differently
            return None
        # If signs are different, use Brent's method
        return brentq(loss_function, 0.001, 5)


def get_iv_surf(ticker, trade_date):
    # Connect to the database
    conn = pymssql.connect(server=config.DB_SERVER, user=config.DB_USER, password=config.DB_PASSWORD, database=config.DB_NAME)
    cursor = conn.cursor()

    # Retrieve stock price on trade_date
    cursor.execute(f"SELECT [close] FROM hist_price_1d WHERE ticker = '{ticker}' AND exch_time = '{trade_date}'")
    S = cursor.fetchone()[0]

    # Retrieve data for the stock and its options
    cursor.execute(f"""SELECT o.ticker, o.expiry, o.strike, h.[close], o.option_type
        FROM options o
        JOIN hist_price_1d h ON o.ticker = h.ticker
        JOIN securities s ON o.ticker = s.ticker
        WHERE s.underlying_ticker = '{ticker}' AND h.exch_time = '{trade_date}' AND o.expiry > '{trade_date}'
        """)
    data = cursor.fetchall()
    conn.close()

    # Calculate implied volatilities
    iv_data = []
    for row in data:
        _, expiry, strike, option_price, option_type = row
        # Filter for OTM puts and calls
        if (option_type == 'put' and strike < S or option_type == 'call' and strike > S):
            T = (expiry - datetime.strptime(trade_date, '%Y-%m-%d').date()).days / 365.0
            iv = implied_volatility(option_price, S, strike, T, r, option_type)
            if iv is not None:
                strike_ratio = strike / S  # Store strike as a ratio of stock price
                iv_data.append((strike_ratio, T, iv))

    # Construct the implied volatility surface
    strike_ratios, Ts, ivs = zip(*iv_data)
    points = list(zip(strike_ratios, Ts))
    iv_surface = CloughTocher2DInterpolator(points, ivs)
    
    return iv_surface


def plot_iv_surf(iv_surface):
    # Create a meshgrid for visualization
    strike_ratios = np.linspace(0.8, 1.2, 100)  # Representing 80% to 120% of stock price
    T_range = np.linspace(1/365, 28/365, 100)
    X, Y = np.meshgrid(strike_ratios, T_range)
    Z = iv_surface(X, Y)

    # Plotting
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    ax.plot_surface(X, Y, Z, cmap='viridis')

    ax.set_xlabel('Strike Ratio (K/S)')
    ax.set_ylabel('Time to Expiry (T)')
    ax.set_zlabel('Implied Volatility (IV)')
    ax.set_title('Implied Volatility Surface')
    plt.show()


def get_option_price(trade_date, iv_surf, price, option_type, expiry_date, strike):
    T = (datetime.strptime(expiry_date, '%Y-%m-%d').date() - datetime.strptime(trade_date, '%Y-%m-%d').date()).days / 365.0
    strike_ratio = strike / price  # Provide strike as a ratio of stock price
    iv = iv_surf(strike_ratio, T)

    # Calculate the option price using the interpolated IV
    option_price = black_scholes(price, strike, T, r, iv, option_type)
    
    return option_price


# Example usage:
if __name__ == "__main__":
    ticker = 'AAPL'
    trade_date = '2023-01-03'
    option_type = 'put'
    expiry_date = '2023-01-13'
    price = 125.07  # Example price
    strike = 120  # Example strike

    iv_surf = get_iv_surf(ticker, trade_date)
    plot_iv_surf(iv_surf)
    price = get_option_price(trade_date, iv_surf, price, option_type, expiry_date, strike)
    print(f"The price for the {option_type} option with expiry on {expiry_date} and strike {strike} is: ${price:.2f}")
