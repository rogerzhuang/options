-- Create the database
CREATE DATABASE Polygon;
GO

-- Use the newly created database
USE Polygon;
GO

-- Create Securities table
CREATE TABLE Securities (
    ticker NVARCHAR(40) PRIMARY KEY,
    security_name NVARCHAR(255),
    security_type NVARCHAR(25),
    underlying_ticker NVARCHAR(40),
    FOREIGN KEY (underlying_ticker) REFERENCES Securities(ticker)
);

-- Create Stocks table
CREATE TABLE Stocks (
    ticker NVARCHAR(40) PRIMARY KEY,
    gics NVARCHAR(255),
    FOREIGN KEY (ticker) REFERENCES Securities(ticker)
);

-- Create Options table
CREATE TABLE Options (
    ticker NVARCHAR(40) PRIMARY KEY,
    option_type NVARCHAR(25),
    option_style NVARCHAR(25),
    expiry DATE,
    strike FLOAT,
    FOREIGN KEY (ticker) REFERENCES Securities(ticker)
);

-- Create HistPrice1D table
CREATE TABLE hist_price_1d (
    ticker NVARCHAR(40),
    exch_time DATETIME,
    [open] FLOAT,
    high FLOAT,
    low FLOAT,
    [close] FLOAT,
    open_adj FLOAT,
    high_adj FLOAT,
    low_adj FLOAT,
    close_adj FLOAT,
    volume INT,
    PRIMARY KEY (ticker, exch_time),
    FOREIGN KEY (ticker) REFERENCES Securities(ticker)
);

-- Create IvSurf table
CREATE TABLE iv_surf (
    ticker NVARCHAR(40),
    exch_time DATETIME,
    iv_surf_data VARBINARY(MAX),
    PRIMARY KEY (ticker, exch_time),
    FOREIGN KEY (ticker) REFERENCES Securities(ticker)
);

-- Create News table
CREATE TABLE news (
    id NVARCHAR(255) PRIMARY KEY,
    exch_time DATETIME,
    published_utc DATETIME,
    publisher_name NVARCHAR(255),
    title NVARCHAR(MAX),
    author NVARCHAR(255),
    article_url NVARCHAR(MAX),
    content NVARCHAR(MAX)
);

-- Create NewsSecurities table
CREATE TABLE news_securities (
    news_id NVARCHAR(255),
    ticker NVARCHAR(40),
    sentiment INT,
    PRIMARY KEY (news_id, ticker),
    FOREIGN KEY (news_id) REFERENCES news(id),
    FOREIGN KEY (ticker) REFERENCES Securities(ticker)
);
