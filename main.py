import os
import requests
import logging
import pandas as pd
from bs4 import BeautifulSoup

URL = 'https://en.wikipedia.org/wiki/Road_safety_in_Europe'


def get_data(url):
    """
    Function to retrieve HTML page from a certain URL
    :param url: String
    :return: BeautifulSoup HTML Content
    """

    response = requests.get(url)
    logging.info("Response Code: {}".format(response.status_code))
    if response.status_code != 200:
        logging.critical("Failed to retrieve data from URL: {} with Status Code: {}".format(URL, response.status_code))
        return None
    else:
        logging.info("Starting parsing process...")
        return BeautifulSoup(response.text, 'html.parser')


def get_data_table(html_soup):
    """
    Function to find tables in HTML file and return them in form of A Pandas Dataframe
    :param html_soup: BeautifulSoup HTML Content
    :return: Pandas Dataframe
    """
    table = html_soup.find(class_='wikitable sortable')
    logging.info("HTML Table Found.")
    return pd.read_html(table.prettify())[0]


def wrangle_pd_dataframe(eu_df):
    """
    Function to perform data wrangling operations on the Pandas Dataframe
    :param eu_df: Pandas Dataframe
    :return: Pandas Dataframe
    """
    eu_df = eu_df.rename(columns={
        'Country': 'Country',
        'Area  (thousands of km  2  )  [24]': 'Area',
        'Population in 2018  [25]': 'Population',
        'GDP per capita in 2018  [26]': 'GDP per capita',
        'Population density  (inhabitants per km  2  ) in 2017  [27]': 'Population density',
        'Vehicle ownership  (per thousand inhabitants) in 2016  [28]': 'Vehicle ownership',
        'Total Road Deaths in 2018  [30]': 'Total road deaths',
        'Road deaths  per Million Inhabitants in 2018  [30]': 'Road Deaths per Million Inhabitants',

    })

    eu_df.drop(['Road Network Length  (in km) in 2013  [29]',
                'Number of People Killed  per Billion km  [30]',
                'Number of Seriously Injured in 2017/2018  [30]'],
               axis=1, inplace=True)

    eu_df.insert(1, 'Year', "2018")

    eu_total = eu_df.tail(1)  # Extracting the EU Total so it will not affect the sort
    eu_df = eu_df.iloc[:-1, :].sort_values(by=['Road Deaths per Million Inhabitants'], ascending=False)
    eu_df = eu_df.append(eu_total)  # Re-Appending the EU total after the sort
    return eu_df


def create_csv(url):
    """
    Main Function to run the program and create the CSV file
    :param url: String
    :return: None
    """
    html_content = get_data(url)
    if html_content is None:
        logging.exception("No data was retrieved from the URL, No CSV will be created")
    else:

        eu_df = get_data_table(html_content)
        eu_df = wrangle_pd_dataframe(eu_df)
        # Saving Dataset to CSV:
        if not os.path.exists('Output'):
            os.makedirs('Output')
        eu_df.to_csv("Output/Dataset.csv")


if __name__ == '__main__':
    create_csv(URL)
