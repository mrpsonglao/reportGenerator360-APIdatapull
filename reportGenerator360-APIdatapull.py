# import all required Python libraries for this code
import pandas as pd
import numpy as np
import requests
import re
import io
import time
import datetime

# define custom functions for use in this code
def normalize_dates(list_dates):
    """
    Converts a list of timeframes (type: string) into a normalized list of timeframes (type: string, all in the format YYYY only).
        Does not affect YYYYQ1-3 or YYYYM01-M11 since these will be filtered out

    Parameters
    ----------
    list_dates: list of strings (each string is a timeframe which can be in the format YYYYQ#, YYYYM##, YYYY-YYYY, or YYYY-YY)

    Returns
    -------
    normed_dates: list of normalized timeframes (type: string, all in the format YYYY only) based on the input list

    Examples
    --------
    >>> normalize_dates(['2011', '2011Q4','2011Q1', '2013M12','2013M09','2011-2012', '2013-14'])
    ['2011', '2011', '2011Q1', '2013', '2013M09', '2011', '2013']

    """

    normed_dates = []  # initialize blank list for collecting normalized timeframes
    not_normed_list = []  # initialize blank list for collection boolean values (from the not_normed variable)
    not_normed = True  # initially set the not_normed variable to False

    # iterate through each timeframe x in the input list_dates
    for x in list_dates:
        not_normed = True
        # initialize the cleaned_date variable as being the same value as the input timeframe x
        cleaned_date = x

        # for ranged timeframes (e.g., YYYY-YYYY or YYYY-YY), we get the earlier timeframe (e.g., for 2008-2009 -- we use 2008)
        m = re.match('(\d{4})-(\d{4})', str(x))  # search the string if it matches the format YYYY-YYYY
        m2 = re.match('(\d{4})-(\d{2})', str(x))  # search the string if it matches the format YYYY-YY
        n = re.match('(\d{4})Q4', str(x))
        o = re.match('(\d{4})M12', str(x))

        if m:
            cleaned_date = m.groups()[0]  # get the earlier timeframe

        elif m2:
            cleaned_date = m2.groups()[0]  # get the earlier timeframe
        elif n:
            cleaned_date = n.groups()[-1]
        elif o:
            cleaned_date = o.groups()[-1]

        # for monthly/quarterly dates (e.g., YYYYQ# or YYYYM##), we just map these to YYYY
        else:
            n = re.match('(\d{4})Q\d{1}', str(x))  # search the string if it matches the format YYYYQ#
            o = re.match('(\d{4})M\d{2}', str(x))  # search the string if it matches the format YYYYM##

            if n:
                not_normed = False  # set the not_normed boolean variable to True
            elif o:
                not_normed = False  # set the not_normed boolean variable to True

        normed_dates.append(cleaned_date)  # collect all normalized dates in the list normed_dates
        not_normed_list.append(not_normed)  # collect all boolean variable not_normed in the list not_normed_list

    # return the required objects
    return normed_dates

def main():
    reports = input("""Type the name of the report you want to pull data for.
    Possible choices include: "Entrepreneurship", "Tourism", "Gender", "FCV".
    You can also press enter if you want to download data for ALL 4 thematic reports.
    """)

    if reports == "":
        report_list = ["Entrepreneurship", "Tourism", "Gender", "FCV"]
    else:
        report_list = [reports]

    # download data360 metadata
    tc_indicators = pd.read_json(requests.get(
        'https://tcdata360-backend.worldbank.org/api/v1/indicators/?fields=id%2Cname%2CvalueType%2Crank').text)
    gv_indicators = pd.read_json(requests.get(
        'https://govdata360-backend.worldbank.org/api/v1/indicators/?fields=id%2Cname%2CvalueType%2Crank').text)
    tc_indicators['site'] = 'tc'
    gv_indicators['site'] = 'gv'
    indicators = pd.concat([tc_indicators, gv_indicators], axis=0)
    all_unique_indicators = sorted(list(set(indicators['id'])))

    countries = pd.read_json(requests.get('https://tcdata360-backend.worldbank.org/api/v1/countries/').text)

    for input_reportID in report_list:

        start = time.time()
        print("Downloading data for topic: %s" % input_reportID)
        # get list of indicators from dataDesc
        dataDesc = pd.read_csv(input_reportID + "_DataDescription.csv", encoding='latin-1')
        indicators_selected = sorted(list(set(dataDesc[dataDesc['tcdata360_id'].notnull()]['tcdata360_id'])))

        # download data via TCdata360 API
        Report_data = pd.DataFrame()
        nondate_cols = ['index', 'indicatorId']

        for ind in indicators_selected:
            if ind not in all_unique_indicators:
                # skip if indicator ID is not a valid ID
                print(
                    "Indicator ID %s was skipped since it's not found on TC/Govdata360 API indicator list." % str(ind))
                continue

            ind_url = "https://tcdata360-backend.worldbank.org/api/v1/data?indicators=%s" % str(ind)
            response = requests.get(ind_url)

            df_ind = pd.DataFrame()

            # parse the JSON data appropriately
            for val in response.json()['data']:
                val_coun = val['id']
                df_temp = pd.DataFrame.from_dict({val_coun: val['indicators'][0]['values']}, orient='index')
                df_ind = df_ind.append(df_temp)
                df_ind['indicatorId'] = ind

            if df_ind.shape[0] > 0:
                df_ind = df_ind.reset_index().set_index(nondate_cols)

                ## normalize timeranges to YYYY ()
                # get first YYYY for dates with "YYYY-YY" or "YYYY-YYYY"
                # remap YYYYQ4 or YYYYM12 as the representative data forYYYY
                date_cols = [x for x in df_ind.columns if x not in nondate_cols]
                df_ind.columns = normalize_dates(date_cols)
                df_ind = df_ind[[x for x in df_ind.columns if len(x) == 4]]

                df_ind = df_ind.reset_index()
                # compile all downloaded data in one dataframe
                Report_data = Report_data.append(df_ind)

        # melt and create Period variable
        Report_data_cleaned = pd.melt(Report_data, id_vars=nondate_cols, var_name='Period', value_name='Observation')

        if Report_data_cleaned['Observation'].dtype == 'O':
            # catch Yes/No values and remap to 1/0 to avoid bind_rows errors for conversion from numeric to factor
            Report_data_cleaned['Observation'].replace({'Yes': 1.0, 'No': 0.0}, inplace=True)

        # use R convention to fill NA with the word "NA" instead of Python convention (blank)
        Report_data_cleaned['Observation'].fillna('NA', inplace=True)
        # fix column names accordingly
        Report_data_cleaned = Report_data_cleaned[['indicatorId', 'index', 'Period', 'Observation']]
        Report_data_cleaned.columns = ['id', 'iso3', 'Period', 'Observation']

        # save as CSV
        Report_data_cleaned.to_csv(input_reportID + "_data.csv", index=False)
        stop = time.time()

        time_diff = str(datetime.timedelta(seconds=stop - start))
        print("Finished downloading data for topic: %s with %d rows and %d columns." % (
        input_reportID, Report_data_cleaned.shape[0], Report_data_cleaned.shape[1]))
        print("Total time elapsed is %s" % time_diff)
        print("")

if  __name__ =='__main__':
    main()