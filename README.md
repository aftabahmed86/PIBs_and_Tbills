# PIBs_and_Tbills
PIBs and Tbills are government securities whereby trillions of rupees are invested by banks so their quick analysis is essential for appropriate and relevant decision making

import pandas as pd
import numpy as np
import dask.dataframe as dd
import pyarrow as pa
from datetime import datetime

PIBs_file_path = "E:\Python\Practice\PIBs Files\MTM Report 30.11.2024 PIBs.csv" # Must enter correct path and file name
Tbills_file_path = "E:\Python\Practice\Tbills Files\MTMReport 30.11.2024 Tbills.csv" # Must enter correct path and file name
MM_Deal_Register_file_path = "E:\Python\Practice\OS Files\MM_DealRegister 30.11.2024.csv" # Must enter correct path and file name
HO_GL_file_path = "E:\Python\Practice\HO GL\GL ABS 30.11.2024.xlsx" # Must enter correct path and file name
Month_Name = "Nov 2024"


Account_Codes_Reloaded_Path = "E:\Python\Practice\Account Codes Reloaded.xlsx"
spread = 2

cols_row_no = pd.read_csv(PIBs_file_path, low_memory=False)
cols_row_no = cols_row_no.apply(lambda row: row.astype(str).str.contains('FACE_AMOUNT').any(), axis=1 ).idxmax() + 1


PIBs_file_clean = pd.read_csv(PIBs_file_path, low_memory=False, skiprows=cols_row_no)
PIBs_file_clean = PIBs_file_clean.dropna(axis = 0, how='all')
PIBs_file_clean = PIBs_file_clean.dropna(axis = 1, how='all')


# Select columns for removing leading and trailing spaces
PIBs_Orig_Columns = PIBs_file_clean.columns

# Remove leading and trailing spaces from each item in the PIBs_Orig_Columns
filtered_rows_col_names = [item.strip() for item in PIBs_Orig_Columns]
PIBs_file_clean.columns = filtered_rows_col_names


PIBs = PIBs_file_clean
Category_Rate_Lookup = PIBs_file_clean
Rate_Lookup = PIBs_file_clean
PIB_Period = pd.read_csv(PIBs_file_path, low_memory=False)
PIB_Period = PIB_Period.loc[:, 'Unnamed: 1'].iloc[2]
Account_Codes_Reloaded = Account_Codes_Reloaded_Path


Auction_Date = pd.read_excel(Account_Codes_Reloaded, sheet_name='Monetary Policy', usecols='K:L', skiprows=3)
Auction_Date = Auction_Date.loc[Auction_Date['Auction Date'] >= PIB_Period, 'Settlement Date'].iloc[0]


Repricing_Tenor = pd.read_excel(Account_Codes_Reloaded, sheet_name='Monetary Policy', usecols='B:D', skiprows=3).dropna()

 
# Specify below the names of columns to keep while droping the rest of columns
PIBs_Cols = ['FACE_AMOUNT', 'ISSUE_DATE', 'MATURITY_DATE', 'ACQUIRED_DATE', 'DTM', 'COUPON_RATE', 
             'ORIGINAL_PRICE', 'ORIGINAL_YIELD', 'BOOK_VALUE', 'PRESENT_PRICE', 'PRESENT_VALUE', 'MTM_YIELD', 
             'MTM_PRICE', 'MARKET_VALUE', 'SURPLUS_DEFICIT', 'CLASS_CODE', 'SECURITY_TYPE', 'NEXTCOUPON',
             'COUPONFRQ', 'BONDNATURE']

# Remove leading and trailing spaces from each column name
PIBs_org_col_names = [each.strip() for each in PIBs.columns]
PIBs.columns = PIBs_org_col_names
PIBs = PIBs.loc[:, PIBs_Cols]

Filter_PIBs_Cols = ['FACE_AMOUNT', 'TYPE', 'ISSUE_DATE', 'MATURITY_DATE', 'ACQUIRED_DATE', 'DTM', 'COUPON_RATE', 'ORIGINAL_YIELD', 
                    'BOOK_VALUE', 'PRESENT_VALUE', 'MTM_YIELD', 'MARKET_VALUE', 'CLASS_CODE', 'SURPLUS_DEFICIT',  'Acquisition Month']


# Apply following looooooooooooooooooooooooop only if date columns are in serial numbers
serial_date_columns = ['ISSUE_DATE', 'MATURITY_DATE', 'ACQUIRED_DATE', 'NEXTCOUPON']

# for column in serial_date_columns:
#     PIBs[column] = pd.to_datetime(PIBs[column],  origin='1899-12-30', unit='D')

# # Apply following looooooooooooooooooooooooop only if date columns are in Date Format
# for column in serial_date_columns:
#     PIBs[column] = pd.to_datetime(PIBs[column], format='mixed', dayfirst=True)



def date_conversion(PIBs, serial_date_columns):
    for column in serial_date_columns:
        PIBs[column] = pd.to_datetime(PIBs[column], format='mixed', dayfirst=True)
    return PIBs

PIBs = date_conversion(PIBs, serial_date_columns)


# If Override rate is less than min original yield rate then input in 'Override_Min_Rate' column below but without decimal places
PIBs_Min_Override = {'Class': ['AFS', 'HFT', 'HTM'],   'Override_Min_Rate': [30, 30, 30]}
PIBs_Min_Override = pd.DataFrame(PIBs_Min_Override)
# print(PIBs_Min_Override)


# Apply following looooooooooooooooooooooooop only if yield columns are NON-DECIMALS
yield_columns = ['COUPON_RATE', 'ORIGINAL_YIELD',  'MTM_YIELD']


for column in yield_columns:
    Min_Yield = PIBs[PIBs[column] != 0]
    Min_Yield = np.floor(Min_Yield[column]).min().astype(int)
    PIBs[column] = PIBs[column].apply(lambda x: x / 100 if Min_Yield > 1 else x)

    Category_Rate_Lookup[column] = Category_Rate_Lookup[column].apply(lambda x: x / 100 if Min_Yield > 1 else x)

    Rate_Lookup[column] = Rate_Lookup[column].apply(lambda x: x / 100 if Min_Yield > 1 else x)


PIBs_Float_Cols = ['FACE_AMOUNT', 'BOOK_VALUE', 'PRESENT_VALUE', 'MARKET_VALUE']

# Covnert the string typed number columns into float
for column in PIBs_Float_Cols:
    if PIBs[column].dtypes == object:
        PIBs[column] = PIBs[column].str.replace(',', '').astype(float)
    else:
        PIBs[column] = PIBs[column].replace(',', '').astype(float)
        

# ####################################################### Variables Decision End #############################################33

# Rename Cols Names
PIBs = PIBs.rename(columns={'BONDNATURE': 'TYPE', 'NEXTCOUPON': 'Repricing_Date'})


PIBs['Tenor_Years'] = (PIBs['MATURITY_DATE'].dt.year - PIBs['ISSUE_DATE'].dt.year).astype(int)

Tenor_Lookup = {'Tenor_Years': [0, 1, 2, 3, 5, 10],
                'Tenor' : ['Up to 1 Year', 'Over 1 to 2 Years', 'Over 2 to 3 Years', 
                           'Over 3 to 5 Years', 'Over 5 to 10 years', 'Over 10 years'],
                }


PIBs_Tenor_Lookup = pd.DataFrame(Tenor_Lookup)
PIBs_Tenor_Lookup['Tenor_Years'] = PIBs_Tenor_Lookup['Tenor_Years'].astype(int)
PIBs_Tenor_Lookup['Sort_Tenor'] = PIBs_Tenor_Lookup.index + 1
# print(PIBs_Tenor_Lookup)

PIBs = PIBs.sort_values(['Tenor_Years'], ascending=True)
PIBs = pd.merge_asof(PIBs, PIBs_Tenor_Lookup[['Tenor_Years', 'Tenor', 'Sort_Tenor']], on='Tenor_Years', 
                   allow_exact_matches=False, suffixes=('_df1', '_df2'), direction='backward')






Yield_Rate = []
Yield_Range = []
min_yield_value = np.floor(Rate_Lookup['ORIGINAL_YIELD'] * 100).min().astype(int)
max_yield_value = np.ceil(Rate_Lookup['ORIGINAL_YIELD'] * 100).max().astype(int)

for each in range(min_yield_value, max_yield_value, spread):
    Yield_Rate.append(each)
for each in range(min_yield_value + spread, max_yield_value + spread, spread):
    Yield_Range.append(each)

Rate_Lookup = pd.DataFrame({'Yield_Rate': Yield_Rate, 'Yield_Range': Yield_Range})
Rate_Lookup['Rate_Bracket'] = '(' + Rate_Lookup['Yield_Rate'].astype(str) + '% to ' + Rate_Lookup['Yield_Range'].astype(str) + '%'+ ')'
Rate_Lookup['Yield_Range'] = Rate_Lookup['Yield_Range'] - .01
Rate_Lookup['Rate_Yield'] = '(' + Rate_Lookup['Yield_Rate'].astype(str) + '% to ' + Rate_Lookup['Yield_Range'].astype(str) + '%'+ ')'
Rate_Lookup['Yield_Rate'] = Rate_Lookup['Yield_Rate'] / 100
Rate_Lookup['Sort_Rate_Bracket'] = Rate_Lookup.index + 1
Rate_Lookup = Rate_Lookup.drop(['Yield_Range'], axis=1)
# print(Rate_Lookup)



def calc_min_yield(group):
    min_value = np.floor(group['ORIGINAL_YIELD']).min().astype(int)
    max_value = np.ceil(group['ORIGINAL_YIELD']).max().astype(int)
    return pd.Series({'Category_Rate': min_value, 'Category_Max_Rate': max_value})


def generate_series(min_value, max_value):
    Category_Rate = []
    for each in range(min_value, max_value, spread):
        Category_Rate.append(each)
    return pd.Series({'Category_Rate': Category_Rate})

Category_Rate_Lookup['ORIGINAL_YIELD'] = Category_Rate_Lookup['ORIGINAL_YIELD'] * 100
Category_Rate_Lookup = Category_Rate_Lookup.rename(columns={'CLASS_CODE': 'Category'})
Category_Rate_Lookup = Category_Rate_Lookup.groupby(['Category']).apply(calc_min_yield)
Category_Rate_Lookup = pd.merge(Category_Rate_Lookup, PIBs_Min_Override, left_index=True, right_on='Class')

Category_Rate_Lookup['Override_Min_Rate'].where(Category_Rate_Lookup['Override_Min_Rate'] < 
                                                  Category_Rate_Lookup['Category_Rate'], 
                                                  other=Category_Rate_Lookup['Category_Rate'], inplace=True)

Category_Rate_Lookup.rename(columns={'Override_Min_Rate' : 'Category_Min_Rate', 'Class' : 'Category'}, inplace=True)
Category_Rate_Lookup = Category_Rate_Lookup.drop(['Category_Rate'], axis=1)
Category_Rate_Lookup['Category_Rate'] = Category_Rate_Lookup.apply(lambda row: generate_series(row['Category_Min_Rate'], row['Category_Max_Rate']), axis=1)
Category_Rate_Lookup['Category_Range'] = Category_Rate_Lookup.apply(lambda row: generate_series(row['Category_Min_Rate'] + spread, row['Category_Max_Rate'] + spread), axis=1)
Category_Rate_Lookup = Category_Rate_Lookup.drop(['Category_Min_Rate', 'Category_Max_Rate'], axis=1)
Category_Rate_Lookup = Category_Rate_Lookup.explode(['Category_Rate', 'Category_Range']).reset_index(drop=True)
Category_Rate_Lookup['Bracket'] = '(' + Category_Rate_Lookup['Category_Rate'].astype(str) + '% to ' + Category_Rate_Lookup['Category_Range'].astype(str) + '%'+ ')'
Category_Rate_Lookup['Category_Range'] = Category_Rate_Lookup['Category_Range'] - .01
Category_Rate_Lookup['Category_Yield'] = '(' + Category_Rate_Lookup['Category_Rate'].astype(str) + '% to ' + Category_Rate_Lookup['Category_Range'].astype(str) + '%'+ ')'
Category_Rate_Lookup['Category_Bracket'] = Category_Rate_Lookup['Category'] + ' ' + Category_Rate_Lookup['Bracket']
Category_Rate_Lookup['Category_Rate'] = Category_Rate_Lookup['Category_Rate'] / 100
Category_Rate_Lookup['Sort_Category'] = Category_Rate_Lookup.index + 1
Category_Rate_Lookup = Category_Rate_Lookup.drop(['Category_Range'], axis=1)
# print(Category_Rate_Lookup)




def yield_list(df_class_code):
    yield_nested_list = Category_Rate_Lookup[Category_Rate_Lookup['Category'] ==  df_class_code]
    yield_nested_list = yield_nested_list.loc[:, ['Category_Rate']]
    yield_nested_list = yield_nested_list['Category_Rate'].tolist()
    return pd.Series({'Yield_List': yield_nested_list})

PIBs['Yield_List'] = PIBs['CLASS_CODE'].apply(lambda row: yield_list(row))

def filter_yield_list(row):
    threshold = row['ORIGINAL_YIELD']
    Yield_List = [num for num in row['Yield_List'] if num <= threshold]
    Yield_List = Yield_List[-1]
    return Yield_List

# Apply the function to the DataFrame
PIBs['Yield_List'] = PIBs.apply(filter_yield_list, axis=1)

PIBs = PIBs.merge(Category_Rate_Lookup[['Category', 'Category_Rate', 'Category_Bracket', 'Sort_Category']], left_on=['CLASS_CODE', 'Yield_List'], 
              right_on=['Category', 'Category_Rate'], how='left', suffixes=('', '_right')).drop(['Yield_List', 'Category', 'Category_Rate'], axis=1)


PIBs = PIBs.sort_values(['ORIGINAL_YIELD'], ascending=True)
PIBs = pd.merge_asof(PIBs, Rate_Lookup[['Yield_Rate', 'Rate_Bracket', 'Sort_Rate_Bracket']], left_on='ORIGINAL_YIELD', 
                   right_on='Yield_Rate', allow_exact_matches=True, suffixes=('_df1', '_df2'), direction='backward')
PIBs = PIBs.drop(['Yield_Rate'], axis=1)





PIB_Period_Date = pd.to_datetime(PIB_Period)
PIB_Period_EOM = pd.offsets.MonthEnd().rollforward(PIB_Period_Date)

def period_dates(PIB_Period_Date):
    PIB_Period_EOM = pd.offsets.MonthEnd().rollforward(PIB_Period_Date)
    Maturity_Months = Maturity_Lookup['Maturity_Months'].tolist()
    Modified_Maturity_Months = [item - 1 for item in Maturity_Months]
    
    if PIB_Period_Date == PIB_Period_EOM:
        dates_list = [PIB_Period_Date + pd.DateOffset(months=1) *  item for item in Maturity_Months]
        dates_list = [pd.offsets.MonthEnd().rollforward(item) for item in dates_list]
        dates_list = [pd.to_datetime((item + pd.DateOffset(days=1))) for item in dates_list]
    else:
        dates_list = [PIB_Period_Date + pd.DateOffset(months=1) * item for item in Modified_Maturity_Months]
        dates_list = [pd.offsets.MonthEnd().rollforward(item) for item in dates_list]
        dates_list = [pd.to_datetime((item + pd.Timedelta(days=PIB_Period_Date.day))) for item in dates_list]
        # dates_list = [item.date() for item in dates_list]
    
    return pd.Series({'Maturity_Date': dates_list})


Maturity_Lookup = {'Maturity_Months': [0, 1, 3, 6, 12, 24, 36, 60, 120],
                   'Maturity': ['Up to 1 Month', 'Over 1 to 3 Months', 'Over 3 to 6 Months', 'Over 6 Months to 1 Year', 
                                'Over 1 to 2 Years', 'Over 2 to 3 Years', 'Over 3 to 5 Years', 'Over 5 to 10 years', 'Over 10 years']}
Maturity_Lookup = pd.DataFrame(Maturity_Lookup)

Maturity_Lookup['Maturity_Date'] = Maturity_Lookup.apply(lambda row: PIB_Period_Date + pd.DateOffset(months=1) *  row['Maturity_Months']  if PIB_Period_Date == PIB_Period_EOM else 
                                                         PIB_Period_Date + pd.DateOffset(months=1) * (row['Maturity_Months'] - 1), axis=1)
Maturity_Lookup['Maturity_Date'] = Maturity_Lookup['Maturity_Date'].apply(lambda x: pd.offsets.MonthEnd().rollforward(x))
Maturity_Lookup['Maturity_Date'] = Maturity_Lookup.apply(lambda row: row['Maturity_Date'] + pd.DateOffset(days=1) if PIB_Period_Date == PIB_Period_EOM else 
                                                         row['Maturity_Date'] +  pd.Timedelta(days=PIB_Period_Date.day), axis=1)
Maturity_Lookup['Maturity_Date'] = pd.to_datetime(Maturity_Lookup['Maturity_Date'].dt.date)
Maturity_Lookup['Period'] = PIB_Period_Date
Maturity_Lookup['Period'] = pd.to_datetime(Maturity_Lookup['Period'].dt.date)
Maturity_Lookup['Sort_Maturity'] = Maturity_Lookup.index + 1




PIBs['Period'] = PIB_Period_Date
PIBs['Maturity_Date'] = PIBs.apply(lambda row: period_dates(row['Period']), axis=1)


def filter_nested_list(row):
    threshold = pd.Timestamp(row['MATURITY_DATE'])
    Maturity_Date = [num for num in row['Maturity_Date'] if num <= threshold]
    Maturity_Date = Maturity_Date[-1]
    return Maturity_Date

# Apply the function to the DataFrame
PIBs['Maturity_Date'] = PIBs.apply(filter_nested_list, axis=1)

PIBs = PIBs.merge(Maturity_Lookup[['Maturity_Date', 'Maturity', 'Sort_Maturity']], on='Maturity_Date', how='left', suffixes=('', '_right')).drop('Maturity_Date', axis=1)

PIBs['Acquisition_Month'] = PIBs['ACQUIRED_DATE'].apply(lambda x: pd.offsets.MonthEnd().rollforward(x))
PIBs['Acquisition_Year'] = PIBs['ACQUIRED_DATE'].dt.year
PIBs['Product_of_PV'] = PIBs['PRESENT_VALUE'] * PIBs['ORIGINAL_YIELD']

PIBs['SURPLUS_DEFICIT'] = PIBs.apply(lambda row: 0 if row['CLASS_CODE'] == "HTM" else row['SURPLUS_DEFICIT'], axis=1)

PIBs['IM_Dates'] = (PIBs['ISSUE_DATE'].dt.day.astype(str) + PIBs['ISSUE_DATE'].dt.month.astype(str) + PIBs['ISSUE_DATE'].apply(lambda x: str(x.year)[-2:]) + 
                  PIBs['MATURITY_DATE'].dt.day.astype(str) + PIBs['MATURITY_DATE'].dt.month.astype(str) + PIBs['MATURITY_DATE'].apply(lambda x: str(x.year)[-2:])).astype('Float64')

PIBs['Start_of_Mat_Month'] = PIBs['MATURITY_DATE'].apply(lambda x: pd.offsets.MonthEnd().rollback(x) + pd.DateOffset(days=1))
PIBs['TYPE'] = PIBs.apply(lambda row: 'FIXED' if row['TYPE'] =='Fixed' else 'FLOATER' if row['TYPE'] =='Floater' else row['TYPE'], axis=1)
PIBs['COUPONFRQ'] = PIBs.apply(lambda row: 1 if row['Tenor_Years'] == 2 else row['COUPONFRQ'], axis=1)


PIBs.loc[PIBs['COUPONFRQ'] == 1, 'Repricing_Date'] = pd.to_datetime(Auction_Date)
PIBs.loc[PIBs['TYPE'] == 'FIXED', 'Repricing_Date'] = PIBs['MATURITY_DATE']

PIBs['Start_of_Rep_Month'] = PIBs['Repricing_Date'].apply(lambda x: pd.offsets.MonthEnd().rollback(x) + pd.DateOffset(days=1))


Repricing_Tenor['Repricing No'] = np.floor(Repricing_Tenor['Repricing No']).astype(int)
Repricing_Tenor['Repricing Period'] = np.floor(Repricing_Tenor['Repricing Period']).astype(int)
PIBs = PIBs.merge(Repricing_Tenor[['Repricing No', 'Repricing Tenor']], left_on='COUPONFRQ', right_on='Repricing No', how='left', suffixes=('', '_right')).drop('Repricing No', axis=1)
PIBs['Repricing Tenor'] = PIBs.apply(lambda row: 'FIXED' if row['TYPE'] == 'FIXED' else row['Repricing Tenor'], axis=1)


PIBs['TYPE'] = PIBs.apply(lambda row: 'Fixed' if row['TYPE'] == 'FIXED' else 'Floater' if row['TYPE'] == 'FLOATER' else row['TYPE'], axis=1)


PIBs = PIBs.rename(columns={"Category_Bracket": "Category", "Acquisition_Month": "Acquisition Month", "Acquisition_Year": "Acquisition Year", "Product_of_PV": "Product of PV",
                        "Repricing_Date": "Repricing Date", "Start_of_Mat_Month": "Start of Mat Month", "Start_of_Rep_Month": "Start of Rep Month"})


PIBs_Filter = PIBs.loc[:, Filter_PIBs_Cols]

# PIBs PRESENT VALUE Grouping
Sum_Grouped_Cols = ['PRESENT_VALUE']

Category_Bracket_Grouped = PIBs.groupby(['Sort_Category', 'Category'])[Sum_Grouped_Cols].sum().reset_index().sort_values('Sort_Category').drop('Sort_Category', axis=1)
Category_Bracket_Grouped[Sum_Grouped_Cols] = Category_Bracket_Grouped[Sum_Grouped_Cols] / 1000000
# print(Category_Bracket_Grouped)

Rate_Bracket_Grouped = PIBs.groupby(['Sort_Rate_Bracket', 'Rate_Bracket'])[Sum_Grouped_Cols].sum().reset_index().sort_values('Sort_Rate_Bracket').drop('Sort_Rate_Bracket', axis=1)
Rate_Bracket_Grouped[Sum_Grouped_Cols] = Rate_Bracket_Grouped[Sum_Grouped_Cols] / 1000000
# print(Category_Bracket_Grouped)

Maturity_Grouped = PIBs.groupby(['Sort_Maturity', 'Maturity'])[Sum_Grouped_Cols].sum().reset_index().sort_values('Sort_Maturity')
Maturity_Grouped[Sum_Grouped_Cols] = Maturity_Grouped[Sum_Grouped_Cols] / 1000000
# print(Maturity_Grouped)



# with pd.ExcelWriter('E:\Python\Practice\PIBs.xlsx') as writer:
#     PIBs.to_excel(writer, sheet_name='PIBs', index=False)
#     Category_Rate_Lookup.to_excel(writer, sheet_name='Category_Rate_Lookup', index=False)
#     Category_Bracket_Grouped.to_excel(writer, sheet_name='Category_Bracket_Grouped', index=False)
#     Rate_Lookup.to_excel(writer, sheet_name='Rate_Lookup', index=False)
#     Rate_Bracket_Grouped.to_excel(writer, sheet_name='Rate_Bracket_Grouped', index=False)
#     PIBs_Tenor_Lookup.to_excel(writer, sheet_name='PIBs_Tenor_Lookup', index=False)
#     Maturity_Lookup.to_excel(writer, sheet_name='Maturity_Lookup', index=False)
#     Maturity_Grouped.to_excel(writer, sheet_name='Maturity_Grouped', index=False)
#     PIBs_Filter.to_excel(writer, sheet_name='PIBs_Filter', index=False)


PIBs_csv = PIBs.to_csv(f"E:\Power BI Data\Power BI Data CSV\Power BI Data - PIBs\PIBs {Month_Name}.csv", index=False)
PIBs








import pandas as pd
import numpy as np
import dask.dataframe as dd
import pyarrow as pa
from datetime import datetime

Tbills_file_name = Tbills_file_path
spread = 1



cols_row_no = pd.read_csv(Tbills_file_name, low_memory=False)
cols_row_no = cols_row_no.apply(lambda row: row.astype(str).str.contains('FACE_AMOUNT').any(), axis=1 ).idxmax() + 1


Tbills_file_clean = pd.read_csv(Tbills_file_name, low_memory=False, skiprows=cols_row_no)
Tbills_file_clean = Tbills_file_clean.dropna(axis = 0, how='all')
Tbills_file_clean = Tbills_file_clean.dropna(axis = 1, how='all')


# Select columns for removing leading and trailing spaces
PIBs_Orig_Columns = Tbills_file_clean.columns

# Remove leading and trailing spaces from each item in the PIBs_Orig_Columns
filtered_rows_col_names = [item.strip() for item in PIBs_Orig_Columns]
Tbills_file_clean.columns = filtered_rows_col_names


Tbills = Tbills_file_clean
Category_Rate_Lookup = Tbills_file_clean
Rate_Lookup = Tbills_file_clean
Tbill_Period = pd.read_csv(Tbills_file_name, low_memory=False)
Tbill_Period = Tbill_Period.loc[:, 'Unnamed: 1'].iloc[2]
Account_Codes_Reloaded = Account_Codes_Reloaded_Path




# Specify below the names of columns to keep while droping the rest of columns
Tbills_Cols = ['FACE_AMOUNT', 'ISSUE_DATE', 'MATURITY_DATE', 'ACQUIRED_DATE', 'DTM', 'COUPON_RATE', 
             'ORIGINAL_PRICE', 'ORIGINAL_YIELD', 'BOOK_VALUE', 'PRESENT_PRICE', 'PRESENT_VALUE', 'MTM_YIELD', 
             'MTM_PRICE', 'MARKET_VALUE', 'SURPLUS_DEFICIT', 'CLASS_CODE', 'SECURITY_TYPE', 'WAG', 'UTILIZEDDAYS']
Tbills = Tbills.loc[:, Tbills_Cols]


Filter_Tbills_Cols = [ 'ISSUE_DATE', 'MATURITY_DATE', 'ACQUIRED_DATE', 'TENOR', 'DTM',  'ORIGINAL_YIELD', 
                    'BOOK_VALUE', 'PRESENT_VALUE', 'MTM_YIELD', 'MARKET_VALUE', 'CLASS_CODE', 'SURPLUS_DEFICIT',  'Acquisition Month']


# Apply following looooooooooooooooooooooooop only if date columns are in serial numbers
serial_date_columns = ['ISSUE_DATE', 'MATURITY_DATE', 'ACQUIRED_DATE']

# for column in serial_date_columns:
#     Tbills[column] = pd.to_datetime(Tbills[column],  origin='1899-12-30', unit='D')

# # Apply following looooooooooooooooooooooooop only if date columns are in Date Format
# for column in serial_date_columns:
#     Tbills[column] = pd.to_datetime(Tbills[column], format='mixed', dayfirst=True)



def date_conversion(Tbills, serial_date_columns):
    for column in serial_date_columns:
        Tbills[column] = pd.to_datetime(Tbills[column], format='mixed', dayfirst=True)
    return Tbills

Tbills = date_conversion(Tbills, serial_date_columns)


# If Override rate is less than min original yield rate then input in 'Override_Min_Rate' column below but without decimal places
PIBs_Min_Override = {'Class': ['AFS', 'HFT', 'HTM'],   'Override_Min_Rate': [30, 30, 30]}
PIBs_Min_Override = pd.DataFrame(PIBs_Min_Override)
# print(PIBs_Min_Override)


# Apply following looooooooooooooooooooooooop only if yield columns are NON-DECIMALS
yield_columns = ['ORIGINAL_YIELD',  'MTM_YIELD' ]


for column in yield_columns:
    Min_Yield = np.floor(Tbills[column]).min().astype(int)
    Tbills[column] = Tbills[column].apply(lambda x: x / 100 if Min_Yield > 1 else x)

    Min_Yield_Category_Rate = np.floor(Category_Rate_Lookup[column]).min().astype(int)
    Category_Rate_Lookup[column] = Category_Rate_Lookup[column].apply(lambda x: x / 100 if Min_Yield > 1 else x)

    Min_Yield_Rate_Lookup = np.floor(Rate_Lookup[column]).min().astype(int)
    Rate_Lookup[column] = Rate_Lookup[column].apply(lambda x: x / 100 if Min_Yield > 1 else x)

# ####################################################### Variables Decision End #############################################33

# # Rename Cols Names
# Tbills = Tbills.rename(columns={'BONDNATURE': 'TYPE', 'NEXTCOUPON': 'Repricing_Date'})


Tbills['Tenor_Days'] = (Tbills['MATURITY_DATE'] - Tbills['ISSUE_DATE']).dt.days

Tbills_Tenor_Lookup = {'Tenor_Days': [75, 180 ,360],
                       'TENOR' : ['3-month', '6-month', '12-month'],
                }


Tbills_Tenor_Lookup = pd.DataFrame(Tbills_Tenor_Lookup)
Tbills_Tenor_Lookup['Tenor_Days'] = Tbills_Tenor_Lookup['Tenor_Days']
# print(Tbills_Tenor_Lookup)

Tbills = Tbills.sort_values(['Tenor_Days'], ascending=True)
Tbills = pd.merge_asof(Tbills, Tbills_Tenor_Lookup[['Tenor_Days', 'TENOR']], on='Tenor_Days', 
                   allow_exact_matches=False, suffixes=('_df1', '_df2'), direction='backward')
Tbills['Maturity Date'] = Tbills['MATURITY_DATE'].apply(lambda x: x.replace(day=1))






Yield_Rate = []
Yield_Range = []
min_yield_value = np.floor(Rate_Lookup['ORIGINAL_YIELD'] * 100).min().astype(int)
max_yield_value = np.ceil(Rate_Lookup['ORIGINAL_YIELD'] * 100).max().astype(int)

for each in range(min_yield_value, max_yield_value, spread):
    Yield_Rate.append(each)
for each in range(min_yield_value + spread, max_yield_value + spread, spread):
    Yield_Range.append(each)

Rate_Lookup = pd.DataFrame({'Yield_Rate': Yield_Rate, 'Yield_Range': Yield_Range})
Rate_Lookup['Rate_Bracket'] = '(' + Rate_Lookup['Yield_Rate'].astype(str) + '% to ' + Rate_Lookup['Yield_Range'].astype(str) + '%'+ ')'
Rate_Lookup['Yield_Range'] = Rate_Lookup['Yield_Range'] - .01
Rate_Lookup['Rate_Yield'] = '(' + Rate_Lookup['Yield_Rate'].astype(str) + '% to ' + Rate_Lookup['Yield_Range'].astype(str) + '%'+ ')'
Rate_Lookup['Yield_Rate'] = Rate_Lookup['Yield_Rate'] / 100
Rate_Lookup['Sort_Rate_Bracket'] = Rate_Lookup.index + 1
Rate_Lookup = Rate_Lookup.drop(['Yield_Range'], axis=1)
# print(Rate_Lookup)



def calc_min_yield(group):
    min_value = np.floor(group['ORIGINAL_YIELD']).min().astype(int)
    max_value = np.ceil(group['ORIGINAL_YIELD']).max().astype(int)
    return pd.Series({'Category_Rate': min_value, 'Category_Max_Rate': max_value})


def generate_series(min_value, max_value):
    Category_Rate = []
    for each in range(min_value, max_value, spread):
        Category_Rate.append(each)
    return pd.Series({'Category_Rate': Category_Rate})

Category_Rate_Lookup['ORIGINAL_YIELD'] = Category_Rate_Lookup['ORIGINAL_YIELD'] * 100
Category_Rate_Lookup = Category_Rate_Lookup.rename(columns={'CLASS_CODE': 'Category'})
Category_Rate_Lookup = Category_Rate_Lookup.groupby(['Category']).apply(calc_min_yield)
Category_Rate_Lookup = pd.merge(Category_Rate_Lookup, PIBs_Min_Override, left_index=True, right_on='Class')

Category_Rate_Lookup['Override_Min_Rate'].where(Category_Rate_Lookup['Override_Min_Rate'] < 
                                                  Category_Rate_Lookup['Category_Rate'], 
                                                  other=Category_Rate_Lookup['Category_Rate'], inplace=True)

Category_Rate_Lookup.rename(columns={'Override_Min_Rate' : 'Category_Min_Rate', 'Class' : 'Category'}, inplace=True)
Category_Rate_Lookup = Category_Rate_Lookup.drop(['Category_Rate'], axis=1)
Category_Rate_Lookup['Category_Rate'] = Category_Rate_Lookup.apply(lambda row: generate_series(row['Category_Min_Rate'], row['Category_Max_Rate']), axis=1)
Category_Rate_Lookup['Category_Range'] = Category_Rate_Lookup.apply(lambda row: generate_series(row['Category_Min_Rate'] + spread, row['Category_Max_Rate'] + spread), axis=1)
Category_Rate_Lookup = Category_Rate_Lookup.drop(['Category_Min_Rate', 'Category_Max_Rate'], axis=1)
Category_Rate_Lookup = Category_Rate_Lookup.explode(['Category_Rate', 'Category_Range']).reset_index(drop=True)
Category_Rate_Lookup['Bracket'] = '(' + Category_Rate_Lookup['Category_Rate'].astype(str) + '% to ' + Category_Rate_Lookup['Category_Range'].astype(str) + '%'+ ')'
Category_Rate_Lookup['Category_Range'] = Category_Rate_Lookup['Category_Range'] - .01
Category_Rate_Lookup['Category_Yield'] = '(' + Category_Rate_Lookup['Category_Rate'].astype(str) + '% to ' + Category_Rate_Lookup['Category_Range'].astype(str) + '%'+ ')'
Category_Rate_Lookup['Category_Bracket'] = Category_Rate_Lookup['Category'] + ' ' + Category_Rate_Lookup['Bracket']
Category_Rate_Lookup['Category_Rate'] = Category_Rate_Lookup['Category_Rate'] / 100
Category_Rate_Lookup['Sort_Category'] = Category_Rate_Lookup.index + 1
Category_Rate_Lookup = Category_Rate_Lookup.drop(['Category_Range'], axis=1)
# print(Category_Rate_Lookup)




def yield_list(df_class_code):
    yield_nested_list = Category_Rate_Lookup[Category_Rate_Lookup['Category'] ==  df_class_code]
    yield_nested_list = yield_nested_list.loc[:, ['Category_Rate']]
    yield_nested_list = yield_nested_list['Category_Rate'].tolist()
    return pd.Series({'Yield_List': yield_nested_list})

Tbills['Yield_List'] = Tbills['CLASS_CODE'].apply(lambda row: yield_list(row))

def filter_yield_list(row):
    threshold = row['ORIGINAL_YIELD']
    Yield_List = [num for num in row['Yield_List'] if num <= threshold]
    Yield_List = Yield_List[-1]
    return Yield_List

# Apply the function to the DataFrame
Tbills['Yield_List'] = Tbills.apply(filter_yield_list, axis=1)

Tbills = Tbills.merge(Category_Rate_Lookup[['Category', 'Category_Rate', 'Category_Bracket', 'Sort_Category']], left_on=['CLASS_CODE', 'Yield_List'], 
              right_on=['Category', 'Category_Rate'], how='left', suffixes=('', '_right')).drop(['Yield_List', 'Category', 'Category_Rate'], axis=1)


Tbills = Tbills.sort_values(['ORIGINAL_YIELD'], ascending=True)
Tbills = pd.merge_asof(Tbills, Rate_Lookup[['Yield_Rate', 'Rate_Bracket', 'Sort_Rate_Bracket']], left_on='ORIGINAL_YIELD', 
                   right_on='Yield_Rate', allow_exact_matches=True, suffixes=('_df1', '_df2'), direction='backward')
Tbills = Tbills.drop(['Yield_Rate'], axis=1)





Tbill_Period_Date = pd.to_datetime(Tbill_Period)
PIB_Period_EOM = pd.offsets.MonthEnd().rollforward(Tbill_Period_Date)

def period_dates(Tbill_Period_Date):
    PIB_Period_EOM = pd.offsets.MonthEnd().rollforward(Tbill_Period_Date)
    Maturity_Months = Maturity_Lookup['Maturity_Months'].tolist()
    Modified_Maturity_Months = [item - 1 for item in Maturity_Months]
    
    if Tbill_Period_Date == PIB_Period_EOM:
        dates_list = [Tbill_Period_Date + pd.DateOffset(months=1) *  item for item in Maturity_Months]
        dates_list = [pd.offsets.MonthEnd().rollforward(item) for item in dates_list]
        dates_list = [pd.to_datetime((item + pd.DateOffset(days=1))) for item in dates_list]
    else:
        dates_list = [Tbill_Period_Date + pd.DateOffset(months=1) * item for item in Modified_Maturity_Months]
        dates_list = [pd.offsets.MonthEnd().rollforward(item) for item in dates_list]
        dates_list = [pd.to_datetime((item + pd.Timedelta(days=Tbill_Period_Date.day))) for item in dates_list]
        # dates_list = [item.date() for item in dates_list]
    
    return pd.Series({'Maturity_Date': dates_list})


Maturity_Lookup = {'Maturity_Months': [0, 1, 3, 6],
                   'Maturity': ['Up to 1 Month', 'Over 1 to 3 Months', 'Over 3 to 6 Months', 'Over 6 Months to 1 Year']}
Maturity_Lookup = pd.DataFrame(Maturity_Lookup)

Maturity_Lookup['Maturity_Date'] = Maturity_Lookup.apply(lambda row: Tbill_Period_Date + pd.DateOffset(months=1) *  row['Maturity_Months']  if Tbill_Period_Date == PIB_Period_EOM else 
                                                         Tbill_Period_Date + pd.DateOffset(months=1) * (row['Maturity_Months'] - 1), axis=1)
Maturity_Lookup['Maturity_Date'] = Maturity_Lookup['Maturity_Date'].apply(lambda x: pd.offsets.MonthEnd().rollforward(x))
Maturity_Lookup['Maturity_Date'] = Maturity_Lookup.apply(lambda row: row['Maturity_Date'] + pd.DateOffset(days=1) if Tbill_Period_Date == PIB_Period_EOM else 
                                                         row['Maturity_Date'] +  pd.Timedelta(days=Tbill_Period_Date.day), axis=1)
Maturity_Lookup['Maturity_Date'] = pd.to_datetime(Maturity_Lookup['Maturity_Date'].dt.date)
Maturity_Lookup['Period'] = Tbill_Period_Date
Maturity_Lookup['Period'] = pd.to_datetime(Maturity_Lookup['Period'].dt.date)
Maturity_Lookup['Sort_Maturity'] = Maturity_Lookup.index + 1




Tbills['Period'] = Tbill_Period_Date
Tbills['Maturity_Date'] = Tbills.apply(lambda row: period_dates(row['Period']), axis=1)


def filter_nested_list(row):
    threshold = pd.Timestamp(row['MATURITY_DATE'])
    Maturity_Date = [num for num in row['Maturity_Date'] if num <= threshold]
    Maturity_Date = Maturity_Date[-1]
    return Maturity_Date

# Apply the function to the DataFrame
Tbills['Maturity_Date'] = Tbills.apply(filter_nested_list, axis=1)

Tbills = Tbills.merge(Maturity_Lookup[['Maturity_Date', 'Maturity', 'Sort_Maturity']], on='Maturity_Date', how='left', suffixes=('', '_right')).drop('Maturity_Date', axis=1)

Tbills['Acquisition_Month'] = Tbills['ACQUIRED_DATE'].apply(lambda x: pd.offsets.MonthEnd().rollforward(x))
Tbills['Acquisition_Year'] = Tbills['ACQUIRED_DATE'].dt.year
Tbills['Product_of_PV'] = Tbills['PRESENT_VALUE'] * Tbills['ORIGINAL_YIELD']

Tbills['SURPLUS_DEFICIT'] = Tbills.apply(lambda row: 0 if row['CLASS_CODE'] == "HTM" else row['SURPLUS_DEFICIT'], axis=1)



Tbills = Tbills.rename(columns={"Category_Bracket": "Category", "Acquisition_Month": "Acquisition Month", "Acquisition_Year": "Acquisition Year", "Product_of_PV": "Product of PV"})


Tbills_Filter = Tbills.loc[:, Filter_Tbills_Cols]

# Tbills PRESENT VALUE Grouping
Sum_Grouped_Cols = ['PRESENT_VALUE']

Category_Bracket_Grouped = Tbills.groupby(['Sort_Category', 'Category'])[Sum_Grouped_Cols].sum().reset_index().sort_values('Sort_Category').drop('Sort_Category', axis=1)
Category_Bracket_Grouped[Sum_Grouped_Cols] = Category_Bracket_Grouped[Sum_Grouped_Cols] / 1000000
# print(Category_Bracket_Grouped)

Rate_Bracket_Grouped = Tbills.groupby(['Sort_Rate_Bracket', 'Rate_Bracket'])[Sum_Grouped_Cols].sum().reset_index().sort_values('Sort_Rate_Bracket').drop('Sort_Rate_Bracket', axis=1)
Rate_Bracket_Grouped[Sum_Grouped_Cols] = Rate_Bracket_Grouped[Sum_Grouped_Cols] / 1000000
# print(Category_Bracket_Grouped)

Maturity_Grouped = Tbills.groupby(['Sort_Maturity', 'Maturity'])[Sum_Grouped_Cols].sum().reset_index().sort_values('Sort_Maturity')
Maturity_Grouped[Sum_Grouped_Cols] = Maturity_Grouped[Sum_Grouped_Cols] / 1000000
# print(Maturity_Grouped)



# with pd.ExcelWriter('E:\PIBs and Tbills\Year 2024\Sep 2024\Tbills.xlsx') as writer:
#     Tbills.to_excel(writer, sheet_name='Tbills', index=False)
#     Category_Rate_Lookup.to_excel(writer, sheet_name='Category_Rate_Lookup', index=False)
#     Category_Bracket_Grouped.to_excel(writer, sheet_name='Category_Bracket_Grouped', index=False)
#     Rate_Lookup.to_excel(writer, sheet_name='Rate_Lookup', index=False)
#     Rate_Bracket_Grouped.to_excel(writer, sheet_name='Rate_Bracket_Grouped', index=False)
#     Tbills_Tenor_Lookup.to_excel(writer, sheet_name='Tbills_Tenor_Lookup', index=False)
#     Maturity_Lookup.to_excel(writer, sheet_name='Maturity_Lookup', index=False)
#     Maturity_Grouped.to_excel(writer, sheet_name='Maturity_Grouped', index=False)
#     Tbills_Filter.to_excel(writer, sheet_name='Tbills_Filter', index=False)

Tbills_csv = Tbills.to_csv(f"E:\Power BI Data\Power BI Data CSV\Power BI Data - Tbills\Tbills {Month_Name}.csv", index=False)
Tbills












import pandas as pd
import numpy as np
import dask.dataframe as dd
import pyarrow as pa
from datetime import datetime

MM_Deal_Register_file_name = MM_Deal_Register_file_path
Account_Codes_Reloaded = Account_Codes_Reloaded_Path
spread = 1


cols_row_no = pd.read_csv(MM_Deal_Register_file_name, low_memory=False)
cols_row_no = cols_row_no.apply(lambda row: row.astype(str).str.contains('Principal').any(), axis=1 ).idxmax() + 1


Borrowing = pd.read_csv(MM_Deal_Register_file_name, low_memory=False, skiprows=cols_row_no)
Borrowing = Borrowing.dropna(axis = 0, how='all')
Borrowing = Borrowing.dropna(axis = 1, how='all')


# Select columns for removing leading and trailing spaces
Borrowing_Orig_Columns = Borrowing.columns

# Remove leading and trailing spaces from each item in the PIBs_Orig_Columns
filtered_rows_col_names = [item.strip() for item in Borrowing_Orig_Columns]
Borrowing.columns = filtered_rows_col_names


Borrowing_Period = pd.read_csv(MM_Deal_Register_file_name, low_memory=False, header=None)
Borrowing_Period = pd.to_datetime(Borrowing_Period.iloc[0,2][-10:], dayfirst=True)
# print(Borrowing_Period)

Borrowing = Borrowing.iloc[:-1]

# Specify below the names of columns to keep while droping the rest of columns
# Tbills_Cols = ['FACE_AMOUNT', 'ISSUE_DATE', 'MATURITY_DATE', 'ACQUIRED_DATE', 'DTM', 'COUPON_RATE', 
#              'ORIGINAL_PRICE', 'ORIGINAL_YIELD', 'BOOK_VALUE', 'PRESENT_PRICE', 'PRESENT_VALUE', 'MTM_YIELD', 
#              'MTM_PRICE', 'MARKET_VALUE', 'SURPLUS_DEFICIT', 'CLASS_CODE', 'SECURITY_TYPE', 'WAG', 'UTILIZEDDAYS']
# Borrowing = Borrowing.loc[:, Tbills_Cols]


# Filter_Tbills_Cols = [ 'ISSUE_DATE', 'MATURITY_DATE', 'ACQUIRED_DATE', 'TENOR', 'DTM',  'ORIGINAL_YIELD', 
#                     'BOOK_VALUE', 'PRESENT_VALUE', 'MTM_YIELD', 'MARKET_VALUE', 'CLASS_CODE', 'SURPLUS_DEFICIT',  'Acquisition Month']


# Apply following looooooooooooooooooooooooop only if date columns are in serial numbers
serial_date_columns = ['TradeDate', 'ValueDate', 'MaturityDate']

# for column in serial_date_columns:
#     Borrowing[column] = pd.to_datetime(Borrowing[column],  origin='1899-12-30', unit='D')

# # Apply following looooooooooooooooooooooooop only if date columns are in Date Format
# for column in serial_date_columns:
#     Borrowing[column] = pd.to_datetime(Borrowing[column], format='mixed', dayfirst=True)



def date_conversion(Borrowing, serial_date_columns):
    for column in serial_date_columns:
        Borrowing[column] = pd.to_datetime(Borrowing[column], format='mixed', dayfirst=True)
    return Borrowing

Borrowing = date_conversion(Borrowing, serial_date_columns)



# Apply following looooooooooooooooooooooooop only if yield columns are NON-DECIMALS
yield_columns = ['ProfitRate']


for column in yield_columns:
    Min_Yield = np.floor(Borrowing[column]).min().astype(int)
    Borrowing[column] = Borrowing[column].apply(lambda x: x / 100 if Min_Yield > 1 else x)

# ####################################################### Variables Decision End #############################################33


Borrowing['DealNo'] = Borrowing['DealNo'].str.replace(',', '').astype(int)
Borrowing['Principal'] = Borrowing['Principal'].str.replace(',', '')
Borrowing['Principal'] = Borrowing['Principal'].apply(lambda x: x.split('.')[0]).astype('int64')
Borrowing = Borrowing[Borrowing['SecType'].notna()]
Borrowing['SecType'] = Borrowing['SecType'].apply(lambda x: 'T-Bill' if x == 'MTB' else x )
Borrowing['Category'] = Borrowing['Action'].apply(lambda x: 'Borrowing' if x == 'Accept' else 'Lending' if x == 'Place' else x )
Borrowing = Borrowing[Borrowing['Category'] == 'Borrowing']
Borrowing['Present Value'] = Borrowing['Principal'] * Borrowing['FirstPrice'] / 100

# print(Borrowing_Period.is_month_end)


EOM = (Borrowing_Period + pd.DateOffset(months=1)).days_in_month + 1
EOM_2 = (Borrowing_Period + pd.DateOffset(months=1, days=1) - Borrowing_Period).days
Days_Period = EOM if Borrowing_Period.is_month_end else EOM_2

Maturity_Days = [1, 2, 8, 15]
Maturity_Days.append(Days_Period)

Maturity_Lookup = {'Maturity Days': Maturity_Days,
                   'Maturity': ['Up to 1 Day', 'Over 1 to 7 Days', 'Over 7 to 14 Days', 'Over 14 Days to 1 Month', 'Over 1 Month']}
Maturity_Lookup = pd.DataFrame(Maturity_Lookup)

Maturity_Lookup['Maturity_Date'] = Maturity_Lookup.apply(lambda row: Borrowing_Period + pd.DateOffset(days=1) * row['Maturity Days'], axis=1)
Maturity_Lookup['Period'] = Borrowing_Period
Maturity_Lookup['Period'] = pd.to_datetime(Maturity_Lookup['Period'].dt.date)
Maturity_Lookup['Sort_Maturity'] = Maturity_Lookup.index + 1
# print(Maturity_Lookup)


def period_dates(Borrowing_Period):
    dates_list = [Borrowing_Period + pd.DateOffset(days=1) * item for item in Maturity_Days]
    return pd.Series({'Maturity_Date': dates_list})

Borrowing['Period'] = Borrowing_Period
Borrowing['Maturity_Date'] = Borrowing.apply(lambda row: period_dates(row['Period']), axis=1)


def filter_nested_list(row):
    threshold = pd.Timestamp(row['MaturityDate'])
    Maturity_Date = [num for num in row['Maturity_Date'] if num <= threshold]
    Maturity_Date = Maturity_Date[-1]
    return Maturity_Date

# Apply the function to the DataFrame
Borrowing['Maturity_Date'] = Borrowing.apply(filter_nested_list, axis=1)

Borrowing = Borrowing.merge(Maturity_Lookup[['Maturity_Date', 'Maturity', 'Sort_Maturity']], on='Maturity_Date', how='left', suffixes=('', '_right')).drop('Maturity_Date', axis=1)
Borrowing['DTM'] = (Borrowing['MaturityDate'] - Borrowing['Period']).dt.days

# Tbills PRESENT VALUE Grouping
Sum_Grouped_Cols = ['Present Value']

Maturity_Grouped = Borrowing.groupby(['Sort_Maturity', 'Maturity'])[Sum_Grouped_Cols].sum().reset_index().sort_values('Sort_Maturity')
Maturity_Grouped[Sum_Grouped_Cols] = Maturity_Grouped[Sum_Grouped_Cols] / 1000000


# with pd.ExcelWriter('E:\PIBs and Tbills\Year 2024\Sep 2024\Borrowing.xlsx') as writer:
#     Borrowing.to_excel(writer, sheet_name='Borrowing', index=False)
#     Maturity_Lookup.to_excel(writer, sheet_name='Maturity_Lookup', index=False)
#     Maturity_Grouped.to_excel(writer, sheet_name='Maturity_Grouped', index=False)


Borrowing_csv = Borrowing.to_csv(f"E:\Power BI Data\Power BI Data CSV\Power BI Data - Borrowings\Borrowing {Month_Name}.csv", index=False)
Borrowing









import pandas as pd
import numpy as np
import dask.dataframe as dd
import pyarrow as pa
from datetime import datetime

HO_GL_file_name = HO_GL_file_path # Must enter correct path and file name
PIBs_file =  PIBs # Must enter correct path and file name & Month corresponding to HO GL
Tbills_file =  Tbills # Must enter correct path and file name & Month corresponding to HO GL
Borrowing_file = Borrowing # Must enter correct path and file name & Month corresponding to HO GL


Account_Codes_Reloaded = Account_Codes_Reloaded_Path

HO_GL_Original_Date_Cols = ['GL Date', 'GL Year']
HO_GL_Original_Cols = ['GL Description', 'GL Code', 'Balance']

GL_Period = pd.read_excel(HO_GL_file_name).dropna(how='all', axis=1).dropna(how='all')
GL_Period.columns = [f'Unnamed_{i}' if 'Unnamed' in str(col) else col for i, col in enumerate(GL_Period.columns)]
GL_Period = GL_Period.loc[:,['Unnamed_14', 'Unnamed_16']]
GL_Period.columns = HO_GL_Original_Date_Cols
GL_Period = GL_Period.dropna()
GL_Period['GL Year'] = GL_Period['GL Year'].astype(int)
GL_year = GL_Period['GL Year'].iloc[0]
GL_Month = pd.to_datetime(GL_Period['GL Date'].iloc[0]).month
GL_Day = pd.to_datetime(GL_Period['GL Date'].iloc[0]).year
GL_Day = GL_Day % 100
GL_Period_Date = datetime(GL_year, GL_Month, GL_Day)


df = pd.read_excel(HO_GL_file_name).dropna(how='all', axis='columns').dropna(how='all')
df.columns = [f'Unnamed_{i}' if 'Unnamed' in str(col) else col for i, col in enumerate(df.columns)]
df = df[df['Unnamed_5'].notna()]
df = df.loc[:,['Unnamed_1', 'Unnamed_5', 'Unnamed_8']]

# Rename the columns
df.columns = HO_GL_Original_Cols
df['GL Code'] = df['GL Code'].apply(lambda x: x[-10:])
df['GL Code'] = df['GL Code'].astype('Int64')
df['Period'] = GL_Period_Date


SAP_GLs = pd.read_excel(Account_Codes_Reloaded, sheet_name='Invest_SAP_GLs', skiprows=3)
SAP_GLs = SAP_GLs.merge(df[['GL Code', 'Balance', 'Period']], left_on='SAP GL', right_on='GL Code',  how='left', suffixes=('', '_right')).drop('GL Code', axis=1)
SAP_GLs['Balance'] = SAP_GLs['Balance'].replace(np.nan, 0)
SAP_GLs['Period'] = SAP_GLs['Period'].ffill().dt.date

SAP_GLs_Diff = SAP_GLs[((SAP_GLs['Security Type'] == 'PIB') | (SAP_GLs['Security Type'] == 'T-Bill') | (SAP_GLs['Security Type'] == 'Borrowing')) & 
                       (~SAP_GLs['Nature'].str.contains('Income')) & (~SAP_GLs['Nature'].str.contains('Cost')) & (~SAP_GLs['Class'].str.contains('Call Money'))]
SAP_GLs_Diff = SAP_GLs_Diff.groupby(['Security Type', 'Class', 'Nature']).sum(['Balance']).drop(['SAP GL'], axis=1).reset_index()

# print(SAP_GLs_Diff)

PIBs_Columns = ['PRESENT_VALUE', 'SURPLUS_DEFICIT', 'CLASS_CODE', 'SECURITY_TYPE']

# PIBs = pd.read_csv(PIBs_file)
# Tbills = pd.read_csv(Tbills_file)
# Borrowing = pd.read_csv(Borrowing_file)
PIBs = PIBs
Tbills = Tbills
Borrowing = Borrowing

Invest_Data_Files = pd.concat([PIBs, Tbills])

Borrowing = Borrowing.groupby(['Category', 'SecType'])['Present Value'].sum().reset_index()
Borrowing['Present Value'] = Borrowing['Present Value'] * -1
Borrowing = Borrowing.rename(columns={'Category': 'SECURITY_TYPE', 'SecType': 'CLASS_CODE', 'Present Value':'PRESENT_VALUE'})
Invest_Data_Files = Invest_Data_Files.loc[:, PIBs_Columns]
Invest_Data_Files = Invest_Data_Files.groupby(['SECURITY_TYPE', 'CLASS_CODE']).sum(['PRESENT_VALUE', 'SURPLUS_DEFICIT']).reset_index()
Invest_Data_Files = pd.concat([Borrowing, Invest_Data_Files])

Invest_Data_Files = Invest_Data_Files.rename(columns={"PRESENT_VALUE": "Outstanding", "SURPLUS_DEFICIT": "Surplus / (Deficit)"})

# Unpivot the DataFrame
Invest_Data_Files = pd.melt(Invest_Data_Files, id_vars=['SECURITY_TYPE', 'CLASS_CODE'], value_vars=['Outstanding', 'Surplus / (Deficit)'], 
                    var_name='Nature', value_name='Data_Balance')
Invest_Data_Files = Invest_Data_Files[Invest_Data_Files['Data_Balance'] != 0]
SAP_GLs_Diff = SAP_GLs_Diff.merge(Invest_Data_Files, how='outer', left_on=['Security Type', 'Class', 'Nature'], 
                        right_on=['SECURITY_TYPE', 'CLASS_CODE', 'Nature']).drop(['SECURITY_TYPE', 'CLASS_CODE'], axis=1)
SAP_GLs_Diff = SAP_GLs_Diff[SAP_GLs_Diff['Security Type'].notna()]

SAP_GLs_Diff['Balance'] = SAP_GLs_Diff['Balance'].replace([np.nan, None, pd.NaT, pd.NA], 0)
SAP_GLs_Diff['Data_Balance'] = SAP_GLs_Diff['Data_Balance'].replace([np.nan, None, pd.NaT, pd.NA], 0)

SAP_GLs_Diff['Balance'] = SAP_GLs_Diff['Balance'].apply(lambda x: x / 1000000)
SAP_GLs_Diff['Data_Balance'] = SAP_GLs_Diff['Data_Balance'].apply(lambda x: x / 1000000)

SAP_GLs_Diff['Diff'] = SAP_GLs_Diff['Balance'] - SAP_GLs_Diff['Data_Balance']
SAP_GLs_Diff['Period'] = GL_Period_Date
SAP_GLs_Diff['Period'] = SAP_GLs_Diff['Period'].dt.date
print(SAP_GLs_Diff)
# print(Invest_Data_Files)


with pd.ExcelWriter(f'E:\Power BI Data\Power BI Data CSV\HO GL ABS\HO_SAP_GL {Month_Name}.xlsx') as writer:
    SAP_GLs_Diff.to_excel(writer, sheet_name='SAP GLs Diff', index=False)
    SAP_GLs.to_excel(writer, sheet_name='SAP GL', index=False)
    df.to_excel(writer, sheet_name='HO GL', index=False)
    
    

# HO_GL = df.to_csv("E:\PIBs and Tbills\Year 2024\Sep 2024\HO GL.csv", index=False)
