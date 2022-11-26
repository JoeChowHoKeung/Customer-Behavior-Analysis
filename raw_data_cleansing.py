import pandas as pd
import os
from statistics import mean

data_file_dir = r"C:\Users\joech\OneDrive\workspace\HKTV " \
                r"OpenDataBank\hktv_transaction_data"
keep_column_file = r"C:\Users\joech\OneDrive\workspace\HKTV " \
                   r"OpenDataBank\RFM_project\official_draft\keep_column.csv"
keep_columns = pd.read_csv(keep_column_file, index_col = 0)
keep_columns = keep_columns[keep_columns.keep].index.tolist()

save_dir = r"C:\Users\joech\OneDrive\workspace\HKTV " \
           r"OpenDataBank\RFM_project\official_draft\dataset"


# %%
def data_format (df) :
    length_col = ["height", "length", "width"]
    weight_col = ["weight"]
    price_col = ["order_value", "total_price", "total_discounts"]
    subcat_list = ['primary_category_name_en', "sub_cat_1_name_en",
                   "sub_cat_2_name_en",
                   "sub_cat_3_name_en",
                   "sub_cat_4_name_en"]

    def price_formatting (price) :
        if type(price) == float :
            return price
        price = price.replace(" ", "")
        if "-" in price :
            price_bin = [float(val) for val in price.split("-")]
            return mean(price_bin)

    def length_measurement_formatting (length) :
        if type(length) == float :
            return length
        if length[-2 :] == "mm" :
            return float(length[:-2])

    def weight_measurement_formatting (weight) :
        if type(weight) == float :
            return weight
        measurement = {
            "kg" : 1000,
            "g" : 1,
            "ml" : 1
        }
        for key, value in measurement.items() :
            if key not in weight :
                continue
            return float(weight[:-len(key)]) * value

    def subcat_filling (row) :
        for subcat in subcat_list :
            val = row[subcat]
            if str(val) != "nan" :
                continue
            if subcat == 'primary_category_name_en' :
                row[subcat] = "unknown"
            previous_subcat_index = subcat_list.index(subcat) - 1
            subcat_val = row[subcat_list[previous_subcat_index]]
            row[subcat] = subcat_val
        return row

    format_dic = (
        (length_col, length_measurement_formatting),
        (price_col, price_formatting),
        (weight_col, weight_measurement_formatting)
    )

    for format_set in format_dic :
        columns, func = format_set
        df.loc[:, columns] = df[columns].applymap(func)

    df.loc[:, subcat_list] = df[subcat_list].apply(subcat_filling, axis = 1)

    return df


# %%
def access_raw_data (file) :
    file = os.path.join(data_file_dir, file)
    chuck = pd.read_csv(
        filepath_or_buffer = file,
        chunksize = 100000,
        low_memory = False,
        compression = 'gzip'
    )

    def data_clean (df) :
        df.dropna(subset = "area",
                  inplace = True)  # we would only observe the customer who
        # contains complete transaction information
        #df = data_format(df)
        return df[["order_number", "store_id"]]

    print("start cleansing : ", file)
    return pd.concat(
        list(map(data_clean, chuck))
    )


# %%
def main () :
    """
    this python file is going to decompress the raw data file from the directory [./datafile/ __.csv.gzip]
    :dependencies: the keep columns file is the datafile describing which column would be kept for data analysis and data cleansing
    :return: the new csv file called Q1_raw.csv, the directory would be [./datafile/__.csv.gzip]
    """
    q1_df = pd.concat(
        list(map(
            access_raw_data,
            filter(lambda x : x.split("-")[1] in ["01", "02", "03"],
                   os.listdir(data_file_dir))
        ))
    )
    q1_df.to_csv(os.path.join(save_dir, "Q1_raw.csv"), index = False)


main()

#%%
