# CreatedBy: Emilia Crow
# CreateDate: 20220627
# Updated: 20220627
# CreateFor: Franklin Young International

import pandas
import datetime
import numpy as np

from Tools.BasicProcess import BasicProcessObject


class FeaturedProductObject(BasicProcessObject):
    req_fields = ['FyCatalogNumber','ManufacturerName', 'ManufacturerPartNumber','FyProductNumber','VendorName','VendorPartNumber', 'IsFeaturedProduct', 'ProductSortOrder']

    sup_fields = []
    att_fields = []
    gen_fields = []
    def __init__(self,df_product, user, password, is_testing, full_run=False):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'Featured Product'
        self.full_run = full_run
        self.lst_compeleted_products = []


    def batch_preprocessing(self):
        self.remove_private_headers()
        self.define_new()
        self.identify_current_featured_products()


    def remove_private_headers(self):
        private_headers = {'ProductId', 'ProductId_y', 'ProductId_x',
                           'ProductPriceId_y', 'ProductPriceId_x',
                           'VendorId', 'VendorId_x', 'VendorId_y',
                           'CategoryId', 'CategoryId_x', 'CategoryId_y',
                           'Report', 'Filter'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
        if remove_headers != []:
            self.df_product = self.df_product.drop(columns=remove_headers)


    def filter_check_in(self, row):
        if row['Filter'] == 'New':
            self.obReporter.update_report('Alert', 'Passed filtering as a new product but not processed')
            return False

        elif row['Filter'] in ['Partial', 'Base Pricing']:
            self.obReporter.update_report('Alert', 'Passed filtering as partial product')
            return False

        elif row['Filter'] in ['Ready', 'Update']:
            self.obReporter.update_report('Alert', 'Passed filtering as updatable')
            return True

        elif row['Filter'] == 'Possible Duplicate':
            self.obReporter.update_report('Alert', 'Review product numbers for possible duplicates')
            return False

        else:
            self.obReporter.update_report('Fail', 'Failed filtering')
            return False


    def identify_current_featured_products(self):
        self.df_featured_products = self.obDal.get_featured_products()
        match_headers = ['ProductSortOrder']
        self.df_product = self.df_product.merge(self.df_featured_products, how='outer', on=match_headers)
        self.df_product.sort_values(by=['ProductSortOrder'], inplace=True)


    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            print(row)
            if self.filter_check_in(row) == False:
                return False, df_collect_product_base_data

        success, return_df_line_product = self.process_changes(df_collect_product_base_data)

        return success, return_df_line_product


    def process_changes(self, df_line_product):

        for colName, row in df_line_product.iterrows():
            new_product_price_id = row['ProductPriceId']
            new_fy_product_number = row['FyProductNumber']

            try:
                old_product_price_id = row['old_ProductPriceId']
            except TypeError:
                old_product_price_id = -1
            except KeyError:
                old_product_price_id = -1

            try:
                old_fy_product_number = row['old_FyProductNumber']
            except TypeError:
                old_fy_product_number = ''
            except KeyError:
                old_fy_product_number = ''

            product_sort_order = row['ProductSortOrder']

            if old_product_price_id in self.lst_compeleted_products:
                old_product_price_id  = -1

            self.obIngester.set_featured_product(old_product_price_id, new_product_price_id, product_sort_order)

            self.lst_compeleted_products.append(new_product_price_id)


        return True, df_line_product


    def trigger_ingest_cleanup(self):
        self.obIngester.set_featured_product_cleanup()



## end ##