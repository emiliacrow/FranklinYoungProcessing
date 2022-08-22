# CreatedBy: Emilia Crow
# CreateDate: 20220627
# Updated: 20220627
# CreateFor: Franklin Young International

import pandas
import datetime
import numpy as np

from Tools.BasicProcess import BasicProcessObject


class FeaturedProductObject(BasicProcessObject):
    req_fields = ['FyProductNumber', 'ProductSortOrder']

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
        self.df_fy_description_lookup = self.obDal.get_fy_product_descriptions()
        self.df_product = self.df_product.merge(self.df_fy_description_lookup,how='left',on=['FyProductNumber'])
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
        if 'ProductDescriptionId' in row:
            return True
        self.obReporter.update_report('Fail','Verify this product has been ingested.')
        return False


    def identify_current_featured_products(self):
        self.df_featured_products = self.obDal.get_fy_featured_products()
        match_headers = ['ProductSortOrder']
        self.df_product = self.df_product.merge(self.df_featured_products, how='left', on=match_headers)
        self.df_product.sort_values(by=['ProductSortOrder'], inplace=True)


    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            if self.filter_check_in(row) == False:
                return False, df_collect_product_base_data

        success, return_df_line_product = self.process_changes(df_collect_product_base_data)

        return success, return_df_line_product


    def process_changes(self, df_line_product):
        for colName, row in df_line_product.iterrows():
            new_product_description_id = row['ProductDescriptionId']
            new_fy_product_number = row['FyProductNumber']

            try:
                old_product_description_id = row['old_ProductDescriptionId']
            except TypeError:
                old_product_description_id = -1
            except KeyError:
                old_product_description_id = -1

            try:
                old_fy_product_number = row['old_FyProductNumber']
            except TypeError:
                old_fy_product_number = ''
            except KeyError:
                old_fy_product_number = ''

            product_sort_order = row['ProductSortOrder']

            if old_product_description_id in self.lst_compeleted_products:
                old_product_description_id  = -1

            if 0 < int(product_sort_order) < 26:
                self.obIngester.set_featured_product(old_product_description_id, new_product_description_id, product_sort_order)
                self.lst_compeleted_products.append(new_product_description_id)
            else:
                self.obReporter.update_report('Fail','ProductSortOrder was out of range')
                return False, df_line_product

        return True, df_line_product


    def trigger_ingest_cleanup(self):
        self.obIngester.set_featured_product_cleanup()



## end ##