# CreatedBy: Emilia Crow
# CreateDate: 20210609
# Updated: 20210609
# CreateFor: Franklin Young International

import pandas
import datetime
import xlrd

from Tools.BasicProcess import BasicProcessObject



class GSAPrice(BasicProcessObject):
    req_fields = ['VendorName','FyProductNumber','FyPartNumber','IsVisible', 'DateCatalogReceived', 'GSAApprovedListPrice',
                  'GSAApprovedPercent', 'MfcDiscountPercent', 'GSAContractModificationNumber', 'GSA_Sin','GSAApprovedPriceDate']

    sup_fields = []
    att_fields = []
    gen_fields = []
    def __init__(self,df_product, is_testing):
        super().__init__(df_product, is_testing)
        self.name = 'GSA Price Ingestion'

    def batch_preprocessing(self):
        # define new, update, non-update
        if 'VendorId' not in self.df_product.columns:
            self.batch_process_vendor()
        self.define_new()

    def batch_process_vendor(self):
        # there should only be one vendor, really.
        df_attribute = self.df_product[['VendorName']]
        df_attribute = df_attribute.drop_duplicates(subset=['VendorName'])
        lst_ids = []
        if 'VendorId' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns = 'VendorId')

        for colName, row in df_attribute.iterrows():
            vendor_name = row['VendorName'].upper()
            if vendor_name in self.df_vendor_translator['VendorCode'].values:
                new_vendor_id = self.df_vendor_translator.loc[
                    (self.df_vendor_translator['VendorCode'] == vendor_name),'VendorId'].values[0]
            elif vendor_name in self.df_vendor_translator['VendorName'].values:
                new_vendor_id = self.df_vendor_translator.loc[
                    (self.df_vendor_translator['VendorName'] == vendor_name),'VendorId'].values[0]
            else:
                new_vendor_id = -1

            lst_ids.append(new_vendor_id)

        df_attribute['VendorId'] = lst_ids

        self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                 how='left', on=['VendorName'])

    def define_new(self):
        self.df_base_price_lookup = self.obDal.get_base_product_price_lookup()
        self.df_gsa_price_lookup = self.obDal.get_gsa_price_lookup()

        match_headers = ['FyProductNumber','FyPartNumber','IsVisible', 'DateCatalogReceived', 'GSAApprovedListPrice',
                         'GSAApprovedPercent', 'MfcDiscountPercent', 'GSAContractModificationNumber','GSAApprovedPriceDate']

        # simple first
        self.df_base_price_lookup['Filter'] = 'Update'

        if 'Filter' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns='Filter')
        if 'ProductPriceId' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns='ProductPriceId')
        if 'BaseProductPriceId' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns='BaseProductPriceId')

        # match all products on FyProdNum
        self.df_update_products = pandas.DataFrame.merge(self.df_product, self.df_base_price_lookup,
                                                         how='left', on=['FyProductNumber','FyPartNumber'])
        # all products that matched on FyProdNum
        self.df_update_products.loc[(self.df_update_products['Filter'] != 'Update'), 'Filter'] = 'Fail'

        self.df_product = self.df_update_products[(self.df_update_products['Filter'] != 'Update')]
        self.df_update_products = self.df_update_products[(self.df_update_products['Filter'] == 'Update')]

        if len(self.df_update_products.index) != 0:
            # this step is going to require a test against the pricing in the contract price table
            # this could end up empty
            self.df_gsa_price_lookup['Filter'] = 'Pass'
            self.df_update_products = self.df_update_products.drop(columns='Filter')
            self.df_update_products = pandas.DataFrame.merge(self.df_update_products, self.df_gsa_price_lookup,
                                                             how='left', on=match_headers)

            # this does not seem to be matching correctly in the above
            # I suspect this has to do with the numbers being strings?
            self.df_update_products.loc[(self.df_update_products['Filter'] != 'Pass'), 'Filter'] = 'Update'

            self.df_product = self.df_product.append(self.df_update_products)

            # this shouldn't always be 0

    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            if 'Filter' in row:
                if row['Filter'] == 'Pass':
                    return True, df_collect_product_base_data
            else:
                return False, df_collect_product_base_data

            success, df_collect_product_base_data = self.process_pricing(df_collect_product_base_data)
            if success == False:
                df_collect_product_base_data['FinalReport'] = ['Failed in process contract']
                return success, df_collect_product_base_data

        success, return_df_line_product = self.gsa_product_price(df_collect_product_base_data)

        return success, return_df_line_product

    def process_pricing(self, df_line_product):
        success = True
        return_df_line_product = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            if 'GSABasePrice' not in row:
                approved_list_price = float(row['GSAApprovedListPrice'])
                approved_percent = float(row['GSAApprovedPercent'])
                gsa_base_price = round(approved_list_price-(approved_list_price*approved_percent),2)
                return_df_line_product['GSABasePrice'] = gsa_base_price

            if 'GSASellPrice' not in row:
                iff_fee_percent = 0.0075
                return_df_line_product['GSASellPrice'] = round(gsa_base_price+(gsa_base_price*iff_fee_percent))

            if 'MfcPrice' not in row:
                mfc_precent = float(row['MfcDiscountPercent'])
                return_df_line_product['MfcPrice'] = round(approved_list_price-(approved_list_price*mfc_precent),2)

        return success, return_df_line_product


    def gsa_product_price(self, df_line_product):
        success = True
        return_df_line_product = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            base_product_price_id = row['BaseProductPriceId']
            fy_product_number = row['FyProductNumber']
            is_visible = row['IsVisible']
            date_catalog_received = int(row['DateCatalogReceived'])
            date_catalog_received = (xlrd.xldate_as_datetime(date_catalog_received, 0)).date()

            contract_number = 'GS-07F-0636W'
            contract_mod_number = row['GSAContractModificationNumber']
            is_pricing_approved = row['GSAPricingApproved']
            approved_price_date = int(row['GSAApprovedPriceDate'])
            approved_price_date = (xlrd.xldate_as_datetime(approved_price_date, 0)).date()

            approved_list_price = row['GSAApprovedListPrice']
            approved_percent = row['GSAApprovedPercent']

            gsa_base_price = row['GSABasePrice']
            gsa_sell_price = row['GSASellPrice']

            mfc_precent = row['MfcDiscountPercent']
            mfc_price = row['MFCPrice']

            sin = row['GSA_Sin']


        self.obIngester.gsa_product_price_cap(self.is_last, base_product_price_id, fy_product_number, is_visible, date_catalog_received, contract_number, contract_mod_number,
                                                             is_pricing_approved, approved_price_date, approved_list_price, approved_percent,
                                                             gsa_base_price, gsa_sell_price, mfc_precent, mfc_price,sin)


        return success, return_df_line_product


## end ##