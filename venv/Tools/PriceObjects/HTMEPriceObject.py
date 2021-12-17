# CreatedBy: Emilia Crow
# CreateDate: 20210609
# Updated: 20210609
# CreateFor: Franklin Young International

import pandas
import xlrd

from Tools.BasicProcess import BasicProcessObject


class HTMEPrice(BasicProcessObject):
    req_fields = ['FyProductNumber','FyPartNumber','OnContract', 'HTMEApprovedListPrice',
                  'HTMEApprovedPercent', 'MfcDiscountPercent', 'HTMEContractModificationNumber', 'HTMEApprovedPriceDate','HTMEPricingApproved']

    sup_fields = []
    att_fields = []
    gen_fields = []
    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'HTME Price Ingestion'

    def batch_preprocessing(self):
        self.remove_private_headers()
        # define new, update, non-update
        if 'VendorId' not in self.df_product.columns:
            self.batch_process_vendor()
        self.define_new()

    def batch_process_vendor(self):
        # there should only be one vendor, really.
        if 'VendorName' not in self.df_product.columns:
            vendor_name = self.vendor_name_selection()
            self.df_product['VendorName'] = vendor_name

        df_attribute = self.df_product[['VendorName']]
        df_attribute = df_attribute.drop_duplicates(subset=['VendorName'])
        lst_ids = []

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
        self.df_htme_price_lookup = self.obDal.get_htme_price_lookup()

        match_headers = ['FyProductNumber','FyPartNumber','OnContract', 'HTMEApprovedListPrice',
                         'HTMEApprovedPercent', 'MfcDiscountPercent', 'HTMEContractModificationNumber','HTMEApprovedPriceDate','HTMEPricingApproved']

        # simple first
        self.df_base_price_lookup['Filter'] = 'Update'

        if 'Filter' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns='Filter')
        if 'ProductPriceId' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns='ProductPriceId')
        if 'BaseProductPriceId' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns='BaseProductPriceId')

        # match all products on FyProdNum
        self.df_update_products = self.df_product.merge(self.df_base_price_lookup,
                                                         how='left', on=['FyProductNumber','FyPartNumber'])
        # all products that matched on FyProdNum
        self.df_update_products.loc[(self.df_update_products['Filter'] != 'Update'), 'Filter'] = 'Fail'

        self.df_product = self.df_update_products.loc[(self.df_update_products['Filter'] != 'Update')]
        self.df_update_products = self.df_update_products.loc[(self.df_update_products['Filter'] == 'Update')]

        if len(self.df_update_products.index) != 0:
            # this step is going to require a test against the pricing in the contract price table
            # this could end up empty
            self.df_htme_price_lookup['Filter'] = 'Pass'
            self.df_update_products = self.df_update_products.drop(columns='Filter')
            self.df_update_products = pandas.DataFrame.merge(self.df_update_products, self.df_htme_price_lookup,
                                                             how='left', on=match_headers)

            # this does not seem to be matching correctly in the above
            # I suspect this has to do with the numbers being strings?
            self.df_update_products.loc[(self.df_update_products['Filter'] != 'Pass'), 'Filter'] = 'Update'

            self.df_product = self.df_product.append(self.df_update_products)

            # this shouldn't always be 0


    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'BaseProductPriceId','BaseProductPriceId_y','BaseProductPriceId_x',
                           'HTMEProductPriceId','HTMEProductPriceId_x','HTMEProductPriceId_y',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryIdId','CategoryIdId_x','CategoryId_y',
                           'Report','Filter'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
        self.df_product = self.df_product.drop(columns=remove_headers)


    def filter_check_in(self, row):
        if 'Filter' in row:
            if row['Filter'] == 'Update':
                self.obReporter.update_report('Pass','This product price is an update')
                return True
            else:
                self.obReporter.update_report('Alert','This product must be ingested in product')
                return False
        else:
            self.obReporter.update_report('Fail','This product price failed filtering')
            return False

    def trigger_ingest_cleanup(self):
        self.obIngester.ingest_htme_product_price_cleanup()

    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            if self.filter_check_in(row) == False:
                return False, df_collect_product_base_data

            if 'Filter' in row:
                if row['Filter'] == 'Pass':
                    self.obReporter.update_report('Fail','This product needs to be ingested')
                    return True, df_collect_product_base_data
            else:
                self.obReporter.update_report('Fail','This product needs to be ingested')
                return False, df_collect_product_base_data

            df_collect_product_base_data = self.process_oncontract(df_collect_product_base_data, row)
            success, df_collect_product_base_data = self.process_pricing(df_collect_product_base_data)
            if success == False:
                self.obReporter.update_report('Fail','Failed in process contract')
                return success, df_collect_product_base_data

        success, return_df_line_product = self.htme_product_price(df_collect_product_base_data)

        return success, return_df_line_product

    def process_oncontract(self, df_collect_product_base_data, row):
        if ('OnContract' not in row):
            df_collect_product_base_data['OnContract'] = [1]
            self.obReporter.update_report('Alert', 'OnContract was assigned')
        elif str(row['OnContract']) == 'N':
            df_collect_product_base_data['OnContract'] = [0]
        elif str(row['OnContract']) == 'Y':
            df_collect_product_base_data['OnContract'] = [1]

        return df_collect_product_base_data

    def process_pricing(self, df_line_product):
        success = True
        return_df_line_product = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            if 'HTMEBasePrice' not in row:
                approved_list_price = float(row['HTMEApprovedListPrice'])
                approved_percent = float(row['HTMEApprovedPercent'])
                htme_base_price = round(approved_list_price-(approved_list_price*approved_percent),2)
                return_df_line_product['HTMEBasePrice'] = htme_base_price
            else:
                htme_base_price = float(row['HTMEBasePrice'])
                return_df_line_product['HTMEBasePrice'] = htme_base_price

            if 'HTMESellPrice' not in row:
                return_df_line_product['HTMESellPrice'] = htme_base_price

            if 'MfcPrice' not in row:
                mfc_precent = float(row['MfcDiscountPercent'])
                return_df_line_product['MfcPrice'] = round(approved_list_price-(approved_list_price*mfc_precent),2)

        return success, return_df_line_product

    def htme_product_price(self, df_line_product):
        success = True
        return_df_line_product = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            base_product_price_id = row['BaseProductPriceId']
            fy_product_number = row['FyProductNumber']
            on_contract = row['OnContract']

            contract_number = 'SPE2DE-21-D-0014'
            contract_mod_number = row['HTMEContractModificationNumber']

            is_pricing_approved = row['HTMEPricingApproved']
            try:
                approved_price_date = int(row['HTMEApprovedPriceDate'])
                approved_price_date = (xlrd.xldate_as_datetime(approved_price_date, 0)).date()
            except ValueError:
                approved_price_date = str(row['HTMEApprovedPriceDate'])

            htme_base_price = row['HTMEBasePrice']
            htme_sell_price = row['HTMESellPrice']

            approved_list_price = row['HTMEApprovedListPrice']
            approved_percent = row['HTMEApprovedPercent']

            mfc_precent = row['MfcDiscountPercent']
            mfc_price = row['MfcPrice']


        self.obIngester.htme_product_price_cap(self.is_last, base_product_price_id, fy_product_number, on_contract, approved_list_price, contract_number, contract_mod_number,
                                                             is_pricing_approved, approved_price_date, approved_percent,
                                                             htme_base_price, htme_sell_price, mfc_precent, mfc_price)
        return success, return_df_line_product



## end ##