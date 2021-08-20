# CreatedBy: Emilia Crow
# CreateDate: 20210602
# Updated: 20210805
# CreateFor: Franklin Young International

import pandas
import datetime

from Tools.BasicProcess import BasicProcessObject


class BasePrice(BasicProcessObject):
    req_fields = ['FyProductNumber','Vendor List Price', 'VendorName', 'Discount', 'Fy Cost', 'Fixed Shipping Cost',
                  'LandedCostMarkupPercent_FYSell']
    sup_fields = []
    att_fields = []
    gen_fields = []

    def __init__(self,df_product,is_testing):
        super().__init__(df_product,is_testing)
        self.name = 'Base Product Price'
        self.lindas_increase = 0.25

    def batch_preprocessing(self):
        # define new, update, non-update
        self.batch_process_vendor()
        self.define_new()

        if 'MfcDiscountPercent' not in self.df_product.columns:
            self.df_product['MfcDiscountPercent'] = 0
        if 'MfcPrice' not in self.df_product.columns:
            self.df_product['MfcPrice'] = 0


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
        self.df_base_price_lookup = self.obDal.get_base_product_price_lookup(lst_ids[0])

        self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                 how='left', on=['VendorName'])


    def define_new(self):
        match_headers = ['FyProductNumber','ProductPriceId','Vendor List Price', 'Discount', 'Fy Cost', 'Fixed Shipping Cost', 'LandedCostMarkupPercent_FYSell']

        # simple first
        self.df_base_price_lookup['Filter'] = 'Update'
        self.df_base_price_check_in = self.df_base_price_lookup[['FyProductNumber','ProductPriceId','Filter']]

        if 'Filter' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns = 'Filter')
        if 'ProductPriceId' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns = 'ProductPriceId')

        # match all products on FyProdNum
        self.df_update_products = pandas.DataFrame.merge(self.df_product, self.df_base_price_check_in,
                                                 how='left', on='FyProductNumber')
        # all products that matched on FyProdNum
        self.df_update_products.loc[(self.df_update_products['Filter'] != 'Update'), 'Filter'] = 'Fail'

        self.df_product = self.df_update_products[(self.df_update_products['Filter'] != 'Update')]
        self.df_update_products = self.df_update_products[(self.df_update_products['Filter'] == 'Update')]

        if len(self.df_update_products.index) != 0:
            # this could end up empty
            self.df_base_price_lookup['Filter'] = 'Pass'
            self.df_update_products = self.df_update_products.drop(columns='Filter')
            self.df_update_products = pandas.DataFrame.merge(self.df_update_products, self.df_base_price_lookup,
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
                elif row['Filter'] != 'Update':
                    return False, df_collect_product_base_data
            else:
                return False, df_collect_product_base_data

            success, df_collect_product_base_data = self.process_pricing(df_collect_product_base_data, row)
            if success == False:
                df_collect_product_base_data['FinalReport'] = ['Failed to identify product price id']
                return success, df_collect_product_base_data

        success, df_line_product = self.base_price(df_collect_product_base_data)
        return success, df_line_product


    def process_pricing(self, df_collect_product_base_data, row):
        vendor_list_price = float(row['Vendor List Price'])
        fy_discount_percent = float(row['Discount'])
        fy_cost = round(float(row['Fy Cost']),2)
        df_collect_product_base_data['Fy Cost'] = [fy_cost]

        # discount and cost
        fy_cost_test = vendor_list_price - round((vendor_list_price * fy_discount_percent), 2)
        check_val = abs(fy_cost_test - fy_cost)
        # we trust the cost provided over the discount given
        if check_val > 0.01:
            fy_discount_percent = (1 - (fy_cost / vendor_list_price)) * 100
            df_collect_product_base_data['Discount'] = fy_discount_percent
        else:
            fy_discount_percent = row['Discount']

        if 'Fixed Shipping Cost' not in row:
            estimated_freight = 0
            df_collect_product_base_data['Fixed Shipping Cost'] = estimated_freight
        else:
            estimated_freight = float(row['Fixed Shipping Cost'])

        fy_landed_cost = round(fy_cost + estimated_freight, 2)
        df_collect_product_base_data['FyLandedPrice'] = [fy_landed_cost]

        mark_up_sell = float(row['LandedCostMarkupPercent_FYSell'])
        fy_sell_price = round(fy_landed_cost * mark_up_sell, 2)
        df_collect_product_base_data['Sell Price'] = [fy_sell_price]

        if 'LandedCostMarkupPercent_FYList' not in row:
            mark_up_list = mark_up_sell + self.lindas_increase
            df_collect_product_base_data['LandedCostMarkupPercent_FYList'] = [mark_up_list]
        else:
            mark_up_list = float(row['LandedCostMarkupPercent_FYList'])

        df_collect_product_base_data['List Price'] = [round(fy_landed_cost * mark_up_list, 2)]

        return True, df_collect_product_base_data


    def base_price(self, df_line_product):
        va_product_price_id = -1
        gsa_product_price_id = -1
        htme_product_price_id = -1
        ecat_product_price_id = -1
        fedmall_product_price_id = -1
        is_visible = 1

        success = True
        df_collect_product_base_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            date_catalog_received = datetime.datetime.now()
            fy_list_price_variance = 0
            mfc_discount_percent = 0
            mfc_price = 0

            if 'IsVisible' in row:
                is_visible = row['IsVisible']

            if 'VAProductPriceId' in row:
                va_product_price_id = row['VAProductPriceId']

            if 'GSAProductPriceId' in row:
                gsa_product_price_id = row['GSAProductPriceId']

            if 'HTMEProductPriceId' in row:
                htme_product_price_id = row['HTMEProductPriceId']

            if 'ECATProductPriceId' in row:
                ecat_product_price_id = row['ECATProductPriceId']

            if 'FEDMALLProductPriceId' in row:
                fedmall_product_price_id = row['FEDMALLProductPriceId']

            vendor_list_price = row['Vendor List Price']
            fy_discount_percent = row['Discount']
            fy_cost = row['Fy Cost']
            estimated_freight = row['Fixed Shipping Cost']
            fy_landed_cost = row['Landed Cost']
            markup_percent_fy_sell = row['LandedCostMarkupPercent_FYSell']
            fy_sell_price = row['Sell Price']
            markup_percent_fy_list = row['LandedCostMarkupPercent_FYList']
            fy_list_price = row['Retail Price']

            if 'List Price Variance' in row:
                fy_list_price_variance = float(row['List Price Variance'])

            if 'Date Catalog Received' in row:
                date_catalog_received = row['Date Catalog Received']

            if 'MfcDiscountPercent' in row:
                mfc_discount_percent = float(row['MfcDiscountPercent'])

            if 'MFC Price' in row:
                mfc_price = float(row['MFC Price'])

            product_price_id = row['ProductPriceId']


        self.obIngester.ingest_base_price(self.is_last, vendor_list_price, fy_discount_percent, fy_cost,
                                                          estimated_freight, fy_landed_cost,
                                                          markup_percent_fy_sell, fy_sell_price,
                                                          markup_percent_fy_list, fy_list_price,
                                                          fy_list_price_variance, is_visible, date_catalog_received,
                                                          mfc_discount_percent, mfc_price,
                                                          product_price_id, va_product_price_id, gsa_product_price_id,
                                                          htme_product_price_id, ecat_product_price_id, fedmall_product_price_id)

        return success, df_line_product






## end ##