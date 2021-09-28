# CreatedBy: Emilia Crow
# CreateDate: 20210602
# Updated: 20210805
# CreateFor: Franklin Young International

import pandas
import datetime
import xlrd

from Tools.BasicProcess import BasicProcessObject


class BasePrice(BasicProcessObject):
    req_fields = ['FyProductNumber', 'VendorName', 'FyCost']
    sup_fields = []
    att_fields = []
    gen_fields = []

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'Base Product Price'
        self.lindas_increase = 0.25

    def batch_preprocessing(self):
        # define new, update, non-update
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
        self.df_base_price_lookup = self.obDal.get_base_product_price_lookup_by_vendor_id(lst_ids[0])

        self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                 how='left', on=['VendorName'])


    def define_new(self):
        match_headers = ['FyProductNumber','ProductPriceId','FyCost']

        # simple first
        self.df_base_price_lookup['Filter'] = 'Update'
        self.df_base_price_check_in = self.df_base_price_lookup[['FyProductNumber','ProductPriceId','Filter']]

        if 'Filter' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns = 'Filter')
        if 'ProductPriceId' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns = 'ProductPriceId')

        # match all products on FyProdNum
        self.df_update_products = self.df_product.merge(self.df_base_price_check_in,
                                                 how='left', on='FyProductNumber')
        # all products that matched on FyProdNum
        self.df_update_products.loc[(self.df_update_products['Filter'] != 'Update'), 'Filter'] = 'Fail'

        self.df_product = self.df_update_products[(self.df_update_products['Filter'] != 'Update')]
        self.df_update_products = self.df_update_products[(self.df_update_products['Filter'] == 'Update')]

        if len(self.df_update_products.index) != 0:
            # this could end up empty
            self.df_base_price_lookup['Filter'] = 'Update'
            self.df_update_products = self.df_update_products.drop(columns='Filter')
            self.df_update_products = pandas.DataFrame.merge(self.df_update_products, self.df_base_price_lookup,
                                                     how='left', on=match_headers)

            # this does not seem to be matching correctly in the above
            # I suspect this has to do with the numbers being strings?
            self.df_update_products.loc[(self.df_update_products['Filter'] != 'Update'), 'Filter'] = 'New'

            self.df_product = self.df_product.append(self.df_update_products)

        # place new at bottom, I don't know if this is needed yet, but it might be?
        self.df_product = self.df_product.sort_values(by=['Filter'], ascending=True)



    def filter_check_in(self, row):
        if 'Filter' in row:
            if row['Filter'] == 'Fail':
                self.obReporter.update_report('Fail', 'This product must be ingested in product price')
                return False
        else:
            self.obReporter.update_report('Fail', 'This product must be ingested in product price')
            return False


    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()

        for colName, row in df_line_product.iterrows():
            if self.filter_check_in(row) == False:
                return False, df_collect_product_base_data

            df_collect_product_base_data = self.process_visibility(df_collect_product_base_data, row)

            success, df_collect_product_base_data = self.process_pricing(df_collect_product_base_data, row)
            if success == False:
                self.obReporter.update_report('Fail','Failed to identify product price id')
                return success, df_collect_product_base_data

        success, df_line_product = self.base_price(df_collect_product_base_data)
        return success, df_line_product


    def process_pricing(self, df_collect_product_base_data, row):
        fy_cost = round(float(row['FyCost']),2)
        df_collect_product_base_data['FyCost'] = [fy_cost]

        if 'VendorListPrice' in row and 'Discount' in row:
            vendor_list_price = round(float(row['VendorListPrice']), 2)
            fy_discount_percent = round(float(row['Discount']), 2)
            if fy_discount_percent != 0:
                # discount and cost
                fy_cost_test = vendor_list_price - round((vendor_list_price * fy_discount_percent), 2)
                check_val = abs(fy_cost_test - fy_cost)
                # we trust the cost provided over the discount given
                if check_val > 0.01:
                    fy_discount_percent = round((1 - (fy_cost / vendor_list_price)), 2)
                    df_collect_product_base_data['Discount'] = [fy_discount_percent]

        elif 'Discount' in row:
            fy_discount_percent = round(float(row['Discount']), 2)
            if fy_discount_percent != 0:
                vendor_list_price = round(fy_cost/(1-fy_discount_percent), 2)
                df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]
            else:
                df_collect_product_base_data['VendorListPrice'] = [0]

        elif 'VendorListPrice' in row:
            vendor_list_price = round(float(row['VendorListPrice']), 2)
            if vendor_list_price != 0:
                fy_discount_percent = round(1 - (fy_cost / vendor_list_price), 2)
                df_collect_product_base_data['Discount'] = fy_discount_percent
            else:
                df_collect_product_base_data['Discount'] = [0]
        else:
            df_collect_product_base_data['Discount'] = [0]
            df_collect_product_base_data['VendorListPrice'] = [0]

        if 'Fixed Shipping Cost' not in row:
            estimated_freight = 0
            df_collect_product_base_data['Fixed Shipping Cost'] = estimated_freight
        else:
            estimated_freight = round(float(row['Fixed Shipping Cost']), 2)

        fy_landed_cost = round(fy_cost + estimated_freight, 2)
        df_collect_product_base_data['Landed Cost'] = [fy_landed_cost]

        if 'LandedCostMarkupPercent_FYSell' in row and 'ECommerceDiscount' not in row and 'Retail Price' not in row:
            mark_up_sell = round(float(row['LandedCostMarkupPercent_FYSell']), 2)
            fy_sell_price = round(fy_landed_cost * mark_up_sell, 2)
            df_collect_product_base_data['Sell Price'] = [fy_sell_price]

            if 'LandedCostMarkupPercent_FYList' not in row:
                mark_up_list = mark_up_sell + self.lindas_increase
                df_collect_product_base_data['LandedCostMarkupPercent_FYList'] = [mark_up_list]
            else:
                mark_up_list = round(float(row['LandedCostMarkupPercent_FYList']), 2)
            fy_list_price = round(fy_landed_cost * mark_up_list, 2)
            df_collect_product_base_data['Retail Price'] = [fy_list_price]

            df_collect_product_base_data['ECommerceDiscount'] = [round(1-float(fy_sell_price/fy_list_price), 2)]

        elif ('ECommerceDiscount' in row or 'MfcDiscountPercent' in row) and 'Retail Price' not in row and 'LandedCostMarkupPercent_FYList' in row:
            mark_up_list = round(float(row['LandedCostMarkupPercent_FYList']), 2)
            fy_list_price = round(fy_landed_cost * mark_up_list, 2)
            df_collect_product_base_data['Retail Price'] = fy_list_price

            if 'ECommerceDiscount' not in row:
                ecommerce_discount = round(float(row['MfcDiscountPercent']), 2)
                df_collect_product_base_data['ECommerceDiscount'] = [ecommerce_discount]
                self.obReporter.update_report('Alert', 'MfcDiscountPercent was used in place of ECommerceDiscount.')
            else:
                ecommerce_discount = round(float(row['ECommerceDiscount']), 2)

            fy_sell_price = round(fy_list_price-(fy_list_price*ecommerce_discount),2)
            df_collect_product_base_data['Sell Price'] = [fy_sell_price]
            df_collect_product_base_data['LandedCostMarkupPercent_FYSell'] = [round(float(fy_sell_price/fy_landed_cost), 2)]


        elif 'Retail Price' in row and ('ECommerceDiscount' in row or 'MfcDiscountPercent' in row):
            fy_list_price = round(float(row['Retail Price']), 2)
            mark_up_list = round(float(fy_list_price/fy_landed_cost), 2)

            if 'LandedCostMarkupPercent_FYList' not in row:
                df_collect_product_base_data['LandedCostMarkupPercent_FYList'] = [mark_up_list]

            if 'ECommerceDiscount' not in row:
                mfc_discount = round(float(row['MfcDiscountPercent']), 2)
                df_collect_product_base_data['ECommerceDiscount'] = [mfc_discount]
                self.obReporter.update_report('Alert', 'MfcDiscountPercent was used in place of ECommerceDiscount.')
            else:
                ecommerce_discount = round(float(row['ECommerceDiscount']), 2)

            fy_sell_price = round(fy_list_price-(fy_list_price*ecommerce_discount),2)
            df_collect_product_base_data['Sell Price'] = [fy_sell_price]
            df_collect_product_base_data['LandedCostMarkupPercent_FYSell'] = [round(float(fy_sell_price/fy_landed_cost), 2)]

        else:
            if 'LandedCostMarkupPercent_FYSell' not in row:
                df_collect_product_base_data['LandedCostMarkupPercent_FYSell'] = [0]

            if 'Sell Price' not in row:
                df_collect_product_base_data['Sell Price'] = [0]

            if 'LandedCostMarkupPercent_FYList' not in row:
                df_collect_product_base_data['LandedCostMarkupPercent_FYList'] = [0]

            if 'Retail Price' not in row:
                df_collect_product_base_data['Retail Price'] = [0]

            if 'ECommerceDiscount' not in row:
                df_collect_product_base_data['ECommerceDiscount'] = [0]

            if 'Retail Price' not in row:
                df_collect_product_base_data['Retail Price'] = [0]

            self.obReporter.update_report('Alert', 'Basic pricing was loaded.')


        return True, df_collect_product_base_data


    def process_visibility(self, df_collect_product_base_data, row):
        if ('IsVisible' not in row):
            df_collect_product_base_data['IsVisible'] = [0]
            self.obReporter.update_report('Alert','IsVisible was assigned')
        elif str(row['IsVisible']) == 'N':
            df_collect_product_base_data['IsVisible'] = [0]
        elif str(row['IsVisible']) == 'Y':
            df_collect_product_base_data['IsVisible'] = [1]

        return df_collect_product_base_data

    def base_price(self, df_line_product):
        va_product_price_id = -1
        gsa_product_price_id = -1
        htme_product_price_id = -1
        ecat_product_price_id = -1
        fedmall_product_price_id = -1

        success = True
        df_collect_product_base_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            date_catalog_received = datetime.datetime.now()
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

            vendor_list_price = row['VendorListPrice']
            fy_discount_percent = row['Discount']
            fy_cost = row['FyCost']
            estimated_freight = row['Fixed Shipping Cost']
            fy_landed_cost = row['Landed Cost']
            markup_percent_fy_sell = row['LandedCostMarkupPercent_FYSell']
            fy_sell_price = row['Sell Price']
            markup_percent_fy_list = row['LandedCostMarkupPercent_FYList']
            fy_list_price = row['Retail Price']
            ecommerce_discount = row['ECommerceDiscount']

            if 'DateCatalogReceived' in row:
                try:
                    date_catalog_received = int(row['DateCatalogReceived'])
                    date_catalog_received = (xlrd.xldate_as_datetime(date_catalog_received, 0)).date()
                except ValueError:
                    date_catalog_received = str(row['DateCatalogReceived'])

            product_price_id = row['ProductPriceId']

        self.obIngester.ingest_base_price(self.is_last, vendor_list_price, fy_discount_percent, fy_cost,
                                                          estimated_freight, fy_landed_cost,
                                                          markup_percent_fy_sell, fy_sell_price,
                                                          markup_percent_fy_list, fy_list_price, ecommerce_discount,
                                                          is_visible, date_catalog_received,
                                                          product_price_id, va_product_price_id, gsa_product_price_id,
                                                          htme_product_price_id, ecat_product_price_id, fedmall_product_price_id)

        return success, df_line_product




class UpdateBasePrice(BasePrice):
    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)

    def filter_check_in(self, row):
        if 'Filter' in row:
            if row['Filter'] == 'Fail':
                self.obReporter.update_report('Fail', 'This product must be ingested in product price')
                return False
        else:
            self.obReporter.update_report('Fail', 'This product must be ingested in product price')
            return False
        return True





## end ##