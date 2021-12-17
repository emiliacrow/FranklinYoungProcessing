# CreatedBy: Emilia Crow
# CreateDate: 20210602
# Updated: 20210805
# CreateFor: Franklin Young International

import pandas
import datetime
import xlrd

from Tools.BasicProcess import BasicProcessObject


class BasePrice(BasicProcessObject):
    req_fields = ['FyProductNumber', 'FyCost']
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
        if 'VendorName' not in self.df_product.columns:
            vendor_name = self.vendor_name_selection()
            self.df_product['VendorName'] = vendor_name

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
        match_headers = ['FyProductNumber','ProductPriceId']

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

    def process_vendor_price(self, df_collect_product_base_data, row):
        vendor_list_price = -1
        if 'VendorListPrice' in row:
            try:
                vendor_list_price = round(float(row['VendorListPrice']), 2)
            except ValueError:
                self.obReporter.update_report('Fail', 'Bad vendor list price.')
                return False, df_collect_product_base_data, vendor_list_price

        elif 'db_VendorListPrice' in row:
            try:
                vendor_list_price = round(float(row['VendorListPrice']), 2)
                df_collect_product_base_data['VendorListPrice'] = row['db_VendorListPrice']
            except ValueError:
                self.obReporter.update_report('Fail', 'Bad DB vendor list price.')
                return False, df_collect_product_base_data, vendor_list_price
        else:
            vendor_list_price = 0
            df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]
            self.obReporter.update_report('Alert', 'Vendor list price set to 0.')

        return True, df_collect_product_base_data, vendor_list_price


    def process_vendor_discount_price(self, df_collect_product_base_data, row):
        fy_discount_percent = -1
        if 'Discount' in row:
            try:
                fy_discount_percent = round(float(row['Discount']), 2)

            except ValueError:
                self.obReporter.update_report('Fail', 'Bad discount.')
                return False, df_collect_product_base_data, fy_discount_percent

        elif 'db_Discount' in row:
            try:
                fy_discount_percent = round(float(row['db_Discount']), 2)
                df_collect_product_base_data['Discount'] = [fy_discount_percent]

            except ValueError:
                self.obReporter.update_report('Fail', 'Bad DB discount.')
                return False, df_collect_product_base_data,fy_discount_percent
        else:
            fy_discount_percent = 0
            df_collect_product_base_data['Discount'] = [fy_discount_percent]
            self.obReporter.update_report('Alert', 'Discount to 0.')

        return True, df_collect_product_base_data, fy_discount_percent


    def set_vendor_list(self, df_collect_product_base_data, fy_cost, fy_discount_percent):
        vendor_list_price = round(fy_cost / (1 - fy_discount_percent), 2)
        df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]
        self.obReporter.update_report('Alert', 'VendorListPrice was calculated.')
        return df_collect_product_base_data

    def set_vendor_discount(self, df_collect_product_base_data, fy_cost, vendor_list_price):
        fy_discount_percent = round(1 - (fy_cost / vendor_list_price), 2)
        df_collect_product_base_data['Discount'] = fy_discount_percent
        self.obReporter.update_report('Alert', 'Discount was calculated.')
        return df_collect_product_base_data


    def process_shipping_cost(self, df_collect_product_base_data, row, fy_cost):
        fy_landed_cost = -1
        if 'Landed Cost' in row:
            try:
                fy_landed_cost = round(float(row['Landed Cost']), 2)
            except ValueError:
                self.obReporter.update_report('Fail', 'Landed cost must be a number.')
                return False, df_collect_product_base_data, fy_landed_cost


        if 'Fixed Shipping Cost' in row:
            try:
                estimated_freight = round(float(row['Fixed Shipping Cost']), 2)

            except ValueError:
                self.obReporter.update_report('Fail', 'Bad Fixed Shipping Cost.')
                return False, df_collect_product_base_data, fy_landed_cost

        elif 'db_shipping_cost' in row:
            try:
                estimated_freight = round(float(row['db_shipping_cost']), 2)
                df_collect_product_base_data['Fixed Shipping Cost'] = [estimated_freight]
            except ValueError:
                self.obReporter.update_report('Fail', 'Bad db_shipping_cost.')
                return False, df_collect_product_base_data, fy_landed_cost

        else:
            estimated_freight = 0
            df_collect_product_base_data['Fixed Shipping Cost'] = [estimated_freight]
            self.obReporter.update_report('Alert', 'Fixed Shipping Cost set to 0.')

        if 'Landed Cost' not in row:
            fy_landed_cost = round(fy_cost + estimated_freight, 2)
            df_collect_product_base_data['Landed Cost'] = [fy_landed_cost]
            self.obReporter.update_report('Alert', 'Landed Cost was calculated.')

        return True, df_collect_product_base_data, fy_landed_cost

    def process_markup_sell(self, df_collect_product_base_data, row):
        markup_sell = -1
        if 'LandedCostMarkupPercent_FYSell' in row:
            try:
                markup_sell = round(float(row['LandedCostMarkupPercent_FYSell']), 2)
            except ValueError:
                self.obReporter.update_report('Fail', 'Bad markup sell value.')
                return False, df_collect_product_base_data, markup_sell
        elif 'db_MarkUp_sell' in row:
            try:
                markup_sell = round(float(row['db_MarkUp_sell']), 2)
            except ValueError:
                self.obReporter.update_report('Fail', 'Bad markup sell value.')
                return False, df_collect_product_base_data, markup_sell

        return True, df_collect_product_base_data, markup_sell


    def process_markup_list(self, df_collect_product_base_data, row, markup_sell):
        markup_list = -1
        if 'LandedCostMarkupPercent_FYList' in row:
            try:
                markup_list = round(float(row['LandedCostMarkupPercent_FYList']), 2)
            except ValueError:
                self.obReporter.update_report('Fail', 'Bad markup list value.')
                return False, df_collect_product_base_data, markup_list

        elif 'db_MarkUp_list' in row:
            try:
                markup_list = round(float(row['db_MarkUp_list']), 2)
                df_collect_product_base_data['LandedCostMarkupPercent_FYList'] = [markup_list]
            except ValueError:
                self.obReporter.update_report('Fail', 'Bad markup list value.')
                return False, df_collect_product_base_data, markup_list
        elif markup_sell > 0:
            markup_list = markup_sell + self.lindas_increase
            df_collect_product_base_data['LandedCostMarkupPercent_FYList'] = [markup_list]

        return True, df_collect_product_base_data, markup_list


    def set_pricing_rons_way(self, df_collect_product_base_data, row, fy_landed_cost, markup_sell, markup_list):
        fy_sell_price = round(fy_landed_cost * markup_sell, 2)
        df_collect_product_base_data['Sell Price'] = [fy_sell_price]

        fy_list_price = round(fy_landed_cost * markup_list, 2)
        df_collect_product_base_data['Retail Price'] = [fy_list_price]

        df_collect_product_base_data['ECommerceDiscount'] = [round(1 - float(fy_sell_price / fy_list_price), 2)]

        return df_collect_product_base_data

    def process_ecom_discount(self, df_collect_product_base_data, row):
        ecommerce_discount = -1
        if 'ECommerceDiscount' in row:
            try:
                ecommerce_discount = round(float(row['ECommerceDiscount']), 2)
            except ValueError:
                self.obReporter.update_report('Fail', 'Bad ECommerceDiscount value.')
                return False, df_collect_product_base_data, ecommerce_discount

        elif 'db_ECommerceDiscount' in row:
            try:
                ecommerce_discount = round(float(row['db_ECommerceDiscount']), 2)
                df_collect_product_base_data['ECommerceDiscount'] = [ecommerce_discount]
                self.obReporter.update_report('Alert', 'db_ECommerceDiscount was used in place of ECommerceDiscount.')
            except ValueError:
                self.obReporter.update_report('Fail', 'Bad MfcDiscountPercent value.')
                return False, df_collect_product_base_data, ecommerce_discount

        elif 'MfcDiscountPercent' in row:
            try:
                ecommerce_discount = round(float(row['MfcDiscountPercent']), 2)
                df_collect_product_base_data['ECommerceDiscount'] = [ecommerce_discount]
                self.obReporter.update_report('Alert', 'MfcDiscountPercent was used in place of ECommerceDiscount.')
            except ValueError:
                self.obReporter.update_report('Fail', 'Bad MfcDiscountPercent value.')
                return False, df_collect_product_base_data, ecommerce_discount

        return True, df_collect_product_base_data, ecommerce_discount


    def process_discount_off_list(self, df_collect_product_base_data, row, fy_landed_cost, markup_list):
        fy_list_price = round(fy_landed_cost * markup_list, 2)
        df_collect_product_base_data['Retail Price'] = [fy_list_price]

        success, df_collect_product_base_data, ecommerce_discount  = self.process_ecom_discount(df_collect_product_base_data, row)
        if success:
            fy_sell_price = round(fy_list_price-(fy_list_price*ecommerce_discount),2)
            df_collect_product_base_data['Sell Price'] = [fy_sell_price]
            df_collect_product_base_data['LandedCostMarkupPercent_FYSell'] = [round(float(fy_sell_price/fy_landed_cost), 2)]
            return True, df_collect_product_base_data
        else:
            self.obReporter.update_report('Fail', 'No ECommerce value to take.')
            return success, df_collect_product_base_data

    def process_from_list_price(self, df_collect_product_base_data, row, fy_landed_cost, markup_sell):
        if 'Retail Price' in row:
            try:
                fy_list_price = round(float(row['Retail Price']), 2)
                markup_list = round(float(fy_list_price/fy_landed_cost), 2)
                df_collect_product_base_data['LandedCostMarkupPercent_FYList'] = [markup_list]

            except ValueError:
                self.obReporter.update_report('Fail', 'Bad retail price.')
                return False, df_collect_product_base_data

            success, df_collect_product_base_data, ecommerce_discount  = self.process_ecom_discount(df_collect_product_base_data, row)
            if success:
                fy_sell_price = round(fy_list_price-(fy_list_price*ecommerce_discount),2)
                df_collect_product_base_data['Sell Price'] = [fy_sell_price]
                df_collect_product_base_data['LandedCostMarkupPercent_FYSell'] = [round(float(fy_sell_price/fy_landed_cost), 2)]
            else:
                self.obReporter.update_report('Fail', 'No ECommerce value to take.')
                return False, df_collect_product_base_data

            return True, df_collect_product_base_data
        else:
            fy_sell_price = round(fy_landed_cost * markup_sell, 2)
            df_collect_product_base_data['Sell Price'] = [fy_sell_price]
            markup_list = markup_sell + self.lindas_increase
            df_collect_product_base_data['LandedCostMarkupPercent_FYList'] = [markup_list]

            fy_list_price = round(fy_landed_cost * markup_list, 2)
            df_collect_product_base_data['Retail Price'] = [fy_list_price]
            df_collect_product_base_data['ECommerceDiscount'] = [round(1-float(fy_sell_price/fy_list_price), 2)]

            return True, df_collect_product_base_data

    def process_pricing(self, df_collect_product_base_data, row):
        # check if we can math the value
        try:
            fy_cost = round(float(row['FyCost']),2)
            df_collect_product_base_data['FyCost'] = [fy_cost]
        except ValueError:
            self.obReporter.update_report('Fail', 'Bad FyCost value.')
            return False, df_collect_product_base_data

        # this is where the calculation for vendor price, and discount
        success, df_collect_product_base_data, vendor_list_price = self.process_vendor_price(df_collect_product_base_data, row)
        if success == False:
            return success, df_collect_product_base_data

        success, df_collect_product_base_data, fy_discount_percent = self.process_vendor_discount_price(df_collect_product_base_data, row)
        if success == False:
            return success, df_collect_product_base_data

        # generate values where possible
        # non-failable
        if fy_discount_percent > 0 and vendor_list_price == 0:
            df_collect_product_base_data = self.set_vendor_list(df_collect_product_base_data, fy_cost, fy_discount_percent)

        if fy_discount_percent == 0 and vendor_list_price > 0:
            df_collect_product_base_data = self.set_vendor_discount(df_collect_product_base_data, fy_cost, vendor_list_price)

        # estimated freight and landed cost
        success, df_collect_product_base_data, fy_landed_cost = self.process_shipping_cost(df_collect_product_base_data, row, fy_cost)
        if success == False:
            return success, df_collect_product_base_data

        # these handle markups if they exist
        success, df_collect_product_base_data, markup_sell = self.process_markup_sell(df_collect_product_base_data, row)
        if success == False:
            return success, df_collect_product_base_data

        success, df_collect_product_base_data, markup_list = self.process_markup_list(df_collect_product_base_data, row, markup_sell)
        if success == False:
            return success, df_collect_product_base_data


        # we usually sell this price, and present List as retail price
        # landed cost + each of these
        if markup_sell > 0 and markup_list > 0:
            df_collect_product_base_data = self.set_pricing_rons_way(df_collect_product_base_data, row, fy_landed_cost, markup_sell, markup_list)
            return True, df_collect_product_base_data

        elif markup_list > 0:
            success, df_collect_product_base_data = self.process_discount_off_list(df_collect_product_base_data, row, fy_landed_cost, markup_list)
            return success, df_collect_product_base_data

        elif markup_sell > 0:
            success, df_collect_product_base_data = self.process_from_list_price(df_collect_product_base_data, row, fy_landed_cost, markup_sell)
            return success, df_collect_product_base_data

        else:
            # at this point we should know that:
            # the data provided was not enough to generate all the values required
            # there is not enough legacy data in the db to use as fall back
            # therefore we can't generate all the values
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

            if 'LandedCostMarkupPercent_FYSell' not in row:
                df_collect_product_base_data['LandedCostMarkupPercent_FYSell'] = [0]
                markup_percent_fy_sell = 0
            else:
                markup_percent_fy_sell = row['LandedCostMarkupPercent_FYSell']

            if 'Sell Price' not in row:
                df_collect_product_base_data['Sell Price'] = [0]
                fy_sell_price = 0
            else:
                fy_sell_price = row['Sell Price']

            if 'LandedCostMarkupPercent_FYSell' not in row:
                df_collect_product_base_data['LandedCostMarkupPercent_FYSell'] = [0]
                markup_percent_fy_list = 0
            else:
                markup_percent_fy_list = row['LandedCostMarkupPercent_FYList']

            if 'Retail Price' not in row:
                df_collect_product_base_data['Retail Price'] = [0]
                fy_list_price = 0
            else:
                fy_list_price = row['Retail Price']

            if 'ECommerceDiscount' not in row:
                df_collect_product_base_data['ECommerceDiscount'] = [0]
                ecommerce_discount = 0
            else:
                ecommerce_discount = row['ECommerceDiscount']

            if 'DateCatalogReceived' in row:
                try:
                    date_catalog_received = int(row['DateCatalogReceived'])
                    date_catalog_received = (xlrd.xldate_as_datetime(date_catalog_received, 0)).date()
                except ValueError:
                    date_catalog_received = str(row['DateCatalogReceived'])

            if 'CatalogProvidedBy' in row:
                catalog_provided_by = str(row['CatalogProvidedBy'])
            else:
                catalog_provided_by = ''

            product_price_id = row['ProductPriceId']

        self.obIngester.ingest_base_price(self.is_last, vendor_list_price, fy_discount_percent, fy_cost,
                                                          estimated_freight, fy_landed_cost,
                                                          markup_percent_fy_sell, fy_sell_price,
                                                          markup_percent_fy_list, fy_list_price, ecommerce_discount,
                                                          is_visible, date_catalog_received, catalog_provided_by,
                                                          product_price_id, va_product_price_id, gsa_product_price_id,
                                                          htme_product_price_id, ecat_product_price_id, fedmall_product_price_id)

        return success, df_line_product

    def trigger_ingest_cleanup(self):
        self.obIngester.ingest_base_price_cleanup()



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