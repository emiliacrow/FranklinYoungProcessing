# CreatedBy: Emilia Crow
# CreateDate: 20210602
# Updated: 20220318
# CreateFor: Franklin Young International

import pandas
import datetime
import xlrd

from Tools.BasicProcess import BasicProcessObject


class BasePrice(BasicProcessObject):
    req_fields = ['FyCatalogNumber', 'ManufacturerName', 'ManufacturerPartNumber','FyProductNumber','VendorName','VendorPartNumber', 'FyCost', 'DateCatalogReceived']
    sup_fields = []
    att_fields = []
    gen_fields = []

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'Base Product Price'
        self.lindas_increase = 0.25
        self.fallback_margin = 0.95

    def batch_preprocessing(self):
        self.remove_private_headers()
        self.define_new()
        self.collect_markups()


    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'BaseProductPriceId','BaseProductPriceId_y','BaseProductPriceId_x',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryId','CategoryId_x','CategoryId_y',
                           'Report','Filter','db_IsDiscontinued'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
        if remove_headers != []:
            self.df_product = self.df_product.drop(columns=remove_headers)


    def collect_markups(self):
        # here we will collect the markups and provide if possible.
        self.df_markup_lookup = self.obDal.get_base_product_price_lookup()

        product_headers = set(self.df_product.columns)
        markup_headers = set(self.df_markup_lookup.columns)
        match_headers = list(product_headers.intersection(markup_headers))

        self.df_product = self.df_product.merge(self.df_markup_lookup,how='left',on=match_headers)


    def filter_check_in(self, row):
        filter_options = ['Base Pricing', 'New', 'Partial', 'Possible Duplicate', 'Ready', 'case_1','case_4']

        if row['Filter'] == 'New':
            self.obReporter.update_report('Alert', 'Passed filtering as a new product but not processed')
            return False

        elif row['Filter'] == 'Partial':
            self.obReporter.update_report('Alert', 'Passed filtering as partial product')
            return False

        elif row['Filter'] in ['Ready', 'Base Pricing']:
            self.obReporter.update_report('Alert', 'Passed filtering as updatable')
            return True

        elif row['Filter'] == 'Possible Duplicate':
            self.obReporter.update_report('Alert', 'Review product numbers for possible duplicates')
            return False

        else:
            self.obReporter.update_report('Fail', 'Failed filtering')
            return False



    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()

        for colName, row in df_line_product.iterrows():
            if self.filter_check_in(row) == False:
                return False, df_collect_product_base_data

            success, return_val = self.process_boolean(row, 'IsVisible')
            if success:
                df_collect_product_base_data['IsVisible'] = [return_val]
            else:
                self.obReporter.update_report('Alert', '{0} was set to 0'.format('IsVisible'))
                df_collect_product_base_data['IsVisible'] = [0]

            success, df_collect_product_base_data = self.process_pricing(df_collect_product_base_data, row)
            if success == False:
                self.obReporter.update_report('Fail','Failed to identify product price id')
                return success, df_collect_product_base_data

            success, is_discontinued = self.process_boolean(row, 'IsDiscontinued')
            if success:
                df_collect_product_base_data['IsDiscontinued'] = [is_discontinued]

            success, allow_purchases = self.process_boolean(row, 'AllowPurchases')
            if success:
                df_collect_product_base_data['AllowPurchases'] = [allow_purchases]

        success, df_line_product = self.base_price(df_collect_product_base_data)
        return success, df_line_product


    def set_vendor_list(self, fy_cost, fy_discount_percent):
        if fy_discount_percent == 0:
            vendor_list_price = round(fy_cost, 2)
        else:
            vendor_list_price = round(fy_cost / (1 - fy_discount_percent), 2)

        self.obReporter.update_report('Alert', 'Vendor List Price was calculated')
        return vendor_list_price


    def set_vendor_discount(self, fy_cost, vendor_list_price):
        if vendor_list_price == fy_cost:
            fy_discount_percent = 0
        else:
            fy_discount_percent = round(1 - (fy_cost / vendor_list_price), 2)

        self.obReporter.update_report('Alert', 'Discount was calculated')
        return fy_discount_percent


    def set_fallback_margin(self, fy_landed_cost, fy_list_price):
        list_margin = fy_list_price - fy_landed_cost
        sell_margin = list_margin * self.fallback_margin
        fy_sell_price = fy_landed_cost + sell_margin

        self.obReporter.update_report('Alert', 'Sell Price was calculated with fallback margin')
        return fy_sell_price

    def set_markup_sell(self, fy_landed_cost, fy_sell_price):
        try:
            markup_sell = round(float(fy_sell_price / fy_landed_cost), 2)
        except ZeroDivisionError:
            markup_sell = 0
        return markup_sell

    def set_pricing_rons_way(self, df_collect_product_base_data, row, fy_landed_cost, markup_sell, markup_list):
        fy_sell_price = round(fy_landed_cost * markup_sell, 2)
        df_collect_product_base_data['Sell Price'] = [fy_sell_price]

        fy_list_price = round(fy_landed_cost * markup_list, 2)
        df_collect_product_base_data['Retail Price'] = [fy_list_price]

        # TODO check that this is working right
        try:
            df_collect_product_base_data['ECommerceDiscount'] = [round(1 - float(fy_sell_price / fy_list_price), 2)]
        except ZeroDivisionError:
            df_collect_product_base_data['ECommerceDiscount'] = [0]

        return df_collect_product_base_data


    def process_ecom_discount(self, df_collect_product_base_data, row):
        ecommerce_discount = -1
        if 'ECommerceDiscount' in row:
            try:
                ecommerce_discount = round(float(row['ECommerceDiscount']), 2)
            except ValueError:
                self.obReporter.update_report('Fail', 'Bad ECommerceDiscount value')
                return False, df_collect_product_base_data, ecommerce_discount
        elif 'MfcDiscountPercent' in row:
            try:
                ecommerce_discount = round(float(row['MfcDiscountPercent']), 2)
            except ValueError:
                self.obReporter.update_report('Fail', 'Bad MfcDiscountPercent value')
                return False, df_collect_product_base_data, ecommerce_discount

        if ecommerce_discount < 0:
            return False, df_collect_product_base_data, ecommerce_discount
        else:
            return True, df_collect_product_base_data, ecommerce_discount


    def process_pricing(self, df_collect_product_base_data, row):
        # Check FyCost is valid, this is a proper fail
        # this prepares FyCost, we can't load if this fails
        success, fy_cost = self.row_check(row,'FyCost')
        if success == False:
            # fail line if missing
            self.obReporter.update_report('Fail', 'FyCost was missing')
            return success, df_collect_product_base_data

        success, fy_cost = self.float_check(fy_cost,'FyCost')
        if success == False or fy_cost == 0:
            # fail if it's negative
            # we could allow these to get through in some way
            self.obReporter.update_report('Fail', 'Please check that FyCost is a positive number')
            return False, df_collect_product_base_data


        # try to get vendor list price from the file
        vlp_success, vendor_list_price = self.row_check(row,'VendorListPrice')
        if vlp_success:
            vlp_success, vendor_list_price = self.float_check(vendor_list_price,'VendorListPrice')
            df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]

        # try to get the discount from the file
        discount_success, fy_discount_percent = self.row_check(row,'Discount')
        if discount_success:
            discount_success, fy_discount_percent = self.float_check(fy_discount_percent,'Discount')
            df_collect_product_base_data['Discount'] = [fy_discount_percent]

        # if neither one worked, we try to get the discount from the db
        if not vlp_success and not discount_success:
            discount_success, fy_discount_percent = self.row_check(row,'db_Discount')
            if discount_success:
                discount_success, fy_discount_percent = self.float_check(fy_discount_percent,'db_Discount')
                if discount_success:
                    self.obReporter.update_report('Alert', 'Discount pulled from database')
                    df_collect_product_base_data['Discount'] = [fy_discount_percent]

            if not discount_success:
                vlp_success, vendor_list_price = self.row_check(row,'db_VendorListPrice')
                if vlp_success:
                    vlp_success, vendor_list_price = self.float_check(vendor_list_price,'db_VendorListPrice')
                    if vlp_success:
                        self.obReporter.update_report('Alert', 'VendorListPrice pulled from database')
                        df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]

                if not vlp_success:
                    self.obReporter.update_report('Alert', 'VendorListPrice, Discount values were set to 0')
                    df_collect_product_base_data['VendorListPrice'] = [0]
                    df_collect_product_base_data['Discount'] = [0]


        if vlp_success and not discount_success:
            fy_discount_percent = self.set_vendor_discount(fy_cost, vendor_list_price)
            df_collect_product_base_data['Discount'] = [fy_discount_percent]

        if not vlp_success and discount_success:
            vendor_list_price = self.set_vendor_list(fy_cost, fy_discount_percent)
            df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]

        # checks for shipping costs
        success, estimated_freight = self.row_check(row, 'Estimated Freight')
        if success:
            success, estimated_freight = self.float_check(estimated_freight,'Estimated Freight')
            df_collect_product_base_data['Estimated Freight'] = [estimated_freight]

        if not success:
            success, estimated_freight = self.row_check(row, 'db_shipping_cost')
            if success:
                success, estimated_freight = self.float_check(estimated_freight,'db_shipping_cost')
                df_collect_product_base_data['Estimated Freight'] = [estimated_freight]

        if not success:
            estimated_freight = 0
            df_collect_product_base_data['Estimated Freight'] = [estimated_freight]
            self.obReporter.update_report('Alert', 'Estimated Freight value was set to 0')

        success, fy_landed_cost = self.row_check(row,'Landed Cost')
        if success:
            success, fy_landed_cost = self.float_check(fy_landed_cost, 'Landed Cost')
            df_collect_product_base_data['Landed Cost'] = [fy_landed_cost]

        if not success:
            fy_landed_cost = fy_cost + estimated_freight
            self.obReporter.update_report('Alert', 'Landed Cost was calculated')
            df_collect_product_base_data['Landed Cost'] = [fy_landed_cost]


        # we get the values from the DB because that's what we rely on
        db_mus_success, db_markup_sell = self.row_check(row, 'db_MarkUp_sell')
        if db_mus_success:
            db_mus_success, db_markup_sell = self.float_check(db_markup_sell, 'db_MarkUp_sell')
            if db_markup_sell <= 0:
                db_mus_success = False

        db_mul_success, db_markup_list = self.row_check(row, 'db_MarkUp_list')
        if db_mul_success:
            db_mul_success, db_markup_list = self.float_check(db_markup_list, 'db_MarkUp_list')
            if db_markup_list <= 0:
                db_mul_success = False

        # get the markups from the file
        mus_success, markup_sell = self.row_check(row, 'LandedCostMarkupPercent_FYSell')
        if mus_success:
            mus_success, markup_sell = self.float_check(markup_sell, 'LandedCostMarkupPercent_FYSell')
            if markup_sell <= 0:
                mus_success = False
            else:
                df_collect_product_base_data['LandedCostMarkupPercent_FYSell'] = [markup_sell]

        mul_success, markup_list = self.row_check(row, 'LandedCostMarkupPercent_FYList')
        if mul_success:
            mul_success, markup_list = self.float_check(markup_list, 'LandedCostMarkupPercent_FYList')
            if markup_list <= 0:
                mul_success = False
            else:
                df_collect_product_base_data['LandedCostMarkupPercent_FYList'] = [markup_list]

        # let's report if they're both missing
        if (not db_mus_success and not mus_success) and (not db_mul_success and not mul_success):
            self.obReporter.update_report('Fail', 'No markups not present')
            return False, df_collect_product_base_data
        elif (db_mus_success and not mus_success) and (db_mul_success and not mul_success):
            self.obReporter.update_report('Alert', 'DB markups were used')
            df_collect_product_base_data['LandedCostMarkupPercent_FYSell'] = [db_markup_sell]
            df_collect_product_base_data['LandedCostMarkupPercent_FYList'] = [db_markup_list]

        elif (db_mus_success and mus_success) and (db_mul_success and mul_success):
            if (markup_list != db_markup_list) or (markup_sell != db_markup_sell):
                self.obReporter.update_report('Alert', 'DB markups will be over-written')

        elif (not db_mus_success and mus_success) and (not db_mul_success and mul_success):
            self.obReporter.update_report('Alert', 'File markups were used')

        df_collect_product_base_data = self.set_pricing_rons_way(df_collect_product_base_data, row, fy_landed_cost, markup_sell, markup_list)
        return True, df_collect_product_base_data


    def base_price(self, df_line_product):
        va_product_price_id = -1
        gsa_product_price_id = -1
        htme_product_price_id = -1
        ecat_product_price_id = -1
        fedmall_product_price_id = -1

        success = True
        df_collect_product_base_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            success, is_visible = self.process_boolean(row, 'IsVisible')
            if success:
                df_collect_product_base_data['IsVisible'] = [is_visible]
            else:
                self.obReporter.update_report('Alert', '{0} was set to 1'.format('IsVisible'))
                is_visible = 1
                df_collect_product_base_data['IsVisible'] = [is_visible]

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
            estimated_freight = row['Estimated Freight']
            fy_landed_cost = row['Landed Cost']

            markup_percent_fy_sell = row['LandedCostMarkupPercent_FYSell']

            if 'Sell Price' not in row:
                df_collect_product_base_data['Sell Price'] = [0]
                fy_sell_price = 0
            else:
                fy_sell_price = row['Sell Price']

            markup_percent_fy_list = row['LandedCostMarkupPercent_FYList']

            if 'Retail Price' not in row:
                df_collect_product_base_data['Retail Price'] = [0]
                fy_list_price = 0
            else:
                fy_list_price = float(row['Retail Price'])

            if 'ECommerceDiscount' not in row:
                df_collect_product_base_data['ECommerceDiscount'] = [0]
                ecommerce_discount = 0
            else:
                ecommerce_discount = row['ECommerceDiscount']

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


        if fy_landed_cost >= fy_sell_price and fy_sell_price != 0:
            self.obReporter.update_report('Fail','Margin was zero')
            return False, df_line_product

        if fy_sell_price > fy_list_price and fy_list_price != 0:
            self.obReporter.update_report('Fail','Sell price too high')
            return False, df_line_product


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







## end ##