# CreatedBy: Emilia Crow
# CreateDate: 20210602
# Updated: 20220318
# CreateFor: Franklin Young International

import pandas
import datetime
import xlrd

from Tools.BasicProcess import BasicProcessObject


class BasePrice(BasicProcessObject):
    req_fields = ['FyCatalogNumber', 'ManufacturerName', 'ManufacturerPartNumber','FyProductNumber','VendorName',
                  'VendorPartNumber', 'FyCost', 'DateCatalogReceived']
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

        self.df_product.sort_values(by=['FyProductNumber'], inplace=True)


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
        self.df_markup_lookup = self.obDal.get_markup_lookup()

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

            success, return_val = self.process_boolean(row, 'FyIsVisible')
            if success:
                df_collect_product_base_data['FyIsVisible'] = [return_val]
            else:
                self.obReporter.update_report('Alert', '{0} was set to 0'.format('FyIsVisible'))
                df_collect_product_base_data['FyIsVisible'] = [1]

            success, df_collect_product_base_data = self.process_pricing(df_collect_product_base_data, row)
            if success == False:
                return success, df_collect_product_base_data

        success, df_line_product = self.base_price(df_collect_product_base_data)
        return success, df_line_product


    def set_vendor_discount(self, fy_cost, vendor_list_price):
        if vendor_list_price == fy_cost or vendor_list_price <= 0:
            fy_discount_percent = 0
        else:
            fy_discount_percent = round(1 - (fy_cost / vendor_list_price), 2)

        self.obReporter.update_report('Alert', 'Discount was calculated')
        return fy_discount_percent


    def set_vendor_list(self, fy_cost, fy_discount_percent):
        if fy_discount_percent == 0:
            vendor_list_price = round(fy_cost, 2)
        else:
            vendor_list_price = round(fy_cost / (1 - fy_discount_percent), 2)

        self.obReporter.update_report('Alert', 'Vendor List Price was calculated')
        return vendor_list_price


    def set_pricing_rons_way(self, df_collect_product_base_data, row, fy_landed_cost, markup_sell, markup_list):
        # do math
        fy_sell_price_long = fy_landed_cost * markup_sell

        #print('printing the long fy sell price val:', fy_sell_price_long)
        # initial rounding and formatting
        fy_sell_price = round(fy_sell_price_long, 4)
        str_fy_sell_price = "{:.4f}".format(fy_sell_price)

        # evaluate the last two digits and do second rounding
        final_digit = str_fy_sell_price[-2:]
        #print('printing the stringy fy sell price val:', str_fy_sell_price)
        if str_fy_sell_price[-2:] == '50':
            fy_sell_price = round(fy_sell_price+0.0001, 2)
        elif str_fy_sell_price[-1] == '5':
            fy_sell_price = round(fy_sell_price+0.00001, 2)
        else:
            fy_sell_price = round(fy_sell_price, 2)

        #print('printing the final fy sell price val:', fy_sell_price)
        df_collect_product_base_data['FySellPrice'] = [fy_sell_price]

        # do math
        fy_list_price_long = float(fy_landed_cost * markup_list)

        #print('printing the long fy list price val:', fy_list_price_long)
        # initial rounding and formatting
        fy_list_price = round(fy_list_price_long, 4)
        str_fy_list_price = "{:.4f}".format(fy_list_price)
        #print('printing the stringy fy list price val:', str_fy_list_price)

        # evaluate the last two digits and do second rounding
        final_digit = str_fy_list_price[-2:]
        if str_fy_list_price[-2:] == '50':
            #print('first cap')
            fy_list_price = round(fy_list_price+0.0001, 2)
        elif str_fy_list_price[-1] == '5':
            #print('second cap')
            fy_list_price = round(fy_list_price+0.00001, 2)
        else:
            #print('third cap')
            fy_list_price = round(fy_list_price, 2)

        #print('printing the final fy list price val:', fy_list_price)
        df_collect_product_base_data['FyListPrice'] = [fy_list_price]

        # TODO check that this is working right
        try:
            df_collect_product_base_data['ECommerceDiscount'] = [round(1 - float(fy_sell_price / fy_list_price), 2)]
        except ZeroDivisionError:
            df_collect_product_base_data['ECommerceDiscount'] = [0]

        return df_collect_product_base_data


    def set_fallback_margin(self, fy_landed_cost, fy_list_price):
        list_margin = fy_list_price - fy_landed_cost
        sell_margin = list_margin * self.fallback_margin
        fy_sell_price = fy_landed_cost + sell_margin

        self.obReporter.update_report('Alert', 'FySellPrice was calculated with fallback margin')
        return fy_sell_price


    def set_markup_sell(self, fy_landed_cost, fy_sell_price):
        try:
            markup_sell = round(float(fy_sell_price / fy_landed_cost), 2)
        except ZeroDivisionError:
            markup_sell = 0
        return markup_sell


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
        if not success:
            success, fy_cost = self.row_check(row, 'db_FyCost')
            if success:
                df_collect_product_base_data['FyCost'] = [fy_cost]
            else:
                # fail line if missing
                self.obReporter.update_report('Fail', 'FyCost was missing')
                return success, df_collect_product_base_data

        # this checks if the value is a positive float value
        success, fy_cost = self.float_check(fy_cost,'FyCost')
        if not success or fy_cost == 0:
            # fail if it's not a float or negative
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
        success, estimated_freight = self.row_check(row, 'EstimatedFreight')
        if success:
            success, estimated_freight = self.float_check(estimated_freight,'EstimatedFreight')
            df_collect_product_base_data['EstimatedFreight'] = [estimated_freight]

        if not success:
            success, estimated_freight = self.row_check(row, 'db_shipping_cost')
            if success:
                success, estimated_freight = self.float_check(estimated_freight,'db_shipping_cost')
                df_collect_product_base_data['EstimatedFreight'] = [estimated_freight]

        if not success:
            estimated_freight = 0
            df_collect_product_base_data['EstimatedFreight'] = [estimated_freight]
            self.obReporter.update_report('Alert', 'EstimatedFreight value was set to 0')

        success, fy_landed_cost = self.row_check(row,'FyLandedCost')
        if success:
            success, fy_landed_cost = self.float_check(fy_landed_cost, 'FyLandedCost')
            fy_landed_cost = round(fy_landed_cost, 2)
            df_collect_product_base_data['FyLandedCost'] = [fy_landed_cost]

        if not success:
            fy_landed_cost = round(fy_cost + estimated_freight, 2)
            self.obReporter.update_report('Alert', 'FyLandedCost was calculated')
            df_collect_product_base_data['FyLandedCost'] = [fy_landed_cost]


        # we get the values from the DB because that's what we rely on
        db_mus_success, db_markup_sell = self.row_check(row, 'db_MarkUp_sell')
        if db_mus_success:
            db_mus_success, db_markup_sell = self.float_check(db_markup_sell, 'db_MarkUp_sell')
            if db_markup_sell <= 0:
                db_mus_success = False
                self.obReporter.update_report('Alert','DB Markup Sell negative')
            elif db_markup_sell < 1:
                db_mus_success = False
                self.obReporter.update_report('Alert','DB Markup Sell too low')

        db_mul_success, db_markup_list = self.row_check(row, 'db_MarkUp_list')
        if db_mul_success:
            db_mul_success, db_markup_list = self.float_check(db_markup_list, 'db_MarkUp_list')
            if db_markup_list <= 0:
                db_mul_success = False
                self.obReporter.update_report('Alert','DB Markup List negative')
            elif db_markup_list <= 1:
                db_mul_success = False
                self.obReporter.update_report('Alert','DB Markup List too low')

        # get the markups from the file
        mus_success, markup_sell = self.row_check(row, 'FyLandedCostMarkupPercent_FySell')
        if mus_success:
            mus_success, markup_sell = self.float_check(markup_sell, 'FyLandedCostMarkupPercent_FySell')
            if markup_sell <= 0:
                mus_success = False
                self.obReporter.update_report('Alert','Markup Sell negative')
            elif markup_sell <= 1:
                mus_success = False
                self.obReporter.update_report('Alert','Markup Sell too low')
            else:
                df_collect_product_base_data['FyLandedCostMarkupPercent_FySell'] = [markup_sell]

        if not mus_success and db_mus_success:
            markup_sell = db_markup_sell
            df_collect_product_base_data['FyLandedCostMarkupPercent_FySell'] = [db_markup_sell]


        mul_success, markup_list = self.row_check(row, 'FyLandedCostMarkupPercent_FyList')
        if mul_success:
            mul_success, markup_list = self.float_check(markup_list, 'FyLandedCostMarkupPercent_FyList')
            if markup_list <= 0:
                mul_success = False
                self.obReporter.update_report('Alert','Markup List negative')
            elif markup_list <= 1:
                mul_success = False
                self.obReporter.update_report('Alert','Markup List too low')
            else:
                df_collect_product_base_data['FyLandedCostMarkupPercent_FyList'] = [markup_list]

        if not mul_success and db_mul_success:
            markup_list = db_markup_list
            df_collect_product_base_data['FyLandedCostMarkupPercent_FyList'] = [db_markup_list]

        # let's report if they're both missing
        if (not db_mus_success and not mus_success) and (not db_mul_success and not mul_success):
            self.obReporter.update_report('Fail', 'No markups not present')
            return False, df_collect_product_base_data

        elif (db_mus_success and not mus_success) and (db_mul_success and not mul_success):
            self.obReporter.update_report('Alert', 'DB markups were used')
            markup_sell = db_markup_sell
            markup_list = db_markup_list
            df_collect_product_base_data['FyLandedCostMarkupPercent_FySell'] = [markup_sell]
            df_collect_product_base_data['FyLandedCostMarkupPercent_FyList'] = [markup_list]

        elif (db_mus_success and mus_success) and (db_mul_success and mul_success):
            if (markup_list != db_markup_list) or (markup_sell != db_markup_sell):
                self.obReporter.update_report('Alert', 'DB markups will be over-written')

        elif (not db_mus_success and mus_success) and (not db_mul_success and mul_success):
            self.obReporter.update_report('Alert', 'File markups were used')

        if 'VendorProductNotes' in row and 'ProductPriceId' in row:
            vendor_product_notes = row['VendorProductNotes']
            if (vendor_product_notes != ''):
                product_price_id = int(row['ProductPriceId'])
                vendor_product_notes = vendor_product_notes.replace('NULL','')
                vendor_product_notes = vendor_product_notes.replace(';','')

                self.obIngester.set_product_notes(-1,'', product_price_id, vendor_product_notes)

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
            is_visible = row['FyIsVisible']

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
            estimated_freight = row['EstimatedFreight']
            fy_landed_cost = row['FyLandedCost']

            try:
                markup_percent_fy_sell = row['FyLandedCostMarkupPercent_FySell']
            except KeyError:
                for colName2, row2 in df_collect_product_base_data.iterrows():
                    print(row2)
                reports = self.obReporter.get_report()
                print('pass',reports[0])
                print('alert',reports[1])
                print('fail',reports[2])
                x = input('Markup failure is a mystery, this shouldn\'t happen')

            if 'FySellPrice' not in row:
                df_collect_product_base_data['FySellPrice'] = [0]
                fy_sell_price = 0
            else:
                fy_sell_price = row['FySellPrice']

            markup_percent_fy_list = row['FyLandedCostMarkupPercent_FyList']

            if 'FyListPrice' not in row:
                df_collect_product_base_data['FyListPrice'] = [0]
                fy_list_price = 0
            else:
                fy_list_price = float(row['FyListPrice'])

            if 'ECommerceDiscount' not in row:
                df_collect_product_base_data['ECommerceDiscount'] = [0]
                ecommerce_discount = 0
            else:
                ecommerce_discount = row['ECommerceDiscount']

            # we check if there's a value
            if 'DateCatalogReceived' in row:
                date_catalog_received = row['DateCatalogReceived']
                # we format the value
                try:
                    date_catalog_received = int(date_catalog_received)
                    date_catalog_received = xlrd.xldate_as_datetime(date_catalog_received, 0)
                except ValueError:
                    date_catalog_received = str(row['DateCatalogReceived'])

            elif 'db_DateCatalogReceived' in row:
                date_catalog_received = str(row['db_DateCatalogReceived'])
            else:
                self.obReporter.update_report('Fail','Catalog received date missing')
                return False, df_line_product


            if isinstance(date_catalog_received, datetime.datetime) == False:
                try:
                    date_catalog_received = datetime.datetime.strptime(date_catalog_received, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    date_catalog_received = str(row['DateCatalogReceived'])
                    self.obReporter.update_report('Alert','Check DateCatalogReceived')
                except TypeError:
                    self.obReporter.update_report('Alert','Check DateCatalogReceived')


            if 'CatalogProvidedBy' in row:
                catalog_provided_by = str(row['CatalogProvidedBy'])
            else:
                catalog_provided_by = ''

            product_price_id = row['ProductPriceId']

        if (fy_landed_cost - fy_sell_price) > 0.005 and fy_sell_price != 0:
            self.obReporter.update_report('Fail','Margin was zero')
            return False, df_line_product

        if fy_sell_price > fy_list_price and fy_list_price != 0:
            self.obReporter.update_report('Fail','Sell price too high')
            return False, df_line_product

        success, b_website_only = self.process_boolean(row, 'WebsiteOnly')
        if success:
            df_collect_product_base_data['WebsiteOnly'] = [b_website_only]
        else:
            b_website_only = -1

        success, va_eligible = self.process_boolean(row, 'VAEligible')
        if success:
            df_collect_product_base_data['VAEligible'] = [va_eligible]
        else:
            va_eligible = -1

        success, gsa_eligible = self.process_boolean(row, 'GSAEligible')
        if success:
            df_collect_product_base_data['GSAEligible'] = [gsa_eligible]
        else:
            gsa_eligible = -1

        success, htme_eligible = self.process_boolean(row, 'HTMEEligible')
        if success:
            df_collect_product_base_data['HTMEEligible'] = [htme_eligible]
        else:
            htme_eligible = -1

        success, ecat_eligible = self.process_boolean(row, 'ECATEligible')
        if success:
            df_collect_product_base_data['ECATEligible'] = [ecat_eligible]
        else:
            ecat_eligible = -1

        success, intramalls_eligible = self.process_boolean(row, 'INTRAMALLSEligible')
        if success:
            df_collect_product_base_data['INTRAMALLSEligible'] = [intramalls_eligible]
        else:
            intramalls_eligible = -1

        success = True
        if str(row['Filter']) == 'Base Pricing':
            self.obIngester.insert_base_price(vendor_list_price, fy_discount_percent, fy_cost,
                                                          estimated_freight, fy_landed_cost, date_catalog_received, catalog_provided_by,
                                                          product_price_id, b_website_only, va_product_price_id, va_eligible, gsa_product_price_id, gsa_eligible,
                                                          htme_product_price_id, htme_eligible, ecat_product_price_id, ecat_eligible, fedmall_product_price_id, intramalls_eligible)
        elif str(row['Filter']) == 'Ready':
            base_price_id = row['BaseProductPriceId']
            self.obIngester.update_base_price(base_price_id, vendor_list_price, fy_discount_percent, fy_cost,
                                                          estimated_freight, fy_landed_cost, date_catalog_received, catalog_provided_by,
                                                          product_price_id, b_website_only, va_product_price_id, va_eligible, gsa_product_price_id, gsa_eligible,
                                                          htme_product_price_id, htme_eligible, ecat_product_price_id, ecat_eligible, fedmall_product_price_id, intramalls_eligible)

        return success, df_line_product


    def trigger_ingest_cleanup(self):
        self.obIngester.set_product_notes_cleanup()
        self.obIngester.insert_base_price_cleanup()
        self.obIngester.update_base_price_cleanup()


class UpdateBasePrice(BasePrice):
    req_fields = ['FyCatalogNumber', 'ManufacturerName', 'ManufacturerPartNumber','FyProductNumber','VendorName',
                  'VendorPartNumber']
    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)







## end ##