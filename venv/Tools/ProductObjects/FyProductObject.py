# CreatedBy: Emilia Crow
# CreateDate: 20220815
# Updated: 20220815
# CreateFor: Franklin Young International

import pandas

from Tools.ProgressBar import YesNoDialog
from Tools.BasicProcess import BasicProcessObject


class FyProductIngest(BasicProcessObject):
    req_fields = ['FyProductNumber']
    sup_fields = ['FyProductName', 'FyProductDescription', 'FyCountryOfOrigin', 'FyUnitOfIssue',
                  'FyUnitOfIssueQuantity','FyUnitOfMeasure','FyLeadTime', 'FyIsHazardous', 'PrimaryVendorName', 'SecondaryVendorName',
                  'FyCost', 'Landed Cost','LandedCostMarkupPercent_FYSell','LandedCostMarkupPercent_FYList']
    att_fields = []
    gen_fields = []

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'FyProduct Ingest'


    def batch_preprocessing(self):
        self.remove_private_headers()
        self.df_uois_lookup = self.obDal.get_unit_of_issue_symbol_lookup()
        if 'FyCountryOfOrigin' in self.df_product.columns:
            self.batch_process_country()

        self.batch_process_primary_vendor()

        self.df_fy_description_lookup = self.obDal.get_fy_product_descriptions()
        self.df_product = self.df_product.merge(self.df_fy_description_lookup,how='left',on=['FyProductNumber'])

        if 'PrimaryVendorId' in self.df_product.columns:
            self.df_fy_vendor_price_lookup = self.obDal.get_fy_product_vendor_prices()
            self.df_product = self.df_product.merge(self.df_fy_vendor_price_lookup,how='left',on=['FyProductNumber','PrimaryVendorId'])

        # add secondary?


    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'BaseProductPriceId','BaseProductPriceId_y','BaseProductPriceId_x',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryId','CategoryId_x','CategoryId_y',
                           'Report','Filter','ProductDescriptionId','db_FyProductName','db_FyProductDescription'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
        if remove_headers != []:
            self.df_product = self.df_product.drop(columns=remove_headers)


    def batch_process_country(self):
        df_attribute = self.df_product[['FyCountryOfOrigin']]
        df_attribute = df_attribute.drop_duplicates(subset=['FyCountryOfOrigin'])
        lst_ids = []
        for colName, row in df_attribute.iterrows():
            country = row['FyCountryOfOrigin']
            country = self.obValidator.clean_country_name(country)
            country = country.upper()
            if (len(country) == 2):
                if country in self.df_country_translator['CountryCode'].tolist():
                    new_country_of_origin_id = self.df_country_translator.loc[
                        (self.df_country_translator['CountryCode'] == country), 'CountryOfOriginId'].values[0]
                    lst_ids.append(new_country_of_origin_id)
                elif country in ['XX','ZZ']:
                    # unknown
                    lst_ids.append(-1)
                else:
                    coo_id = self.obIngester.manual_ingest_country(atmp_code = country)
                    lst_ids.append(coo_id)

            elif (len(country) == 3):
                if country in self.df_country_translator['ECATCountryCode'].tolist():
                    new_country_of_origin_id = self.df_country_translator.loc[
                        (self.df_country_translator['ECATCountryCode'] == country), 'CountryOfOriginId'].values[0]
                    lst_ids.append(new_country_of_origin_id)
                else:
                    coo_id = self.obIngester.manual_ingest_country(ecat_code = country)
                    lst_ids.append(coo_id)

            elif (len(country) > 3):
                if country in self.df_country_translator['CountryName'].tolist():
                    new_country_of_origin_id = self.df_country_translator.loc[
                        (self.df_country_translator['CountryName'] == country), 'CountryOfOriginId'].values[0]
                    lst_ids.append(new_country_of_origin_id)
                else:
                    coo_id = self.obIngester.manual_ingest_country(atmp_name = country)
                    lst_ids.append(coo_id)
            else:
                lst_ids.append(-1)

        df_attribute['FyCountryOfOriginId'] = lst_ids
        self.df_product = self.df_product.merge(df_attribute,
                                                          how='left', on=['FyCountryOfOrigin'])


    def batch_process_primary_vendor(self):
        if 'VendorName' in self.df_product.columns:
            df_attribute = self.df_product[['VendorName']]
            df_attribute = df_attribute.drop_duplicates(subset=['VendorName'])
            lst_ids = []
            for colName, row in df_attribute.iterrows():
                vendor_name = row['VendorName'].upper()
                if vendor_name == '':
                    new_vendor_id = -1
                elif vendor_name in self.df_vendor_translator['VendorCode'].values:
                    new_vendor_id = self.df_vendor_translator.loc[
                        (self.df_vendor_translator['VendorCode'] == vendor_name),'VendorId'].values[0]
                elif vendor_name in self.df_vendor_translator['VendorName'].values:
                    new_vendor_id = self.df_vendor_translator.loc[
                        (self.df_vendor_translator['VendorName'] == vendor_name),'VendorId'].values[0]
                else:
                    vendor_name_list = self.df_vendor_translator["VendorName"].tolist()
                    vendor_name_list = list(dict.fromkeys(vendor_name_list))

                    new_vendor_id = self.obIngester.manual_ingest_vendor(atmp_name=vendor_name,atmp_code=vendor_name,lst_vendor_names=vendor_name_list)

                lst_ids.append(new_vendor_id)

            df_attribute['VendorId'] = lst_ids

            self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                     how='left', on=['VendorName'])

        if 'PrimaryVendorName' in self.df_product.columns:
            df_attribute = self.df_product[['PrimaryVendorName']]
            df_attribute = df_attribute.drop_duplicates(subset=['PrimaryVendorName'])
            lst_ids = []
            for colName, row in df_attribute.iterrows():
                vendor_name = row['PrimaryVendorName'].upper()
                if vendor_name == '':
                    new_vendor_id = -1
                elif vendor_name in self.df_vendor_translator['VendorCode'].values:
                    new_vendor_id = self.df_vendor_translator.loc[
                        (self.df_vendor_translator['VendorCode'] == vendor_name),'VendorId'].values[0]
                elif vendor_name in self.df_vendor_translator['VendorName'].values:
                    new_vendor_id = self.df_vendor_translator.loc[
                        (self.df_vendor_translator['VendorName'] == vendor_name),'VendorId'].values[0]
                else:
                    vendor_name_list = self.df_vendor_translator["VendorName"].tolist()
                    vendor_name_list = list(dict.fromkeys(vendor_name_list))

                    new_vendor_id = self.obIngester.manual_ingest_vendor(atmp_name=vendor_name,atmp_code=vendor_name,lst_vendor_names=vendor_name_list)

                lst_ids.append(new_vendor_id)

            df_attribute['PrimaryVendorId'] = lst_ids

            self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                     how='left', on=['PrimaryVendorName'])

        if 'SecondaryVendorName' in self.df_product.columns:
            df_attribute = self.df_product[['SecondaryVendorName']]
            df_attribute = df_attribute.drop_duplicates(subset=['SecondaryVendorName'])
            lst_ids = []
            for colName, row in df_attribute.iterrows():
                vendor_name = row['SecondaryVendorName'].upper()

                if vendor_name == '':
                    new_vendor_id = -1
                elif vendor_name in self.df_vendor_translator['VendorCode'].values:
                    new_vendor_id = self.df_vendor_translator.loc[
                        (self.df_vendor_translator['VendorCode'] == vendor_name),'VendorId'].values[0]
                elif vendor_name in self.df_vendor_translator['VendorName'].values:
                    new_vendor_id = self.df_vendor_translator.loc[
                        (self.df_vendor_translator['VendorName'] == vendor_name),'VendorId'].values[0]
                else:
                    vendor_name_list = self.df_vendor_translator["VendorName"].tolist()
                    vendor_name_list = list(dict.fromkeys(vendor_name_list))

                    new_vendor_id = self.obIngester.manual_ingest_vendor(atmp_name=vendor_name,atmp_code=vendor_name,lst_vendor_names=vendor_name_list)

                lst_ids.append(new_vendor_id)

            df_attribute['SecondaryVendorId'] = lst_ids

            self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                     how='left', on=['SecondaryVendorName'])



    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            df_collect_product_base_data = self.identify_units(df_collect_product_base_data, row)

        df_line_product = df_collect_product_base_data.copy()
        for colName, row in df_line_product.iterrows():
            success, df_collect_product_base_data = self.process_pricing(df_collect_product_base_data, row)
            if not success:
                return success, df_collect_product_base_data

        df_line_product = df_collect_product_base_data.copy()
        for colName, row in df_line_product.iterrows():
            # check if it is update or not
            if 'ProductDescriptionId' in df_line_product.columns:
                success, df_collect_product_base_data  = self.update_fy_description(df_collect_product_base_data, row)
            else:
                success, df_collect_product_base_data  = self.process_fy_description(df_collect_product_base_data, row)

        return success, df_collect_product_base_data


    def identify_units(self, df_collect_product_base_data, row):
        # set quantities
        fy_unit_of_issue = -1
        if 'FyUnitOfIssue' in row:
            fy_unit_of_issue = self.normalize_units(row['FyUnitOfIssue'])
            df_collect_product_base_data['FyUnitOfIssue'] = [fy_unit_of_issue]

        if fy_unit_of_issue == -1:
            fy_unit_of_issue_symbol_id = -1
        else:
            try:
                fy_unit_of_issue_symbol_id = self.df_uois_lookup.loc[
                    (self.df_uois_lookup['UnitOfIssueSymbol'] == fy_unit_of_issue), 'UnitOfIssueSymbolId'].values[0]
            except IndexError:
                fy_unit_of_issue_symbol_id = self.obIngester.ingest_uoi_symbol(fy_unit_of_issue)

        df_collect_product_base_data['FyUnitOfIssueSymbolId'] = [fy_unit_of_issue_symbol_id]

        fy_unit_of_measure = -1
        if 'FyUnitOfMeasure' in row:
            fy_unit_of_measure = self.normalize_units(row['FyUnitOfMeasure'])
            df_collect_product_base_data['FyUnitOfMeasure'] = [fy_unit_of_measure]

        if fy_unit_of_measure == -1:
            # force to each?
            fy_unit_of_measure_symbol_id = 6

        else:
            try:
                fy_unit_of_measure_symbol_id = self.df_uois_lookup.loc[
                    (self.df_uois_lookup['UnitOfIssueSymbol'] == fy_unit_of_measure), 'UnitOfIssueSymbolId'].values[0]
            except IndexError:
                fy_unit_of_measure_symbol_id = self.obIngester.ingest_uoi_symbol(fy_unit_of_measure)

        df_collect_product_base_data['FyUnitOfMeasureSymbolId'] = [fy_unit_of_measure_symbol_id]

        return df_collect_product_base_data


    # plug this in somewhere
    def process_pricing(self, df_collect_product_base_data, row):
        # from the file
        success, fy_landed_cost = self.row_check(row,'Landed Cost')
        if not success:
            # from the db
            success, fy_landed_cost = self.row_check(row,'CurrentFyLandedCost')

        if success:
            success, fy_landed_cost = self.float_check(fy_landed_cost, 'Landed Cost')
            df_collect_product_base_data['Landed Cost'] = [fy_landed_cost]

        if not success:
            # we could try to perform the calculations here
            self.obReporter.update_report('Alert', 'Landed Cost not provided')
            vlp_success, vendor_list_price = self.row_check(row, 'VendorListPrice')
            if vlp_success:
                vlp_success, vendor_list_price = self.float_check(vendor_list_price,'VendorListPrice')
                df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]
            else:
                vlp_success, vendor_list_price = self.row_check(row, 'PrimaryVendorListPrice')
                if vlp_success:
                    vlp_success, vendor_list_price = self.float_check(vendor_list_price,'PrimaryVendorListPrice')
                    df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]
                else:
                    vlp_success, vendor_list_price = self.row_check(row, 'CurrentVendorListPrice')
                    if vlp_success:
                        vlp_success, vendor_list_price = self.float_check(vendor_list_price,'CurrentVendorListPrice')
                        df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]
                    else:
                        vendor_list_price = 0
                        df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]

            if vlp_success:
                discount_success, fy_discount_percent = self.row_check(row,'Discount')
                if discount_success:
                    discount_success, fy_discount_percent = self.float_check(fy_discount_percent,'Discount')
                    df_collect_product_base_data['Discount'] = [fy_discount_percent]
                else:
                    discount_success, fy_discount_percent = self.row_check(row,'PrimaryDiscount')
                    if discount_success:
                        discount_success, fy_discount_percent = self.float_check(fy_discount_percent,'PrimaryDiscount')
                        df_collect_product_base_data['Discount'] = [fy_discount_percent]
                    else:
                        discount_success, fy_discount_percent = self.row_check(row, 'CurrentDiscount')
                        if discount_success:
                            discount_success, fy_discount_percent = self.float_check(fy_discount_percent,
                                                                                     'CurrentDiscount')
                            df_collect_product_base_data['Discount'] = [fy_discount_percent]
                        else:
                            fy_discount_percent = 0
                            df_collect_product_base_data['Discount'] = [fy_discount_percent]
            else:
                discount_success = False


            success, fy_cost = self.row_check(row,'FyCost')
            if success:
                success, fy_cost = self.float_check(fy_cost, 'FyCost')
            else:
                success, fy_cost = self.row_check(row, 'PrimaryFyCost')
                if success:
                    success, fy_cost = self.float_check(fy_cost, 'PrimaryFyCost')
                    df_collect_product_base_data['FyCost'] = [fy_cost]
                else:
                    success, fy_cost = self.row_check(row, 'CurrentFyCost')
                    if success:
                        success, fy_cost = self.float_check(fy_cost, 'CurrentFyCost')
                        df_collect_product_base_data['FyCost'] = [fy_cost]
                    else:
                        # fail line if missing
                        self.obReporter.update_report('Alert', 'FyCost was missing')
                        fy_cost = 0
                        df_collect_product_base_data['FyCost'] = [fy_cost]


            if vlp_success and not discount_success:
                fy_discount_percent = self.set_vendor_discount(fy_cost, vendor_list_price)
                df_collect_product_base_data['Discount'] = [fy_discount_percent]

            elif not vlp_success and discount_success:
                vendor_list_price = self.set_vendor_list(fy_cost, fy_discount_percent)
                df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]

            else:
                vendor_list_price = 0
                df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]




            # checks for shipping costs
            success, estimated_freight = self.row_check(row, 'Estimated Freight')
            if success:
                success, estimated_freight = self.float_check(estimated_freight,'Estimated Freight')
                df_collect_product_base_data['Estimated Freight'] = [estimated_freight]

            else:
                success, estimated_freight = self.row_check(row, 'PrimaryEstimatedFrieght')
                if success:
                    success, estimated_freight = self.float_check(estimated_freight,'PrimaryEstimatedFrieght')
                    df_collect_product_base_data['Estimated Freight'] = [estimated_freight]
                else:
                    success, estimated_freight = self.row_check(row, 'CurrentEstimatedFrieght')
                    if success:
                        success, estimated_freight = self.float_check(estimated_freight,'CurrentEstimatedFrieght')
                        df_collect_product_base_data['Estimated Freight'] = [estimated_freight]
                    else:
                        estimated_freight = 0
                        df_collect_product_base_data['Estimated Freight'] = [estimated_freight]
                        self.obReporter.update_report('Alert', 'Estimated Freight value was set to 0')


            fy_landed_cost = fy_cost + estimated_freight
            self.obReporter.update_report('Alert', 'Landed Cost was calculated')
            df_collect_product_base_data['Landed Cost'] = [fy_landed_cost]



        # we get the values from the DB because that's what we rely on
        db_mus_success, db_markup_sell = self.row_check(row, 'CurrentMarkUp_sell')
        if db_mus_success:
            db_mus_success, db_markup_sell = self.float_check(db_markup_sell, 'CurrentMarkUp_sell')
            if db_markup_sell <= 0:
                db_mus_success = False
                self.obReporter.update_report('Alert','DB Markup Sell negative')
            elif db_markup_sell < 1:
                db_mus_success = False
                self.obReporter.update_report('Alert','DB Markup Sell too low')

        db_mul_success, db_markup_list = self.row_check(row, 'CurrentMarkUp_list')
        if db_mul_success:
            db_mul_success, db_markup_list = self.float_check(db_markup_list, 'CurrentMarkUp_list')
            if db_markup_list <= 0:
                db_mul_success = False
                self.obReporter.update_report('Alert','DB Markup List negative')
            elif db_markup_list <= 1:
                db_mul_success = False
                self.obReporter.update_report('Alert','DB Markup List too low')

        # get the markups from the file
        mus_success, markup_sell = self.row_check(row, 'LandedCostMarkupPercent_FYSell')
        if mus_success:
            mus_success, markup_sell = self.float_check(markup_sell, 'LandedCostMarkupPercent_FYSell')
            if markup_sell <= 0:
                mus_success = False
                self.obReporter.update_report('Alert','Markup Sell negative')
            elif markup_sell <= 1:
                mus_success = False
                self.obReporter.update_report('Alert','Markup Sell too low')
            else:
                df_collect_product_base_data['LandedCostMarkupPercent_FYSell'] = [markup_sell]

        if not mus_success and db_mus_success:
            markup_sell = db_markup_sell
            df_collect_product_base_data['LandedCostMarkupPercent_FYSell'] = [db_markup_sell]


        mul_success, markup_list = self.row_check(row, 'LandedCostMarkupPercent_FYList')
        if mul_success:
            mul_success, markup_list = self.float_check(markup_list, 'LandedCostMarkupPercent_FYList')
            if markup_list <= 0:
                mul_success = False
                self.obReporter.update_report('Alert','Markup List negative')
            elif markup_list <= 1:
                mul_success = False
                self.obReporter.update_report('Alert','Markup List too low')
            else:
                df_collect_product_base_data['LandedCostMarkupPercent_FYList'] = [markup_list]

        if not mul_success and db_mul_success:
            markup_list = db_markup_list
            df_collect_product_base_data['LandedCostMarkupPercent_FYList'] = [db_markup_list]

        # let's report if they're both missing
        if (not db_mus_success and not mus_success) and (not db_mul_success and not mul_success):
            self.obReporter.update_report('Fail', 'No markups not present')
            markup_sell = 0
            markup_list = 0
            df_collect_product_base_data['LandedCostMarkupPercent_FYSell'] = [markup_sell]
            df_collect_product_base_data['LandedCostMarkupPercent_FYList'] = [markup_list]

        elif (db_mus_success and not mus_success) and (db_mul_success and not mul_success):
            self.obReporter.update_report('Alert', 'DB markups were used')
            markup_sell = db_markup_sell
            markup_list = db_markup_list
            df_collect_product_base_data['LandedCostMarkupPercent_FYSell'] = [markup_sell]
            df_collect_product_base_data['LandedCostMarkupPercent_FYList'] = [markup_list]

        elif (db_mus_success and mus_success) and (db_mul_success and mul_success):
            if (markup_list != db_markup_list) or (markup_sell != db_markup_sell):
                self.obReporter.update_report('Alert', 'DB markups will be over-written')

        elif (not db_mus_success and mus_success) and (not db_mul_success and mul_success):
            self.obReporter.update_report('Alert', 'File markups were used')

        if 'FyProductNotes' in row and 'ProductPriceId' in row:
            fy_product_notes = row['FyProductNotes']
            if (fy_product_notes != ''):
                product_price_id = int(row['ProductPriceId'])
                fy_product_notes = fy_product_notes.replace('NULL','')
                fy_product_notes = fy_product_notes.replace(';','')

                self.obIngester.set_product_notes(product_price_id, fy_product_notes)

        df_collect_product_base_data = self.set_pricing_rons_way(df_collect_product_base_data, row, fy_landed_cost, markup_sell, markup_list)
        return True, df_collect_product_base_data


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

        # initial rounding and formatting
        fy_sell_price = round(fy_sell_price_long, 4)
        str_fy_sell_price = "{:.4f}".format(fy_sell_price)
        # evaluate the last two digits and do second rounding
        final_digit = str_fy_sell_price[-2:]
        if str_fy_sell_price[-2:] == '50':
            fy_sell_price = round(fy_sell_price+0.0001, 2)

        elif str_fy_sell_price[-1] == '5':
            fy_sell_price = round(fy_sell_price+0.00001, 2)

        else:
            fy_sell_price = round(fy_sell_price, 2)

        df_collect_product_base_data['Sell Price'] = [fy_sell_price]

        # do math
        fy_list_price_long = float(fy_landed_cost * markup_list)

        # initial rounding and formatting
        fy_list_price = round(fy_list_price_long, 4)
        str_fy_list_price = "{:.4f}".format(fy_list_price)
        # evaluate the last two digits and do second rounding
        final_digit = str_fy_list_price[-2:]
        if str_fy_list_price[-2:] == '50':
            fy_list_price = round(fy_list_price+0.0001, 2)

        elif str_fy_list_price[-1] == '5':
            fy_list_price = round(fy_list_price+0.00001, 2)

        else:
            fy_list_price = round(fy_list_price, 2)

        df_collect_product_base_data['Retail Price'] = [fy_list_price]

        return df_collect_product_base_data


    def process_fy_description(self, df_collect_product_base_data, row):
        fy_product_number = row['FyProductNumber']
        if 'FyProductName' not in row:
            fy_product_name = ''
        else:
            fy_product_name = row['FyProductName']
            fy_product_name = self.obValidator.clean_description(fy_product_name)
            df_collect_product_base_data['FyProductName'] = fy_product_name

        if 'FyProductDescription' not in row:
            fy_product_description = ''
        else:
            fy_product_description = row['FyProductDescription']
            fy_product_description = self.obValidator.clean_description(fy_product_description)
            df_collect_product_base_data['FyProductDescription'] = fy_product_description

        if 'FyCountryOfOriginId' in row:
            fy_coo_id = int(row['FyCountryOfOriginId'])
            if fy_coo_id == 259:
                fy_coo_id = -1
        else:
            fy_coo_id = -1

        if 'FyUnitOfIssueSymbolId' in row:
            fy_uoi_id = int(row['FyUnitOfIssueSymbolId'])
        else:
            fy_uoi_id = -1

        if 'FyUnitOfMeasureSymbolId' in row:
            fy_uom_id = int(row['FyUnitOfMeasureSymbolId'])
        else:
            fy_uom_id = -1

        if 'FyUnitOfIssueQuantity' in row:
            fy_uoi_qty = int(row['FyUnitOfIssueQuantity'])
        else:
            fy_uoi_qty = -1

        if 'FyLeadTime' in row:
            fy_lead_time = int(row['FyLeadTime'])
        else:
            fy_lead_time = -1

        if len(fy_product_name) > 80:
            self.obReporter.update_report('Alert','FyProductName might be too long for some contracts.')

        if len(fy_product_description) > 800:
            self.obReporter.update_report('Alert','FyProductDescription might be too long for some contracts.')

        if 'FyIsHazardous' in row:
            success, fy_is_hazardous = self.process_boolean(row, 'FyIsHazardous')
            if success:
                df_collect_product_base_data['FyIsHazardous'] = [fy_is_hazardous]
            else:
                fy_is_hazardous = -1
        else:
            fy_is_hazardous = -1

        if 'PrimaryVendorId' in row:
            primary_vendor_id = int(row['PrimaryVendorId'])
        elif 'VendorId' in row:
            primary_vendor_id = int(row['VendorId'])
        else:
            primary_vendor_id = -1

        if 'SecondaryVendorId' in row:
            secondary_vendor_id = int(row['SecondaryVendorId'])
        else:
            secondary_vendor_id = -1

        fy_landed_cost = row['Landed Cost']
        try:
            markup_percent_fy_sell = row['LandedCostMarkupPercent_FYSell']
        except KeyError:
            for colName2, row2 in df_collect_product_base_data.iterrows():
                print(row2)
            reports = self.obReporter.get_report()
            print('pass', reports[0])
            print('alert', reports[1])
            print('fail', reports[2])
            x = input('Markup failure is a mystery, this shouldn\'t happen')

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

        # note that these will get set automatically
        is_discontinued = -1
        success, is_discontinued = self.process_boolean(row, 'IsDiscontinued')
        if success:
            df_collect_product_base_data['IsDiscontinued'] = [is_discontinued]
        else:
            is_discontinued = 1

        is_visible = -1
        success, is_visible = self.process_boolean(row, 'IsVisible')
        if success:
            df_collect_product_base_data['IsVisible'] = [is_visible]
        else:
            is_visible = 0

        allow_purchases = -1
        success, allow_purchases = self.process_boolean(row, 'AllowPurchases')
        if success:
            df_collect_product_base_data['AllowPurchases'] = [allow_purchases]
        else:
            allow_purchases = 0

        success, price_toggle = self.process_boolean(row, 'BCPriceUpdateToggle')
        if success:
            df_collect_product_base_data['BCPriceUpdateToggle'] = [price_toggle]
        else:
            price_toggle = 1

        success, data_toggle = self.process_boolean(row, 'BCDataUpdateToggle')
        if success:
            df_collect_product_base_data['BCDataUpdateToggle'] = [data_toggle]
        else:
            data_toggle = 1

        report = ''
        if (fy_product_name != '' and fy_product_description != '' and fy_coo_id != -1 and fy_uoi_id != -1 and
                fy_uom_id != -1 and fy_uoi_qty != -1 and fy_lead_time != -1 and primary_vendor_id != -1):
            # this needs to proper ingest the info
            self.obIngester.insert_fy_product_description(fy_product_number, fy_product_name, fy_product_description,
                                                          fy_coo_id, fy_uoi_id, fy_uom_id, fy_uoi_qty, fy_lead_time,
                                                          fy_is_hazardous, primary_vendor_id, secondary_vendor_id,
                                                          fy_landed_cost, markup_percent_fy_sell, fy_sell_price, markup_percent_fy_list, fy_list_price,
                                                          is_discontinued, is_visible, allow_purchases, price_toggle, data_toggle)

            # TO DO: we need to insert a record in toggles at the same time.

            return True, df_collect_product_base_data

        if fy_product_name == '':
            report = 'Missing FyProductName'

        if fy_product_description == '':
            if report != '':
                report = report + ', FyProductDescription'
            else:
                report = 'Missing FyProductDescription'

        if fy_coo_id == -1:
            if report != '':
                report = report + ', FyCountryOfOrigin'
            else:
                report = 'Missing FyCountryOfOrigin'

        if fy_uoi_id == -1:
            if report != '':
                report = report + ', FyUnitOfIssue'
            else:
                report = 'Missing FyUnitOfIssue'

        if fy_uom_id == -1:
            if report != '':
                report = report + ', FyUnitOfMeasure'
            else:
                report = 'Missing FyUnitOfMeasure'

        if fy_uoi_qty == -1:
            if report != '':
                report = report + ', FyUnitOfIssueQuantity'
            else:
                report = 'Missing FyUnitOfIssueQuantity'

        if fy_lead_time == -1:
            if report != '':
                report = report + ', FyLeadTime'
            else:
                report = 'Missing FyLeadTime'

        if primary_vendor_id == -1:
            if report != '':
                report = report + ', PrimaryVendorName'
            else:
                report = 'Missing PrimaryVendorName'

        self.obReporter.update_report('Fail', report)
        return False, df_collect_product_base_data


    def update_fy_description(self, df_collect_product_base_data, row):
        fy_product_desc_id = row['ProductDescriptionId']
        if 'FyProductName' not in row:
            fy_product_name = ''
        else:
            fy_product_name = row['FyProductName']
            fy_product_name = self.obValidator.clean_description(fy_product_name)
            df_collect_product_base_data['FyProductName'] = fy_product_name


        if 'FyProductDescription' not in row:
            fy_product_description = ''
        else:
            fy_product_description = row['FyProductDescription']
            fy_product_description = self.obValidator.clean_description(fy_product_description)
            df_collect_product_base_data['FyProductDescription'] = fy_product_description

        if 'FyCountryOfOriginId' in row:
            fy_coo_id = int(row['FyCountryOfOriginId'])
            if fy_coo_id == 259:
                fy_coo_id = -1
        else:
            fy_coo_id = -1

        if 'FyUnitOfIssueSymbolId' in row:
            fy_uoi_id = int(row['FyUnitOfIssueSymbolId'])
        else:
            fy_uoi_id = -1

        if 'FyUnitOfMeasureSymbolId' in row:
            fy_uom_id = int(row['FyUnitOfMeasureSymbolId'])
        else:
            fy_uom_id = -1

        if 'FyUnitOfIssueQuantity' in row:
            fy_uoi_qty = int(row['FyUnitOfIssueQuantity'])
        else:
            fy_uoi_qty = -1

        if 'FyLeadTime' in row:
            fy_lead_time = int(row['FyLeadTime'])
        else:
            fy_lead_time = -1

        if len(fy_product_name) > 80 and fy_product_name != '':
            self.obReporter.update_report('Alert','FyProductName might be too long for some contracts.')

        if len(fy_product_description) > 800 and fy_product_description != '':
            self.obReporter.update_report('Alert','FyProductDescription might be too long for some contracts.')

        if 'FyIsHazardous' in row:
            success, fy_is_hazardous = self.process_boolean(row, 'FyIsHazardous')
            if success:
                df_collect_product_base_data['FyIsHazardous'] = [fy_is_hazardous]
            else:
                fy_is_hazardous = -1
        else:
            fy_is_hazardous = -1

        if 'PrimaryVendorId' in row:
            primary_vendor_id = int(row['PrimaryVendorId'])
        else:
            primary_vendor_id = -1

        if 'SecondaryVendorId' in row:
            secondary_vendor_id = int(row['SecondaryVendorId'])
        else:
            secondary_vendor_id = -1

        fy_landed_cost = row['Landed Cost']
        try:
            markup_percent_fy_sell = row['LandedCostMarkupPercent_FYSell']
        except KeyError:
            for colName2, row2 in df_collect_product_base_data.iterrows():
                print(row2)
            reports = self.obReporter.get_report()
            print('pass', reports[0])
            print('alert', reports[1])
            print('fail', reports[2])
            x = input('Markup failure is a mystery, this shouldn\'t happen')

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

        is_discontinued = -1
        success, is_discontinued = self.process_boolean(row, 'IsDiscontinued')
        if success:
            df_collect_product_base_data['IsDiscontinued'] = [is_discontinued]
        else:
            is_discontinued = -1

        is_visible = -1
        success, is_visible = self.process_boolean(row, 'IsVisible')
        if success:
            df_collect_product_base_data['IsVisible'] = [is_visible]
        else:
            is_visible = -1

        allow_purchases = -1
        success, allow_purchases = self.process_boolean(row, 'AllowPurchases')
        if success:
            df_collect_product_base_data['AllowPurchases'] = [allow_purchases]
        else:
            allow_purchases = -1

        success, price_toggle = self.process_boolean(row, 'BCPriceUpdateToggle')
        if success:
            df_collect_product_base_data['BCPriceUpdateToggle'] = [price_toggle]
        else:
            price_toggle = 1

        success, data_toggle = self.process_boolean(row, 'BCDataUpdateToggle')
        if success:
            df_collect_product_base_data['BCDataUpdateToggle'] = [data_toggle]
        else:
            data_toggle = 1

        if (fy_product_name != '' or fy_product_description != '' or fy_coo_id != -1 or fy_uoi_id != -1 or fy_uom_id != -1
                or fy_uoi_qty != -1 or fy_lead_time != -1 or fy_is_hazardous != -1 or primary_vendor_id != -1 or secondary_vendor_id != -1
                or is_discontinued != -1 or is_visible != -1 or allow_purchases != -1 or price_toggle != -1 or data_toggle != -1):
            self.obIngester.update_fy_product_description_short(fy_product_desc_id, fy_product_name, fy_product_description,
                                                          fy_coo_id, fy_uoi_id, fy_uom_id, fy_uoi_qty, fy_lead_time, fy_is_hazardous,
                                                          primary_vendor_id, secondary_vendor_id,
                                                          fy_landed_cost, markup_percent_fy_sell, fy_sell_price, markup_percent_fy_list, fy_list_price,
                                                          is_discontinued, is_visible, allow_purchases, price_toggle, data_toggle)

        return True, df_collect_product_base_data


    def trigger_ingest_cleanup(self):
        self.obIngester.insert_fy_product_description_cleanup()
        self.obIngester.update_fy_product_description_short_cleanup()


## end ##