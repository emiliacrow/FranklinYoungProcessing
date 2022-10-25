# CreatedBy: Emilia Crow
# CreateDate: 20220815
# Updated: 20220815
# CreateFor: Franklin Young International

import pandas
import datetime
import xlrd

from Tools.ProgressBar import YesNoDialog
from Tools.BasicProcess import BasicProcessObject

# handle date catalog recieved for primary here?

class FyProductUpdate(BasicProcessObject):
    req_fields = ['FyProductNumber']
    sup_fields = ['FyProductName', 'FyProductDescription', 'FyCountryOfOrigin', 'FyUnitOfIssue',
                  'FyUnitOfIssueQuantity','FyUnitOfMeasure','FyLeadTime', 'FyIsHazardous', 'VendorName', 'PrimaryVendorName', 'SecondaryVendorName',
                  'ManufacturerPartNumber', 'ManufacturerName', 'VendorPartNumber','DateCatalogReceived',
                  'FyCategory', 'FyNAICSCode', 'FyUNSPSCCode', 'FyHazardousSpecialHandlingCode',
                  'FyShelfLifeMonths','FyControlledCode','FyIsLatexFree','FyIsGreen','FyColdChain',
                  'FyProductNotes', 'VendorListPrice','FyCost', 'PrimaryVendorListPrice','PrimaryFyCost',
                  'Landed Cost','FyLandedCostMarkupPercent_FYSell','FyLandedCostMarkupPercent_FYList',
                  'BCDataUpdateToggle', 'BCPriceUpdateToggle','FyIsDiscontinued','FyAllowPurchases','FyIsVisible',
                  'FyDenyGSAContract', 'FyDenyGSAContractDate','FyDenyVAContract', 'FyDenyVAContractDate',
                  'FyDenyECATContract', 'FyDenyECATContractDate','FyDenyHTMEContractDate', 'FyDenyHTMEContractDate']
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

        if 'ManufacturerName' in self.df_product.columns:
            self.batch_process_manufacturer()

        if 'FyCategory' in self.df_product.columns:
            self.batch_process_category()

        if 'FyNAICSCode' in self.df_product.columns:
            # get naics
            self.df_naics_codes = self.obDal.get_naics_codes()
            self.batch_process_naics()

        if 'FyUNSPSCCode' in self.df_product.columns:
            # get unspsc
            self.df_unspsc_codes = self.obDal.get_unspsc_codes()
            self.batch_process_unspsc()

        if 'FyHazardousSpecialHandlingCode' in self.df_product.columns:
            # get special_handling
            self.df_special_handling_codes = self.obDal.get_special_handling_codes()
            self.batch_process_special_handling()

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
        print('Batch process country')
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
        print('Batch process vendor')
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


    def batch_process_category(self):
        print('Batch processing Categories')
        # this needs to be handled better
        if 'Category' in self.df_product.columns:
            df_attribute = self.df_product[['Category']]
            df_attribute = df_attribute.drop_duplicates(subset=['Category'])
            lst_ids = []
            for colName, row in df_attribute.iterrows():
                category = str(row['Category']).strip()

                while '/ ' in category:
                    category = category.replace('/ ', '/')

                while ' /' in category:
                    category = category.replace(' /', '/')

                category_name = (category.rpartition('/')[2]).strip()

                self.df_category_names['CategoryNameLower'] = self.df_category_names['CategoryName'].str.lower()

                if category_name in self.df_category_names['CategoryName'].values:
                    new_category_id = self.df_category_names.loc[
                        (self.df_category_names['CategoryName'] == category_name), 'CategoryId'].values[0]

                elif category_name.lower() in self.df_category_names['CategoryNameLower'].values:
                    new_category_id = self.df_category_names.loc[
                        (self.df_category_names['CategoryNameLower'] == category_name.lower()), 'CategoryId'].values[0]

                elif category in self.df_category_names['Category'].values:
                    new_category_id = self.df_category_names.loc[
                        (self.df_category_names['Category'] == category), 'CategoryId'].values[0]

                else:
                    categories_to_ship = []
                    if (category != '') and (category_name != '') and ('All Products/' in category):
                        # collect the biggest one
                        categories_to_ship.append([category_name, category])

                        # we set the name of the category
                        category = category.rpartition('/')[0]
                        category_name = category.rpartition('/')[2]

                        # as long as the hierarchy exists, we split it out
                        while ('/' in category):
                            category_name = category_name.strip()
                            category = category.strip()

                            if category_name in self.df_category_names['CategoryName'].values:
                                break
                            elif category in self.df_category_names['Category'].values:
                                break

                            categories_to_ship.append([category_name, category])

                            # we set the name of the category
                            category = category.rpartition('/')[0]
                            category_name = category.rpartition('/')[2]

                        # this is the magic
                        # this sets the order smallest to largest
                        # this puts the mapping into the DB in the right order
                        # and returns the correct id at the end
                        categories_to_ship.sort(key=lambda x:len(x[1]))

                        for each_category in categories_to_ship:
                            new_category_id = self.obDal.category_cap(each_category[0], each_category[1])

                lst_ids.append(new_category_id)

            df_attribute['CategoryId'] = lst_ids

            self.df_product = self.df_product.merge(df_attribute,
                                                     how='left', on=['Category'])
        else:
            self.df_fill_category = self.obDal.get_product_category()
            self.df_product = self.df_product.merge(self.df_fill_category,how='left',on=['ProductId'])
            del self.df_fill_category


    def batch_process_naics(self):
        if 'FyNAICSCode' in self.df_product.columns:
            df_attribute = self.df_product[['FyNAICSCode']]
            df_attribute = df_attribute.drop_duplicates(subset=['FyNAICSCode'])
            lst_ids = []
            for colName, row in df_attribute.iterrows():
                naics_code = row['FyNAICSCode'].upper()
                if naics_code == '':
                    fy_naics_code_id = -1
                elif naics_code in self.df_naics_codes['FyNAICSCode'].values:
                    fy_naics_code_id = self.df_naics_codes.loc[
                        (self.df_naics_codes['FyNAICSCode'] == naics_code),'FyNAICSCodeId'].values[0]

                lst_ids.append(fy_naics_code_id)

            df_attribute['FyNAICSCodeId'] = lst_ids

            self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                     how='left', on=['FyNAICSCode'])


    def batch_process_unspsc(self):
        if 'FyUNSPSCCode' in self.df_product.columns:
            df_attribute = self.df_product[['FyUNSPSCCode']]
            df_attribute = df_attribute.drop_duplicates(subset=['FyUNSPSCCode'])
            lst_ids = []
            for colName, row in df_attribute.iterrows():
                unspsc_code = row['FyUNSPSCCode'].upper()
                if unspsc_code == '':
                    fy_unspsc_code_id = -1
                elif unspsc_code in self.df_unspsc_codes['FyUNSPSCCode'].values:
                    fy_unspsc_code_id = self.df_unspsc_codes.loc[
                        (self.df_unspsc_codes['FyUNSPSCCode'] == unspsc_code), 'FyUNSPSCCodeId'].values[0]

                lst_ids.append(fy_unspsc_code_id)

            df_attribute['FyUNSPSCCodeId'] = lst_ids

            self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                     how='left', on=['FyUNSPSCCode'])


    def batch_process_special_handling(self):
        if 'FyHazardousSpecialHandlingCode' in self.df_product.columns:
            df_attribute = self.df_product[['FyHazardousSpecialHandlingCode']]
            df_attribute = df_attribute.drop_duplicates(subset=['FyHazardousSpecialHandlingCode'])
            lst_ids = []
            for colName, row in df_attribute.iterrows():
                special_handling_code = row['FyHazardousSpecialHandlingCode'].upper()
                if special_handling_code == '':
                    fy_special_handling_id = -1
                elif special_handling_code in self.df_special_handling_codes['FyHazardousSpecialHandlingCode'].values:
                    fy_special_handling_id = self.df_special_handling_codes.loc[
                        (self.df_special_handling_codes['FyHazardousSpecialHandlingCode'] == special_handling_code), 'FyHazardousSpecialHandlingCodeId'].values[0]

                lst_ids.append(fy_special_handling_id)

            df_attribute['FyHazardousSpecialHandlingCodeId'] = lst_ids

            self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                     how='left', on=['FyHazardousSpecialHandlingCode'])


    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            for each_bool in ['FyIsGreen', 'FyIsLatexFree', 'FyIsHazardous','FyIsDiscontinued','FyIsVisible','FyAllowPurchases','BCPriceUpdateToggle','BCDataUpdateToggle']:
                success, return_val = self.process_boolean(row, each_bool)
                if success:
                    df_collect_product_base_data[each_bool] = [return_val]
                else:
                    df_collect_product_base_data[each_bool] = [-1]


            # check if it is update or not
            if 'ProductDescriptionId' in df_line_product.columns:
                self.obReporter.update_report('Pass','This is an FyProduct update')
            else:
                self.obReporter.update_report('Fail','This is an FyProduct insert')
                return False, df_collect_product_base_data

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


    def process_pricing(self, df_collect_product_base_data, row):
        # from the file
        success, fy_landed_cost = self.row_check(row,'Landed Cost')

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

                    vlp_success, vendor_list_price = self.row_check(row, 'db_PrimaryVendorListPrice')
                    if vlp_success:
                        vlp_success, vendor_list_price = self.float_check(vendor_list_price,'db_PrimaryVendorListPrice')
                        df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]
                    else:
                        vlp_success, vendor_list_price = self.row_check(row, 'CurrentVendorListPrice')
                        if vlp_success:
                            vlp_success, vendor_list_price = self.float_check(vendor_list_price,'CurrentVendorListPrice')
                            df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]
                        else:
                            vendor_list_price = -1
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
                        discount_success, fy_discount_percent = self.row_check(row, 'db_PrimaryDiscount')
                        if discount_success:
                            discount_success, fy_discount_percent = self.float_check(fy_discount_percent,
                                                                                     'db_PrimaryDiscount')
                            df_collect_product_base_data['Discount'] = [fy_discount_percent]
                        else:
                            discount_success, fy_discount_percent = self.row_check(row, 'CurrentDiscount')
                            if discount_success:
                                discount_success, fy_discount_percent = self.float_check(fy_discount_percent,
                                                                                         'CurrentDiscount')
                                df_collect_product_base_data['Discount'] = [fy_discount_percent]
                            else:
                                fy_discount_percent = -1
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
                    success, fy_cost = self.row_check(row, 'db_PrimaryFyCost')
                    if success:
                        success, fy_cost = self.float_check(fy_cost, 'db_PrimaryFyCost')
                        df_collect_product_base_data['FyCost'] = [fy_cost]
                    else:
                        success, fy_cost = self.row_check(row, 'CurrentFyCost')
                        if success:
                            success, fy_cost = self.float_check(fy_cost, 'CurrentFyCost')
                            df_collect_product_base_data['FyCost'] = [fy_cost]
                        else:
                            # fail line if missing
                            self.obReporter.update_report('Alert', 'FyCost was missing')
                            fy_cost = -1
                            df_collect_product_base_data['FyCost'] = [fy_cost]


            if vlp_success and not discount_success:
                fy_discount_percent = self.set_vendor_discount(fy_cost, vendor_list_price)
                df_collect_product_base_data['Discount'] = [fy_discount_percent]

            elif not vlp_success and discount_success:
                vendor_list_price = self.set_vendor_list(fy_cost, fy_discount_percent)
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
                    success, estimated_freight = self.row_check(row, 'db_PrimaryEstimatedFrieght')
                    if success:
                        success, estimated_freight = self.float_check(estimated_freight,'db_PrimaryEstimatedFrieght')
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

        is_hot_price = False
        fy_landed_cost = row['Landed Cost']
        if fy_landed_cost != 0:
            is_hot_price = True

        if 'LandedCostMarkupPercent_FYSell' not in row:
            df_collect_product_base_data['LandedCostMarkupPercent_FYSell'] = [0]
            markup_percent_fy_sell = 0
        else:
            markup_percent_fy_sell = float(row['LandedCostMarkupPercent_FYSell'])

        if 'Sell Price' not in row:
            df_collect_product_base_data['Sell Price'] = [0]
            fy_sell_price = 0
        else:
            fy_sell_price = row['Sell Price']

        if is_hot_price and fy_sell_price == 0:
            self.obReporter.update_report('Fail','Sell price did not calculate')
            return False, df_collect_product_base_data

        if 'LandedCostMarkupPercent_FYList' not in row:
            df_collect_product_base_data['LandedCostMarkupPercent_FYList'] = [0]
            markup_percent_fy_list = 0
        else:
            markup_percent_fy_list = float(row['LandedCostMarkupPercent_FYList'])

        if 'Retail Price' not in row:
            df_collect_product_base_data['Retail Price'] = [0]
            fy_list_price = 0
        else:
            fy_list_price = float(row['Retail Price'])

        if is_hot_price and fy_list_price == 0:
            self.obReporter.update_report('Fail','Retail price did not calculate')
            return False, df_collect_product_base_data

        if 'FyCategoryId' not in row:
            df_collect_product_base_data['FyCategoryId'] = [-1]
            fy_category_id = -1
        else:
            fy_category_id = int(row['FyCategoryId'])

        fy_is_green = int(row['FyIsGreen'])
        fy_is_latex_free = int(row['FyIsLatexFree'])

        if 'FyColdChain' not in row:
            df_collect_product_base_data['FyColdChain'] = ['']
            fy_cold_chain = ''
        else:
            fy_cold_chain = str(row['FyColdChain'])
            if fy_cold_chain not in ['A','F','R']:
                fy_cold_chain = ''
                df_collect_product_base_data['FyColdChain'] = ['']
            elif fy_cold_chain.lower() in ['ambient']:
                fy_cold_chain = 'A'
                df_collect_product_base_data['FyColdChain'] = ['A']
            elif fy_cold_chain.lower() in ['frozen']:
                fy_cold_chain = 'F'
                df_collect_product_base_data['FyColdChain'] = ['F']
            elif fy_cold_chain.lower() in ['refrigerated']:
                fy_cold_chain = 'R'
                df_collect_product_base_data['FyColdChain'] = ['R']

        if 'FyControlledCode' not in row:
            df_collect_product_base_data['FyControlledCode'] = [-1]
            fy_controlled_code = -1
        else:
            fy_controlled_code = int(row['FyControlledCode'])
            if -1 > fy_controlled_code > 5:
                fy_controlled_code = -1

        if 'FyNAICSCodeId' not in row:
            df_collect_product_base_data['FyNAICSCodeId'] = [-1]
            fy_naics_code_id = -1
        else:
            fy_naics_code_id = int(row['FyNAICSCodeId'])

        if 'FyUNSPSCCodeId' not in row:
            df_collect_product_base_data['FyUNSPSCCodeId'] = [-1]
            fy_unspsc_code_id = -1
        else:
            fy_unspsc_code_id = int(row['FyUNSPSCCodeId'])

        if 'FyHazardousSpecialHandlingCodeId' not in row:
            df_collect_product_base_data['FyHazardousSpecialHandlingCodeId'] = [-1]
            fy_special_handling_id = -1
        else:
            fy_special_handling_id = int(row['FyHazardousSpecialHandlingCodeId'])

        if 'FyShelfLifeMonths' not in row:
            df_collect_product_base_data['FyShelfLifeMonths'] = [-1]
            fy_shelf_life_months = -1
        else:
            fy_shelf_life_months = int(row['FyShelfLifeMonths'])

        if 'FyProductNotes' not in row:
            df_collect_product_base_data['FyProductNotes'] = ['']
            fy_product_notes = ''
        else:
            fy_product_notes = str(row['FyProductNotes'])

        # note that these will get set automatically
        fy_is_discontinued = -1
        success, fy_is_discontinued = self.process_boolean(row, 'FyIsDiscontinued')
        if success:
            df_collect_product_base_data['FyIsDiscontinued'] = [fy_is_discontinued]
        else:
            fy_is_discontinued = 0

        is_visible = -1
        success, is_visible = self.process_boolean(row, 'FyIsVisible')
        if success:
            df_collect_product_base_data['FyIsVisible'] = [is_visible]
        else:
            is_visible = 1

        allow_purchases = -1
        success, allow_purchases = self.process_boolean(row, 'FyAllowPurchases')
        if success:
            df_collect_product_base_data['FyAllowPurchases'] = [allow_purchases]
        else:
            allow_purchases = 1

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

        if ' ' in fy_product_number:
            fy_catalog_number = fy_product_number.partition(' ')[0]
        else:
            fy_catalog_number = fy_product_number

        manufacturer_part_number = ''
        if 'ManufacturerPartNumber' in row:
            manufacturer_part_number = str(row['ManufacturerPartNumber'])

        manufacturer_id = -1
        if 'ManufacturerId' in row:
            manufacturer_id = int(row['ManufacturerId'])

        is_product_number_override = 0
        if 'IsProductNumberOverride' in row:
            is_product_number_override = int(row['IsProductNumberOverride'])

        product_tax_class = 'Default Tax Class'
        if 'ProductTaxClass' in row:
            product_tax_class = str(row['ProductTaxClass'])

        vendor_part_number = ''
        if 'VendorPartNumber' in row:
            vendor_part_number = str(row['VendorPartNumber'])

        date_catalog_received = -1
        if 'DateCatalogReceived' in row:
            try:
                date_catalog_received = int(row['DateCatalogReceived'])
                date_catalog_received = (xlrd.xldate_as_datetime(date_catalog_received, 0)).date()
            except ValueError:
                date_catalog_received = str(row['DateCatalogReceived'])

        catalog_provided_by = ''
        if 'CatalogProvidedBy' in row:
            catalog_provided_by = str(row['CatalogProvidedBy'])

        if 'VendorListPrice' in row:
            vendor_list_price = float(row['VendorListPrice'])
        else:
            vendor_list_price = 0

        if 'Discount' in row:
            fy_discount_percent = float(row['Discount'])
        else:
            fy_discount_percent = 0

        if 'FyCost' in row:
            fy_cost = float(row['FyCost'])
        else:
            fy_cost = 0

        if 'Estimated Freight' in row:
            estimated_freight = float(row['Estimated Freight'])
        else:
            estimated_freight = 0


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


        success, deny_gsa = self.process_boolean(row, 'FyDenyGSAContract')
        if success:
            df_collect_product_base_data['FyDenyGSAContract'] = [deny_gsa]
        else:
            deny_gsa = -1

        deny_gsa_date = -1
        if 'FyDenyGSAContractDate' in row:
            try:
                deny_gsa_date = int(row['FyDenyGSAContractDate'])
                deny_gsa_date = (xlrd.xldate_as_datetime(deny_gsa_date, 0)).date()
            except ValueError:
                deny_gsa_date = str(row['FyDenyGSAContractDate'])

        success, deny_va = self.process_boolean(row, 'FyDenyVAContract')
        if success:
            df_collect_product_base_data['FyDenyVAContract'] = [deny_va]
        else:
            deny_va = -1

        deny_va_date = -1
        if 'FyDenyVAContractDate' in row:
            try:
                deny_va_date = int(row['FyDenyVAContractDate'])
                deny_va_date = (xlrd.xldate_as_datetime(deny_va_date, 0)).date()
            except ValueError:
                deny_va_date = str(row['FyDenyVAContractDate'])

        success, deny_ecat = self.process_boolean(row, 'FyDenyECATContract')
        if success:
            df_collect_product_base_data['FyDenyECATContract'] = [deny_ecat]
        else:
            deny_ecat = -1

        deny_ecat_date = -1
        if 'FyDenyECATContractDate' in row:
            try:
                deny_ecat_date = int(row['FyDenyECATContractDate'])
                deny_ecat_date = (xlrd.xldate_as_datetime(deny_ecat_date, 0)).date()
            except ValueError:
                deny_ecat_date = str(row['FyDenyECATContractDate'])

        success, deny_htme = self.process_boolean(row, 'FyDenyHTMEContractDate')
        if success:
            df_collect_product_base_data['FyDenyHTMEContractDate'] = [deny_htme]
        else:
            deny_htme = -1

        deny_htme_date = -1
        if 'FyDenyHTMEContractDate' in row:
            try:
                deny_htme_date = int(row['FyDenyHTMEContractDate'])
                deny_htme_date = (xlrd.xldate_as_datetime(deny_htme_date, 0)).date()
            except ValueError:
                deny_htme_date = str(row['FyDenyHTMEContractDate'])

        report = ''
        if (fy_product_name != '' and fy_product_description != '' and fy_coo_id != -1 and fy_uoi_id != -1 and
                fy_uom_id != -1 and fy_uoi_qty != -1 and fy_lead_time != -1 and primary_vendor_id != -1 and manufacturer_part_number != '' and
                manufacturer_id != -1 and vendor_part_number != '' and date_catalog_received != -1):
            # this needs to proper ingest the info
            self.obIngester.insert_fy_product_description(fy_catalog_number, manufacturer_part_number, is_product_number_override,
                                                          manufacturer_id, fy_product_number, fy_product_name, fy_product_description,
                                                          fy_coo_id, fy_uoi_id, fy_uom_id, fy_uoi_qty, product_tax_class,
                                                          vendor_part_number, fy_lead_time, fy_is_hazardous, primary_vendor_id,
                                                          secondary_vendor_id, fy_category_id, fy_is_green, fy_is_latex_free,
                                                          fy_cold_chain, fy_controlled_code, fy_naics_code_id, fy_unspsc_code_id,
                                                          fy_special_handling_id, fy_shelf_life_months, fy_product_notes,
                                                          b_website_only, gsa_eligible, va_eligible, ecat_eligible, htme_eligible,
                                                          vendor_list_price, fy_discount_percent, fy_cost, estimated_freight, fy_landed_cost,
                                                          markup_percent_fy_sell, fy_sell_price, markup_percent_fy_list,
                                                          fy_list_price, fy_is_discontinued, is_visible, allow_purchases,
                                                          price_toggle, data_toggle,
                                                              deny_gsa, deny_gsa_date, deny_va, deny_va_date,
                                                              deny_ecat, deny_ecat_date, deny_htme, deny_htme_date,
                                                              date_catalog_received, catalog_provided_by)

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

        if manufacturer_id == -1:
            if report != '':
                report = report + ', ManufacturerId'
            else:
                report = 'Missing ManufacturerId'

        if manufacturer_part_number == '':
            if report != '':
                report = report + ', ManufacturerPartNumber'
            else:
                report = 'Missing ManufacturerPartNumber'

        if vendor_part_number == '':
            if report != '':
                report = report + ', VendorPartNumber'
            else:
                report = 'Missing VendorPartNumber'

        if date_catalog_received == -1:
            if report != '':
                report = report + ', DateCatalogReceived'
            else:
                report = 'Missing DateCatalogReceived'


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

        if 'FyManufacturerPartNumber' in row:
            fy_manufacturer_part_number = str(row['FyManufacturerPartNumber'])
        else:
            fy_manufacturer_part_number = ''


        if 'PrimaryVendorId' in row:
            primary_vendor_id = int(row['PrimaryVendorId'])
        elif 'VendorId' in row:
            primary_vendor_id = int(row['VendorId'])
        else:
            primary_vendor_id = -1

        if 'VendorListPrice' in row:
            vendor_list_price = float(row['VendorListPrice'])
        else:
            vendor_list_price = -1

        if 'Discount' in row:
            discount = float(row['Discount'])
        else:
            discount = -1

        if 'FyCost' in row:
            fy_cost = float(row['FyCost'])
        else:
            fy_cost = -1

        if 'Estimated Freight' in row:
            estimated_freight = float(row['Estimated Freight'])
        else:
            estimated_freight = -1


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


        if 'FyCategoryId' not in row:
            df_collect_product_base_data['FyCategoryId'] = [-1]
            fy_category_id = -1
        else:
            fy_category_id = int(row['FyCategoryId'])

        fy_is_green = int(row['FyIsGreen'])
        fy_is_latex_free = int(row['FyIsLatexFree'])

        if 'FyColdChain' not in row:
            df_collect_product_base_data['FyColdChain'] = ['']
            fy_cold_chain = ''
        else:
            fy_cold_chain = str(row['FyColdChain'])
            if fy_cold_chain.lower() in ['ambient']:
                fy_cold_chain = 'A'
                df_collect_product_base_data['FyColdChain'] = ['A']
            elif fy_cold_chain.lower() in ['frozen']:
                fy_cold_chain = 'F'
                df_collect_product_base_data['FyColdChain'] = ['F']
            elif fy_cold_chain.lower() in ['refrigerated']:
                fy_cold_chain = 'R'
                df_collect_product_base_data['FyColdChain'] = ['R']

            if fy_cold_chain not in ['A','F','R']:
                fy_cold_chain = ''
                df_collect_product_base_data['FyColdChain'] = ['']

        if 'FyControlledCode' not in row:
            df_collect_product_base_data['FyControlledCode'] = [-1]
            fy_controlled_code = -1
        else:
            fy_controlled_code = int(row['FyControlledCode'])
            if -1 > fy_controlled_code > 5:
                fy_controlled_code = -1


        if 'FyNAICSCodeId' not in row:
            df_collect_product_base_data['FyNAICSCodeId'] = [-1]
            fy_naics_code_id = -1
        else:
            fy_naics_code_id = int(row['FyNAICSCodeId'])

        if 'FyUNSPSCCodeId' not in row:
            df_collect_product_base_data['FyUNSPSCCodeId'] = [-1]
            fy_unspsc_code_id = -1
        else:
            fy_unspsc_code_id = int(row['FyUNSPSCCodeId'])

        if 'FyHazardousSpecialHandlingCodeId' not in row:
            df_collect_product_base_data['FyHazardousSpecialHandlingCodeId'] = [-1]
            fy_special_handling_id = -1
        else:
            fy_special_handling_id = int(row['FyHazardousSpecialHandlingCodeId'])

        if 'FyShelfLifeMonths' not in row:
            df_collect_product_base_data['FyShelfLifeMonths'] = [-1]
            fy_shelf_life_months = -1
        else:
            fy_shelf_life_months = int(row['FyShelfLifeMonths'])

        if 'FyProductNotes' not in row:
            df_collect_product_base_data['FyProductNotes'] = ['']
            fy_product_notes = ''
        else:
            fy_product_notes = str(row['FyProductNotes'])

        fy_is_discontinued = row['FyIsDiscontinued']
        is_visible = row['FyIsVisible']
        allow_purchases = row['FyAllowPurchases']

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

        date_catalog_received = -1
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


        success, deny_gsa = self.process_boolean(row, 'FyDenyGSAContract')
        if success:
            df_collect_product_base_data['FyDenyGSAContract'] = [deny_gsa]
        else:
            deny_gsa = -1

        deny_gsa_date = -1
        if 'FyDenyGSAContractDate' in row:
            try:
                deny_gsa_date = int(row['FyDenyGSAContractDate'])
                deny_gsa_date = (xlrd.xldate_as_datetime(deny_gsa_date, 0)).date()
            except ValueError:
                deny_gsa_date = str(row['FyDenyGSAContractDate'])

        success, deny_va = self.process_boolean(row, 'FyDenyVAContract')
        if success:
            df_collect_product_base_data['FyDenyVAContract'] = [deny_va]
        else:
            deny_va = -1

        deny_va_date = -1
        if 'FyDenyVAContractDate' in row:
            try:
                deny_va_date = int(row['FyDenyVAContractDate'])
                deny_va_date = (xlrd.xldate_as_datetime(deny_va_date, 0)).date()
            except ValueError:
                deny_va_date = str(row['FyDenyVAContractDate'])

        success, deny_ecat = self.process_boolean(row, 'FyDenyECATContract')
        if success:
            df_collect_product_base_data['FyDenyECATContract'] = [deny_ecat]
        else:
            deny_ecat = -1

        deny_ecat_date = -1
        if 'FyDenyECATContractDate' in row:
            try:
                deny_ecat_date = int(row['FyDenyECATContractDate'])
                deny_ecat_date = (xlrd.xldate_as_datetime(deny_ecat_date, 0)).date()
            except ValueError:
                deny_ecat_date = str(row['FyDenyECATContractDate'])

        success, deny_htme = self.process_boolean(row, 'FyDenyHTMEContractDate')
        if success:
            df_collect_product_base_data['FyDenyHTMEContractDate'] = [deny_htme]
        else:
            deny_htme = -1

        deny_htme_date = -1
        if 'FyDenyHTMEContractDate' in row:
            try:
                deny_htme_date = int(row['FyDenyHTMEContractDate'])
                deny_htme_date = (xlrd.xldate_as_datetime(deny_htme_date, 0)).date()
            except ValueError:
                deny_htme_date = str(row['FyDenyHTMEContractDate'])


        if (fy_product_name != '' or fy_product_description != '' or fy_manufacturer_part_number != '' or fy_coo_id != -1 or fy_uoi_id != -1 or fy_uom_id != -1
                or fy_uoi_qty != -1 or fy_lead_time != -1 or fy_is_hazardous != -1 or primary_vendor_id != -1 or secondary_vendor_id != -1
                or fy_is_discontinued != -1 or is_visible != -1 or allow_purchases != -1 or price_toggle != -1 or data_toggle != -1):
            self.obIngester.update_fy_product_description(fy_product_desc_id, fy_product_name, fy_product_description,
                                                          fy_coo_id, fy_manufacturer_part_number, fy_uoi_id, fy_uom_id, fy_uoi_qty, fy_lead_time, fy_is_hazardous,
                                                          primary_vendor_id, secondary_vendor_id,
                                                          fy_category_id, fy_is_green, fy_is_latex_free, fy_cold_chain, fy_controlled_code,
                                                          fy_naics_code_id, fy_unspsc_code_id, fy_special_handling_id, fy_shelf_life_months, fy_product_notes,
                                                          vendor_list_price, discount, fy_cost, estimated_freight,
                                                          fy_landed_cost, markup_percent_fy_sell, fy_sell_price, markup_percent_fy_list, fy_list_price,
                                                          fy_is_discontinued, is_visible, allow_purchases, price_toggle, data_toggle,
                                                          deny_gsa, deny_gsa_date, deny_va, deny_va_date,
                                                          deny_ecat, deny_ecat_date, deny_htme, deny_htme_date,
                                                          date_catalog_received, catalog_provided_by)

        return True, df_collect_product_base_data


    def trigger_ingest_cleanup(self):
        self.obIngester.insert_fy_product_description_cleanup()
        self.obIngester.update_fy_product_description_cleanup()



class FyProductIngest(FyProductUpdate):
    req_fields = ['FyProductNumber', 'FyProductName', 'FyProductDescription', 'FyCountryOfOrigin', 'FyUnitOfIssue',
                  'FyUnitOfIssueQuantity','FyLeadTime', 'ManufacturerPartNumber',
                  'ManufacturerName', 'VendorPartNumber','DateCatalogReceived']

    sup_fields = ['VendorName', 'PrimaryVendorName', 'SecondaryVendorName', 'FyIsHazardous',
                  'FyCategory', 'FyNAICSCode', 'FyUNSPSCCode', 'FyHazardousSpecialHandlingCode',
                  'FyShelfLifeMonths','FyControlledCode','FyIsLatexFree','FyIsGreen','FyColdChain',
                  'FyProductNotes', 'VendorListPrice','FyCost','FyUnitOfMeasure',
                  'Landed Cost','FyLandedCostMarkupPercent_FYSell','FyLandedCostMarkupPercent_FYList',
                  'BCDataUpdateToggle', 'BCPriceUpdateToggle','FyIsDiscontinued','FyAllowPurchases','FyIsVisible']
    att_fields = []
    gen_fields = []

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'FyProduct Ingest'


    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            for each_bool in ['FyIsGreen', 'FyIsLatexFree', 'FyIsHazardous','FyIsDiscontinued','FyIsVisible','FyAllowPurchases','BCPriceUpdateToggle','BCDataUpdateToggle']:
                success, return_val = self.process_boolean(row, each_bool)
                if success:
                    df_collect_product_base_data[each_bool] = [return_val]
                else:
                    df_collect_product_base_data[each_bool] = [-1]

            # check if it is update or not
            if 'ProductDescriptionId' in df_line_product.columns:
                self.obReporter.update_report('Fail','This is an FyProduct update')
                return False, df_collect_product_base_data
            else:
                self.obReporter.update_report('Pass','This is an FyProduct insert')

            df_collect_product_base_data = self.identify_units(df_collect_product_base_data, row)

        df_line_product = df_collect_product_base_data.copy()
        for colName, row in df_line_product.iterrows():
            success, df_collect_product_base_data = self.process_pricing(df_collect_product_base_data, row)
            if not success:
                return success, df_collect_product_base_data

        df_line_product = df_collect_product_base_data.copy()
        for colName, row in df_line_product.iterrows():
            success, df_collect_product_base_data  = self.process_fy_description(df_collect_product_base_data, row)

        return success, df_collect_product_base_data




## end ##