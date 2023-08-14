# CreatedBy: Emilia Crow
# CreateDate: 20220815
# Updated: 20220815
# CreateFor: Franklin Young International

import xlrd
import time
import pandas
import datetime

from Tools.ProgressBar import YesNoDialog
from Tools.BasicProcess import BasicProcessObject


class FyProductUpdate(BasicProcessObject):
    req_fields = ['FyProductNumber']
    sup_fields = ['FyProductName', 'FyProductDescription', 'FyCountryOfOrigin', 'FyUnitOfIssue',
                  'FyUnitOfIssueQuantity','FyUnitOfMeasure','FyLeadTimes', 'FyIsHazardous', 'VendorName', 'PrimaryVendorName', 'SecondaryVendorName',
                  'ManufacturerPartNumber', 'ManufacturerName', 'VendorPartNumber','DateCatalogReceived',
                  'FyCategory', 'FyNAICSCode', 'FyUNSPSCCode', 'FyHazardousSpecialHandlingCode',
                  'FyShelfLifeMonths','FyControlledCode','FyIsLatexFree','FyIsGreen','FyColdChain',
                  'FyProductNotes', 'VendorListPrice','FyCost', 'PrimaryVendorListPrice','PrimaryFyCost',
                  'FyLandedCost','FyLandedCostMarkupPercent_FySell','FyLandedCostMarkupPercent_FyList',
                  'BCDataUpdateToggle', 'BCPriceUpdateToggle','FyIsDiscontinued',
                  'FyDenyGSAContract', 'FyDenyGSAContractDate','FyDenyVAContract', 'FyDenyVAContractDate',
                  'FyDenyECATContract', 'FyDenyECATContractDate','FyDenyHTMEContractDate', 'FyDenyHTMEContractDate', 'FyDenyINTRAMALLSContractDate', 'FyDenyINTRAMALLSContractDate',
                  'GSA_Sin','VA_Sin', 'GSADiscountPercent','VADiscountPercent','MfcDiscountPercent',
                  'WebsiteOnly','GSAEligible','VAEligible','ECATEligible','HTMEEligible','INTRAMALLSEligible',
                  'VendorIsDiscontinued','VendorProductNotes']
    att_fields = []
    gen_fields = []

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'FyProduct Ingest'


    def batch_preprocessing(self):


        self.remove_private_headers()
        self.df_uois_lookup = self.obDal.get_unit_of_issue_symbol_lookup()
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

        self.df_gsa_contract_ids = self.obDal.get_gsa_contract_ids()
        self.df_product = self.df_product.merge(self.df_gsa_contract_ids,how='left',on=['FyProductNumber'])

        self.df_va_contract_ids = self.obDal.get_va_contract_ids()
        self.df_product = self.df_product.merge(self.df_va_contract_ids,how='left',on=['FyProductNumber'])

        if 'PrimaryVendorId' in self.df_product.columns:
            self.df_fy_vendor_price_lookup = self.obDal.get_fy_product_vendor_prices()
            self.df_fy_vendor_price_lookup['PrimaryVendorId'] = self.df_fy_vendor_price_lookup['PrimaryVendorId'].astype(int)
            self.df_product['PrimaryVendorId'] = self.df_product['PrimaryVendorId'].astype(int)
            self.df_product = self.df_product.merge(self.df_fy_vendor_price_lookup,how='left',on=['FyProductNumber','PrimaryVendorId'])

        # add secondary?


    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'BaseProductPriceId','BaseProductPriceId_y','BaseProductPriceId_x',
                           'VendorId','VendorId_x','VendorId_y','UpdateManufacturerName','ManufacturerId',
                           'CategoryId','CategoryId_x','CategoryId_y',
                           'Report','Filter','ProductDescriptionId','db_FyProductName','db_FyProductDescription',
                           'GSAProductPriceId','VAProductPriceId','ECATProductPriceId','HTMEProductPriceId','INTRAMALLSProductPriceId',
                           'db_GSAProductPriceId','db_VAProductPriceId','db_ECATProductPriceId','db_HTMEProductPriceId','db_INTRAMALLSProductPriceId'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
        if remove_headers != []:
            self.df_product = self.df_product.drop(columns=remove_headers)


    def batch_process_country(self):
        if 'CountryOfOriginId' not in self.df_product.columns and 'CountryOfOrigin' in self.df_product.columns:

            df_attribute = self.df_product[['CountryOfOrigin']]
            df_attribute = df_attribute.drop_duplicates(subset=['CountryOfOrigin'])
            lst_ids = []
            for colName, row in df_attribute.iterrows():
                country = row['CountryOfOrigin']
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

            df_attribute['CountryOfOriginId'] = lst_ids
            self.df_product = self.df_product.merge(df_attribute,
                                                              how='left', on=['CountryOfOrigin'])

        if 'FyCountryOfOrigin' in self.df_product.columns:

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
                    new_vendor_id = -1

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
                    new_vendor_id = 0
                elif vendor_name in self.df_vendor_translator['VendorCode'].values:
                    new_vendor_id = self.df_vendor_translator.loc[
                        (self.df_vendor_translator['VendorCode'] == vendor_name),'VendorId'].values[0]
                elif vendor_name in self.df_vendor_translator['VendorName'].values:
                    new_vendor_id = self.df_vendor_translator.loc[
                        (self.df_vendor_translator['VendorName'] == vendor_name),'VendorId'].values[0]
                else:
                    new_vendor_id = 0

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
                    new_vendor_id = 0
                elif vendor_name in self.df_vendor_translator['VendorCode'].values:
                    new_vendor_id = self.df_vendor_translator.loc[
                        (self.df_vendor_translator['VendorCode'] == vendor_name),'VendorId'].values[0]
                elif vendor_name in self.df_vendor_translator['VendorName'].values:
                    new_vendor_id = self.df_vendor_translator.loc[
                        (self.df_vendor_translator['VendorName'] == vendor_name),'VendorId'].values[0]
                else:
                    new_vendor_id = 0

                lst_ids.append(new_vendor_id)

            df_attribute['SecondaryVendorId'] = lst_ids

            self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                     how='left', on=['SecondaryVendorName'])


    def batch_process_category(self):
        print('Batch processing Categories')
        # this needs to be handled better
        if 'FyCategory' in self.df_product.columns:
            df_attribute = self.df_product[['FyCategory']]
            df_attribute = df_attribute.drop_duplicates(subset=['FyCategory'])
            lst_ids = []
            for colName, row in df_attribute.iterrows():
                category = str(row['FyCategory']).strip()
                if category == '':
                    new_category_id = -1
                    lst_ids.append(new_category_id)
                    continue

                #replace gaps
                while '/ ' in category:
                    category = category.replace('/ ', '/')

                while ' /' in category:
                    category = category.replace(' /', '/')

                # the name of the category is the bottom-most level
                category_name = (category.rpartition('/')[2]).strip()

                # normalize casing
                self.df_category_names['CategoryNameLower'] = self.df_category_names['CategoryName'].str.lower()

                # ident by name
                if category_name in self.df_category_names['CategoryName'].values:
                    new_category_id = self.df_category_names.loc[
                        (self.df_category_names['CategoryName'] == category_name), 'CategoryId'].values[0]

                # compare by normalized name
                elif category_name.lower() in self.df_category_names['CategoryNameLower'].values:
                    new_category_id = self.df_category_names.loc[
                        (self.df_category_names['CategoryNameLower'] == category_name.lower()), 'CategoryId'].values[0]

                # try the whole category
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

            df_attribute['FyCategoryId'] = lst_ids

            self.df_product = self.df_product.merge(df_attribute,
                                                     how='left', on=['FyCategory'])
        # try to pull the current values
        # this might not be necessary
        elif 'ProductDescriptionId' in self.df_product.columns:
            print('get category by product description')
            self.df_fill_category = self.obDal.get_fy_product_category()
            self.df_product = self.df_product.merge(self.df_fill_category,how='left',on=['ProductDescriptionId'])
            del self.df_fill_category

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
            fy_product_number = row['FyProductNumber']
            if self.test_product_number(fy_product_number) == False:
                self.obReporter.update_report('Fail','There\'s an error in your product number')
                return False, df_collect_product_base_data

            if 'BlockedManufacturer' in row:
                if int(row['BlockedManufacturer']) == 1:
                    self.obReporter.update_report('Fail','This manufacturer name is blocked from processing')
                    return False, df_collect_product_base_data


            for each_bool in ['FyIsGreen', 'FyIsLatexFree', 'FyIsHazardous','FyIsDiscontinued','BCPriceUpdateToggle','BCDataUpdateToggle',
                              'GSAOnContract','GSAPricingApproved','VAOnContract','VAPricingApproved']:
                success, return_val = self.process_boolean(row, each_bool)
                if success:
                    df_collect_product_base_data[each_bool] = [return_val]
                else:
                    df_collect_product_base_data[each_bool] = [-1]

            if ('VendorId' in row):
                vendor_id = row['VendorId']
                if vendor_id == -1:
                    self.obReporter.update_report('Fail','Bad vendor name.')
                    return False, df_collect_product_base_data

            if ('PrimaryVendorId' in row):
                primary_vendor_id = row['PrimaryVendorId']
                if primary_vendor_id == -1:
                    self.obReporter.update_report('Fail','Bad primary vendor name.')
                    return False, df_collect_product_base_data

            if ('SecondaryVendorId' in row):
                secondary_vendor_id = row['SecondaryVendorId']
                if secondary_vendor_id == -1:
                    self.obReporter.update_report('Fail','Bad secondary vendor name.')
                    return False, df_collect_product_base_data


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
                if success:
                    self.gsa_product_price(row)
                    self.va_product_price(row)


        return success, df_collect_product_base_data


    def identify_units(self, df_collect_product_base_data, row):
        # set quantities
        fy_unit_of_issue = -1
        if 'FyUnitOfIssue' in row:
            fy_unit_of_issue = self.normalize_units(row['FyUnitOfIssue'])
            df_collect_product_base_data['FyUnitOfIssue'] = [fy_unit_of_issue]

        # if it tries to look it up and it doesn't work, we set it -1
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

        vlp_success, vendor_list_price = self.row_check(row, 'VendorListPrice')
        if vlp_success:
            vlp_success, vendor_list_price = self.float_check(vendor_list_price, 'VendorListPrice')
            df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]
        else:

            vlp_success, vendor_list_price = self.row_check(row, 'PrimaryVendorListPrice')
            if vlp_success:
                vlp_success, vendor_list_price = self.float_check(vendor_list_price, 'PrimaryVendorListPrice')
                df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]
            else:
                vlp_success, vendor_list_price = self.row_check(row, 'db_PrimaryVendorListPrice')
                if vlp_success:
                    vlp_success, vendor_list_price = self.float_check(vendor_list_price, 'db_PrimaryVendorListPrice')
                    df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]
                else:
                    vlp_success, vendor_list_price = self.row_check(row, 'CurrentVendorListPrice')
                    if vlp_success:
                        vlp_success, vendor_list_price = self.float_check(vendor_list_price, 'CurrentVendorListPrice')
                        df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]
                    else:
                        vendor_list_price = -1
                        df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]

        discount_success, fy_discount_percent = self.row_check(row, 'Discount')
        if discount_success:
            discount_success, fy_discount_percent = self.float_check(fy_discount_percent, 'Discount')
            df_collect_product_base_data['Discount'] = [fy_discount_percent]
        else:
            discount_success, fy_discount_percent = self.row_check(row, 'PrimaryDiscount')
            if discount_success:
                discount_success, fy_discount_percent = self.float_check(fy_discount_percent, 'PrimaryDiscount')
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

        fy_cost_success, fy_cost = self.row_check(row, 'FyCost')
        if fy_cost_success:
            fy_cost_success, fy_cost = self.float_check(fy_cost, 'FyCost')
        else:
            fy_cost_success, fy_cost = self.row_check(row, 'PrimaryFyCost')
            if fy_cost_success:
                fy_cost_success, fy_cost = self.float_check(fy_cost, 'PrimaryFyCost')
                df_collect_product_base_data['FyCost'] = [fy_cost]
            else:
                fy_cost_success, fy_cost = self.row_check(row, 'db_PrimaryFyCost')
                if fy_cost_success:
                    fy_cost_success, fy_cost = self.float_check(fy_cost, 'db_PrimaryFyCost')
                    df_collect_product_base_data['FyCost'] = [fy_cost]
                else:
                    fy_cost_success, fy_cost = self.row_check(row, 'CurrentFyCost')
                    if fy_cost_success:
                        fy_cost_success, fy_cost = self.float_check(fy_cost, 'CurrentFyCost')
                        df_collect_product_base_data['FyCost'] = [fy_cost]
                    else:
                        # fail line if missing
                        self.obReporter.update_report('Alert', 'FyCost was missing')
                        fy_cost = -1
                        df_collect_product_base_data['FyCost'] = [fy_cost]

        if (fy_cost_success and vlp_success) and (not discount_success or fy_discount_percent == 0):
            fy_discount_percent = self.set_vendor_discount(fy_cost, vendor_list_price)
            df_collect_product_base_data['Discount'] = [fy_discount_percent]

        elif (fy_cost_success and discount_success) and not vlp_success:
            vendor_list_price = self.set_vendor_list(fy_cost, fy_discount_percent)
            df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]

        # checks for shipping costs
        freight_success, estimated_freight = self.row_check(row, 'EstimatedFreight')
        if freight_success:
            freight_success, estimated_freight = self.float_check(estimated_freight,'EstimatedFreight')
            if freight_success:
                df_collect_product_base_data['EstimatedFreight'] = [estimated_freight]
            else:
                estimated_freight = -1
                df_collect_product_base_data['EstimatedFreight'] = [estimated_freight]

        if not freight_success:
            freight_success, estimated_freight = self.row_check(row, 'PrimaryEstimatedFreight')
            if freight_success:
                freight_success, estimated_freight = self.float_check(estimated_freight,'PrimaryEstimatedFreight')
                if freight_success:
                    df_collect_product_base_data['EstimatedFreight'] = [estimated_freight]
                else:
                    estimated_freight = -1
                    df_collect_product_base_data['EstimatedFreight'] = [estimated_freight]

        if not freight_success:
            freight_success, estimated_freight = self.row_check(row, 'db_PrimaryEstimatedFreight')
            if freight_success:
                freight_success, estimated_freight = self.float_check(estimated_freight,'db_PrimaryEstimatedFreight')
                if freight_success:
                    df_collect_product_base_data['EstimatedFreight'] = [estimated_freight]
                else:
                    estimated_freight = -1
                    df_collect_product_base_data['EstimatedFreight'] = [estimated_freight]

        if not freight_success:
            freight_success, estimated_freight = self.row_check(row, 'CurrentEstimatedFreight')
            if freight_success:
                freight_success, estimated_freight = self.float_check(estimated_freight,'CurrentEstimatedFreight')
                if freight_success:
                    df_collect_product_base_data['EstimatedFreight'] = [estimated_freight]
                else:
                    estimated_freight = -1
                    df_collect_product_base_data['EstimatedFreight'] = [estimated_freight]

            elif estimated_freight != -1:
                estimated_freight = 0
                df_collect_product_base_data['EstimatedFreight'] = [estimated_freight]
                self.obReporter.update_report('Alert', 'EstimatedFreight value was set to 0')

        if 'FyLandedCost' in row:
            fy_landed_cost = round(float(row['FyLandedCost']), 2)
            df_collect_product_base_data['FyLandedCost'] = [fy_landed_cost]

        elif fy_cost_success and freight_success:
            fy_landed_cost = round(fy_cost + estimated_freight, 2)
            self.obReporter.update_report('Alert', 'FyLandedCost was calculated')
            df_collect_product_base_data['FyLandedCost'] = [fy_landed_cost]
        else:
            fy_landed_cost = -1


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
            if db_mul_success:
                if db_markup_list <= 0:
                    db_mul_success = False
                    self.obReporter.update_report('Alert','DB Markup List negative')
                elif db_markup_list <= 1:
                    db_mul_success = False
                    self.obReporter.update_report('Alert','DB Markup List too low')
            else:
                mus_success = False
                self.obReporter.update_report('Fail','DB Markup List too low was not a number')

        # get the markups from the file
        mus_success, markup_sell = self.row_check(row, 'FyLandedCostMarkupPercent_FySell')
        if mus_success:
            mus_success, markup_sell = self.float_check(markup_sell, 'FyLandedCostMarkupPercent_FySell')
            if mus_success:
                if markup_sell <= 0:
                    mus_success = False
                    self.obReporter.update_report('Alert','Markup Sell negative')
                elif markup_sell <= 1:
                    mus_success = False
                    self.obReporter.update_report('Alert','Markup Sell too low')
                else:
                    df_collect_product_base_data['FyLandedCostMarkupPercent_FySell'] = [markup_sell]
            else:
                mus_success = False
                self.obReporter.update_report('Fail','Markup Sell was not a number')


        if not mus_success and db_mus_success:
            markup_sell = db_markup_sell
            df_collect_product_base_data['FyLandedCostMarkupPercent_FySell'] = [db_markup_sell]


        mul_success, markup_list = self.row_check(row, 'FyLandedCostMarkupPercent_FyList')
        if mul_success:
            mul_success, markup_list = self.float_check(markup_list, 'FyLandedCostMarkupPercent_FyList')
            if mul_success:
                if markup_list <= 0:
                    mul_success = False
                    self.obReporter.update_report('Alert','Markup List negative')
                elif markup_list <= 1:
                    mul_success = False
                    self.obReporter.update_report('Alert','Markup List too low')
                else:
                    df_collect_product_base_data['FyLandedCostMarkupPercent_FyList'] = [markup_list]
            else:
                self.obReporter.update_report('Fail','Markup List was not a number')

        if not mul_success and db_mul_success:
            markup_list = db_markup_list
            df_collect_product_base_data['FyLandedCostMarkupPercent_FyList'] = [db_markup_list]

        # let's report if they're both missing
        if (not db_mus_success and not mus_success) and (not db_mul_success and not mul_success):
            self.obReporter.update_report('Fail', 'No markups not present')
            markup_sell = 0
            markup_list = 0
            df_collect_product_base_data['FyLandedCostMarkupPercent_FySell'] = [markup_sell]
            df_collect_product_base_data['FyLandedCostMarkupPercent_FyList'] = [markup_list]

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
        try:
            fy_sell_price_long = float(fy_landed_cost) * markup_sell
        except TypeError:
            print('landed cost', fy_landed_cost)
            print('markup', markup_sell)
            x = input('x')

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
        try:
            fy_list_price_long = float(fy_landed_cost) * markup_list
        except TypeError:
            print('landed cost', fy_landed_cost)
            print('mark up', markup_list)
            x = input('x')

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

        if 'FyLeadTimes' in row:
            fy_lead_time = int(row['FyLeadTimes'])
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
        fy_landed_cost = row['FyLandedCost']
        if fy_landed_cost != 0:
            is_hot_price = True

        if 'FyLandedCostMarkupPercent_FySell' not in row:
            df_collect_product_base_data['FyLandedCostMarkupPercent_FySell'] = [0]
            markup_percent_fy_sell = 0
        else:
            markup_percent_fy_sell = float(row['FyLandedCostMarkupPercent_FySell'])

        if 'FySellPrice' not in row:
            df_collect_product_base_data['FySellPrice'] = [0]
            fy_sell_price = 0
        else:
            fy_sell_price = row['FySellPrice']

        if is_hot_price and fy_sell_price == 0:
            self.obReporter.update_report('Fail','Sell price did not calculate')
            return False, df_collect_product_base_data

        if 'FyLandedCostMarkupPercent_FyList' not in row:
            df_collect_product_base_data['FyLandedCostMarkupPercent_FyList'] = [0]
            markup_percent_fy_list = 0
        else:
            markup_percent_fy_list = float(row['FyLandedCostMarkupPercent_FyList'])

        if 'FyListPrice' not in row:
            df_collect_product_base_data['FyListPrice'] = [0]
            fy_list_price = 0
        else:
            fy_list_price = float(row['FyListPrice'])

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
            if 0 > fy_controlled_code or fy_controlled_code > 5:
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

        success, av_toggle = self.process_boolean(row, 'AVInclusionToggle')
        if success:
            df_collect_product_base_data['AVInclusionToggle'] = [av_toggle]
        else:
            av_toggle = 1

        if ' ' in fy_product_number:
            fy_catalog_number = fy_product_number.partition(' ')[0]
        else:
            fy_catalog_number = fy_product_number

        manufacturer_part_number = ''
        if 'ManufacturerPartNumber' in row:
            manufacturer_part_number = str(row['ManufacturerPartNumber'])

        fy_manufacturer_part_number = manufacturer_part_number
        if 'FyManufacturerPartNumber' in row:
            fy_manufacturer_part_number = str(row['FyManufacturerPartNumber'])

        # this checks for dropped 0
        if fy_catalog_number[5] == '0':
            if manufacturer_part_number[0] != '0':
                self.obReporter.update_report('Fail', 'ManufacturerPartNumber dropped zeros')
                return False, df_collect_product_base_data
            if fy_manufacturer_part_number[0] != '0':
                self.obReporter.update_report('Fail', 'FyManufacturerPartNumber dropped zeros')
                return False, df_collect_product_base_data


        manufacturer_id = -1
        if 'ManufacturerId' in row:
            manufacturer_id = int(row['ManufacturerId'])

        default_image_id = -1
        if 'DefaultImageId' in row:
            default_image_id = int(row['DefaultImageId'])

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
            date_catalog_received = row['DateCatalogReceived']
            try:
                date_catalog_received = int(date_catalog_received)
                date_catalog_received = xlrd.xldate_as_datetime(date_catalog_received, 0)
            except ValueError:
                date_catalog_received = str(row['DateCatalogReceived'])

            if isinstance(date_catalog_received, datetime.datetime) == False:
                try:
                    date_catalog_received = datetime.datetime.strptime(date_catalog_received, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    date_catalog_received = str(row['DateCatalogReceived'])
                    self.obReporter.update_report('Alert','Check DateCatalogReceived')
                except TypeError:
                    self.obReporter.update_report('Alert','Check DateCatalogReceived')

        catalog_provided_by = ''
        if 'CatalogProvidedBy' in row:
            catalog_provided_by = str(row['CatalogProvidedBy'])

        if 'VendorListPrice' in row:
            vendor_list_price = float(row['VendorListPrice'])
        else:
            vendor_list_price = 0

        if 'Discount' in row:
            fy_discount_percent = float(row['Discount'])
            success, fy_discount_percent = self.handle_percent_val(fy_discount_percent)
            if not success:
                fy_discount_percent = 0
        else:
            fy_discount_percent = 0

        if 'FyCost' in row:
            fy_cost = float(row['FyCost'])
        else:
            fy_cost = 0

        if 'EstimatedFreight' in row:
            estimated_freight = float(row['EstimatedFreight'])
        else:
            estimated_freight = 0


        vendor_product_notes = ''
        if 'VendorProductNotes' in row:
            vendor_product_notes = str(row['VendorProductNotes'])

        success, vendor_is_discontinued = self.process_boolean(row, 'VendorIsDiscontinued')
        if success:
            df_collect_product_base_data['VendorIsDiscontinued'] = [vendor_is_discontinued]
        else:
            vendor_is_discontinued = -1

        if 'VendorProductName' in row:
            vendor_product_name = str(row['VendorProductName'])
        else:
            vendor_product_name = ''
        if 'VendorProductDescription' in row:
            vendor_product_description = str(row['VendorProductDescription'])
        else:
            vendor_product_description = ''

        if 'CountryOfOriginId' in row:
            country_of_origin_id = int(row['CountryOfOriginId'])
        else:
            country_of_origin_id = -1

        success, b_website_only = self.process_boolean(row, 'WebsiteOnly')
        if success:
            df_collect_product_base_data['WebsiteOnly'] = [b_website_only]
        else:
            b_website_only = -1

        success, gsa_eligible = self.process_boolean(row, 'GSAEligible')
        if success:
            df_collect_product_base_data['GSAEligible'] = [gsa_eligible]
        else:
            gsa_eligible = -1

        success, ecat_eligible = self.process_boolean(row, 'ECATEligible')
        if success:
            df_collect_product_base_data['ECATEligible'] = [ecat_eligible]
        else:
            ecat_eligible = -1

        success, htme_eligible = self.process_boolean(row, 'HTMEEligible')
        if success:
            df_collect_product_base_data['HTMEEligible'] = [htme_eligible]
        else:
            htme_eligible = -1

        success, intramalls_eligible = self.process_boolean(row, 'INTRAMALLSEligible')
        if success:
            df_collect_product_base_data['INTRAMALLSEligible'] = [intramalls_eligible]
        else:
            intramalls_eligible = -1

        success, va_eligible = self.process_boolean(row, 'VAEligible')
        if success:
            df_collect_product_base_data['VAEligible'] = [va_eligible]
        else:
            va_eligible = -1


        if (fy_product_name != '' and fy_product_description != '' and fy_coo_id != -1 and fy_uoi_id != -1 and
                fy_uom_id != -1 and fy_uoi_qty != -1 and fy_lead_time != -1 and primary_vendor_id != -1 and manufacturer_part_number != '' and
                manufacturer_id != -1 and vendor_part_number != '' and date_catalog_received != -1):

            success, gsa_on_contract = self.process_boolean(row, 'GSAOnContract')
            if success:
                df_collect_product_base_data['GSAOnContract'] = [gsa_on_contract]
            else:
                gsa_on_contract = -1

            gsa_contract_number = 'GS-07F-0636W'

            if 'GSAContractModificationNumber' in row:
                gsa_contract_mod_number = row['GSAContractModificationNumber']
            else:
                gsa_contract_mod_number = ''

            if 'GSAPricingApproved' in row:
                gsa_is_pricing_approved = float(row['GSAPricingApproved'])
            else:
                gsa_is_pricing_approved = -1

            gsa_approved_price_date = -1
            if 'GSAApprovedPriceDate' in row:
                gsa_approved_price_date = row['GSAApprovedPriceDate']
                try:
                    gsa_approved_price_date = int(gsa_approved_price_date)
                    gsa_approved_price_date = xlrd.xldate_as_datetime(gsa_approved_price_date, 0)
                except ValueError:
                    gsa_approved_price_date = str(row['GSAApprovedPriceDate'])

                if isinstance(gsa_approved_price_date, datetime.datetime) == False:
                    try:
                        gsa_approved_price_date = datetime.datetime.strptime(gsa_approved_price_date, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        gsa_approved_price_date = str(row['GSAApprovedPriceDate'])
                        self.obReporter.update_report('Alert','Check GSAApprovedPriceDate')
                    except TypeError:
                        self.obReporter.update_report('Alert','Check GSAApprovedPriceDate')

            if 'GSAApprovedBasePrice' in row:
                gsa_approved_base_price = float(row['GSAApprovedBasePrice'])
            else:
                gsa_approved_base_price = -1

            if 'GSAApprovedSellPrice' in row:
                gsa_approved_sell_price = float(row['GSAApprovedSellPrice'])
            else:
                gsa_approved_sell_price = -1

            if 'GSAApprovedListPrice' in row:
                gsa_approved_list_price = float(row['GSAApprovedListPrice'])
            else:
                gsa_approved_list_price = -1


            if 'GSADiscountPercent' in row:
                gsa_approved_percent = row['GSADiscountPercent']
                success, gsa_approved_percent = self.handle_percent_val(gsa_approved_percent)
                if not success:
                    return success, df_collect_product_base_data
            else:
                gsa_approved_percent = -1

            if 'MfcDiscountPercent' in row:
                mfc_percent = row['MfcDiscountPercent']
                success, mfc_percent = self.handle_percent_val(mfc_percent)
                if not success:
                    return success, df_collect_product_base_data
            else:
                mfc_percent = -1

            if 'GSA_Sin' in row:
                gsa_sin = row['GSA_Sin']
            else:
                gsa_sin = ''

            gsa_product_notes = ''
            if 'GSAProductNotes' in row:
                gsa_product_notes = str(row['GSAProductNotes'])


            success, va_on_contract = self.process_boolean(row, 'VAOnContract')
            if success:
                df_collect_product_base_data['VAOnContract'] = [va_on_contract]
            else:
                va_on_contract = -1

            va_contract_number = 'VA797H-16-D-0024/SPE2D1-16-D-0019'
            if 'VAContractModificationNumber' in row:
                va_contract_mod_number = row['VAContractModificationNumber']
            else:
                va_contract_mod_number = ''

            if 'VAPricingApproved' in row:
                va_is_pricing_approved = float(row['VAPricingApproved'])
            else:
                va_is_pricing_approved = -1

            va_approved_price_date = -1
            if 'VAApprovedPriceDate' in row:
                va_approved_price_date = row['VAApprovedPriceDate']
                try:
                    va_approved_price_date = int(va_approved_price_date)
                    va_approved_price_date = xlrd.xldate_as_datetime(va_approved_price_date, 0)
                except ValueError:
                    va_approved_price_date = str(row['VAApprovedPriceDate'])

                if isinstance(va_approved_price_date, datetime.datetime) == False:
                    try:
                        va_approved_price_date = datetime.datetime.strptime(va_approved_price_date, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        va_approved_price_date = str(row['VAApprovedPriceDate'])
                        self.obReporter.update_report('Alert','Check VAApprovedPriceDate')
                    except TypeError:
                        self.obReporter.update_report('Alert','Check VAApprovedPriceDate')

            if 'VAApprovedBasePrice' in row:
                va_approved_base_price = float(row['VAApprovedBasePrice'])
            else:
                va_approved_base_price = -1

            if 'VAApprovedSellPrice' in row:
                va_approved_sell_price = float(row['VAApprovedSellPrice'])
            else:
                va_approved_sell_price = -1

            if 'VAApprovedListPrice' in row:
                va_approved_list_price = float(row['VAApprovedListPrice'])
            else:
                va_approved_list_price = -1

            if 'VADiscountPercent' in row:
                va_approved_percent = float(row['VADiscountPercent'])
                success, va_approved_percent = self.handle_percent_val(va_approved_percent)
                if not success:
                    return success, df_collect_product_base_data
            else:
                va_approved_percent = -1

            if 'VA_Sin' in row:
                va_sin = row['VA_Sin']
            else:
                va_sin = ''

            va_product_notes = ''
            if 'VAProductNotes' in row:
                va_product_notes = str(row['VAProductNotes'])


            if (mfc_percent != -1 or gsa_approved_percent != -1 or va_approved_percent != -1 or gsa_sin != '' or va_sin != ''):
                self.obIngester.insert_fy_product_description_contract(fy_catalog_number, fy_manufacturer_part_number, manufacturer_part_number, is_product_number_override,
                                                                        manufacturer_id, default_image_id, fy_product_number, fy_product_name, fy_product_description,
                                                                        fy_coo_id, fy_uoi_id, fy_uom_id, fy_uoi_qty, product_tax_class,
                                                                        vendor_part_number, fy_lead_time, fy_is_hazardous, primary_vendor_id,
                                                                        secondary_vendor_id, fy_category_id, fy_is_green, fy_is_latex_free,
                                                                        fy_cold_chain, fy_controlled_code, fy_naics_code_id, fy_unspsc_code_id,
                                                                        fy_special_handling_id, fy_shelf_life_months, fy_product_notes,
                                                                        vendor_product_notes, vendor_is_discontinued, vendor_product_name, vendor_product_description, country_of_origin_id,
                                                                        b_website_only, gsa_eligible, va_eligible, ecat_eligible, htme_eligible, intramalls_eligible,
                                                                        vendor_list_price, fy_discount_percent, fy_cost, estimated_freight, fy_landed_cost,
                                                                        markup_percent_fy_sell, fy_sell_price, markup_percent_fy_list,
                                                                        fy_list_price, fy_is_discontinued,
                                                                        price_toggle, data_toggle, av_toggle,
                                                                        date_catalog_received, catalog_provided_by,

                                                                        gsa_on_contract, gsa_approved_base_price,
                                                                        gsa_approved_sell_price, gsa_approved_list_price,
                                                                        gsa_contract_number, gsa_contract_mod_number,
                                                                        gsa_is_pricing_approved,
                                                                        gsa_approved_price_date, gsa_approved_percent,
                                                                        mfc_percent, gsa_sin, gsa_product_notes,

                                                                        va_on_contract, va_approved_base_price,
                                                                        va_approved_sell_price, va_approved_list_price,
                                                                        va_contract_number, va_contract_mod_number,
                                                                        va_is_pricing_approved,
                                                                        va_approved_price_date, va_approved_percent,
                                                                        va_sin, va_product_notes

                                                                       )
                return True, df_collect_product_base_data
            else:
                # this needs to proper ingest the info
                self.obIngester.insert_fy_product_description(fy_catalog_number, fy_manufacturer_part_number, manufacturer_part_number, is_product_number_override,
                                                              manufacturer_id, default_image_id, fy_product_number, fy_product_name, fy_product_description,
                                                              fy_coo_id, fy_uoi_id, fy_uom_id, fy_uoi_qty, product_tax_class,
                                                              vendor_part_number, fy_lead_time, fy_is_hazardous, primary_vendor_id,
                                                              secondary_vendor_id, fy_category_id, fy_is_green, fy_is_latex_free,
                                                              fy_cold_chain, fy_controlled_code, fy_naics_code_id, fy_unspsc_code_id,
                                                              fy_special_handling_id, fy_shelf_life_months, fy_product_notes,
                                                              vendor_product_notes, vendor_is_discontinued, vendor_product_name, vendor_product_description, country_of_origin_id,
                                                              b_website_only, gsa_eligible, va_eligible, ecat_eligible, htme_eligible, intramalls_eligible,
                                                              vendor_list_price, fy_discount_percent, fy_cost, estimated_freight, fy_landed_cost,
                                                              markup_percent_fy_sell, fy_sell_price, markup_percent_fy_list,
                                                              fy_list_price, fy_is_discontinued,
                                                              price_toggle, data_toggle, av_toggle,
                                                                  date_catalog_received, catalog_provided_by)

                return True, df_collect_product_base_data

        report = ''
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
                report = report + ', FyLeadTimes'
            else:
                report = 'Missing FyLeadTimes'

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

        if 'FyLeadTimes' in row:
            fy_lead_time = int(row['FyLeadTimes'])
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

        fy_catalog_number = str(row['FyCatalogNumber'])
        # this checks for dropped 0
        if fy_catalog_number[5] == '0':
            if fy_manufacturer_part_number[0] != '0':
                self.obReporter.update_report('Fail', 'FyManufacturerPartNumber dropped zeros')
                return False, df_collect_product_base_data

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

        if 'EstimatedFreight' in row:
            try:
                estimated_freight = float(row['EstimatedFreight'])
            except ValueError:
                estimated_freight = -1
        else:
            estimated_freight = -1

        if 'SecondaryVendorId' in row:
            secondary_vendor_id = int(row['SecondaryVendorId'])
        else:
            secondary_vendor_id = -1

        fy_landed_cost = row['FyLandedCost']

        try:
            markup_percent_fy_sell = row['FyLandedCostMarkupPercent_FySell']
        except KeyError:
            for colName2, row2 in df_collect_product_base_data.iterrows():
                print(row2)
            reports = self.obReporter.get_report()
            print('pass', reports[0])
            print('alert', reports[1])
            print('fail', reports[2])
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

        success, av_toggle = self.process_boolean(row, 'AVInclusionToggle')
        if success:
            df_collect_product_base_data['AVInclusionToggle'] = [av_toggle]
        else:
            av_toggle = - 1

        date_catalog_received = -1
        if 'DateCatalogReceived' in row:
            date_catalog_received = row['DateCatalogReceived']
            try:
                date_catalog_received = int(date_catalog_received)
                date_catalog_received = xlrd.xldate_as_datetime(date_catalog_received, 0)
            except ValueError:
                date_catalog_received = str(row['DateCatalogReceived'])

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

        vendor_product_notes = ''
        if 'VendorProductNotes' in row:
            vendor_product_notes = str(row['VendorProductNotes'])

        success, vendor_is_discontinued = self.process_boolean(row, 'VendorIsDiscontinued')
        if success:
            df_collect_product_base_data['VendorIsDiscontinued'] = [vendor_is_discontinued]
        else:
            vendor_is_discontinued = -1

        if 'VendorProductName' in row:
            vendor_product_name = str(row['VendorProductName'])
        else:
            vendor_product_name = ''
        if 'VendorProductDescription' in row:
            vendor_product_description = str(row['VendorProductDescription'])
        else:
            vendor_product_description = ''
        if 'CountryOfOriginId' in row:
            country_of_origin_id = int(row['CountryOfOriginId'])
        else:
            country_of_origin_id = -1


        success, b_website_only = self.process_boolean(row, 'WebsiteOnly')
        if success:
            df_collect_product_base_data['WebsiteOnly'] = [b_website_only]
        else:
            b_website_only = -1

        success, ecat_eligible = self.process_boolean(row, 'ECATEligible')
        if success:
            df_collect_product_base_data['ECATEligible'] = [ecat_eligible]
        else:
            ecat_eligible = -1

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

        success, intramalls_eligible = self.process_boolean(row, 'INTRAMALLSEligible')
        if success:
            df_collect_product_base_data['INTRAMALLSEligible'] = [intramalls_eligible]
        else:
            intramalls_eligible = -1

        success, va_eligible = self.process_boolean(row, 'VAEligible')
        if success:
            df_collect_product_base_data['VAEligible'] = [va_eligible]
        else:
            va_eligible = -1


        if (fy_product_name != '' or fy_product_description != '' or fy_manufacturer_part_number != '' or fy_coo_id != -1 or fy_uoi_id != -1 or fy_uom_id != -1
                or fy_uoi_qty != -1 or fy_lead_time != -1 or fy_is_hazardous != -1 or primary_vendor_id != -1 or secondary_vendor_id != -1
                or fy_is_discontinued != -1 or price_toggle != -1 or data_toggle != -1 or date_catalog_received != -1):

            self.obIngester.update_fy_product_description(fy_product_desc_id, fy_product_name, fy_product_description,
                                                          fy_coo_id, fy_manufacturer_part_number, fy_uoi_id, fy_uom_id, fy_uoi_qty, fy_lead_time, fy_is_hazardous,
                                                          primary_vendor_id, secondary_vendor_id,
                                                          fy_category_id, fy_is_green, fy_is_latex_free, fy_cold_chain, fy_controlled_code,
                                                          fy_naics_code_id, fy_unspsc_code_id, fy_special_handling_id, fy_shelf_life_months, fy_product_notes,
                                                          vendor_product_notes, vendor_is_discontinued, vendor_product_name, vendor_product_description, country_of_origin_id,
                                                          b_website_only, gsa_eligible, va_eligible, ecat_eligible, htme_eligible, intramalls_eligible,
                                                          vendor_list_price, discount, fy_cost, estimated_freight,
                                                          fy_landed_cost, markup_percent_fy_sell, fy_sell_price, markup_percent_fy_list, fy_list_price,
                                                          fy_is_discontinued, price_toggle, data_toggle, av_toggle,
                                                          date_catalog_received, catalog_provided_by)



        gsa_sin = ''
        if (gsa_sin != ''):
            self.obIngester.gsa_product_price_update()

        va_sin = ''
        if (va_sin != ''):
            self.obIngester.va_product_price_update()


        return True, df_collect_product_base_data


    def gsa_product_price(self, row):
        product_description_id = row['ProductDescriptionId']
        fy_product_number = row['FyProductNumber']
        contract_number = 'GS-07F-0636W'

        if 'GSAOnContract' in row:
            on_contract = row['GSAOnContract']
        else:
            on_contract = -1

        if 'GSAContractModificationNumber' in row:
            contract_mod_number = row['GSAContractModificationNumber']
        else:
            contract_mod_number = ''

        if 'GSAPricingApproved' in row:
            is_pricing_approved = float(row['GSAPricingApproved'])
        else:
            is_pricing_approved = -1

        gsa_approved_price_date = -1
        if 'GSAApprovedPriceDate' in row:
            gsa_approved_price_date = row['GSAApprovedPriceDate']
            try:
                gsa_approved_price_date = int(gsa_approved_price_date)
                gsa_approved_price_date = xlrd.xldate_as_datetime(gsa_approved_price_date, 0)
            except ValueError:
                gsa_approved_price_date = str(row['GSAApprovedPriceDate'])

            if isinstance(gsa_approved_price_date, datetime.datetime) == False:
                try:
                    gsa_approved_price_date = datetime.datetime.strptime(gsa_approved_price_date, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    gsa_approved_price_date = str(row['GSAApprovedPriceDate'])
                    self.obReporter.update_report('Alert','Check GSAApprovedPriceDate')
                except TypeError:
                    self.obReporter.update_report('Alert','Check GSAApprovedPriceDate')


        if 'GSAApprovedBasePrice' in row:
            approved_base_price = float(row['GSAApprovedBasePrice'])
        else:
            approved_base_price = -1

        if 'GSAApprovedSellPrice' in row:
            approved_sell_price = float(row['GSAApprovedSellPrice'])
        else:
            approved_sell_price = -1

        if 'GSAApprovedListPrice' in row:
            approved_list_price = float(row['GSAApprovedListPrice'])
        else:
            approved_list_price = -1


        if 'GSADiscountPercent' in row:
            approved_percent = float(row['GSADiscountPercent'])
        else:
            approved_percent = -1

        if 'MfcDiscountPercent' in row:
            mfc_percent = float(row['MfcDiscountPercent'])
        else:
            mfc_percent = -1

        if 'GSA_Sin' in row:
            sin = row['GSA_Sin']
        else:
            sin = ''

        gsa_product_notes = ''
        if 'GSAProductNotes' in row:
            gsa_product_notes = str(row['GSAProductNotes'])

        gsa_product_price_id = -1
        if 'GSAProductPriceId' in row:
            gsa_product_price_id = int(row['GSAProductPriceId'])

        if gsa_product_price_id == -1 and 'db_GSAProductPriceId' in row:
            gsa_product_price_id = int(row['db_GSAProductPriceId'])


        if (on_contract != -1 or contract_mod_number != '' or is_pricing_approved != -1 or gsa_approved_price_date != -1
                or approved_base_price != -1 or approved_sell_price != -1 or approved_list_price != -1 or approved_percent != -1
                or sin != '' or gsa_product_notes != ''):
            if gsa_product_price_id == -1:
                self.obIngester.gsa_product_price_insert(product_description_id, fy_product_number, on_contract, approved_base_price,
                                                  approved_sell_price, approved_list_price,
                                                  contract_number, contract_mod_number, is_pricing_approved,
                                                  gsa_approved_price_date, approved_percent, mfc_percent,
                                                  sin, gsa_product_notes)
            else:
                self.obIngester.gsa_product_price_update(gsa_product_price_id, product_description_id, fy_product_number, on_contract, approved_base_price,
                                                  approved_sell_price, approved_list_price,
                                                  contract_number, contract_mod_number, is_pricing_approved,
                                                  gsa_approved_price_date, approved_percent, mfc_percent,
                                                  sin, gsa_product_notes)


    def va_product_price(self, row):
        product_description_id = row['ProductDescriptionId']
        fy_product_number = row['FyProductNumber']
        contract_number = 'GS-07F-0636W'

        if 'VAOnContract' in row:
            on_contract = row['VAOnContract']
        else:
            on_contract = -1

        if 'VAContractModificationNumber' in row:
            contract_mod_number = row['VAContractModificationNumber']
        else:
            contract_mod_number = ''

        if 'VAPricingApproved' in row:
            is_pricing_approved = float(row['VAPricingApproved'])
        else:
            is_pricing_approved = -1

        va_approved_price_date = -1
        if 'VAApprovedPriceDate' in row:
            va_approved_price_date = row['VAApprovedPriceDate']
            try:
                va_approved_price_date = int(va_approved_price_date)
                va_approved_price_date = xlrd.xldate_as_datetime(va_approved_price_date, 0)
            except ValueError:
                va_approved_price_date = str(row['VAApprovedPriceDate'])

            if isinstance(va_approved_price_date, datetime.datetime) == False:
                try:
                    va_approved_price_date = datetime.datetime.strptime(va_approved_price_date, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    va_approved_price_date = str(row['VAApprovedPriceDate'])
                    self.obReporter.update_report('Alert','Check VAApprovedPriceDate')
                except TypeError:
                    self.obReporter.update_report('Alert','Check VAApprovedPriceDate')


        if 'VAApprovedBasePrice' in row:
            approved_base_price = float(row['VAApprovedBasePrice'])
        else:
            approved_base_price = -1

        if 'VAApprovedSellPrice' in row:
            approved_sell_price = float(row['VAApprovedSellPrice'])
        else:
            approved_sell_price = -1

        if 'VAApprovedListPrice' in row:
            approved_list_price = float(row['VAApprovedListPrice'])
        else:
            approved_list_price = -1


        if 'VADiscountPercent' in row:
            approved_percent = float(row['VADiscountPercent'])
        else:
            approved_percent = -1

        if 'MfcDiscountPercent' in row:
            mfc_percent = float(row['MfcDiscountPercent'])
        else:
            mfc_percent = -1

        if 'VA_Sin' in row:
            sin = row['VA_Sin']
        else:
            sin = ''

        va_product_notes = ''
        if 'VAProductNotes' in row:
            va_product_notes = str(row['VAProductNotes'])

        va_product_price_id = -1
        if 'VAProductPriceId' in row:
            va_product_price_id = int(row['VAProductPriceId'])

        if va_product_price_id == -1 and 'db_VAProductPriceId' in row:
            va_product_price_id = int(row['db_VAProductPriceId'])

        if (on_contract != -1 or contract_mod_number != '' or is_pricing_approved != -1 or va_approved_price_date != -1
                or approved_base_price != -1 or approved_sell_price != -1 or approved_list_price != -1 or approved_percent != -1
                or sin != '' or va_product_notes != ''):
            if va_product_price_id == -1:
                self.obIngester.va_product_price_insert(product_description_id, fy_product_number, on_contract, approved_base_price,
                                                  approved_sell_price, approved_list_price,
                                                  contract_number, contract_mod_number, is_pricing_approved,
                                                  va_approved_price_date, approved_percent, mfc_percent,
                                                  sin, va_product_notes)
            else:
                self.obIngester.va_product_price_update(va_product_price_id, product_description_id, fy_product_number, on_contract, approved_base_price,
                                                  approved_sell_price, approved_list_price,
                                                  contract_number, contract_mod_number, is_pricing_approved,
                                                  va_approved_price_date, approved_percent, mfc_percent,
                                                  sin, va_product_notes)


    def trigger_ingest_cleanup(self):
        self.obIngester.insert_fy_product_description_contract_cleanup()
        self.obIngester.insert_fy_product_description_cleanup()
        self.obIngester.update_fy_product_description_cleanup()
        time.sleep(1)
        self.obIngester.insert_gsa_product_price_cleanup()
        self.obIngester.update_gsa_product_price_cleanup()
        time.sleep(1)
        self.obIngester.va_product_price_insert_cleanup()
        self.obIngester.va_product_price_update_cleanup()



class FyProductIngest(FyProductUpdate):
    req_fields = ['FyProductNumber', 'FyProductName', 'FyProductDescription', 'FyCountryOfOrigin', 'FyUnitOfIssue',
                  'FyUnitOfIssueQuantity','FyLeadTimes', 'ManufacturerPartNumber',
                  'ManufacturerName', 'VendorPartNumber', 'DateCatalogReceived', 'CatalogProvidedBy']

    sup_fields = ['VendorName', 'PrimaryVendorName', 'SecondaryVendorName', 'FyIsHazardous',
                  'FyCategory', 'FyNAICSCode', 'FyUNSPSCCode', 'FyHazardousSpecialHandlingCode',
                  'FyShelfLifeMonths','FyControlledCode','FyIsLatexFree','FyIsGreen','FyColdChain',
                  'FyProductNotes', 'VendorListPrice','FyCost','FyUnitOfMeasure',
                  'FyLandedCost','FyLandedCostMarkupPercent_FySell','FyLandedCostMarkupPercent_FyList',
                  'BCDataUpdateToggle', 'BCPriceUpdateToggle','FyIsDiscontinued',
                  'WebsiteOnly','ECATEligible','GSAEligible','HTMEEligible','INTRAMALLSEligible','VAEligible',
                  'VendorIsDiscontinued','VendorProductNotes']
    att_fields = []
    gen_fields = []

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'FyProduct Ingest'


    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            for each_bool in ['FyIsGreen', 'FyIsLatexFree', 'FyIsHazardous','FyIsDiscontinued','BCPriceUpdateToggle','BCDataUpdateToggle']:
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
            success, df_collect_product_base_data = self.process_fy_description(df_collect_product_base_data, row)

        return success, df_collect_product_base_data




## end ##