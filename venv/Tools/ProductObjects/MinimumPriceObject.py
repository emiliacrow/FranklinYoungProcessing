# CreatedBy: Emilia Crow
# CreateDate: 20210528
# Updated: 20220318
# CreateFor: Franklin Young International

import pandas

from Tools.BasicProcess import BasicProcessObject


class MinimumProductPrice(BasicProcessObject):
    req_fields = ['FyCatalogNumber','ManufacturerName', 'ManufacturerPartNumber','FyProductNumber','VendorName','VendorPartNumber']
    sup_fields = []
    gen_fields = ['ProductId', 'VendorId', 'UnitOfIssueId']
    att_fields = []

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'Minimum Product Price'
        self.dct_fy_product_description = {}

    def batch_preprocessing(self):
        self.remove_private_headers()
        self.df_uois_lookup = self.obDal.get_unit_of_issue_symbol_lookup()
        if 'VendorId' not in self.df_product.columns:
            self.batch_process_vendor()

        self.batch_process_country()

        self.define_new()
        # pull the current descriptions
        self.df_fy_description_lookup = self.obDal.get_fy_product_descriptions_short()

        self.df_product = self.df_product.merge(self.df_fy_description_lookup,how='left',on=['FyProductNumber'])

        self.df_product.sort_values(by=['FyProductNumber'], inplace=True)


    def process_primary_vendor(self, df_collect_product_base_data, row):
        if 'PrimaryVendorName' in row:
            vendor_name = row['PrimaryVendorName'].upper()
            if vendor_name in self.df_vendor_translator['VendorCode'].values:
                new_vendor_id = self.df_vendor_translator.loc[
                    (self.df_vendor_translator['VendorCode'] == vendor_name),'VendorId'].values[0]
                df_collect_product_base_data['PrimaryVendorId'] = new_vendor_id
            elif vendor_name in self.df_vendor_translator['VendorName'].values:
                new_vendor_id = self.df_vendor_translator.loc[
                    (self.df_vendor_translator['VendorName'] == vendor_name),'VendorId'].values[0]
                df_collect_product_base_data['PrimaryVendorId'] = new_vendor_id
            else:
                self.obReporter.update_report('Alert','PrimaryVendorName did not match an existing vendor')

        if 'SecondaryVendorName' in row:
            vendor_name = row['SecondaryVendorName'].upper()
            if vendor_name in self.df_vendor_translator['VendorCode'].values:
                new_vendor_id = self.df_vendor_translator.loc[
                    (self.df_vendor_translator['VendorCode'] == vendor_name),'VendorId'].values[0]
                df_collect_product_base_data['SecondaryVendorId'] = new_vendor_id
            elif vendor_name in self.df_vendor_translator['VendorName'].values:
                new_vendor_id = self.df_vendor_translator.loc[
                    (self.df_vendor_translator['VendorName'] == vendor_name),'VendorId'].values[0]
                df_collect_product_base_data['SecondaryVendorId'] = new_vendor_id
            else:
                self.obReporter.update_report('Alert','SecondaryVendorName did not match an existing vendor')

        return df_collect_product_base_data


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


    def batch_process_vendor(self):
        if 'VendorName' not in self.df_product.columns:
            vendor_name = self.vendor_name_selection()
            self.df_product['VendorName'] = vendor_name

        if 'VendorName' in self.df_product.columns:
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


    def filter_check_in(self, row):
        filter_options = ['Base Pricing', 'New', 'Partial', 'Possible Duplicate', 'Ready', 'case_1','case_4']

        if row['Filter'] == 'New':
            self.obReporter.update_report('Alert', 'Passed filtering as a new product')
            return False

        elif row['Filter'] == 'check_vendor':
            self.obReporter.update_report('Alert', 'Filtered as check vendor, please review')
            return True

        elif row['Filter'] in ['Ready', 'Partial', 'Base Pricing','ConfigurationChange']:
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
            df_collect_product_base_data = self.identify_units(df_collect_product_base_data, row)
            if 'PrimaryVendorName' in row or 'SecondaryVendorName' in row:
                df_collect_product_base_data = self.process_primary_vendor(df_collect_product_base_data, row)

        df_line_product = df_collect_product_base_data.copy()

        # step-wise product processing
        for colName, row in df_line_product.iterrows():
            if self.filter_check_in(row) == False:
                return False, df_collect_product_base_data

            # this is also stupid, but it gets the point across for testing purposes
            success, df_collect_product_base_data = self.process_vendor(df_collect_product_base_data, row)
            if success == False:
                self.obReporter.update_report('Fail','Failed at vendor identification')
                return success, df_collect_product_base_data

            df_collect_product_base_data = self.identify_units(df_collect_product_base_data, row)

            b_override = False
            success, return_val = self.process_boolean(row, 'FyProductNumberOverride')
            if success and return_val == 1:
                b_override = True
            else:
                success, return_val = self.process_boolean(row, 'db_ProductNumberOverride')
                if success and return_val == 1:
                    b_override = True

            fy_product_number = row['FyProductNumber']
            b_pass_number_check = self.obValidator.review_product_number(fy_product_number)
            if not b_pass_number_check:
                if not b_override:
                    return False, df_collect_product_base_data
                else:
                    self.obReporter.update_report('Alert',
                                                  'Your product number contains outlawed characters, you must include the FyProductNumberOverride column.')

            if ('ProductTaxClass' not in row):
                df_collect_product_base_data['ProductTaxClass'] = 'Default Tax Class'


            # all the products that need the info to be ingested
            if fy_product_number in self.dct_fy_product_description:
                if 'ProductDescriptionId' not in row:
                    fy_product_desc_id = self.dct_fy_product_description[fy_product_number]
                    df_collect_product_base_data['ProductDescriptionId'] = [fy_product_desc_id]

            else:
                if 'ProductDescriptionId' not in row:
                    success, df_collect_product_base_data = self.process_fy_description(df_collect_product_base_data, row)
                    if success:
                        df_collect_product_base_data['ProductDescriptionId'] = [-1]
                    else:
                        df_collect_product_base_data['ProductDescriptionId'] = [-1]
                else:
                    fy_product_desc_id = row['ProductDescriptionId']
                    self.dct_fy_product_description[fy_product_number] = fy_product_desc_id

        is_discontinued = -1
        success, is_discontinued = self.process_boolean(row, 'VendorIsDiscontinued')
        if success:
            df_collect_product_base_data['VendorIsDiscontinued'] = [is_discontinued]
        else:
            is_discontinued = -1
            df_collect_product_base_data['VendorIsDiscontinued'] = [is_discontinued]

        success, df_collect_product_base_data = self.minimum_product_price(df_collect_product_base_data)

        if success:
            self.obReporter.price_report(success)
            return True, df_collect_product_base_data
        else:
            self.obReporter.price_report(success)
            return False, df_collect_product_base_data

        return True, df_collect_product_base_data


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

        if 'PrimaryVendorId' in row:
            primary_vendor_id = int(row['PrimaryVendorId'])
        else:
            primary_vendor_id = -1

        if 'SecondaryVendorId' in row:
            secondary_vendor_id = int(row['SecondaryVendorId'])
        else:
            secondary_vendor_id = -1


        if (fy_product_name != '' or fy_product_description != '' or fy_coo_id != -1 or fy_uoi_id != -1 or fy_uom_id != -1 or fy_uoi_qty != -1 or fy_lead_time != -1 or fy_is_hazardous != -1 or primary_vendor_id != -1 or secondary_vendor_id != -1):
            self.obIngester.update_fy_product_description_short(fy_product_desc_id, fy_product_name, fy_product_description, fy_coo_id, fy_uoi_id, fy_uom_id, fy_uoi_qty, fy_lead_time, fy_is_hazardous, primary_vendor_id, secondary_vendor_id)

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

        if 'FyUnitOfIssueQuantity' in row:
            fy_uoi_qty = int(row['FyUnitOfIssueQuantity'])
        else:
            fy_uoi_qty = -1

        if 'FyLeadTimes' in row:
            fy_lead_time = int(row['FyLeadTimes'])
        else:
            fy_lead_time = -1

        if 'ManufacturerPartNumber' in row:
            manufacturer_part_number = str(row['ManufacturerPartNumber'])
        else:
            manufacturer_part_number = ''

        if 'FyManufacturerPartNumber' in row:
            fy_manufacturer_part_number = str(row['FyManufacturerPartNumber'])
        else:
            fy_manufacturer_part_number = manufacturer_part_number

        fy_catalog_number = str(row['FyCatalogNumber'])
        if fy_catalog_number[5] == '0':
            if manufacturer_part_number[0] != '0' and manufacturer_part_number != '':
                self.obReporter.update_report('Fail', 'ManufacturerPartNumber dropped zeros')
                return False, df_collect_product_base_data
            if fy_manufacturer_part_number[0] != '0' and fy_manufacturer_part_number != '':
                self.obReporter.update_report('Fail', 'FyManufacturerPartNumber dropped zeros')
                return False, df_collect_product_base_data


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
        else:
            primary_vendor_id = -1

        if 'SecondaryVendorId' in row:
            secondary_vendor_id = int(row['SecondaryVendorId'])
        else:
            secondary_vendor_id = -1

        # for speed sake this is a one-off
        if (fy_product_name != '' or fy_product_description != '' or fy_coo_id != -1 or fy_uoi_id != -1 or fy_uoi_qty != -1 or fy_lead_time != -1 or fy_is_hazardous != -1 or primary_vendor_id != -1 or secondary_vendor_id != -1):
            #lst_descriptions = [(fy_product_number, fy_product_name, fy_manufacturer_part_number, fy_product_description, fy_coo_id, fy_uoi_id, fy_uoi_qty, fy_lead_time, fy_is_hazardous, primary_vendor_id, secondary_vendor_id)]
            #self.obDal.fy_product_description_insert_short(lst_descriptions)
            self.obIngester.insert_fy_product_description_short(fy_product_number, fy_product_name, fy_manufacturer_part_number, fy_product_description, fy_coo_id, fy_uoi_id, fy_uoi_qty, fy_lead_time, fy_is_hazardous, primary_vendor_id, secondary_vendor_id)

            return True, df_collect_product_base_data
        else:
            return False, df_collect_product_base_data


    def identify_units(self, df_collect_product_base_data, row):
        # set quantities
        if 'UnitOfIssueQuantity' in row:
            unit_of_issue_quantity = row['UnitOfIssueQuantity']
        else:
            unit_of_issue_quantity = -1
            df_collect_product_base_data['UnitOfIssueQuantity'] = [unit_of_issue_quantity]


        if 'FyUnitOfIssueQuantity' in row:
            fy_unit_of_issue_quantity = row['FyUnitOfIssueQuantity']
        else:
            fy_unit_of_issue_quantity = -1

        if unit_of_issue_quantity == -1 and fy_unit_of_issue_quantity != -1:
            unit_of_issue_quantity = fy_unit_of_issue_quantity
            df_collect_product_base_data['UnitOfIssueQuantity'] = [unit_of_issue_quantity]


        if 'UnitOfMeasure' not in row:
            unit_of_measure = self.normalize_units('EA')
            df_collect_product_base_data['UnitOfMeasure'] = [unit_of_measure]
        else:
            unit_of_measure = self.normalize_units(row['UnitOfMeasure'])
            df_collect_product_base_data['UnitOfMeasure'] = [unit_of_measure]


        unit_of_issue = -1
        fy_unit_of_issue = -1
        if 'UnitOfIssue' in row:
            unit_of_issue = self.normalize_units(row['UnitOfIssue'])
            df_collect_product_base_data['UnitOfIssue'] = [unit_of_issue]


        if 'FyUnitOfIssue' in row:
            fy_unit_of_issue = self.normalize_units(row['FyUnitOfIssue'])
            df_collect_product_base_data['FyUnitOfIssue'] = [fy_unit_of_issue]

        if unit_of_issue == -1 and fy_unit_of_issue != -1:
            unit_of_issue = fy_unit_of_issue
            df_collect_product_base_data['UnitOfIssue'] = [unit_of_issue]


        if fy_unit_of_issue == -1:
            fy_unit_of_issue_symbol_id = -1
        else:
            try:
                fy_unit_of_issue_symbol_id = self.df_uois_lookup.loc[
                    (self.df_uois_lookup['UnitOfIssueSymbol'] == fy_unit_of_issue), 'UnitOfIssueSymbolId'].values[0]
            except IndexError:
                fy_unit_of_issue_symbol_id = self.obIngester.ingest_uoi_symbol(fy_unit_of_issue)

        df_collect_product_base_data['FyUnitOfIssueSymbolId'] = [fy_unit_of_issue_symbol_id]

        if unit_of_issue == -1:
            unit_of_issue_symbol_id = fy_unit_of_issue_symbol_id
        else:
            try:
                unit_of_issue_symbol_id = self.df_uois_lookup.loc[(self.df_uois_lookup['UnitOfIssueSymbol'] == unit_of_issue),'UnitOfIssueSymbolId'].values[0]
            except IndexError:
                unit_of_issue_symbol_id = self.obIngester.ingest_uoi_symbol(unit_of_issue)

        df_collect_product_base_data['UnitOfIssueSymbolId'] = [unit_of_issue_symbol_id]

        if unit_of_measure == -1:
            unit_of_measure_symbol_id = -1
        else:
            try:
                unit_of_measure_symbol_id = self.df_uois_lookup.loc[(self.df_uois_lookup['UnitOfIssueSymbol'] == unit_of_measure),'UnitOfIssueSymbolId'].values[0]
            except IndexError:
                unit_of_measure_symbol_id = self.obIngester.ingest_uoi_symbol(unit_of_measure)

        df_collect_product_base_data['UnitOfMeasureSymbolId'] = [unit_of_measure_symbol_id]



        return df_collect_product_base_data


    def process_vendor(self, df_collect_product_base_data, row):
        if 'VendorId' not in row:
            self.obReporter.update_report('Fail','Missing VendorName and Code')
            return False, df_collect_product_base_data
        elif row['VendorId'] == -1:
            self.obReporter.update_report('Fail','Vendor must be ingested')
            return False, df_collect_product_base_data

        return True, df_collect_product_base_data


    def minimum_product_price(self, df_line_product):
        fy_part_number = ''
        vendor_product_notes = ''
        for colName, row in df_line_product.iterrows():
            fy_product_number = row['FyProductNumber']
            if 'FyPartNumber' in row:
                fy_part_number = row['FyPartNumber']

            vendor_product_notes = ''
            if 'VendorProductNotes' in row:
                vendor_product_notes = row['VendorProductNotes']
                vendor_product_notes = vendor_product_notes.replace('NULL','')
                vendor_product_notes = vendor_product_notes.replace(';','')

            product_tax_class = row['ProductTaxClass']
            vendor_part_number = row['VendorPartNumber']

            product_id = row['ProductId']
            vendor_id = row['VendorId']

            unit_of_issue_symbol_id = row['UnitOfIssueSymbolId']
            unit_of_measure_symbol_id = row['UnitOfMeasureSymbolId']
            unit_of_issue_quantity = row['UnitOfIssueQuantity']

            product_description_id = row['ProductDescriptionId']

            is_discontinued = row['VendorIsDiscontinued']

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

            if 'MinimumOrderQty' in row:
                minimum_order_qty = str(row['MinimumOrderQty'])
            else:
                minimum_order_qty = ''

            if 'LeadTimeDays' in row:
                vendor_lead_time_days = int(row['LeadTimeDays'])
            else:
                vendor_lead_time_days = -1

            if 'IsHazardous' in row:
                success, is_hazardous = self.process_boolean(row, 'IsHazardous')
                if success:
                    df_line_product['IsHazardous'] = [is_hazardous]
                else:
                    is_hazardous = -1
            else:
                is_hazardous = -1

        if str(row['Filter']) in ['Partial','ConfigurationChange','check_vendor']:
            if (unit_of_issue_symbol_id != -1) and (unit_of_measure_symbol_id != -1) and (unit_of_issue_quantity != -1):
                self.obIngester.insert_product_price(fy_product_number, fy_part_number,
                                                     product_tax_class, vendor_part_number, product_id, vendor_id,
                                                     unit_of_issue_symbol_id, unit_of_measure_symbol_id, unit_of_issue_quantity, product_description_id, vendor_product_notes,is_discontinued, vendor_product_name, vendor_product_description, country_of_origin_id, minimum_order_qty, vendor_lead_time_days, is_hazardous)
            else:
                self.obReporter.update_report('Fail','Check UOI, QTY, UOM')

        elif str(row['Filter']) == 'Ready' or str(row['Filter']) == 'Base Pricing':
            price_id = row['ProductPriceId']
            self.obIngester.update_product_price_nouoi(price_id, fy_product_number, fy_part_number,
                                                 product_tax_class, vendor_part_number, product_id, vendor_id,
                                                 product_description_id, vendor_product_notes,is_discontinued, vendor_product_name, vendor_product_description, country_of_origin_id, minimum_order_qty, vendor_lead_time_days, is_hazardous)

        # this pathway will be needed at some point I'm sure
        elif 'DEPRICATED' == 'UNIT CHANGE PATH':
            price_id = row['ProductPriceId']
            self.obIngester.update_product_price(price_id, fy_product_number, fy_part_number,
                                                 product_tax_class, vendor_part_number, product_id, vendor_id,
                                                 unit_of_issue_symbol_id, unit_of_measure_symbol_id, unit_of_issue_quantity, product_description_id, vendor_product_notes,is_discontinued, vendor_product_name, vendor_product_description, country_of_origin_id, minimum_order_qty, vendor_lead_time_days, is_hazardous)



        return True, df_line_product

    def trigger_ingest_cleanup(self):
        self.obIngester.update_fy_product_description_short_cleanup()
        self.obIngester.insert_fy_product_description_short_cleanup()
        self.obIngester.update_product_price_nouoi_cleanup()
        self.obIngester.update_product_price_cleanup()
        self.obIngester.insert_product_price_cleanup()


class UpdateMinimumProductPrice(MinimumProductPrice):
    req_fields = ['FyCatalogNumber','ManufacturerName', 'ManufacturerPartNumber','FyProductNumber','VendorName','VendorPartNumber']
    sup_fields = []
    gen_fields = ['ProductId', 'VendorId', 'UnitOfIssueId']
    att_fields = []

    def __init__(self,df_product, user, password, is_testing, full_process=False):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'Update Minimum Product Price'
        self.full_process = full_process




# end ##