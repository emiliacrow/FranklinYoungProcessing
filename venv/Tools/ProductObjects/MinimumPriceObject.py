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

        if 'FyCountryOfOrigin' in self.df_product.columns:
            self.batch_process_country()

        self.define_new()
        # pull the current descriptions
        self.df_fy_description_lookup = self.obDal.get_fy_product_descriptions()

        self.df_product = self.df_product.merge(self.df_fy_description_lookup,how='left',on=['FyProductNumber'])

        # and do something with them
        # like what if we could predict the next ID from this and
        # do the insert blind?
        # in the current situation there's a a max ID of 1
        # so the next insert would be 2
        # we can prep accordingly and pre assign 2 to the next insert

        self.df_next_fy_description_id = self.obDal.get_next_fy_product_description_id()
        self.next_fy_description_id = int(self.df_next_fy_description_id['AUTO_INCREMENT'].iloc[0])

        self.df_product.sort_values(by=['FyProductNumber'], inplace=True)


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

        if 'CountryOfOriginId' not in self.df_product.columns and 'FyCountryOfOrigin' in self.df_product.columns:

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

            df_attribute['CountryOfOriginId'] = lst_ids
            self.df_product = self.df_product.merge(df_attribute,
                                                              how='left', on=['FyCountryOfOrigin'])


    def batch_process_vendor(self):
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
                vendor_name_list = self.df_vendor_translator["VendorName"].tolist()
                vendor_name_list = list(dict.fromkeys(vendor_name_list))

                new_vendor_id = self.obIngester.manual_ingest_vendor(atmp_name=vendor_name,atmp_code=vendor_name,lst_vendor_names=vendor_name_list)

            lst_ids.append(new_vendor_id)

        df_attribute['VendorId'] = lst_ids

        self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                 how='left', on=['VendorName'])


    def filter_check_in(self, row):
        filter_options = ['Base Pricing', 'New', 'Partial', 'Possible Duplicate', 'Ready', 'case_1','case_4']

        if row['Filter'] == 'New':
            self.obReporter.update_report('Alert', 'Passed filtering as a new product')
            return False

        elif row['Filter'] in ['Ready', 'Partial', 'Base Pricing']:
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
                self.obReporter.update_report('Alert', 'Your product number contains outlawed characters, you must include the FyProductNumberOverride column.')
                if not b_override:
                    return False, df_collect_product_base_data

            if ('ProductTaxClass' not in row):
                df_collect_product_base_data['ProductTaxClass'] = 'Default Tax Class'

            for each_bool in ['IsDiscontinued','AllowPurchases']:
                success, return_val = self.process_boolean(row, each_bool)
                if success:
                    df_collect_product_base_data[each_bool] = [return_val]
                else:
                    if each_bool == 'AllowPurchases':
                        self.obReporter.update_report('Alert', '{0} was set to 1'.format(each_bool))
                        df_collect_product_base_data[each_bool] = [1]
                    else:
                        self.obReporter.update_report('Alert', '{0} was set to 0'.format(each_bool))
                        df_collect_product_base_data[each_bool] = [0]


            # all the products that need the info to be ingested
            if fy_product_number in self.dct_fy_product_description:
                if 'ProductDescriptionId' not in row:
                    fy_product_desc_id = self.dct_fy_product_description[fy_product_number]
                    df_collect_product_base_data['ProductDescriptionId'] = [fy_product_desc_id]

            else:
                if 'ProductDescriptionId' not in row:
                    success, df_collect_product_base_data = self.process_fy_description(df_collect_product_base_data, row)
                    if success:
                        self.next_fy_description_id += 1
                        df_collect_product_base_data['ProductDescriptionId'] = [self.next_fy_description_id]
                        self.dct_fy_product_description[fy_product_number] = self.next_fy_description_id
                    else:
                        df_collect_product_base_data['ProductDescriptionId'] = [-1]
                else:
                    fy_product_desc_id = row['ProductDescriptionId']
                    self.dct_fy_product_description[fy_product_number] = fy_product_desc_id
                    df_collect_product_base_data = self.update_fy_description(df_collect_product_base_data, row)

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

        if 'FyProductDescription' not in row:
            fy_product_description = ''
        else:
            fy_product_description = row['FyProductDescription']

        if 'FyCountryOfOriginId' in row:
            fy_coo_id = int(row['FyCountryOfOriginId'])
        if 'CountryOfOriginId' in row:
            fy_coo_id = int(row['CountryOfOriginId'])
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

        if 'FyLeadTime' in row:
            fy_lead_time = int(row['FyLeadTime'])
        else:
            fy_lead_time = -1

        if len(fy_product_name) > 80 and fy_product_name != '':
            self.obReporter.update_report('Alert','FyProductName might be too long for some contracts.')

        if len(fy_product_description) > 800 and fy_product_description != '':
            self.obReporter.update_report('Alert','FyProductDescription might be too long for some contracts.')

        if (fy_product_name != '' or fy_product_description != '' or fy_coo_id != -1 or fy_uoi_id != -1 or fy_uoi_qty != -1 or fy_lead_time != -1):
            pass
        else:
            self.obIngester.update_fy_product_description(fy_product_desc_id, fy_product_name, fy_product_description, fy_coo_id, fy_uoi_id, fy_uoi_qty, fy_lead_time)


        return df_collect_product_base_data


    def process_fy_description(self, df_collect_product_base_data, row):
        fy_product_number = row['FyProductNumber']
        if 'FyProductName' not in row:
            fy_product_name = ''
        else:
            fy_product_name = row['FyProductName']

        if 'FyProductDescription' not in row:
            fy_product_description = ''
        else:
            fy_product_description = row['FyProductDescription']

        if 'FyCountryOfOriginId' in row:
            fy_coo_id = int(row['FyCountryOfOriginId'])
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

        if 'FyLeadTime' in row:
            fy_lead_time = int(row['FyLeadTime'])
        else:
            fy_lead_time = -1

        if len(fy_product_name) > 80:
            self.obReporter.update_report('Alert','FyProductName might be too long for some contracts.')

        if len(fy_product_description) > 800:
            self.obReporter.update_report('Alert','FyProductDescription might be too long for some contracts.')

        # for speed sake this is a one-off
        if (fy_product_name != '' or fy_product_description != '' or fy_coo_id != -1 or fy_uoi_id != -1 or fy_uoi_qty != -1 or fy_lead_time != -1):
            lst_descriptions = [(fy_product_number, fy_product_name, fy_product_description, fy_coo_id, fy_uoi_id, fy_uoi_qty, fy_lead_time)]
            self.obDal.fy_product_description_insert(lst_descriptions)
            return True, df_collect_product_base_data
        else:
            return False, df_collect_product_base_data


    def identify_units(self, df_collect_product_base_data, row):
        if 'Conv Factor/QTY UOM' not in row:
            if 'FyUnitOfIssueQuantity' not in row:
                df_collect_product_base_data['Conv Factor/QTY UOM'] = [-1]
            else:
                fy_unit_of_issue_quantity = row['FyUnitOfIssueQuantity']
                df_collect_product_base_data['Conv Factor/QTY UOM'] = [fy_unit_of_issue_quantity]

        if 'UnitOfMeasure' not in row:
            unit_of_measure = -1
        else:
            unit_of_measure = self.normalize_units(row['UnitOfMeasure'])
            df_collect_product_base_data['UnitOfMeasure'] = [unit_of_measure]


        unit_of_issue = -1
        fy_unit_of_issue = -1
        if 'UnitOfIssue' in row:
            unit_of_issue = self.normalize_units(row['UnitOfIssue'])
            df_collect_product_base_data['UnitOfIssue'] = [unit_of_issue]

        elif 'FyUnitOfIssue' in row:
            fy_unit_of_issue = self.normalize_units(row['FyUnitOfIssue'])
            unit_of_issue = fy_unit_of_issue
            df_collect_product_base_data['UnitOfIssue'] = [fy_unit_of_issue]
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
        fy_product_notes = ''
        for colName, row in df_line_product.iterrows():
            fy_product_number = row['FyProductNumber']
            allow_purchases = row['AllowPurchases']
            if 'FyPartNumber' in row:
                fy_part_number = row['FyPartNumber']

            if 'FyProductNotes' in row:
                fy_product_notes = row['FyProductNotes']
                fy_product_notes = fy_product_notes.replace('NULL','')
                fy_product_notes = fy_product_notes.replace(';','')

            product_tax_class = row['ProductTaxClass']
            vendor_part_number = row['VendorPartNumber']
            is_discontinued = row['IsDiscontinued']

            product_id = row['ProductId']
            vendor_id = row['VendorId']

            unit_of_issue_symbol_id = row['UnitOfIssueSymbolId']
            unit_of_measure_symbol_id = row['UnitOfMeasureSymbolId']
            unit_of_issue_quantity = row['Conv Factor/QTY UOM']

            product_description_id = row['ProductDescriptionId']

        if str(row['Filter']) == 'Partial':
            if (unit_of_issue_symbol_id != -1) and (unit_of_measure_symbol_id != -1) and (unit_of_issue_quantity != -1):
                self.obIngester.insert_product_price(fy_product_number, allow_purchases, fy_part_number,
                                                     product_tax_class, vendor_part_number, is_discontinued, product_id, vendor_id,
                                                     unit_of_issue_symbol_id, unit_of_measure_symbol_id, unit_of_issue_quantity, product_description_id, fy_product_notes)
            else:
                self.obReporter.update_report('Fail','Check UOI, QTY, UOM')

        elif str(row['Filter']) == 'Ready' or str(row['Filter']) == 'Base Pricing':
            price_id = row['ProductPriceId']
            self.obIngester.update_product_price_nouoi(price_id, fy_product_number, allow_purchases, fy_part_number,
                                                 product_tax_class, vendor_part_number, is_discontinued, product_id, vendor_id,
                                                 product_description_id, fy_product_notes)

        # this pathway will be needed at some point I'm sure
        elif 'DEPRICATED' == 'UNIT CHANGE PATH':
            price_id = row['ProductPriceId']
            self.obIngester.update_product_price(price_id, fy_product_number, allow_purchases, fy_part_number,
                                                 product_tax_class, vendor_part_number, is_discontinued, product_id, vendor_id,
                                                 unit_of_issue_symbol_id, unit_of_measure_symbol_id, unit_of_issue_quantity, product_description_id, fy_product_notes)



        return True, df_line_product

    def trigger_ingest_cleanup(self):
        self.obIngester.update_product_price_nouoi_cleanup()
        self.obIngester.update_product_price_cleanup()
        self.obIngester.insert_product_price_cleanup()
        self.obIngester.update_fy_product_description_cleanup()


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