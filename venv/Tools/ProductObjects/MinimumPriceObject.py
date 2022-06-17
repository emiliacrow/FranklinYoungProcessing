# CreatedBy: Emilia Crow
# CreateDate: 20210528
# Updated: 20220318
# CreateFor: Franklin Young International

import pandas

from Tools.BasicProcess import BasicProcessObject


class MinimumProductPrice(BasicProcessObject):
    req_fields = ['FyCatalogNumber','ManufacturerName', 'ManufacturerPartNumber','FyProductNumber','VendorName','VendorPartNumber'
        ,'Conv Factor/QTY UOM','UnitOfIssue']
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
                           'Report','Filter','ProductDescriptionId','db_FyProductName','db_FyShortDescription','db_FyLongDescription'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
        if remove_headers != []:
            self.df_product = self.df_product.drop(columns=remove_headers)


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
                    df_collect_product_base_data['ProductDescriptionId'] = [self.next_fy_description_id]
                    self.next_fy_description_id += 1
                else:
                    df_collect_product_base_data['ProductDescriptionId'] = [-1]
            else:
                fy_product_desc_id = row['ProductDescriptionId']
                self.dct_fy_product_description[fy_product_number] = fy_product_desc_id

        success, df_collect_product_base_data = self.minimum_product_price(df_collect_product_base_data)

        if success:
            self.obReporter.price_report(success)
            return True, df_collect_product_base_data
        else:
            self.obReporter.price_report(success)
            return False, df_collect_product_base_data

        return True, df_collect_product_base_data

    def process_fy_description(self, df_collect_product_base_data, row):
        fy_product_number = row['FyProductNumber']
        if 'FyProductName' not in row:
            return False, df_collect_product_base_data
        else:
            fy_product_name = row['FyProductName']

        if 'FyShortDescription' not in row:
            return False, df_collect_product_base_data
        else:
            fy_short_description = row['FyShortDescription']

        if 'FyLongDescription' not in row:
            return False, df_collect_product_base_data
        else:
            fy_long_description = row['FyLongDescription']

        # for speed sake this is a one-off
        lst_descriptions = [(fy_product_number, fy_product_name, fy_short_description, fy_long_description)]
        self.obDal.fy_product_description_insert(lst_descriptions)

        return True, df_collect_product_base_data


    def identify_units(self, df_collect_product_base_data, row):
        if ('Conv Factor/QTY UOM' not in row):
            df_collect_product_base_data['Conv Factor/QTY UOM'] = [-1]

        if 'UnitOfMeasure' not in row:
            unit_of_measure = -1
        else:
            unit_of_measure = self.normalize_units(row['UnitOfMeasure'])
            df_collect_product_base_data['UnitOfMeasure'] = [unit_of_measure]

        if 'UnitOfIssue' not in row:
            unit_of_issue = -1
        else:
            unit_of_issue = self.normalize_units(row['UnitOfIssue'])
            df_collect_product_base_data['UnitOfIssue'] = [unit_of_issue]

        if unit_of_issue == -1:
            unit_of_issue_symbol_id = -1
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


    def minimum_product_price(self,df_line_product):
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

        elif str(row['Filter']) == 'Ready' or str(row['Filter']) == 'Base Pricing':
            # price_id = row['ProductPriceId']
            # self.obIngester.update_product_price_nouoi(price_id, fy_product_number, allow_purchases, fy_part_number,
            #                                      product_tax_class, vendor_part_number, is_discontinued, product_id, vendor_id,
            #                                      product_description_id, fy_product_notes)

        # this pathway will be needed at some point I'm sure
        #elif 'DEPRICATED' == 'UNIT CHANGE PATH':
            price_id = row['ProductPriceId']
            self.obIngester.update_product_price(price_id, fy_product_number, allow_purchases, fy_part_number,
                                                 product_tax_class, vendor_part_number, is_discontinued, product_id, vendor_id,
                                                 unit_of_issue_symbol_id, unit_of_measure_symbol_id, unit_of_issue_quantity, product_description_id, fy_product_notes)



        return True, df_line_product

    def trigger_ingest_cleanup(self):
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