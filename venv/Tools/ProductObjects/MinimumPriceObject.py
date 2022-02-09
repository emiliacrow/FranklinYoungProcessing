# CreatedBy: Emilia Crow
# CreateDate: 20210528
# Updated: 20210813
# CreateFor: Franklin Young International

import pandas

from Tools.BasicProcess import BasicProcessObject


class MinimumProductPrice(BasicProcessObject):
    req_fields = ['FyCatalogNumber','ManufacturerPartNumber','FyProductNumber','VendorPartNumber']
    sup_fields = []
    gen_fields = ['ProductId', 'VendorId', 'UnitOfIssueId']
    att_fields = []

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'Minimum Product Price'

    def batch_preprocessing(self):
        self.remove_private_headers()
        self.df_uois_lookup = self.obDal.get_unit_of_issue_symbol_lookup()
        if 'VendorId' not in self.df_product.columns:
            self.batch_process_vendor()
        self.define_new()


    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'BaseProductPriceId','BaseProductPriceId_y','BaseProductPriceId_x',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryId','CategoryId_x','CategoryId_y',
                           'Report','Filter'}
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
        filter_options = ['New', 'Ready', 'Partial', 'Possible Duplicate', 'Update_in_Product_Price', 'Update_in_Base_Price']

        if row['Filter'] == 'New':
            self.obReporter.update_report('Alert', 'Passed filtering as new product')
            return False

        elif row['Filter'] == 'Ready' or row['Filter'] == 'Partial' or row['Filter'] == 'Update_in_Product_Price' or row['Filter'] == 'Update_in_Base_Price':
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

            success, df_collect_product_base_data = self.identify_fy_product_number(df_collect_product_base_data, row)
            if success == False:
                self.obReporter.update_report('Fail','Can\'t identify product number')
                return success, df_collect_product_base_data

            if ('ProductTaxClass' not in row):
                df_collect_product_base_data['ProductTaxClass'] = 'Default Tax Class'

            for each_bool in ['IsDiscontinued','AllowPurchases']:
                success, return_val = self.process_boolean(row, each_bool)
                if success:
                    df_collect_product_base_data[each_bool] = [return_val]
                else:
                    self.obReporter.update_report('Alert', '{0} was set to 0'.format(each_bool))
                    df_collect_product_base_data[each_bool] = [0]

        try:
            success, df_collect_product_base_data = self.minimum_product_price(df_collect_product_base_data)
        except KeyError:
            self.obReporter.update_report('Fail','Failed in send to DB: KeyError')
            return False, df_collect_product_base_data

        if success:
            self.obReporter.price_report(success)
            return True, df_collect_product_base_data
        else:
            self.obReporter.price_report(success)
            return False, df_collect_product_base_data

        return True, df_collect_product_base_data

    def identify_fy_product_number(self, df_collect_product_base_data, row):
        if ('Conv Factor/QTY UOM' not in row):
            df_collect_product_base_data['Conv Factor/QTY UOM'] = [1]

        if ('UnitOfMeasure' not in row):
            unit_of_measure = 'EA'
            df_collect_product_base_data['UnitOfMeasure'] = [unit_of_measure]
        else:
            unit_of_measure = self.normalize_units(row['UnitOfMeasure'])
            df_collect_product_base_data['UnitOfMeasure'] = [unit_of_measure]

        unit_of_issue = 'EA'
        if 'UnitOfIssue' in row:
            unit_of_issue = self.normalize_units(row['UnitOfIssue'])
            df_collect_product_base_data['UnitOfIssue'] = [unit_of_issue]

        try:
            unit_of_issue_symbol_id = self.df_uois_lookup.loc[(self.df_uois_lookup['UnitOfIssueSymbol'] == unit_of_issue),'UnitOfIssueSymbolId'].values[0]
        except IndexError:
            unit_of_issue_symbol_id = self.obIngester.ingest_uoi_symbol(unit_of_issue)

        df_collect_product_base_data['UnitOfIssueSymbolId'] = [unit_of_issue_symbol_id]

        try:
            unit_of_measure_symbol_id = self.df_uois_lookup.loc[(self.df_uois_lookup['UnitOfIssueSymbol'] == unit_of_measure),'UnitOfIssueSymbolId'].values[0]
        except IndexError:
            unit_of_measure_symbol_id = self.obIngester.ingest_uoi_symbol(unit_of_measure)

        df_collect_product_base_data['UnitOfMeasureSymbolId'] = [unit_of_measure_symbol_id]


        if 'FyCatalogNumber' in row:
            fy_catalog_number = df_collect_product_base_data.at[row.name,'FyCatalogNumber']
        else:
            self.obReporter.update_report('Fail','Missing catalog number')
            return False, df_collect_product_base_data

        if ('FyProductNumber' not in row):
            if unit_of_issue != 'EA':
                fy_product_number = fy_catalog_number + ' ' + unit_of_issue
                df_collect_product_base_data['FyProductNumber'] = [fy_product_number]
            else:
                fy_product_number = fy_catalog_number
                df_collect_product_base_data['FyProductNumber'] = [fy_product_number]

        elif (row['FyProductNumber'] == fy_catalog_number + ' ' + unit_of_issue) or (row['FyProductNumber'] == fy_catalog_number):
            fy_product_number = row['FyProductNumber']
            df_collect_product_base_data['FyProductNumber'] = [fy_product_number]
        else:
            self.obReporter.update_report('Fail','Check FyCatalog and Product numbers for problems')
            return False, df_collect_product_base_data

        if ('FyPartNumber' not in row):
            df_collect_product_base_data['FyPartNumber'] = [fy_product_number]

        if ('VendorPartNumber' not in row):
            self.obReporter.update_report('Fail','VendorPartNumber was missing; srsly, how did this happen?!')
            return False, df_collect_product_base_data

        return True, df_collect_product_base_data

    def process_vendor(self, df_collect_product_base_data, row):
        if 'VendorId' not in row:
            self.obReporter.update_report('Fail','Missing VendorName and Code')
            return False, df_collect_product_base_data
        elif row['VendorId'] == -1:
            self.obReporter.update_report('Fail','Vendor must be ingested')
            return False, df_collect_product_base_data

        return True, df_collect_product_base_data

    def minimum_product_price(self,df_line_product):
        for colName, row in df_line_product.iterrows():
            fy_product_number = row['FyProductNumber']
            allow_purchases = row['AllowPurchases']
            fy_part_number = row['FyPartNumber']
            product_tax_class = row['ProductTaxClass']
            vendor_part_number = row['VendorPartNumber']
            is_discontinued = row['IsDiscontinued']

            product_id = row['ProductId']
            vendor_id = row['VendorId']
            unit_of_issue_symbol_id = row['UnitOfIssueSymbolId']
            unit_of_measure_symbol_id = row['UnitOfMeasureSymbolId']
            unit_of_issue_quantity = row['Conv Factor/QTY UOM']

        self.obIngester.ingest_product_price(self.is_last, fy_product_number, allow_purchases, fy_part_number,
                                             product_tax_class, vendor_part_number, is_discontinued, product_id, vendor_id,
                                             unit_of_issue_symbol_id, unit_of_measure_symbol_id, unit_of_issue_quantity)

        return True, df_line_product

    def trigger_ingest_cleanup(self):
        self.obIngester.ingest_product_price_cleanup()

class UpdateMinimumProductPrice(MinimumProductPrice):
    req_fields = ['VendorPartNumber','FyCatalogNumber','FyProductNumber']
    sup_fields = []
    gen_fields = ['ProductId', 'VendorId', 'UnitOfIssueId']
    att_fields = []

    def __init__(self,df_product, user, password, is_testing, full_process=False):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'Update Minimum Product Price'
        self.full_process = full_process




# end ##