# CreatedBy: Emilia Crow
# CreateDate: 20210528
# Updated: 20210813
# CreateFor: Franklin Young International

import pandas

from Tools.BasicProcess import BasicProcessObject


class MinimumProductPrice(BasicProcessObject):
    req_fields = ['VendorPartNumber','FyCatalogNumber','FyProductNumber']
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


    def define_new(self):
        # these are the df's for assigning data.
        self.df_product_lookup = self.obDal.get_product_lookup()
        self.df_product_price_lookup = self.obDal.get_product_price_lookup()

        self.df_product_lookup['Filter'] = 'Update'
        # match all products on FyProdNum and Manufacturer part, clearly
        if 'ManufacturerPartNumber' in self.df_product.columns:
            self.df_product = self.df_product.merge(self.df_product_lookup,
                                                            how='left',
                                                            on=['FyCatalogNumber', 'ManufacturerPartNumber'])
        else:
            self.df_product = self.df_product.merge(self.df_product_lookup,
                                                            how='left', on=['FyCatalogNumber'])


        if 'ProductId_y' in self.df_product.columns:
            print(self.df_product.columns)
            self.df_product['ProductId'] = self.df_product[['ProductId_y']]
            self.df_product = self.df_product.drop(columns=['ProductId_x'])
            self.df_product = self.df_product.drop(columns=['ProductId_y'])

        # we assign a label to the products that haven't been loaded through product yet
        if 'Filter' not in self.df_product.columns:
            self.df_product['Filter'] = 'Fail'
        else:
            self.df_product.loc[(self.df_product['Filter'] != 'Update'), 'Filter'] = 'Fail'

        # split the data for a moment
        self.df_update_product = self.df_product[(self.df_product['Filter'] == 'Update')]
        self.df_product = self.df_product[(self.df_product['Filter'] != 'Update')]

        if len(self.df_update_product.index) != 0:
            self.df_product_price_lookup['Filter'] = 'Update'
            # this section evaluates if these have product data loaded
            # drop some columns to ease processing
            if 'Filter' in self.df_update_product.columns:
                self.df_update_product = self.df_update_product.drop(columns=['Filter'])

            # this gets the productId again
            self.df_update_product = self.df_update_product.merge(self.df_product_price_lookup,
                                                             how='left', on=['FyProductNumber'])

            self.df_update_product.loc[(self.df_update_product['Filter'] != 'Update'), 'Filter'] = 'New'

            if 'ProductId_x' in self.df_update_product.columns:
                self.df_update_product['ProductId'] = self.df_update_product[['ProductId_x']]
                self.df_update_product = self.df_update_product.drop(columns=['ProductId_x'])
                self.df_update_product = self.df_update_product.drop(columns=['ProductId_y'])
            # recombine with product

        self.df_product = self.df_product.append(self.df_update_product)

    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryIdId','CategoryIdId_x','CategoryId_y',
                           'Report','Filter'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
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
                new_vendor_id = self.obIngester.manual_ingest_vendor(atmp_name=vendor_name,atmp_code=vendor_name)

            lst_ids.append(new_vendor_id)

        df_attribute['VendorId'] = lst_ids

        self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                 how='left', on=['VendorName'])

    def filter_check_in(self, row):
        if 'Filter' in row:
            if row['Filter'] == 'Update':
                self.obReporter.update_report('Alert','This product price is an update')
                return False
            elif row['Filter'] == 'New':
                self.obReporter.update_report('Pass','This product price is new')
                return True
            else:
                self.obReporter.update_report('Alert','This product must be ingested in product')
                return False
        else:
            self.obReporter.update_report('Fail','This product price failed filtering')
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

            success, df_collect_product_base_data = self.process_pricing(df_collect_product_base_data, row)
            if success == False:
                self.obReporter.update_report('Fail','Failed at pricing processing')
                return success, df_collect_product_base_data
            df_collect_product_base_data = self.process_discontinued(df_collect_product_base_data, row)


        success, df_collect_product_base_data = self.minimum_product_price(df_collect_product_base_data)
        df_line_product = df_collect_product_base_data

        if success:
            self.obReporter.price_report(success)
            return True, df_line_product
        else:
            self.obReporter.price_report(success)
            return False, df_line_product

        return True, df_line_product

    def identify_fy_product_number(self, df_collect_product_base_data, row):
        if ('Conv Factor/QTY UOM' not in row):
            df_collect_product_base_data['Conv Factor/QTY UOM'] = 1

        if ('UnitOfMeasure' not in row):
            unit_of_measure = 'EA'
            df_collect_product_base_data['UnitOfMeasure'] = unit_of_measure
        else:
            unit_of_measure = row['UnitOfMeasure']

        unit_of_issue = 'EA'
        if 'UnitOfIssue' in row:
            unit_of_issue = row['UnitOfIssue']

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
            self.obReporter.update_report('Fail','VendorPartNumber was missing')
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

    def process_pricing(self, df_collect_product_base_data, row):
        if ('AllowPurchases' not in row):
            df_collect_product_base_data['AllowPurchases'] = [0]
            self.obReporter.update_report('Alert','AllowPurchases was assigned')
        elif str(row['AllowPurchases']) == 'N':
            df_collect_product_base_data['AllowPurchases'] = [0]
        elif str(row['AllowPurchases']) == 'Y':
            df_collect_product_base_data['AllowPurchases'] = [1]

        if ('ProductTaxClass' not in row):
            df_collect_product_base_data['ProductTaxClass'] = 'Default Tax Class'

        return True, df_collect_product_base_data


    def process_discontinued(self, df_collect_product_base_data, row):
        if ('IsDiscontinued' not in row):
            df_collect_product_base_data['IsDiscontinued'] = [0]
            self.obReporter.update_report('Alert','IsDiscontinued was assigned')
        elif str(row['IsDiscontinued']) == 'N':
            df_collect_product_base_data['IsDiscontinued'] = [0]
        elif str(row['IsDiscontinued']) == 'Y':
            df_collect_product_base_data['IsDiscontinued'] = [1]

        return df_collect_product_base_data


    def minimum_product_price(self,df_line_product):
        # ship it!
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

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'Update Minimum Product Price'

    def filter_check_in(self, row):
        if 'Filter' in row:
            if row['Filter'] == 'Update':
                self.obReporter.update_report('Pass','This product price is an update')
                return True
            elif row['Filter'] == 'New':
                self.obReporter.update_report('Alert','This product price is new')
                return False
            else:
                self.obReporter.update_report('Alert','This product must be ingested in product')
                return False
        else:
            self.obReporter.update_report('Fail','This product price failed filtering')
            return False

## end ##