# CreatedBy: Emilia Crow
# CreateDate: 20210528
# Updated: 20210813
# CreateFor: Franklin Young International

import pandas

from Tools.BasicProcess import BasicProcessObject


class MinimumProductPrice(BasicProcessObject):
    req_fields = ['VendorName','FyCatalogNumber','AllowPurchases']
    sup_fields = []
    gen_fields = ['FyProductNumber', 'ProductId', 'VendorId', 'UnitOfIssueId']
    att_fields = []

    def __init__(self,df_product,is_testing):
        super().__init__(df_product,is_testing)
        self.name = 'Minimum Product Price'

    def batch_preprocessing(self):
        self.df_uoi_lookup = self.obDal.get_unit_of_issue_lookup()
        if 'VendorId' not in self.df_product.columns:
            self.batch_process_vendor()
        self.define_new()


    def define_new(self):
        # these are the df's for assigning data.
        self.df_product_lookup = self.obDal.get_product_lookup()
        self.df_product_price_lookup = self.obDal.get_product_price_lookup()

        # if there's already a filter column, we remove it.
        if 'Filter' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns=['Filter'])

        self.df_product_lookup['Filter'] = 'Update'
        # match all products on FyProdNum and Manufacturer part, clearly
        if 'ManufacturerPartNumber' in self.df_product.columns:
            self.df_product = pandas.DataFrame.merge(self.df_product, self.df_product_lookup,
                                                            how='left',
                                                            on=['FyCatalogNumber', 'ManufacturerPartNumber'])
        else:
            self.df_product = pandas.DataFrame.merge(self.df_product, self.df_product_lookup,
                                                            how='left', on=['FyCatalogNumber'])

        # we assign a label to the products that are haven't been loaded through product yet
        self.df_product.loc[(self.df_product['Filter'] != 'Update'), 'Report'] = 'This product must be loaded.'
        self.df_product.loc[(self.df_product['Filter'] != 'Update'), 'Filter'] = 'Fail'

        # split the data for a moment
        self.df_update_product = self.df_product[(self.df_product['Filter'] == 'Update')]
        self.df_product = self.df_product[(self.df_product['Filter'] != 'Update')]

        if len(self.df_update_product.index) != 0:
            self.df_product_price_lookup['Filter'] = 'Pass'
            # this section evaluates if these have product data loaded
            # drop some columns to ease processing
            if 'Filter' in self.df_update_product.columns:
                self.df_update_product = self.df_update_product.drop(columns=['Filter'])

            # this gets the productId
            self.df_update_product = pandas.DataFrame.merge(self.df_update_product, self.df_product_price_lookup,
                                                             how='left', on=['FyProductNumber'])

            self.df_update_product.loc[(self.df_update_product['Filter'] != 'Pass'), 'Filter'] = 'Update'

            if 'ProductId_x' in self.df_update_product.columns:
                self.df_update_product['ProductId'] = self.df_update_product[['ProductId_x']]
                self.df_update_product = self.df_update_product.drop(columns=['ProductId_x'])
                self.df_update_product = self.df_update_product.drop(columns=['ProductId_y'])
            # recombine with product
            self.df_product = self.df_product.append(self.df_update_product)



    def batch_process_something(self, df_row):
        some_val = 1
        return some_val

    def batch_process_vendor(self):
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


    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()

        # step-wise product processing
        for colName, row in df_line_product.iterrows():
            if 'Filter' in row:
                if row['Filter'] == 'Pass':
                    return True, df_collect_product_base_data
                elif row['Filter'] != 'Update':
                    return True, df_collect_product_base_data
            else:
                return False, df_collect_product_base_data

            # this is also stupid, but it gets the point across for testing purposes
            success, df_collect_product_base_data = self.process_vendor(df_collect_product_base_data, row)
            if success == False:
                df_collect_product_base_data['FinalReport'] = ['Failed at vendor identification']
                return success, df_collect_product_base_data

            success, df_collect_product_base_data = self.identify_fy_product_number(df_collect_product_base_data, row)
            if success == False:
                df_collect_product_base_data['FinalReport'] = ['Can\'t identify product number']
                return success, df_collect_product_base_data

            success, df_collect_product_base_data = self.process_pricing(df_collect_product_base_data, row)
            if success == False:
                df_collect_product_base_data['FinalReport'] = ['Failed at pricing processing']
                return success, df_collect_product_base_data


        success, df_collect_product_base_data = self.minimum_product_price(df_collect_product_base_data)
        df_line_product = df_collect_product_base_data

        if success:
            df_line_product['FinalReport'] = ['Min product price Success']
            return True, df_line_product
        else:
            df_line_product['FinalReport'] = ['Min product price ingestion Failure']
            return False, df_line_product

        return True, df_line_product


    def identify_fy_product_number(self, df_collect_product_base_data, row):
        if ('UnitOfIssueId' not in row):
            if ('UnitOfMeasure' not in row) or ('Conv Factor/QTY UOM' not in row):
                df_collect_product_base_data['Report'] = ['Unit of measure missing']
                return False, df_collect_product_base_data

            unit_of_issue = 'EA'
            if 'UnitOfIssue' in row:
                unit_of_issue = row['UnitOfIssue']

            unit_count = row['Conv Factor/QTY UOM']
            unit_of_measure = row['UnitOfMeasure']
            # this is where it will break if there is a new one.
            # and that's stupid

            try:
                unit_of_issue_id = self.df_uoi_lookup[(self.df_uoi_lookup['UnitOfIssueSymbol'] == unit_of_issue) &
                                                      (self.df_uoi_lookup['Count'] == unit_count) &
                                                      (self.df_uoi_lookup['UnitOfMeasureSymbol'] == unit_of_measure),'UnitOfIssueId'].values[0]
            except:
                unit_of_issue_id = self.obIngester.ingest_uoi_by_symbol(unit_of_issue, unit_count, unit_of_measure)

            df_collect_product_base_data['UnitOfIssueId'] = [unit_of_issue_id]

        elif ('UnitOfIssue' not in row):
            unit_of_issue_id = row['UnitOfIssueId']
            unit_of_issue = self.df_uoi_lookup[(self.df_uoi_lookup['UnitOfIssueId'] == unit_of_issue_id),'UnitOfIssueSymbol'].values[0]
        else:
            unit_of_issue = row['UnitOfIssue']


        if 'FyCatalogNumber' in row:
            fy_catalog_number = df_collect_product_base_data.at[row.name,'FyCatalogNumber']
        else:
            df_collect_product_base_data['Report'] = ['Missing catalog number']
            return False, df_collect_product_base_data

        if ('FyProductNumber' not in row):
            if unit_of_issue != 'EA':
                fy_product_number = fy_catalog_number + ' ' + unit_of_issue
                df_collect_product_base_data['FyProductNumber'] = [fy_product_number]
            elif unit_of_measure != 'EA':
                fy_product_number = fy_catalog_number + ' ' + unit_of_measure
                df_collect_product_base_data['FyProductNumber'] = [fy_product_number]
            else:
                fy_product_number = fy_catalog_number
                df_collect_product_base_data['FyProductNumber'] = [fy_product_number]

        elif (row['FyProductNumber'] == fy_catalog_number + ' ' + unit_of_issue) or (row['FyProductNumber'] == fy_catalog_number):
            fy_product_number = row['FyProductNumber']
            df_collect_product_base_data['FyProductNumber'] = [fy_product_number]
        else:
            df_collect_product_base_data['Report'] = ['There was a conflict at FyCatalogNumber']
            return False, df_collect_product_base_data

        if ('FyPartNumber' not in row):
            df_collect_product_base_data['FyPartNumber'] = [fy_product_number]

        if ('VendorPartNumber' not in row):
            df_collect_product_base_data['Report'] = ['VendorPartNumber was missing']
            return False, df_collect_product_base_data

        return True, df_collect_product_base_data


    def process_vendor(self, df_collect_product_base_data, row):
        if 'VendorId' not in row:
            df_collect_product_base_data['Report'] = ['Missing vendor name and code']
            return False, df_collect_product_base_data
        elif row['VendorId'] == -1:
            df_collect_product_base_data['Report'] = ['Missing vendor name and code']
            return False, df_collect_product_base_data

        return True, df_collect_product_base_data

    def process_pricing(self, df_collect_product_base_data, row):
        if ('AllowPurchases' not in row):
            df_collect_product_base_data['Report'] = ['AllowPurchases was missing']
            return False, df_collect_product_base_data
        elif row['AllowPurchases'] == 'N':
            df_collect_product_base_data['AllowPurchases'] = 0
        elif row['AllowPurchases'] == 'Y':
            df_collect_product_base_data['AllowPurchases'] = 1

        if ('ProductTaxClass' not in row):
            df_collect_product_base_data['ProductTaxClass'] = 'Default Tax Class'

        return True, df_collect_product_base_data


    def minimum_product_price(self,df_line_product):
        # ship it!
        for colName, row in df_line_product.iterrows():
            fy_product_number = row['FyProductNumber']
            allow_purchases = row['AllowPurchases']
            fy_part_number = row['FyPartNumber']
            peoduct_tax_class = row['ProductTaxClass']
            vendor_part_number = row['VendorPartNumber']

            product_id = row['ProductId']
            vendor_id = row['VendorId']
            unit_of_issue_id = row['UnitOfIssueId']

        self.obIngester.ingest_product_price(self.is_last, fy_product_number,allow_purchases,
                                                             fy_part_number, peoduct_tax_class, vendor_part_number,
                                                             product_id, vendor_id, unit_of_issue_id)

        return True, df_line_product




## end ##