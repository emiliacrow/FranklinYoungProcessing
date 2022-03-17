# CreatedBy: Emilia Crow
# CreateDate: 20210609
# Updated: 20220318
# CreateFor: Franklin Young International

import pandas
import xlrd

from Tools.BasicProcess import BasicProcessObject

class HTMEPrice(BasicProcessObject):
    req_fields = ['FyCatalogNumber','FyProductNumber','ManufacturerName', 'ManufacturerPartNumber','VendorName','VendorPartNumber',
                  'HTMEOnContract', 'HTMEApprovedListPrice', 'HTMEMaxMarkup', 'HTMEContractModificationNumber',
                  'HTMEApprovedPriceDate','HTMEPricingApproved']
    sup_fields = []
    att_fields = []
    gen_fields = ['ContractedManufacturerPartNumber']

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'HTME Price Ingestion'


    def batch_preprocessing(self):
        self.remove_private_headers()
        self.define_new()


    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'BaseProductPriceId','BaseProductPriceId_y','BaseProductPriceId_x',
                           'HTMEProductPriceId','HTMEProductPriceId_x','HTMEProductPriceId_y',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryId','CategoryId_x','CategoryId_y',
                           'Report','Filter'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
        if remove_headers != []:
            self.df_product = self.df_product.drop(columns=remove_headers)


    def filter_check_in(self, row):
        filter_options = ['Base Pricing', 'New', 'Partial', 'Possible Duplicate', 'Ready', 'case_1','case_4']

        if row['Filter'] == 'New':
            self.obReporter.update_report('Alert', 'Passed filtering as a new product but not processed')
            return False

        elif row['Filter'] in ['Partial', 'Base Pricing']:
            self.obReporter.update_report('Alert', 'Passed filtering as partial product')
            return False

        elif row['Filter'] == 'Ready':
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

            for each_bool in ['HTMEOnContract','HTMEPricingApproved']:
                success, return_val = self.process_boolean(row, each_bool)
                if success:
                    df_collect_product_base_data[each_bool] = [return_val]
                else:
                    self.obReporter.update_report('Alert', '{0} was set to 0'.format(each_bool))
                    df_collect_product_base_data[each_bool] = [0]


            success, df_collect_product_base_data = self.process_pricing(df_collect_product_base_data)
            if success == False:
                self.obReporter.update_report('Fail','Failed in process contract')
                return success, df_collect_product_base_data

        success, return_df_line_product = self.htme_product_price(df_collect_product_base_data)

        return success, return_df_line_product



    def process_pricing(self, df_line_product):
        success = True
        return_df_line_product = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            if 'ContractedManufacturerPartNumber' in row:
                contract_manu_number = row['ContractedManufacturerPartNumber']

                if 'db_ContractedManufacturerPartNumber' in row and contract_manu_number == '':
                    db_contract_manu_number = row['db_ContractedManufacturerPartNumber']
                    contract_manu_number = db_contract_manu_number
                    return_df_line_product['ContractedManufacturerPartNumber'] = db_contract_manu_number

            elif 'db_ContractedManufacturerPartNumber' in row:
                contract_manu_number = db_contract_manu_number
                return_df_line_product['ContractedManufacturerPartNumber'] = db_contract_manu_number
            else:
                return_df_line_product['ContractedManufacturerPartNumber'] = ''

            if 'HTMESellPrice' in row:
                htme_sell_price = float(row['HTMESellPrice'])
                return_df_line_product['HTMESellPrice'] = htme_sell_price

            elif 'db_fy_cost' in row:
                fy_cost = float(row['db_fy_cost'])
                max_markup = float(row['HTMEMaxMarkup'])
                htme_sell_price = round((fy_cost*max_markup),2)
                return_df_line_product['HTMESellPrice'] = htme_sell_price
            else:
                self.obReporter.update_report('Fail', 'Check for db_fy_cost  or HTMESellPrice')
                return False, return_df_line_product

        return success, return_df_line_product


    def htme_product_price(self, df_line_product):
        success = True
        return_df_line_product = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            base_product_price_id = row['BaseProductPriceId']
            fy_product_number = row['FyProductNumber']
            on_contract = row['HTMEOnContract']

            contract_number = 'SPE2DE-21-D-0014'
            contract_mod_number = row['HTMEContractModificationNumber']
            is_pricing_approved = row['HTMEPricingApproved']

            try:
                approved_price_date = int(row['HTMEApprovedPriceDate'])
                approved_price_date = (xlrd.xldate_as_datetime(approved_price_date, 0)).date()
            except ValueError:
                approved_price_date = str(row['HTMEApprovedPriceDate'])

            contract_manu_number = row['ContractedManufacturerPartNumber']

            if 'HTMEApprovedBasePrice' in row:
                approved_base_price = round(float(row['HTMEApprovedBasePrice']), 2)
            else:
                approved_base_price = ''

            approved_sell_price = round(float(row['HTMEApprovedSellPrice']), 2)
            approved_list_price = round(float(row['HTMEApprovedListPrice']), 2)

            htme_sell_price = round(float(row['HTMESellPrice']), 2)
            max_markup = row['HTMEMaxMarkup']

        self.obIngester.htme_product_price_cap(base_product_price_id, fy_product_number, on_contract,
                                               approved_sell_price, approved_list_price, contract_manu_number,
                                               contract_number, contract_mod_number, is_pricing_approved,
                                               approved_price_date, htme_sell_price,max_markup)

        return success, return_df_line_product


    def trigger_ingest_cleanup(self):
        self.obIngester.ingest_htme_product_price_cleanup()


## end ##
