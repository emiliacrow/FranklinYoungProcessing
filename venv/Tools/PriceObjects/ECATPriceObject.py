# CreatedBy: Emilia Crow
# CreateDate: 20210609
# Updated: 20220318
# CreateFor: Franklin Young International

import pandas
import xlrd

from Tools.BasicProcess import BasicProcessObject

class ECATPrice(BasicProcessObject):
    req_fields = ['FyCatalogNumber','FyProductNumber','ManufacturerName', 'ManufacturerPartNumber','VendorName','VendorPartNumber',
                  'ECATOnContract', 'ECATApprovedListPrice', 'ECATMaxMarkup', 'ECATContractModificationNumber',
                  'ECATApprovedPriceDate','ECATPricingApproved']
    sup_fields = []
    att_fields = []
    gen_fields = ['ContractedManufacturerPartNumber']

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'ECAT Price Ingestion'


    def batch_preprocessing(self):
        self.remove_private_headers()
        self.define_new()
        if 'FySellPrice' not in self.df_product.columns:
            self.get_fy_sell_price()


    def get_fy_sell_price(self):
        self.ecat_price_lookup = self.obDal.get_ecat_price_lookup()
        merge_heads = ['FyCatalogNumber', 'FyProductNumber', 'ManufacturerName', 'ManufacturerPartNumber', 'VendorName', 'VendorPartNumber']
        self.df_product = self.df_product.merge(self.ecat_price_lookup, how= 'left', on = merge_heads)


    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'BaseProductPriceId','BaseProductPriceId_y','BaseProductPriceId_x',
                           'ECATProductPriceId','ECATProductPriceId_x','ECATProductPriceId_y',
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

            for each_bool in ['ECATOnContract','ECATPricingApproved']:
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

        success, return_df_line_product = self.ecat_product_price(df_collect_product_base_data)

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

            if 'ECATSellPrice' in row:
                ecat_sell_price = float(row['ECATSellPrice'])
                return_df_line_product['ECATSellPrice'] = ecat_sell_price

            elif 'db_FyCost' in row:
                fy_cost = float(row['db_FyCost'])
                max_markup = float(row['ECATMaxMarkup'])
                ecat_sell_price = round((fy_cost*max_markup),2)
                return_df_line_product['ECATSellPrice'] = ecat_sell_price
            else:
                self.obReporter.update_report('Fail', 'Check for db_FyCost  or ECATSellPrice')
                return False, return_df_line_product

        return success, return_df_line_product


    def ecat_product_price(self, df_line_product):
        success = True
        return_df_line_product = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            ecat_product_price_id = -1
            base_product_price_id = row['BaseProductPriceId']
            fy_product_number = row['FyProductNumber']
            on_contract = row['ECATOnContract']

            contract_number = 'SPE2DE-21-D-0014'
            contract_mod_number = row['ECATContractModificationNumber']
            is_pricing_approved = row['ECATPricingApproved']

            try:
                approved_price_date = int(row['ECATApprovedPriceDate'])
                approved_price_date = (xlrd.xldate_as_datetime(approved_price_date, 0)).date()
            except ValueError:
                approved_price_date = str(row['ECATApprovedPriceDate'])

            contract_manu_number = row['ContractedManufacturerPartNumber']

            approved_sell_price = float(row['ECATApprovedSellPrice'])
            approved_list_price = float(row['ECATApprovedListPrice'])

            ecat_sell_price = round(float(row['ECATSellPrice']), 2)
            max_markup = row['ECATMaxMarkup']

            product_notes = ''
            if 'ECATProductNotes' in row:
                product_notes = str(row['ECATProductNotes'])

            ecat_product_price_id = -1
            if 'ECATProductPriceId' in row:
                ecat_product_price_id = int(row['ECATProductPriceId'])

        if ecat_product_price_id == -1:
            self.obIngester.ecat_product_price_insert(base_product_price_id, fy_product_number, on_contract,
                                               approved_sell_price, approved_list_price, contract_manu_number,
                                               contract_number, contract_mod_number, is_pricing_approved,
                                               approved_price_date, ecat_sell_price,max_markup,product_notes)
        else:
            product_price_id = int(row['ProductPriceId'])
            self.obIngester.ecat_product_price_update(ecat_product_price_id, base_product_price_id, product_price_id, fy_product_number, on_contract,
                                               approved_sell_price, approved_list_price, contract_manu_number,
                                               contract_number, contract_mod_number, is_pricing_approved,
                                               approved_price_date, ecat_sell_price,max_markup,product_notes)

        return success, return_df_line_product


    def trigger_ingest_cleanup(self):
        self.obIngester.insert_ecat_product_price_cleanup()
        self.obIngester.update_ecat_product_price_cleanup()


## end ##
