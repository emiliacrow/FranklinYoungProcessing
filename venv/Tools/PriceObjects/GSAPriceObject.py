# CreatedBy: Emilia Crow
# CreateDate: 20210609
# Updated: 20220318
# CreateFor: Franklin Young International

import pandas
import xlrd

from Tools.BasicProcess import BasicProcessObject

class GSAPrice(BasicProcessObject):
    req_fields = ['FyCatalogNumber','FyProductNumber','ManufacturerName','ManufacturerPartNumber','VendorName','VendorPartNumber',
                  'GSAOnContract', 'GSAApprovedListPrice', 'GSAApprovedPercent', 'MfcDiscountPercent',
                  'GSAContractModificationNumber', 'GSA_Sin','GSAApprovedPriceDate','GSAPricingApproved']
    sup_fields = []
    att_fields = []
    gen_fields = ['ContractedManufacturerPartNumber']

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'GSA Price Ingestion'


    def batch_preprocessing(self):
        self.remove_private_headers()
        self.define_new()
        self.assign_contract_ids()


    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'BaseProductPriceId','BaseProductPriceId_y','BaseProductPriceId_x',
                           'db_GSAProductPriceId','GSAProductPriceId','GSAProductPriceId_x','GSAProductPriceId_y',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryId','CategoryId_x','CategoryId_y',
                           'Report','Filter'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
        if remove_headers != []:
            self.df_product = self.df_product.drop(columns=remove_headers)

    def assign_contract_ids(self):
        self.df_contract_ids = self.obDal.get_gsa_contract_ids()
        self.df_product = self.df_product.merge(self.df_contract_ids, how='left', on=['FyProductNumber'])


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

            for each_bool in ['GSAOnContract','GSAPricingApproved']:
                success, return_val = self.process_boolean(row, each_bool)
                if success:
                    df_collect_product_base_data[each_bool] = [return_val]
                else:
                    df_collect_product_base_data[each_bool] = [-1]

            success, df_collect_product_base_data = self.process_pricing(df_collect_product_base_data)
            if success == False:
                self.obReporter.update_report('Fail','Failed in process contract')
                return success, df_collect_product_base_data

        success, return_df_line_product = self.gsa_product_price(df_collect_product_base_data)

        return success, return_df_line_product


    def process_pricing(self, df_line_product):
        success = True
        return_df_line_product = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            if 'ContractedManufacturerPartNumber' in row:
                contract_manu_number = str(row['ContractedManufacturerPartNumber'])

                if 'db_ContractedManufacturerPartNumber' in row:
                    if contract_manu_number == '':
                        db_contract_manu_number = str(row['db_ContractedManufacturerPartNumber'])
                        return_df_line_product['ContractedManufacturerPartNumber'] = db_contract_manu_number
                        self.obReporter.update_report('Alert','ContractedManufacturerPartNumber from DB')

            elif 'db_ContractedManufacturerPartNumber' in row:
                db_contract_manu_number = str(row['db_ContractedManufacturerPartNumber'])
                return_df_line_product['ContractedManufacturerPartNumber'] = db_contract_manu_number
                self.obReporter.update_report('Alert','ContractedManufacturerPartNumber from DB')
            else:
                return_df_line_product['ContractedManufacturerPartNumber'] = ''

            # this is where we should try to capture the string literal '10.00%'
            # and make it what it should be
            # I think there's some code for this in base price
            if 'GSAApprovedPercent' in row:
                approved_percent = float(row['GSAApprovedPercent'])


        return success, return_df_line_product


    def gsa_product_price(self, df_line_product):
        success = True
        return_df_line_product = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            base_product_price_id = row['BaseProductPriceId']
            fy_product_number = row['FyProductNumber']
            if 'GSAOnContract' in row:
                on_contract = row['GSAOnContract']
            else:
                on_contract = -1

            contract_number = 'GS-07F-0636W'

            if 'GSAContractModificationNumber' in row:
                contract_mod_number = row['GSAContractModificationNumber']
            else:
                contract_mod_number = ''

            if 'GSAPricingApproved' in row:
                is_pricing_approved = float(row['GSAPricingApproved'])
            else:
                is_pricing_approved = -1

            if 'GSAApprovedPriceDate' in row:
                try:
                    approved_price_date = int(row['GSAApprovedPriceDate'])
                    approved_price_date = (xlrd.xldate_as_datetime(approved_price_date, 0)).date()
                except ValueError:
                    approved_price_date = str(row['GSAApprovedPriceDate'])
            else:
                approved_price_date = '0000-00-00 00:00:00'

            contract_manu_number = row['ContractedManufacturerPartNumber']

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


            if 'GSAApprovedPercent' in row:
                approved_percent = float(row['GSAApprovedPercent'])
            else:
                approved_percent = -1

            if 'MfcDiscountPercent' in row:
                mfc_percent = row['MfcDiscountPercent']
            else:
                mfc_percent = -1

            if 'GSA_Sin' in row:
                sin = row['GSA_Sin']
            else:
                sin = ''

            product_notes = ''
            if 'GSAProductNotes' in row:
                product_notes = str(row['GSAProductNotes'])

            gsa_product_price_id = -1
            if 'GSAProductPriceId' in row:
                gsa_product_price_id = int(row['GSAProductPriceId'])

            if 'db_GSAProductPriceId' in row and gsa_product_price_id == -1:
                gsa_product_price_id = int(row['db_GSAProductPriceId'])

        if gsa_product_price_id == -1:
            self.obIngester.gsa_product_price_insert(base_product_price_id, fy_product_number, on_contract, approved_base_price,
                                              approved_sell_price, approved_list_price, contract_manu_number,
                                              contract_number, contract_mod_number, is_pricing_approved,
                                              approved_price_date, approved_percent, mfc_percent,
                                              sin, product_notes)
        else:
            product_price_id = int(row['ProductPriceId'])
            self.obIngester.gsa_product_price_update(gsa_product_price_id, base_product_price_id, product_price_id, fy_product_number, on_contract, approved_base_price,
                                              approved_sell_price, approved_list_price, contract_manu_number,
                                              contract_number, contract_mod_number, is_pricing_approved,
                                              approved_price_date, approved_percent, mfc_percent,
                                              sin, product_notes)


        return success, return_df_line_product


    def trigger_ingest_cleanup(self):
        self.obIngester.insert_gsa_product_price_cleanup()
        self.obIngester.update_gsa_product_price_cleanup()


class UpdateGSAPrice(GSAPrice):
    req_fields = ['FyCatalogNumber','FyProductNumber','ManufacturerName','ManufacturerPartNumber','VendorName','VendorPartNumber']
    sup_fields = []
    att_fields = []
    gen_fields = ['GSAOnContract', 'GSAApprovedListPrice', 'GSAApprovedPercent', 'MfcDiscountPercent',
                  'GSAContractModificationNumber', 'GSA_Sin','GSAApprovedPriceDate','GSAPricingApproved','ContractedManufacturerPartNumber']

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'GSA Price Ingestion'


## end ##
