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
        self.assign_contract_ids()

        self.df_fy_description_lookup = self.obDal.get_fy_product_descriptions_short()
        self.df_product = self.df_product.merge(self.df_fy_description_lookup,how='left',on=['FyProductNumber'])


    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'BaseProductPriceId','BaseProductPriceId_y','BaseProductPriceId_x',
                           'db_ECATProductPriceId','ECATProductPriceId','ECATProductPriceId_x','ECATProductPriceId_y',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryId','CategoryId_x','CategoryId_y',
                           'Report','Filter'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
        if remove_headers != []:
            self.df_product = self.df_product.drop(columns=remove_headers)


    def assign_contract_ids(self):
        self.df_contract_ids = self.obDal.get_ecat_contract_ids()
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

            for each_bool in ['ECATOnContract','ECATPricingApproved']:
                success, return_val = self.process_boolean(row, each_bool)
                if success:
                    df_collect_product_base_data[each_bool] = [return_val]
                else:
                    df_collect_product_base_data[each_bool] = [-1]

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

                if 'db_ContractedManufacturerPartNumber' in row:
                    if contract_manu_number == '':
                        db_contract_manu_number = row['db_ContractedManufacturerPartNumber']
                        contract_manu_number = db_contract_manu_number
                        return_df_line_product['ContractedManufacturerPartNumber'] = db_contract_manu_number
                        self.obReporter.update_report('Alert','ContractedManufacturerPartNumber from DB')

            elif 'db_ContractedManufacturerPartNumber' in row:
                contract_manu_number = db_contract_manu_number
                return_df_line_product['ContractedManufacturerPartNumber'] = db_contract_manu_number
                self.obReporter.update_report('Alert','ContractedManufacturerPartNumber from DB')
            else:
                return_df_line_product['ContractedManufacturerPartNumber'] = ''


        return success, return_df_line_product


    def ecat_product_price(self, df_line_product):
        success = True
        return_df_line_product = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            product_description_id = row['ProductDescriptionId']
            fy_product_number = row['FyProductNumber']
            if 'ECATOnContract' in row:
                on_contract = row['ECATOnContract']
            else:
                on_contract = -1

            contract_number = 'SPE2DE-21-D-0014'

            if 'ECATContractModificationNumber' in row:
                contract_mod_number = row['ECATContractModificationNumber']
            else:
                contract_mod_number = ''

            if 'ECATPricingApproved' in row:
                is_pricing_approved = float(row['ECATPricingApproved'])
            else:
                is_pricing_approved = -1

            ecat_approved_price_date = -1
            if 'ECATApprovedPriceDate' in row:
                ecat_approved_price_date = row['ECATApprovedPriceDate']
                try:
                    ecat_approved_price_date = int(ecat_approved_price_date)
                    ecat_approved_price_date = xlrd.xldate_as_datetime(ecat_approved_price_date, 0)
                except ValueError:
                    ecat_approved_price_date = str(row['ECATApprovedPriceDate'])

                if isinstance(ecat_approved_price_date, datetime.datetime) == False:
                    try:
                        ecat_approved_price_date = datetime.datetime.strptime(ecat_approved_price_date, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        ecat_approved_price_date = str(row['ECATApprovedPriceDate'])
                        self.obReporter.update_report('Alert','Check ECATApprovedPriceDate')
                    except TypeError:
                        self.obReporter.update_report('Alert','Check ECATApprovedPriceDate')

            contract_manu_number = row['ContractedManufacturerPartNumber']

            if 'ECATApprovedSellPrice' in row:
                approved_sell_price = float(row['ECATApprovedSellPrice'])
            else:
                approved_sell_price = -1

            if 'ECATApprovedListPrice' in row:
                approved_list_price = float(row['ECATApprovedListPrice'])
            else:
                approved_list_price = -1

            if 'ECATMaxMarkup' in row:
                max_markup = float(row['ECATMaxMarkup'])
            else:
                max_markup = -1

            ecat_product_notes = ''
            if 'ECATProductNotes' in row:
                ecat_product_notes = str(row['ECATProductNotes'])

            ecat_product_price_id = -1
            if 'ECATProductPriceId' in row:
                ecat_product_price_id = int(row['ECATProductPriceId'])

            if 'db_ECATProductPriceId' in row and ecat_product_price_id == -1:
                ecat_product_price_id = int(row['db_ECATProductPriceId'])

        if ecat_product_price_id == -1:
            self.obIngester.ecat_product_price_insert(product_description_id, fy_product_number, on_contract,
                                               approved_sell_price, approved_list_price, contract_manu_number,
                                               contract_number, contract_mod_number, is_pricing_approved,
                                               ecat_approved_price_date, max_markup, ecat_product_notes)
        else:
            self.obIngester.ecat_product_price_update(ecat_product_price_id, product_description_id, fy_product_number, on_contract,
                                               approved_sell_price, approved_list_price, contract_manu_number,
                                               contract_number, contract_mod_number, is_pricing_approved,
                                               ecat_approved_price_date, max_markup, ecat_product_notes)

        return success, return_df_line_product


    def trigger_ingest_cleanup(self):
        self.obIngester.insert_ecat_product_price_cleanup()
        self.obIngester.update_ecat_product_price_cleanup()

class UpdateECATPrice(ECATPrice):
    req_fields = ['FyCatalogNumber','FyProductNumber','ManufacturerName', 'ManufacturerPartNumber','VendorName','VendorPartNumber']
    sup_fields = []
    att_fields = []
    gen_fields = ['ContractedManufacturerPartNumber','ECATOnContract', 'ECATApprovedListPrice', 'ECATMaxMarkup', 'ECATContractModificationNumber',
                  'ECATApprovedPriceDate','ECATPricingApproved']

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'ECAT Price Update'

## end ##
