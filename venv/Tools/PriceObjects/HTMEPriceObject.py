# CreatedBy: Emilia Crow
# CreateDate: 20210609
# Updated: 20220318
# CreateFor: Franklin Young International

import xlrd
import pandas
import datetime

from Tools.BasicProcess import BasicProcessObject


class HTMEPrice(BasicProcessObject):
    req_fields = ['FyProductNumber']
    sup_fields = []
    att_fields = []
    gen_fields = ['ContractedManufacturerPartNumber']

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'HTME Price Ingestion'


    def batch_preprocessing(self):
        self.remove_private_headers()
        self.batch_process_manufacturer()

        self.df_fy_description_lookup = self.obDal.get_fy_product_descriptions()
        self.df_product = self.df_product.merge(self.df_fy_description_lookup,how='left',on=['FyProductNumber'])

        self.df_htme_contract_ids = self.obDal.get_htme_contract_ids()
        self.df_product = self.df_product.merge(self.df_htme_contract_ids,how='left',on=['FyProductNumber'])


    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'BaseProductPriceId','BaseProductPriceId_y','BaseProductPriceId_x',
                           'HTMEProductPriceId','HTMEProductPriceId_x','HTMEProductPriceId_y',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryId','CategoryId_x','CategoryId_y',
                           'Report','Filter','ProductDescriptionId','db_FyProductName','db_FyProductDescription'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
        if remove_headers != []:
            self.df_product = self.df_product.drop(columns=remove_headers)


    def filter_check_in(self, row):
        if 'BlockedManufacturer' in row:
            if int(row['BlockedManufacturer']) == 1:
                self.obReporter.update_report('Fail', 'This manufacturer name is blocked from processing')
                return False
            
        if 'ProductDescriptionId' in row:
            self.obReporter.update_report('Pass', 'This is an FyProduct update')
            return True
        else:
            self.obReporter.update_report('Fail', 'This is an FyProduct insert')
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

            htme_approved_price_date = -1
            if 'HTMEApprovedPriceDate' in row:
                htme_approved_price_date = row['HTMEApprovedPriceDate']
                try:
                    htme_approved_price_date = int(htme_approved_price_date)
                    htme_approved_price_date = xlrd.xldate_as_datetime(htme_approved_price_date, 0)
                except ValueError:
                    htme_approved_price_date = str(row['HTMEApprovedPriceDate'])

                if isinstance(htme_approved_price_date, datetime.datetime) == False:
                    try:
                        htme_approved_price_date = datetime.datetime.strptime(htme_approved_price_date, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        htme_approved_price_date = str(row['HTMEApprovedPriceDate'])
                        self.obReporter.update_report('Alert','Check HTMEApprovedPriceDate')
                    except TypeError:
                        self.obReporter.update_report('Alert','Check HTMEApprovedPriceDate')

            contract_manu_number = row['ContractedManufacturerPartNumber']

            if 'HTMEApprovedBasePrice' in row:
                approved_base_price = round(float(row['HTMEApprovedBasePrice']), 2)
            else:
                approved_base_price = ''

            approved_sell_price = round(float(row['HTMEApprovedSellPrice']), 2)
            approved_list_price = round(float(row['HTMEApprovedListPrice']), 2)

            max_markup = row['HTMEMaxMarkup']

            htme_product_notes = ''
            if 'HTMEProductNotes' in row:
                htme_product_notes = str(row['HTMEProductNotes'])

            htme_product_price_id = -1
            if 'HTMEProductPriceId' in row:
                htme_product_price_id = int(row['HTMEProductPriceId'])

            if 'db_HTMEProductPriceId' in row and htme_product_price_id == -1:
                htme_product_price_id = int(row['db_HTMEProductPriceId'])

        if htme_product_price_id == -1:
            self.obIngester.htme_product_price_insert(base_product_price_id, fy_product_number, on_contract,
                                               approved_sell_price, approved_list_price, contract_manu_number,
                                               contract_number, contract_mod_number, is_pricing_approved,
                                               approved_price_date,max_markup,htme_product_notes)
        else:
            product_price_id = int(row['ProductPriceId'])
            self.obIngester.htme_product_price_cap()
            self.obIngester.htme_product_price_update(base_product_price_id, fy_product_number, on_contract,
                                               approved_sell_price, approved_list_price, contract_manu_number,
                                               contract_number, contract_mod_number, is_pricing_approved,
                                               approved_price_date,max_markup,htme_product_notes)

        return success, return_df_line_product


    def trigger_ingest_cleanup(self):
        self.obIngester.insert_htme_product_price_cleanup()
        self.obIngester.update_htme_product_price_cleanup()


## end ##
