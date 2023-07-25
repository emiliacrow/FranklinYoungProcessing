# CreatedBy: Emilia Crow
# CreateDate: 20210609
# Updated: 20230328
# CreateFor: Franklin Young International

import xlrd
import pandas
import datetime

from Tools.BasicProcess import BasicProcessObject

class GSAPrice(BasicProcessObject):
    req_fields = ['FyProductNumber']
    sup_fields = []
    att_fields = []
    gen_fields = []

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'GSA Price Ingestion'


    def batch_preprocessing(self):
        self.remove_private_headers()
        self.batch_process_manufacturer()

        self.df_fy_description_lookup = self.obDal.get_fy_product_descriptions()
        self.df_product = self.df_product.merge(self.df_fy_description_lookup,how='left',on=['FyProductNumber'])

        self.df_gsa_contract_ids = self.obDal.get_gsa_contract_ids()
        self.df_product = self.df_product.merge(self.df_gsa_contract_ids,how='left',on=['FyProductNumber'])


    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'BaseProductPriceId','BaseProductPriceId_y','BaseProductPriceId_x',
                           'ProductDescriptionId','db_GSAProductPriceId','GSAProductPriceId','GSAProductPriceId_x','GSAProductPriceId_y',
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

            for each_bool in ['GSAOnContract','GSAPricingApproved']:
                success, return_val = self.process_boolean(row, each_bool)
                if success:
                    df_collect_product_base_data[each_bool] = [return_val]
                else:
                    df_collect_product_base_data[each_bool] = [-1]

        success, return_df_line_product = self.gsa_product_price(df_collect_product_base_data)

        return success, return_df_line_product


    def gsa_product_price(self, df_line_product):
        success = True
        return_df_line_product = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            product_description_id = int(row['ProductDescriptionId'])

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

            gsa_approved_price_date = -1
            if 'GSAApprovedPriceDate' in row:
                gsa_approved_price_date = row['GSAApprovedPriceDate']
                try:
                    gsa_approved_price_date = int(gsa_approved_price_date)
                    gsa_approved_price_date = xlrd.xldate_as_datetime(gsa_approved_price_date, 0)
                except ValueError:
                    gsa_approved_price_date = str(row['GSAApprovedPriceDate'])

                if isinstance(gsa_approved_price_date, datetime.datetime) == False:
                    try:
                        gsa_approved_price_date = datetime.datetime.strptime(gsa_approved_price_date, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        gsa_approved_price_date = str(row['GSAApprovedPriceDate'])
                        self.obReporter.update_report('Alert','Check GSAApprovedPriceDate')
                    except TypeError:
                        self.obReporter.update_report('Alert','Check GSAApprovedPriceDate')

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


            if 'GSADiscountPercent' in row:
                approved_percent = float(row['GSADiscountPercent'])
                success, approved_percent = self.handle_percent_val(approved_percent)
                if not success:
                    return success, return_df_line_product
            else:
                approved_percent = -1

            if 'MfcDiscountPercent' in row:
                mfc_percent = float(row['MfcDiscountPercent'])
                success, mfc_percent = self.handle_percent_val(mfc_percent)
                if not success:
                    return success, return_df_line_product
            else:
                mfc_percent = -1

            if 'GSA_Sin' in row:
                sin = row['GSA_Sin']
            else:
                sin = ''

            gsa_product_notes = ''
            if 'GSAProductNotes' in row:
                gsa_product_notes = str(row['GSAProductNotes'])

            gsa_product_price_id = -1
            if 'GSAProductPriceId' in row:
                gsa_product_price_id = int(row['GSAProductPriceId'])

            if 'db_GSAProductPriceId' in row and gsa_product_price_id == -1:
                gsa_product_price_id = int(row['db_GSAProductPriceId'])

        if gsa_product_price_id == -1:
            self.obIngester.gsa_product_price_insert(product_description_id, fy_product_number, on_contract, approved_base_price,
                                              approved_sell_price, approved_list_price,
                                              contract_number, contract_mod_number, is_pricing_approved,
                                              gsa_approved_price_date, approved_percent, mfc_percent,
                                              sin, gsa_product_notes)
        else:
            self.obIngester.gsa_product_price_update(gsa_product_price_id, product_description_id, fy_product_number, on_contract, approved_base_price,
                                              approved_sell_price, approved_list_price,
                                              contract_number, contract_mod_number, is_pricing_approved,
                                              gsa_approved_price_date, approved_percent, mfc_percent,
                                              sin, gsa_product_notes)


        return success, return_df_line_product


    def trigger_ingest_cleanup(self):
        self.obIngester.insert_gsa_product_price_cleanup()
        self.obIngester.update_gsa_product_price_cleanup()


class UpdateGSAPrice(GSAPrice):
    req_fields = ['FyProductNumber']
    sup_fields = []
    att_fields = []
    gen_fields = ['GSAOnContract', 'GSAApprovedListPrice', 'GSADiscountPercent', 'MfcDiscountPercent',
                  'GSAContractModificationNumber', 'GSA_Sin','GSAApprovedPriceDate','GSAPricingApproved']

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'GSA Price Ingestion'


## end ##
