# CreatedBy: Emilia Crow
# CreateDate: 20210602
# Updated: 20220318
# CreateFor: Franklin Young International

import xlrd
import pandas
import datetime

from Tools.BasicProcess import BasicProcessObject

class VAPrice(BasicProcessObject):
    req_fields = ['FyProductNumber']
    sup_fields = []
    att_fields = []
    gen_fields = []

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'VA Price Ingestion'


    def batch_preprocessing(self):
        self.remove_private_headers()
        self.batch_process_manufacturer()

        self.df_fy_description_lookup = self.obDal.get_fy_product_descriptions()
        self.df_product = self.df_product.merge(self.df_fy_description_lookup,how='left',on=['FyProductNumber'])

        self.df_va_contract_ids = self.obDal.get_va_contract_ids()
        self.df_product = self.df_product.merge(self.df_va_contract_ids,how='left',on=['FyProductNumber'])



    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'BaseProductPriceId','BaseProductPriceId_y','BaseProductPriceId_x',
                           'db_VAProductPriceId','VAProductPriceId','VAProductPriceId_x','VAProductPriceId_y',
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

            for each_bool in ['VAOnContract','VAPricingApproved']:
                success, return_val = self.process_boolean(row, each_bool)
                if success:
                    df_collect_product_base_data[each_bool] = [return_val]
                else:
                    df_collect_product_base_data[each_bool] = [-1]

            if success == False:
                self.obReporter.update_report('Fail','Failed in process contract')
                return success, df_collect_product_base_data

        success, return_df_line_product = self.va_product_price(df_collect_product_base_data)

        return success, return_df_line_product


    def va_product_price(self, df_line_product):
        success = True
        return_df_line_product = df_line_product.copy()
        for colName, row in df_line_product.iterrows():

            product_description_id = row['ProductDescriptionId']
            fy_product_number = row['FyProductNumber']
            if 'VAOnContract' in row:
                on_contract = row['VAOnContract']
            else:
                on_contract = -1

            contract_number = '36F797-20-D-0158'
            if 'VAContractModificationNumber' in row:
                contract_mod_number = row['VAContractModificationNumber']
            else:
                contract_mod_number = ''

            if 'VAPricingApproved' in row:
                is_pricing_approved = float(row['VAPricingApproved'])
            else:
                is_pricing_approved = -1

            va_approved_price_date = -1
            if 'VAApprovedPriceDate' in row:
                va_approved_price_date = row['VAApprovedPriceDate']
                try:
                    va_approved_price_date = int(va_approved_price_date)
                    va_approved_price_date = xlrd.xldate_as_datetime(va_approved_price_date, 0)
                except ValueError:
                    va_approved_price_date = str(row['VAApprovedPriceDate'])

                if isinstance(va_approved_price_date, datetime.datetime) == False:
                    try:
                        va_approved_price_date = datetime.datetime.strptime(va_approved_price_date, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        va_approved_price_date = str(row['VAApprovedPriceDate'])
                        self.obReporter.update_report('Alert','Check VAApprovedPriceDate')
                    except TypeError:
                        self.obReporter.update_report('Alert','Check VAApprovedPriceDate')

            if 'VAApprovedBasePrice' in row:
                approved_base_price = float(row['VAApprovedBasePrice'])
            else:
                approved_base_price = -1

            if 'VAApprovedSellPrice' in row:
                approved_sell_price = float(row['VAApprovedSellPrice'])
            else:
                approved_sell_price = -1

            if 'VAApprovedListPrice' in row:
                approved_list_price = float(row['VAApprovedListPrice'])
            else:
                approved_list_price = -1


            if 'VADiscountPercent' in row:
                approved_percent = row['VADiscountPercent']
                success, approved_percent = self.handle_percent_val(approved_percent)
                if not success:
                    return success, return_df_line_product
            else:
                approved_percent = -1


            if 'MfcDiscountPercent' in row:
                mfc_percent = row['MfcDiscountPercent']
                success, mfc_percent = self.handle_percent_val(mfc_percent)
                if not success:
                    return success, return_df_line_product
            else:
                mfc_percent = -1

            if 'VA_Sin' in row:
                sin = row['VA_Sin']
            else:
                sin = ''

            va_product_notes = ''
            if 'VAProductNotes' in row:
                va_product_notes = str(row['VAProductNotes'])

            va_product_price_id = -1
            if 'VAProductPriceId' in row:
                va_product_price_id = int(row['VAProductPriceId'])

            if 'db_VAProductPriceId' in row and va_product_price_id == -1:
                va_product_price_id = int(row['db_VAProductPriceId'])

        if va_product_price_id == -1:
            self.obIngester.va_product_price_insert(product_description_id, fy_product_number, on_contract, approved_base_price,
                                             approved_sell_price, approved_list_price,
                                             contract_number, contract_mod_number, is_pricing_approved,
                                             va_approved_price_date, approved_percent,
                                             mfc_percent, sin, va_product_notes)
        else:
            # this may need to collect a reason why it's being put off contract
            self.obIngester.va_product_price_update(va_product_price_id, product_description_id, fy_product_number, on_contract, approved_base_price,
                                             approved_sell_price, approved_list_price,
                                             contract_number, contract_mod_number, is_pricing_approved,
                                             va_approved_price_date, approved_percent,
                                             mfc_percent, sin, va_product_notes)

        return success, return_df_line_product


    def trigger_ingest_cleanup(self):
        self.obIngester.va_product_price_insert_cleanup()
        self.obIngester.va_product_price_update_cleanup()





class UpdateVAPrice(VAPrice):
    req_fields = ['FyProductNumber']
    sup_fields = []
    att_fields = []
    gen_fields = ['VAOnContract', 'VAApprovedListPrice', 'VADiscountPercent', 'MfcDiscountPercent',
                  'VAContractModificationNumber', 'VA_Sin','VAApprovedPriceDate','VAPricingApproved']

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'VA Price Update'


## end ##
