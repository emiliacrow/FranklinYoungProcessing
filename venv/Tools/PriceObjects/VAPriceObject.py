# CreatedBy: Emilia Crow
# CreateDate: 20210602
# Updated: 20220209
# CreateFor: Franklin Young International

import pandas
import xlrd

from Tools.BasicProcess import BasicProcessObject

class VAPrice(BasicProcessObject):
    req_fields = ['FyCatalogNumber','FyProductNumber','ManufacturerName', 'ManufacturerPartNumber','VendorName','VendorPartNumber',
                  'VAOnContract', 'VAApprovedListPrice', 'VAApprovedPercent', 'MfcDiscountPercent',
                  'VAContractModificationNumber', 'VA_Sin','VAApprovedPriceDate','VAPricingApproved']
    sup_fields = []
    att_fields = []
    gen_fields = ['ContractedManufacturerPartNumber']

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'VA Price Ingestion'


    def batch_preprocessing(self):
        self.remove_private_headers()
        self.define_new()


    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'BaseProductPriceId','BaseProductPriceId_y','BaseProductPriceId_x',
                           'VAProductPriceId','VAProductPriceId_x','VAProductPriceId_y',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryId','CategoryId_x','CategoryId_y',
                           'Report','Filter'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
        if remove_headers != []:
            self.df_product = self.df_product.drop(columns=remove_headers)


    def filter_check_in(self, row):
        filter_options = ['Base Pricing', 'ConfigurationChanges', 'New', 'PartNumberOverride', 'Partial', 'Possible Duplicate', 'Ready', 'Update-product', 'Update-vendor', 'VendorPartNumberChange']

        if row['Filter'] == 'New':
            self.obReporter.update_report('Alert', 'Passed filtering as a new product but not processed')
            return False

        elif row['Filter'] in ['Partial', 'Update-product', 'Update-vendor', 'ConfigurationChanges','PartNumberOverride', 'Base Pricing','VendorPartNumberChange']:
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

            for each_bool in ['VAOnContract','VAPricingApproved']:
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

        success, return_df_line_product = self.va_product_price(df_collect_product_base_data)

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

            if 'VABasePrice' not in row:
                approved_list_price = float(row['VAApprovedListPrice'])
                approved_percent = float(row['VAApprovedPercent'])
                va_base_price = round(approved_list_price-(approved_list_price*approved_percent),4)
                self.obReporter.update_report('Alert','VABasePrice was calculated')

                return_df_line_product['VABasePrice'] = va_base_price
            else:
                va_base_price = round(float(row['VABasePrice']), 4)
                return_df_line_product['VABasePrice'] = va_base_price
                self.obReporter.update_report('Alert','VABasePrice was rounded to 4')

            if 'VASellPrice' not in row:
                iff_fee_percent = 0.995
                va_sell_price = round(va_base_price/iff_fee_percent, 2)
                return_df_line_product['VASellPrice'] = va_sell_price
                self.obReporter.update_report('Alert','VASellPrice was calculated')

            if 'MfcPrice' not in row:
                mfc_precent = float(row['MfcDiscountPercent'])
                return_df_line_product['MfcPrice'] = round(approved_list_price-(approved_list_price*mfc_precent),2)
                self.obReporter.update_report('Alert','MfcPrice was calculated')

        return success, return_df_line_product


    def va_product_price(self, df_line_product):
        success = True
        return_df_line_product = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            base_product_price_id = row['BaseProductPriceId']
            fy_product_number = row['FyProductNumber']
            on_contract = row['VAOnContract']

            contract_number = 'VA797H-16-D-0024/SPE2D1-16-D-0019'
            contract_mod_number = row['VAContractModificationNumber']
            is_pricing_approved = row['VAPricingApproved']

            try:
                approved_price_date = int(row['VAApprovedPriceDate'])
                approved_price_date = (xlrd.xldate_as_datetime(approved_price_date, 0)).date()
            except ValueError:
                approved_price_date = str(row['VAApprovedPriceDate'])

            contract_manu_number = row['ContractedManufacturerPartNumber']

            if 'VAApprovedBasePrice' in row:
                approved_base_price = float(row['VAApprovedBasePrice'])
            else:
                approved_base_price = ''

            approved_sell_price = float(row['VAApprovedSellPrice'])
            approved_list_price = float(row['VAApprovedListPrice'])
            approved_percent = float(row['VAApprovedPercent'])

            va_base_price = row['VABasePrice']
            va_sell_price = row['VASellPrice']

            mfc_precent = row['MfcDiscountPercent']
            mfc_price = row['MfcPrice']

            sin = row['VA_Sin']


        print(base_product_price_id, fy_product_number, on_contract, approved_base_price,
                                             approved_sell_price, approved_list_price, contract_manu_number,
                                             contract_number, contract_mod_number, is_pricing_approved,
                                             approved_price_date, approved_percent, va_base_price, va_sell_price,
                                             mfc_precent, mfc_price, sin)

        self.obIngester.va_product_price_cap(base_product_price_id, fy_product_number, on_contract, approved_base_price,
                                             approved_sell_price, approved_list_price, contract_manu_number,
                                             contract_number, contract_mod_number, is_pricing_approved,
                                             approved_price_date, approved_percent, va_base_price, va_sell_price,
                                             mfc_precent, mfc_price, sin)

        return success, return_df_line_product


    def trigger_ingest_cleanup(self):
        self.obIngester.ingest_va_product_price_cleanup()

## end ##
