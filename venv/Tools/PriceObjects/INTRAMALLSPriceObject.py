# CreatedBy: Emilia Crow
# CreateDate: 20230711
# Updated: 20230711
# CreateFor: Franklin Young International

import xlrd
import pandas
import datetime

from Tools.BasicProcess import BasicProcessObject


class INTRAMALLSPrice(BasicProcessObject):
    req_fields = ['FyProductNumber']
    sup_fields = []
    att_fields = []
    gen_fields = []

    def __init__(self, df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'INTRAMALLS Price Ingestion'

    def batch_preprocessing(self):
        self.remove_private_headers()
        self.batch_process_manufacturer()

        self.df_fy_description_lookup = self.obDal.get_fy_product_descriptions()
        self.df_product = self.df_product.merge(self.df_fy_description_lookup, how='left', on=['FyProductNumber'])

        self.df_intramalls_contract_ids = self.obDal.get_intramalls_contract_ids()
        self.df_product = self.df_product.merge(self.df_intramalls_contract_ids, how='left', on=['FyProductNumber'])

    def remove_private_headers(self):
        private_headers = {'ProductId', 'ProductId_y', 'ProductId_x',
                           'ProductPriceId', 'ProductPriceId_y', 'ProductPriceId_x',
                           'BaseProductPriceId', 'BaseProductPriceId_y', 'BaseProductPriceId_x',
                           'ProductDescriptionId', 'db_INTRAMALLSProductPriceId', 'INTRAMALLSProductPriceId', 'INTRAMALLSProductPriceId_x',
                           'INTRAMALLSProductPriceId_y',
                           'VendorId', 'VendorId_x', 'VendorId_y',
                           'CategoryId', 'CategoryId_x', 'CategoryId_y',
                           'Report', 'Filter', 'ProductDescriptionId', 'db_FyProductName', 'db_FyProductDescription'}
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

            for each_bool in ['INTRAMALLSOnContract', 'INTRAMALLSPricingApproved']:
                success, return_val = self.process_boolean(row, each_bool)
                if success:
                    df_collect_product_base_data[each_bool] = [return_val]
                else:
                    df_collect_product_base_data[each_bool] = [-1]

        success, return_df_line_product = self.intramalls_product_price(df_collect_product_base_data)

        return success, return_df_line_product


    def intramalls_product_price(self, df_line_product):
        success = True
        return_df_line_product = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            product_description_id = int(row['ProductDescriptionId'])

            fy_product_number = row['FyProductNumber']
            if 'INTRAMALLSOnContract' in row:
                on_contract = row['INTRAMALLSOnContract']
            else:
                on_contract = -1

            contract_number = 'IntraMalls7-2023'

            if 'INTRAMALLSContractModificationNumber' in row:
                contract_mod_number = row['INTRAMALLSContractModificationNumber']
            else:
                contract_mod_number = ''

            if 'INTRAMALLSPricingApproved' in row:
                is_pricing_approved = float(row['INTRAMALLSPricingApproved'])
            else:
                is_pricing_approved = -1

            intramalls_approved_price_date = -1
            if 'INTRAMALLSApprovedPriceDate' in row:
                intramalls_approved_price_date = row['INTRAMALLSApprovedPriceDate']
                try:
                    intramalls_approved_price_date = int(intramalls_approved_price_date)
                    intramalls_approved_price_date = xlrd.xldate_as_datetime(intramalls_approved_price_date, 0)
                except ValueError:
                    intramalls_approved_price_date = str(row['INTRAMALLSApprovedPriceDate'])

                if isinstance(intramalls_approved_price_date, datetime.datetime) == False:
                    try:
                        intramalls_approved_price_date = datetime.datetime.strptime(intramalls_approved_price_date,
                                                                             '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        intramalls_approved_price_date = str(row['INTRAMALLSApprovedPriceDate'])
                        self.obReporter.update_report('Alert', 'Check INTRAMALLSApprovedPriceDate')
                    except TypeError:
                        self.obReporter.update_report('Alert', 'Check INTRAMALLSApprovedPriceDate')

            if 'INTRAMALLSApprovedBasePrice' in row:
                approved_base_price = float(row['INTRAMALLSApprovedBasePrice'])
            else:
                approved_base_price = -1

            if 'INTRAMALLSApprovedSellPrice' in row:
                approved_sell_price = float(row['INTRAMALLSApprovedSellPrice'])
            else:
                approved_sell_price = -1

            if 'INTRAMALLSApprovedListPrice' in row:
                approved_list_price = float(row['INTRAMALLSApprovedListPrice'])
            else:
                approved_list_price = -1

            intramalls_product_notes = ''
            if 'INTRAMALLSProductNotes' in row:
                intramalls_product_notes = str(row['INTRAMALLSProductNotes'])

            intramalls_product_price_id = -1
            if 'INTRAMALLSProductPriceId' in row:
                intramalls_product_price_id = int(row['INTRAMALLSProductPriceId'])

            if 'db_INTRAMALLSProductPriceId' in row and intramalls_product_price_id == -1:
                intramalls_product_price_id = int(row['db_INTRAMALLSProductPriceId'])

        if intramalls_product_price_id == -1:
            self.obIngester.intramalls_product_price_insert(product_description_id, fy_product_number,
                                                     on_contract, approved_base_price,
                                                     approved_sell_price, approved_list_price,
                                                     contract_number, contract_mod_number, is_pricing_approved,
                                                     intramalls_approved_price_date, intramalls_product_notes)
        else:
            newINTRAMALLSProductPriceId, newProductDescriptionId, newFyProductNumber, newOnContract,
            newApprovedBasePrice,
            newApprovedSellPrice, newApprovedListPrice,
            newContractNumber, newContractModificatactionNumber, newINTRAMALLSPricingApproved,
            newINTRAMALLSApprovedPriceDate, newINTRAMALLSProductNotes

            self.obIngester.intramalls_product_price_update(intramalls_product_price_id, product_description_id, fy_product_number,
                                                            on_contract, approved_base_price,
                                                            approved_sell_price, approved_list_price,
                                                            contract_mod_number, is_pricing_approved,
                                                            intramalls_approved_price_date, intramalls_product_notes)

        return success, return_df_line_product

    def trigger_ingest_cleanup(self):
        self.obIngester.insert_intramalls_product_price_cleanup()
        self.obIngester.update_intramalls_product_price_cleanup()


class UpdateINTRAMALLSPrice(INTRAMALLSPrice):
    req_fields = ['FyProductNumber']
    sup_fields = []
    att_fields = []
    gen_fields = []

    def __init__(self, df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'INTRAMALLS Price Ingestion'




## end ##
