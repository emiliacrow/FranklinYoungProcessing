# CreatedBy: Emilia Crow
# CreateDate: 20210609
# Updated: 20210609
# CreateFor: Franklin Young International

import pandas
import xlrd

from Tools.BasicProcess import BasicProcessObject

class ECATPrice(BasicProcessObject):
    req_fields = ['FyProductNumber','VendorPartNumber','ECATOnContract', 'ECATApprovedListPrice',
                  'ECATMaxMarkup', 'ECATContractModificationNumber', 'ECATApprovedPriceDate','ECATPricingApproved']
    sup_fields = []
    att_fields = []
    gen_fields = ['ContractedManufacturerPartNumber']

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'ECAT Price Ingestion'


    def batch_preprocessing(self):
        self.remove_private_headers()
        # define new, update, non-update
        if 'VendorId' not in self.df_product.columns:
            self.batch_process_vendor()
        self.define_new()


    def batch_process_vendor(self):
        # there should only be one vendor, really.
        if 'VendorName' not in self.df_product.columns:
            vendor_name = self.vendor_name_selection()
            self.df_product['VendorName'] = vendor_name

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


    def define_new(self):
        self.df_base_price_lookup = self.obDal.get_base_product_price_lookup()
        self.df_ecat_price_lookup = self.obDal.get_ecat_price_lookup()

        match_headers = ['FyProductNumber','VendorPartNumber','ECATOnContract', 'ECATApprovedListPrice',
                        'ECATContractModificationNumber','ECATApprovedPriceDate','ECATPricingApproved']

        # simple first
        self.df_base_price_lookup['Filter'] = 'Pass'

        # match all products on FyProdNum
        self.df_update_products = self.df_product.merge(self.df_base_price_lookup,
                                                         how='left', on=['FyProductNumber','VendorPartNumber'])
        # all products that matched on FyProdNum
        self.df_update_products.loc[(self.df_update_products['Filter'] != 'Pass'), 'Filter'] = 'Fail'

        self.df_product = self.df_update_products.loc[(self.df_update_products['Filter'] != 'Pass')]
        self.df_update_products = self.df_update_products.loc[(self.df_update_products['Filter'] == 'Pass')]

        if len(self.df_update_products.index) != 0:
            # this step is going to require a test against the pricing in the contract price table
            # this could end up empty
            self.df_ecat_price_lookup['Filter'] = 'Update'
            self.df_update_products = self.df_update_products.drop(columns='Filter')
            self.df_update_products = self.df_update_products.merge(self.df_ecat_price_lookup,
                                                             how='left', on=match_headers)

            # this does not seem to be matching correctly in the above
            # I suspect this has to do with the numbers being strings?
            self.df_update_products.loc[(self.df_update_products['Filter'] != 'Update'), 'Filter'] = 'Pass'

            self.df_product = self.df_product.append(self.df_update_products)

            # this shouldn't always be 0


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
        if 'Filter' in row:
            if row['Filter'] == 'Pass':
                self.obReporter.update_report('Pass','This contract price is a new')
                return True
            if row['Filter'] == 'Update':
                self.obReporter.update_report('Pass','This contract price is an update')
                return True
            else:
                self.obReporter.update_report('Alert','This product must be ingested in product')
                return False
        else:
            self.obReporter.update_report('Fail','This product price failed filtering')
            return False


    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()

        for colName, row in df_line_product.iterrows():
            if self.filter_check_in(row) == False:
                return False, df_collect_product_base_data

            df_collect_product_base_data = self.process_oncontract(df_collect_product_base_data, row)
            success, df_collect_product_base_data = self.process_pricing(df_collect_product_base_data)
            if success == False:
                self.obReporter.update_report('Fail','Failed in process contract')
                return success, df_collect_product_base_data

        success, return_df_line_product = self.ecat_product_price(df_collect_product_base_data)

        return success, return_df_line_product


    def process_oncontract(self, df_collect_product_base_data, row):
        if ('ECATOnContract' not in row):
            df_collect_product_base_data['ECATOnContract'] = [1]
            self.obReporter.update_report('Alert', 'OnContract was assigned')
        elif str(row['ECATOnContract']) == 'N':
            df_collect_product_base_data['ECATOnContract'] = [0]
        elif str(row['ECATOnContract']) == 'Y':
            df_collect_product_base_data['ECATOnContract'] = [1]

        return df_collect_product_base_data


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

            elif 'db_fy_cost' in row:
                fy_cost = float(row['db_fy_cost'])
                max_markup = float(row['ECATMaxMarkup'])
                ecat_sell_price = round((fy_cost*max_markup),2)
                return_df_line_product['ECATSellPrice'] = ecat_sell_price
            else:
                self.obReporter.update_report('Fail', 'Check for db_fy_cost  or ECATSellPrice')
                return False, return_df_line_product

        return success, return_df_line_product


    def ecat_product_price(self, df_line_product):
        success = True
        return_df_line_product = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
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

            if 'ECATApprovedBasePrice' in row:
                approved_base_price = row['ECATApprovedBasePrice']
            else:
                approved_base_price = ''

            approved_sell_price = row['ECATApprovedSellPrice']
            approved_list_price = row['ECATApprovedListPrice']

            ecat_sell_price = row['ECATSellPrice']
            max_markup = row['ECATMaxMarkup']

        self.obIngester.ecat_product_price_cap(base_product_price_id, fy_product_number, on_contract,
                                               approved_sell_price, approved_list_price, contract_manu_number,
                                               contract_number, contract_mod_number, is_pricing_approved,
                                               approved_price_date, ecat_sell_price,max_markup)

        return success, return_df_line_product


    def trigger_ingest_cleanup(self):
        self.obIngester.ingest_ecat_product_price_cleanup()


## end ##
