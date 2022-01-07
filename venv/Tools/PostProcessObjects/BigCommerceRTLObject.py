# CreatedBy: Emilia Crow
# CreateDate: 20210927
# Updated: 20210927
# CreateFor: Franklin Young International

import pandas

from Tools.BasicProcess import BasicProcessObject


class BigCommerceRTLObject(BasicProcessObject):
    req_fields = ['ProductPriceId','FyProductNumber']

    sup_fields = ['BCPriceUpdateToggle','BCDataUpdateToggle']
    att_fields = []
    gen_fields = []
    def __init__(self,df_product, user, password, is_testing,full_run=False):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'BC Ready To Load'
        self.full_run = full_run

    def batch_preprocessing(self):
        self.remove_private_headers()
        self.define_new()

    def define_new(self):
        self.df_current_BC_toggles = self.obDal.get_bc_rtl_state()
        match_headers = ['ProductPriceId','FyProductNumber','BCPriceUpdateToggle','BCDataUpdateToggle']
        self.df_current_BC_toggles['Filter'] = 'Fail'
        self.df_product = self.df_product.merge(self.df_current_BC_toggles,
                                                         how='left', on=match_headers)
        self.df_product.loc[(self.df_product['Filter'] != 'Fail'), 'Filter'] = 'Update'

    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId_y','ProductPriceId_x',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryId','CategoryId_x','CategoryId_y',
                           'Report','Filter'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
        if remove_headers != []:
            self.df_product = self.df_product.drop(columns=remove_headers)


    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            if 'Filter' in row:
                if row['Filter'] == 'Fail' and not self.full_run:
                    self.obReporter.update_report('Alert','No change')
                    return True, df_collect_product_base_data
            else:
                self.obReporter.update_report('Alert','This product was processed anyway')
                # return False, df_collect_product_base_data

        success, return_df_line_product = self.process_changes(df_collect_product_base_data)

        return success, return_df_line_product

    def process_changes(self, df_collect_product_base_data):
        for colName, row in df_collect_product_base_data.iterrows():
            price_toggle = 0
            data_toggle = 0

            product_price_id = row['ProductPriceId']
            fy_product_number = row['FyProductNumber']

            if 'BCPriceUpdateToggle' in row:
                price_toggle = row['BCPriceUpdateToggle']
            if 'BCDataUpdateToggle' in row:
                data_toggle = row['BCDataUpdateToggle']

        self.obIngester.set_bigcommerce_rtl(self.is_last, product_price_id, fy_product_number, price_toggle, data_toggle)
        return True, df_collect_product_base_data

    def trigger_ingest_cleanup(self):
        self.obIngester.set_bigcommerce_rtl_cleanup()



## end ##