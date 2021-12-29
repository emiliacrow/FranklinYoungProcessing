# CreatedBy: Emilia Crow
# CreateDate: 20211109
# Updated: 20211109
# CreateFor: Franklin Young International


from Tools.BasicProcess import BasicProcessObject



class DiscontinueObject(BasicProcessObject):
    req_fields = ['FyProductNumber', 'VendorPartNumber', 'VendorName','IsDiscontinued']

    sup_fields = []
    att_fields = []
    gen_fields = []
    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'The Reaper'

    def batch_preprocessing(self):
        self.remove_private_headers()
        self.define_new()

    def define_new(self):
        self.df_discon_products = self.obDal.get_discon_products()
        match_headers = ['FyProductNumber','VendorPartNumber','VendorName']
        self.df_discon_products['Filter'] = 'Update'
        # products in db marked update
        self.df_product = self.df_product.merge(self.df_discon_products,
                                                         how='left', on=match_headers)
        # products not in db
        self.df_product.loc[(self.df_product['Filter'] != 'Update'), 'Filter'] = 'Fail'
        # products that need to be updated
        self.df_product.loc[(self.df_product['IsDiscontinued_x'] == self.df_product['IsDiscontinued_y']) & (self.df_product['Filter'] == 'Update'), 'Filter'] = 'Good'
        # products that need to be updated, reset the discon column to the new value
        self.df_product['IsDiscontinued'] = self.df_product['IsDiscontinued_x'].copy()


    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryId','CategoryId_x','CategoryId_y',
                           'IsDiscontinued_x','IsDiscontinued_y',
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
                if row['Filter'] == 'Fail':
                    self.obReporter.update_report('Fail','Failed to match a DB product')
                    return False, df_collect_product_base_data
                if row['Filter'] == 'Good':
                    self.obReporter.update_report('Alert','No change')
                    return True, df_collect_product_base_data
            else:
                self.obReporter.update_report('Alert','This product was processed anyway')
                # return False, df_collect_product_base_data

        success, return_df_line_product = self.process_changes(df_collect_product_base_data)

        return success, return_df_line_product

    def process_changes(self, df_collect_product_base_data):
        for colName, row in df_collect_product_base_data.iterrows():
            price_id = row['ProductPriceId']
            is_discontinued = row['IsDiscontinued']

            self.obIngester.set_discon_product_price(self.is_last, price_id, is_discontinued)

        return True, df_collect_product_base_data

    def trigger_ingest_cleanup(self):
        self.obIngester.set_discon_product_price_cleanup()


## end ##