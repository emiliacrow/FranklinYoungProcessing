# CreatedBy: Emilia Crow
# CreateDate: 20211109
# Updated: 20211109
# CreateFor: Franklin Young International


from Tools.BasicProcess import BasicProcessObject



class DiscontinueObject(BasicProcessObject):
    req_fields = ['ProductPriceId','FyProductNumber', 'FyCatalogNumber']

    sup_fields = ['ManufacturerPartNumber', 'VendorPartNumber']
    att_fields = []
    gen_fields = []
    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'The Reaper'

    def define_new(self):
        self.df_discon_products = self.obDal.get_discon_products()
        match_headers = ['ProductPriceId','FyProductNumber','BCPriceUpdateToggle','BCDataUpdateToggle']
        self.df_current_BC_toggles['Filter'] = 'Fail'
        self.df_product = pandas.DataFrame.merge(self.df_product, self.df_current_BC_toggles,
                                                         how='left', on=match_headers)
        self.df_product.loc[(self.df_product['Filter'] != 'Fail'), 'Filter'] = 'Update'


    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            if 'Filter' in row:
                if row['Filter'] == 'Fail':
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

        # self.obIngester.set_bigcommerce_rtl(self.is_last, product_price_id, fy_product_number, price_toggle, data_toggle)
        return True, df_collect_product_base_data




## end ##