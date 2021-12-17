# CreatedBy: Emilia Crow
# CreateDate: 20211001
# Updated: 20211001
# CreateFor: Franklin Young International

from Tools.BasicProcess import BasicProcessObject

class ProductAgniKaiObject(BasicProcessObject):
    req_fields = ['FyProductNumber','ManufacturerPartNumber']
    sup_fields = []
    att_fields = []
    gen_fields = []
    def __init__(self,df_product, user, password, is_testing, proc_to_set, include_discontinues = False):
        self.proc_to_run = proc_to_set
        self.include_discontinues = include_discontinues
        super().__init__(df_product, user, password, is_testing)
        self.name = 'File Processor'
        self.product_collector = {}

    def batch_preprocessing(self):
        self.remove_private_headers()
        vendor_filter = self.vendor_name_selection()

        self.df_product_price_lookup = self.obDal.get_product_action_review_lookup(vendor_filter)
        self.define_new()
        return self.df_product

    def define_new(self):
        # simple first
        self.df_product['Filter X'] = 'Update'
        self.df_product_price_lookup['Filter Y'] = 'Update'

        # match all products on FyProdNum
        self.df_product = self.df_product.merge(self.df_product_price_lookup, how='outer',on=['FyProductNumber','ManufacturerPartNumber'])

        # counts FyProductNumber occurance as series
        self.srs_matched_product = self.df_product.loc[:,'FyProductNumber'].value_counts()

        self.srs_matched_product.rename_axis()
        # sets series to dataframe
        self.df_matched_product = self.srs_matched_product.to_frame().reset_index()
        # names columns in new dataframe
        self.df_matched_product.rename(columns = {'FyProductNumber':'number','index':'FyProductNumber'}, inplace = 1)

        self.df_matched_product['is_duplicated'] = 'Y'
        self.df_matched_product = self.df_matched_product.loc[(self.df_matched_product['number'] > 1),['FyProductNumber','is_duplicated']]

        self.df_product = self.df_product.merge(self.df_matched_product, how='left', on='FyProductNumber')

        self.df_product.loc[(self.df_product['Filter X'] == 'Update') & (self.df_product['Filter Y'] == 'Update'), 'Filter'] = 'Product Update'
        self.df_product.loc[(self.df_product['Filter X'] == 'Update') & (self.df_product['Filter Y'] == 'Update'), 'Alert'] = 'Product Update'
        self.df_product.loc[(self.df_product['Filter X'] != 'Update') & (self.df_product['Filter Y'] == 'Update'), 'Filter'] = 'Possible discontinue'
        self.df_product.loc[(self.df_product['Filter X'] != 'Update') & (self.df_product['Filter Y'] == 'Update'), 'Alert'] = 'Possible discontinue'
        self.df_product.loc[(self.df_product['Filter X'] == 'Update') & (self.df_product['Filter Y'] != 'Update'), 'Filter'] = 'New Product'
        self.df_product.loc[(self.df_product['Filter X'] == 'Update') & (self.df_product['Filter Y'] != 'Update'), 'Alert'] = 'New Product'

        self.df_product.loc[(self.df_product['is_duplicated'] == 'Y'), 'Filter'] = 'Possible Duplicate'

        self.df_product.loc[(self.df_product['ProductPriceId'] == 'Load Product Price'), 'Filter'] = 'Process in Product Price'
        self.df_product.loc[(self.df_product['ProductPriceId'] == 'Load Product Price'), 'Alert'] = 'Process in Product Price'
        self.df_product.loc[(self.df_product['ProductPriceId'] == 'Load Product Price'), 'ProductPriceId'] = ''

        self.df_product.loc[(self.df_product['BaseProductPriceId'] == 'Load Pricing'), 'Filter'] = 'Process in Base Price'
        self.df_product.loc[(self.df_product['BaseProductPriceId'] == 'Load Pricing'), 'Alert'] = 'Process in Base Price'
        self.df_product.loc[(self.df_product['BaseProductPriceId'] == 'Load Pricing'), 'BaseProductPriceId'] = ''

        if self.include_discontinues == False:
            self.df_product = self.df_product.loc[self.df_product['Filter'] != 'Possible discontinue']

        self.df_product = self.df_product.drop(columns = ['Filter X','Filter Y','is_duplicated'])


    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryId','CategoryId_x','CategoryId_y',
                           'is_duplicated','is_duplicated_x','Iis_duplicated_y',
                           'Report','Filter'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
        if remove_headers != []:
            self.df_product = self.df_product.drop(columns=remove_headers)


    def run_process(self):
        self.set_progress_bar(10, 'Batch preprocessing')
        self.obProgressBarWindow.update_unknown()
        self.batch_preprocessing()
        self.obProgressBarWindow.close()

        count_of_items = len(self.df_product.index)
        self.return_df_product = self.df_product.copy()

        self.df_product = self.return_df_product
        self.message = '{1}: {0} evaluated.'.format(count_of_items,self.name)

        return True, self.message


## end ##