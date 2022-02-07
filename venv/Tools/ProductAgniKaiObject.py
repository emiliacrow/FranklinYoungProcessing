# CreatedBy: Emilia Crow
# CreateDate: 20211001
# Updated: 20211001
# CreateFor: Franklin Young International

from Tools.BasicProcess import BasicProcessObject

class ProductAgniKaiObject(BasicProcessObject):
    req_fields = ['FyCatalogNumber','FyProductNumber','ManufacturerPartNumber','VendorPartNumber']
    sup_fields = []
    att_fields = []
    gen_fields = []
    def __init__(self,df_product, user, password, is_testing, proc_to_set):
        self.proc_to_run = proc_to_set
        super().__init__(df_product, user, password, is_testing)
        self.name = 'File Processor'
        self.product_collector = {}

    def batch_preprocessing(self):
        self.remove_private_headers()
        self.define_new()
        return self.df_product

    def define_new(self):
        # step 1
        self.df_product_price_lookup = self.obDal.get_product_action_review_lookup()
        self.df_product_price_lookup['Filter'] = 'Ready'

        # match on everything
        self.df_product = self.df_product.merge(self.df_product_price_lookup, how='left',on=['FyCatalogNumber','FyProductNumber','ManufacturerPartNumber','VendorPartNumber'])

        # set aside the good matches
        self.df_full_matched_product = self.df_product[(self.df_product['Filter'] == 'Ready')]
        self.df_full_matched_product.loc[(self.df_full_matched_product['BaseProductPriceId'] == 'Load Pricing'), 'Filter'] = 'Update in Base Price'
        self.df_full_matched_product.loc[(self.df_full_matched_product['BaseProductPriceId'] == 'Load Pricing'), 'BaseProductPriceId'] = ''

        # prep next step data
        self.df_product = self.df_product[(self.df_product['Filter'] != 'Ready')]
        self.df_product = self.df_product.drop(columns = ['Filter','ProductId','ProductPriceId','BaseProductPriceId'])

        # these columns would just be junk anyway
        self.df_product_price_lookup = self.df_product_price_lookup.drop(columns = ['FyProductNumber','VendorPartNumber','BaseProductPriceId'])
        self.df_product_price_lookup['Filter'] = 'Partial'


        # step 2
        # match on everything
        self.df_product = self.df_product.merge(self.df_product_price_lookup, how='left',on=['FyCatalogNumber', 'ManufacturerPartNumber'])

        # set aside the good matches
        self.df_partial_matched_product = self.df_product[(self.df_product['Filter'] == 'Partial')]
        self.df_partial_matched_product.loc[(self.df_partial_matched_product['ProductPriceId'] == 'Load Product Price'), 'Filter'] = 'Update in Product Price'
        self.df_partial_matched_product.loc[(self.df_partial_matched_product['ProductPriceId'] == 'Load Product Price'), 'ProductPriceId'] = ''


        # prep next step data
        self.df_product = self.df_product[(self.df_product['Filter'] != 'Partial')]
        self.df_product['Filter'] = 'Partial'

        if len(self.df_full_matched_product.index) > 0:
            self.df_product = self.df_product.append(self.df_full_matched_product)

        if len(self.df_partial_matched_product.index) > 0:
            self.df_product = self.df_product.append(self.df_partial_matched_product)



        # counts FyProductNumber occurance as series
        self.srs_matched_product = self.df_product.loc[:,'FyProductNumber'].value_counts()

        self.srs_matched_product.rename_axis()
        # sets series to dataframe
        self.df_matched_product = self.srs_matched_product.to_frame().reset_index()
        # names columns in new dataframe
        self.df_matched_product.rename(columns = {'FyProductNumber':'number','index':'FyProductNumber'}, inplace = 1)

        # assign duplicate marker
        self.df_matched_product['is_duplicated'] = 'Y'
        self.df_matched_product = self.df_matched_product.loc[(self.df_matched_product['number'] > 1),['FyProductNumber','is_duplicated']]

        # merge the duplicate mark back in
        self.df_product = self.df_product.merge(self.df_matched_product, how='left', on='FyProductNumber')

        self.df_product.loc[(self.df_product['is_duplicated'] == 'Y'), 'Filter'] = 'Possible Duplicate'


        self.df_product = self.df_product.drop(columns = ['is_duplicated'])


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