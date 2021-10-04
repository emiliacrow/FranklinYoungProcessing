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
    def __init__(self,df_product, user, password, is_testing, proc_to_set):
        self.proc_to_run = proc_to_set
        super().__init__(df_product, user, password, is_testing)
        self.name = 'File Processor'
        self.product_collector = {}

    def batch_preprocessing(self):
        self.df_product_price_lookup = self.obDal.get_product_price_lookup()
        self.define_new()
        return self.df_product

    def define_new(self):
        if 'Filter' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns = 'Filter')
        if 'Filter X' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns = 'Filter X')
        if 'Filter Y' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns = 'Filter Y')

        # simple first
        self.df_product['Filter X'] = 'Update'
        self.df_product_price_lookup['Filter Y'] = 'Update'
        self.df_product_price_lookup = self.df_product_price_lookup.drop(columns = ['ProductId','ProductPriceId'])

        # match all products on FyProdNum
        self.df_product = self.df_product.merge(self.df_product_price_lookup, how='left',on='FyProductNumber')

        self.df_product.loc[(self.df_product['Filter X'] == 'Update') & (self.df_product['Filter Y'] == 'Update'), 'Filter'] = 'Update'
        self.df_product.loc[(self.df_product['Filter X'] != 'Update') & (self.df_product['Filter Y'] == 'Update'), 'Filter'] = 'Discon'
        self.df_product.loc[(self.df_product['Filter X'] == 'Update') & (self.df_product['Filter Y'] != 'Update'), 'Filter'] = 'New'

        # counts FyProductNumber occurance as series
        self.srs_matched_product = self.df_product.loc[(self.df_product['Filter'] == 'Update'),'FyProductNumber'].value_counts()

        self.srs_matched_product.rename_axis()
        # sets series to dataframe
        self.df_matched_product = self.srs_matched_product.to_frame().reset_index()
        # names columns in new dataframe
        self.df_matched_product.rename(columns = {'FyProductNumber':'number','index':'FyProductNumber'}, inplace = 1)

        self.df_matched_product['is_duplicated'] = 'Y'
        self.df_matched_product = self.df_matched_product.loc[(self.df_matched_product['number'] > 1),['FyProductNumber','is_duplicated']]

        self.df_product = self.df_product.merge(self.df_matched_product, how='left', on='FyProductNumber')


    def process_product_line(self, df_line_product):
        return True, df_line_product


## end ##