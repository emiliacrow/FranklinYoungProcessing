# CreatedBy: Emilia Crow
# CreateDate: 20211001
# Updated: 20220318
# CreateFor: Franklin Young International

from Tools.BasicProcess import BasicProcessObject

class ProductAgniKaiObject(BasicProcessObject):
    req_fields = ['FyCatalogNumber','FyProductNumber','ManufacturerName','ManufacturerPartNumber','VendorName','VendorPartNumber']
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
        if 'AssetPath' in self.df_product.columns:
            self.define_new(b_match_vendor = True, is_asset = True)
        else:
            self.define_new(b_match_vendor = True)


    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryId','CategoryId_x','CategoryId_y',
                           'is_duplicated','is_duplicated_x','is_duplicated_y',
                           'db_IsProductNumberOverride','Report','Filter'}
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